#!/usr/bin/env python3
import json
import time
import uuid
import random
import os
import threading
import hashlib
from confluent_kafka import Producer

BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("TOPIC", "raw_state_events")
RUN_ID = os.getenv("RUN_ID", str(uuid.uuid4()))

NUM_USERS = int(os.getenv("NUM_USERS", "3"))
DURATION_SEC = int(os.getenv("DURATION_SEC", "120"))
MODE = os.getenv("MODE", "relapse_risk")
# baseline_only / stress_rising / craving_rising / low_motivation / relapse_risk / mixed_risk

# Real time control for demos/experiments.
# In the theoretical model, data updates every 5 minutes.
# For demos you can compress time, e.g. SIM_MINUTE_SEC=5 => 5 logical minutes = 25 seconds.
SIM_MINUTE_SEC = float(os.getenv("SIM_MINUTE_SEC", "5"))
EMA_UPDATE_MINUTES = float(os.getenv("EMA_UPDATE_MINUTES", "5"))
EMIT_INTERVAL_SEC = float(os.getenv("EMIT_INTERVAL_SEC", str(SIM_MINUTE_SEC * EMA_UPDATE_MINUTES)))
BASE_SEED = int(os.getenv("BASE_SEED", "42"))
VERBOSE = os.getenv("VERBOSE", "0") == "1"

producer = Producer({
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "linger.ms": 20,
    "batch.num.messages": 10000,
    "acks": "1",
})

sent_count = 0
sent_lock = threading.Lock()


def stable_seed(user_id: str, base_seed: int) -> int:
    h = hashlib.md5(user_id.encode("utf-8")).hexdigest()
    return base_seed + int(h[:8], 16)


def delivery_report(err, msg):
    if VERBOSE and err is not None:
        print("delivery failed:", err)


def clamp(v: float, lo: int = 0, hi: int = 10) -> int:
    return max(lo, min(hi, int(round(v))))


def make_event(user_id: str, state: dict, scenario: str, logical_step: int) -> dict:
    now_ms = int(time.time() * 1000)
    return {
        "event_id": str(uuid.uuid4()),
        "run_id": RUN_ID,
        "timestamp": now_ms,
        "user_id": user_id,
        "event_type": "ema_state",
        "logical_step": logical_step,
        "logical_minutes_per_step": EMA_UPDATE_MINUTES,
        "craving": clamp(state["craving"]),
        "stress": clamp(state["stress"]),
        "motivation": clamp(state["motivation"]),
        "self_efficacy": clamp(state["self_efficacy"]),
        "strategy_use": int(state["strategy_use"]),  # 1=smoke, 2=do nothing, 3=regulate
        "receptivity": int(state["receptivity"]),    # 0/1
        "is_moving": int(state.get("is_moving", 0)),
        "in_meeting": int(state.get("in_meeting", 0)),
        "location_type": state.get("location_type", "other"),
        "scenario": scenario,
        "sent_at_ms": now_ms,
    }


def send_event(user_id: str, state: dict, scenario: str, logical_step: int):
    global sent_count
    event = make_event(user_id, state, scenario, logical_step)
    producer.produce(
        TOPIC,
        key=user_id.encode("utf-8"),
        value=json.dumps(event).encode("utf-8"),
        callback=delivery_report,
    )
    producer.poll(0)
    with sent_lock:
        sent_count += 1


# ----- scenario generators -----
def baseline_state(rng: random.Random) -> dict:
    return {
        "craving": 2 + rng.uniform(-0.5, 0.5),
        "stress": 2 + rng.uniform(-0.5, 0.5),
        "motivation": 8 + rng.uniform(-0.5, 0.5),
        "self_efficacy": 8 + rng.uniform(-0.5, 0.5),
        "strategy_use": 3,
        "receptivity": 1,
        "is_moving": 0,
        "in_meeting": 0,
        "location_type": rng.choice(["home", "work", "other"]),
    }


def stress_rising_state(step: int, rng: random.Random) -> dict:
    return {
        "craving": min(8.5, 2 + step * 0.7 + rng.uniform(-0.4, 0.4)),
        "stress": min(9.5, 3 + step * 1.2 + rng.uniform(-0.4, 0.4)),
        "motivation": max(2.0, 8 - step * 0.7 + rng.uniform(-0.4, 0.4)),
        "self_efficacy": max(2.0, 8 - step * 0.8 + rng.uniform(-0.4, 0.4)),
        "strategy_use": 2,
        "receptivity": 1,
        "is_moving": 0,
        "in_meeting": 0,
        "location_type": "work",
    }


def craving_rising_state(step: int, rng: random.Random) -> dict:
    return {
        "craving": min(9.5, 3 + step * 1.2 + rng.uniform(-0.4, 0.4)),
        "stress": min(7.5, 2 + step * 0.6 + rng.uniform(-0.4, 0.4)),
        "motivation": max(3.0, 7 - step * 0.6 + rng.uniform(-0.4, 0.4)),
        "self_efficacy": max(3.0, 7 - step * 0.6 + rng.uniform(-0.4, 0.4)),
        "strategy_use": rng.choice([2, 3]),
        "receptivity": 1,
        "is_moving": 0,
        "in_meeting": 0,
        "location_type": rng.choice(["home", "other"]),
    }


def low_motivation_state(step: int, rng: random.Random) -> dict:
    return {
        "craving": min(7.5, 4 + step * 0.5 + rng.uniform(-0.4, 0.4)),
        "stress": min(6.5, 4 + step * 0.3 + rng.uniform(-0.4, 0.4)),
        "motivation": max(1.0, 4 - step * 0.9 + rng.uniform(-0.3, 0.3)),
        "self_efficacy": max(2.0, 5 - step * 0.7 + rng.uniform(-0.3, 0.3)),
        "strategy_use": 2,
        "receptivity": 1,
        "is_moving": 0,
        "in_meeting": 0,
        "location_type": "home",
    }


def relapse_risk_state(step: int, rng: random.Random) -> dict:
    # stronger combined deterioration + occasional low receptivity context
    receptivity = 0 if step >= 4 and rng.random() < 0.35 else 1
    in_meeting = 1 if step >= 4 and rng.random() < 0.20 else 0
    is_moving = 1 if step >= 4 and rng.random() < 0.25 else 0
    return {
        "craving": min(10.0, 5 + step * 1.0 + rng.uniform(-0.4, 0.4)),
        "stress": min(10.0, 5 + step * 0.9 + rng.uniform(-0.4, 0.4)),
        "motivation": max(1.0, 5 - step * 0.9 + rng.uniform(-0.3, 0.3)),
        "self_efficacy": max(1.0, 5 - step * 1.0 + rng.uniform(-0.3, 0.3)),
        "strategy_use": rng.choice([1, 2, 2, 3]),
        "receptivity": receptivity,
        "is_moving": is_moving,
        "in_meeting": in_meeting,
        "location_type": rng.choice(["work", "other", "outdoor"]),
    }


def mixed_risk_state(cycle_step: int, rng: random.Random) -> tuple[str, dict]:
    phase = cycle_step % 12
    if phase < 4:
        return "baseline", baseline_state(rng)
    if phase < 7:
        return "stress_rising", stress_rising_state(phase - 3, rng)
    if phase < 10:
        return "craving_rising", craving_rising_state(phase - 6, rng)
    return "relapse_risk", relapse_risk_state(phase - 9, rng)


def user_worker(user_id: str, stop_time: float, mode: str):
    rng = random.Random(stable_seed(user_id, BASE_SEED))
    logical_step = 0

    while time.time() < stop_time:
        if mode == "baseline_only":
            scenario = "baseline"
            state = baseline_state(rng)
        elif mode == "stress_rising":
            scenario = "stress_rising"
            state = stress_rising_state(logical_step, rng)
        elif mode == "craving_rising":
            scenario = "craving_rising"
            state = craving_rising_state(logical_step, rng)
        elif mode == "low_motivation":
            scenario = "low_motivation"
            state = low_motivation_state(logical_step, rng)
        elif mode == "relapse_risk":
            scenario = "relapse_risk"
            state = relapse_risk_state(logical_step, rng)
        elif mode == "mixed_risk":
            scenario, state = mixed_risk_state(logical_step, rng)
        else:
            raise ValueError(f"Unsupported MODE: {mode}")

        send_event(user_id, state, scenario, logical_step)
        logical_step += 1
        time.sleep(EMIT_INTERVAL_SEC)


def stats_printer(stop_time: float):
    last_count = 0
    while time.time() < stop_time:
        time.sleep(5)
        with sent_lock:
            current = sent_count
        delta = current - last_count
        last_count = current
        print(f"[stats] run_id={RUN_ID} total_sent={current} last_5s={delta}")


def main():
    users = [f"user_{i+1}" for i in range(NUM_USERS)]
    stop_time = time.time() + DURATION_SEC

    print(f"RUN_ID={RUN_ID}")
    print(f"TOPIC={TOPIC}")
    print(f"MODE={MODE}")
    print(f"NUM_USERS={NUM_USERS}")
    print(f"DURATION_SEC={DURATION_SEC}")
    print(f"EMA_UPDATE_MINUTES={EMA_UPDATE_MINUTES}")
    print(f"SIM_MINUTE_SEC={SIM_MINUTE_SEC}")
    print(f"EMIT_INTERVAL_SEC={EMIT_INTERVAL_SEC}")

    threads = []
    stat_thread = threading.Thread(target=stats_printer, args=(stop_time,), daemon=True)
    stat_thread.start()

    for user_id in users:
        t = threading.Thread(target=user_worker, args=(user_id, stop_time, MODE), daemon=True)
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    producer.flush(10)
    print(f"Done. RUN_ID={RUN_ID}, sent_count={sent_count}")


if __name__ == "__main__":
    main()
