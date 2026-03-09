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
TOPIC = os.getenv("TOPIC", "raw_events")
RUN_ID = os.getenv("RUN_ID", str(uuid.uuid4()))

NUM_USERS = int(os.getenv("NUM_USERS", "3"))
DURATION_SEC = int(os.getenv("DURATION_SEC", "60"))
MODE = os.getenv("MODE", "mixed")
# baseline_only / trigger_only / mixed / periodic_trigger / random_spike

BASELINE_INTERVAL_SEC = float(os.getenv("BASELINE_INTERVAL_SEC", "1.0"))
TRIGGER_INTERVAL_SEC = float(os.getenv("TRIGGER_INTERVAL_SEC", "0.8"))
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

def send_event(user_id: str, reaction_time: int, scenario: str, event_type: str = "hand_swipe"):
    global sent_count

    event_time_ms = int(time.time() * 1000)

    event = {
        "event_id": str(uuid.uuid4()),
        "run_id": RUN_ID,
        "timestamp": event_time_ms,     # logical event time
        "user_id": user_id,
        "event_type": event_type,
        "reaction_time_ms": reaction_time,
        "scenario": scenario,
        "sent_at_ms": int(time.time() * 1000)  # actual send time
    }

    producer.produce(
        TOPIC,
        key=user_id.encode("utf-8"),
        value=json.dumps(event).encode("utf-8"),
        callback=delivery_report
    )
    producer.poll(0)

    with sent_lock:
        sent_count += 1

def baseline_rt(rng: random.Random) -> int:
    return 300 + rng.randint(-20, 20)

def trigger_sequence():
    return [300, 350, 420, 500, 650, 800]

def user_worker(user_id: str, stop_time: float, mode: str):
    rng = random.Random(stable_seed(user_id, BASE_SEED))
    seq = trigger_sequence()
    trigger_idx = 0

    while time.time() < stop_time:
        if mode == "baseline_only":
            rt = baseline_rt(rng)
            send_event(user_id, rt, "baseline")
            time.sleep(BASELINE_INTERVAL_SEC)

        elif mode == "trigger_only":
            rt = seq[trigger_idx % len(seq)]
            trigger_idx += 1
            send_event(user_id, rt, "trigger")
            time.sleep(TRIGGER_INTERVAL_SEC)

        elif mode == "mixed":
            # Mostly baseline, occasionally a trigger burst
            if rng.random() < 0.75:
                rt = baseline_rt(rng)
                send_event(user_id, rt, "baseline")
                time.sleep(BASELINE_INTERVAL_SEC)
            else:
                for _ in range(len(seq)):
                    if time.time() >= stop_time:
                        break
                    rt = seq[trigger_idx % len(seq)]
                    trigger_idx += 1
                    send_event(user_id, rt, "trigger")
                    time.sleep(TRIGGER_INTERVAL_SEC)

        elif mode == "periodic_trigger":
            # Repeated cycle: 10s baseline + 1 trigger burst
            phase_end = time.time() + 10
            while time.time() < phase_end and time.time() < stop_time:
                rt = baseline_rt(rng)
                send_event(user_id, rt, "baseline")
                time.sleep(BASELINE_INTERVAL_SEC)

            for _ in range(len(seq)):
                if time.time() >= stop_time:
                    break
                rt = seq[trigger_idx % len(seq)]
                trigger_idx += 1
                send_event(user_id, rt, "trigger")
                time.sleep(TRIGGER_INTERVAL_SEC)

        elif mode == "random_spike":
            # Mostly baseline, occasional abrupt high-latency spikes
            if rng.random() < 0.15:
                rt = rng.choice([500, 650, 800])
                send_event(user_id, rt, "spike")
                time.sleep(TRIGGER_INTERVAL_SEC)
            else:
                rt = baseline_rt(rng)
                send_event(user_id, rt, "baseline")
                time.sleep(BASELINE_INTERVAL_SEC)

        else:
            raise ValueError(f"Unsupported MODE: {mode}")

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
    print(f"BASELINE_INTERVAL_SEC={BASELINE_INTERVAL_SEC}")
    print(f"TRIGGER_INTERVAL_SEC={TRIGGER_INTERVAL_SEC}")

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