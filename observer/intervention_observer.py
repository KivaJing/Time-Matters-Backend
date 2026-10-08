#!/usr/bin/env python3
import json
import builtins
import time
import os
from datetime import datetime
from collections import Counter
from confluent_kafka import Consumer

BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("TOPIC", "intervention_events")

WARMUP_SEC = int(os.getenv("WARMUP_SEC", "5"))
MEASURE_SEC = int(os.getenv("MEASURE_SEC", "60"))
TARGET_RUN_ID = os.getenv("RUN_ID")

TOP_N_USERS = int(os.getenv("TOP_N_USERS", "10"))



OUTPUT_FILE = os.getenv("OUTPUT_FILE", "intervention_output.txt")


def log(*args, **kwargs):
    builtins.print(*args, **kwargs)
    file_kwargs = {}
    for key in ("sep", "end", "flush"):
        if key in kwargs:
            file_kwargs[key] = kwargs[key]
    with open(OUTPUT_FILE, "a", encoding="utf-8") as f:
        builtins.print(*args, file=f, **file_kwargs)


def log_session_start(name):
    with open(OUTPUT_FILE, "a", encoding="utf-8") as f:
        builtins.print(f"\n===== {name} started at {datetime.now().isoformat(timespec='seconds')} =====", file=f)


def percentile(data, p):
    if not data:
        return None
    data = sorted(data)
    k = int((len(data) - 1) * p / 100)
    return data[k]


def report(name, data):
    if not data:
        log(f"{name}: n=0")
        return
    log(
        f"{name}: n={len(data)} "
        f"mean={sum(data)/len(data):.1f}ms "
        f"p50={percentile(data, 50)}ms "
        f"p95={percentile(data, 95)}ms "
        f"p99={percentile(data, 99)}ms "
        f"max={max(data)}ms"
    )


def main():
    c = Consumer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": f"intervention-observer-{int(time.time() * 1000)}",
        "auto.offset.reset": "latest",
        "enable.auto.commit": True,
    })
    c.subscribe([TOPIC])
    log_session_start(__file__)

    log(f"Intervention observer started. topic={TOPIC}")
    log(f"Warmup={WARMUP_SEC}s, Measure={MEASURE_SEC}s, RUN_ID={TARGET_RUN_ID or 'ALL'}")

    start = time.time()
    measure_start = start + WARMUP_SEC
    measure_end = measure_start + MEASURE_SEC

    intervention_latency_ms = []
    end_to_end_ms = []

    polled = 0
    accepted = 0

    action_counts = Counter()
    suppression_reason_counts = Counter()
    decision_counts = Counter()
    user_counts = Counter()
    unique_users = set()

    while time.time() < measure_end:
        msg = c.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            log("consumer error:", msg.error())
            continue

        polled += 1
        now_ms = int(time.time() * 1000)

        try:
            event = json.loads(msg.value().decode("utf-8"))
        except Exception:
            continue

        if TARGET_RUN_ID and event.get("run_id") != TARGET_RUN_ID:
            continue

        reasoning_ts = event.get("reasoning_ts_ms")
        intervention_ts = event.get("intervention_ts_ms")
        source_last_sent = event.get("source_last_sent_at_ms")
        user_id = event.get("user_id")

        if reasoning_ts is None or intervention_ts is None:
            continue

        reasoning_ts = int(reasoning_ts)
        intervention_ts = int(intervention_ts)

        if reasoning_ts < int(measure_start * 1000):
            continue

        # latency
        intervention_latency_ms.append(intervention_ts - reasoning_ts)

        if source_last_sent is not None:
            end_to_end_ms.append(now_ms - int(source_last_sent))

        # distributions
        action_counts[event.get("action", "UNKNOWN")] += 1
        suppression_reason_counts[event.get("suppression_reason", "NONE")] += 1
        decision_counts[event.get("decision", "UNKNOWN")] += 1
        user_counts[user_id or "UNKNOWN"] += 1

        if user_id:
            unique_users.add(user_id)

        accepted += 1

    c.close()

    elapsed = max(MEASURE_SEC, 1)
    throughput = accepted / elapsed

    log()
    log(f"[{datetime.now().isoformat(timespec='seconds')}] Done. polled={polled}, accepted={accepted}")
    log(f"throughput={throughput:.2f} intervention_events/sec")
    log(f"unique_users={len(unique_users)}")

    report("intervention_latency(intervention-reasoning)", intervention_latency_ms)
    report("end_to_end_final(now-source_last_sent)", end_to_end_ms)

    log("action_counts:", dict(action_counts.most_common()))
    log("suppression_reason_counts:", dict(suppression_reason_counts.most_common()))
    log("decision_counts:", dict(decision_counts.most_common()))
    log(f"top_{TOP_N_USERS}_users:", dict(user_counts.most_common(TOP_N_USERS)))

    if accepted == 0:
        log("No intervention events observed.")


if __name__ == "__main__":
    main()