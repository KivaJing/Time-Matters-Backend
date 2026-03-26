#!/usr/bin/env python3
import json
import time
import os
from datetime import datetime
from collections import Counter
from confluent_kafka import Consumer

BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("TOPIC", "semantic_events")
WARMUP_SEC = int(os.getenv("WARMUP_SEC", "5"))
MEASURE_SEC = int(os.getenv("MEASURE_SEC", "60"))
TARGET_RUN_ID = os.getenv("RUN_ID")
TOP_N_USERS = int(os.getenv("TOP_N_USERS", "10"))


def percentile(data, p):
    if not data:
        return None
    data = sorted(data)
    k = int((len(data) - 1) * p / 100)
    return data[k]


def report(name, data):
    if not data:
        print(f"{name}: n=0")
        return
    print(
        f"{name}: n={len(data)} "
        f"mean={sum(data)/len(data):.1f}ms "
        f"p50={percentile(data, 50)}ms "
        f"p95={percentile(data, 95)}ms "
        f"p99={percentile(data, 99)}ms "
        f"max={max(data)}ms"
    )


def safe_int(v):
    if v is None:
        return None
    try:
        return int(v)
    except Exception:
        return None


def main():
    c = Consumer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": f"rule-observer-{int(time.time() * 1000)}",
        "auto.offset.reset": "latest",
        "enable.auto.commit": True,
    })
    c.subscribe([TOPIC])

    print(f"Rule observer started. topic={TOPIC}")
    print(f"Warmup={WARMUP_SEC}s, Measure={MEASURE_SEC}s, RUN_ID={TARGET_RUN_ID or 'ALL'}")

    start = time.time()
    measure_start = start + WARMUP_SEC
    measure_end = measure_start + MEASURE_SEC
    measure_start_ms = int(measure_start * 1000)
    measure_end_ms = int(measure_end * 1000)

    total_from_ksql_ms = []
    rule_processing_ms = []
    rule_transport_ms = []
    total_from_raw_sent_ms = []

    polled = 0
    accepted = 0

    rule_counts = Counter()
    semantic_counts = Counter()
    severity_counts = Counter()
    user_counts = Counter()
    unique_users = set()

    while time.time() < measure_end:
        msg = c.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print("consumer error:", msg.error())
            continue

        polled += 1
        now_ms = int(time.time() * 1000)

        try:
            event = json.loads(msg.value().decode("utf-8"))
        except Exception:
            continue

        if TARGET_RUN_ID and event.get("run_id") != TARGET_RUN_ID:
            continue

        source_ksql_emit_ts_ms = safe_int(event.get("source_ksql_emit_ts_ms"))
        engine_ts_ms = safe_int(event.get("engine_ts_ms"))
        source_last_sent_at_ms = safe_int(event.get("source_last_sent_at_ms"))
        user_id = event.get("user_id")

        if source_ksql_emit_ts_ms is None or engine_ts_ms is None:
            continue

        # Accept only events whose source ksql emit time falls inside the measurement window
        in_window = measure_start_ms <= source_ksql_emit_ts_ms <= measure_end_ms
        if not in_window:
            continue

        total_from_ksql_ms.append(now_ms - source_ksql_emit_ts_ms)
        rule_processing_ms.append(engine_ts_ms - source_ksql_emit_ts_ms)
        rule_transport_ms.append(now_ms - engine_ts_ms)

        if source_last_sent_at_ms is not None:
            total_from_raw_sent_ms.append(now_ms - source_last_sent_at_ms)

        rule_counts[event.get("rule_id", "UNKNOWN")] += 1
        semantic_counts[event.get("semantic_type", "UNKNOWN")] += 1
        severity_counts[event.get("severity", "UNKNOWN")] += 1
        user_counts[user_id or "UNKNOWN"] += 1
        if user_id:
            unique_users.add(user_id)

        accepted += 1

    c.close()

    elapsed = max(MEASURE_SEC, 1)
    throughput = accepted / elapsed

    print()
    print(f"[{datetime.now().isoformat(timespec='seconds')}] Done. polled={polled}, accepted={accepted}")
    print(f"throughput={throughput:.2f} semantic_events/sec")
    print(f"unique_users={len(unique_users)}")

    report("rule_total(now-source_ksql_emit)", total_from_ksql_ms)
    report("rule_processing(engine-source_ksql_emit)", rule_processing_ms)
    report("rule_transport(now-engine)", rule_transport_ms)
    report("rule_total_from_raw(now-source_last_sent_at)", total_from_raw_sent_ms)

    print("rule_counts:", dict(rule_counts.most_common()))
    print("semantic_counts:", dict(semantic_counts.most_common()))
    print("severity_counts:", dict(severity_counts.most_common()))
    print(f"top_{TOP_N_USERS}_users:", dict(user_counts.most_common(TOP_N_USERS)))

    if accepted == 0:
        print("No semantic events observed in measurement window.")


if __name__ == "__main__":
    main()