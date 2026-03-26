#!/usr/bin/env python3
# reasoning_requests_observer.py

import json
import time
import os
from datetime import datetime
from collections import Counter
from confluent_kafka import Consumer

BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("TOPIC", "reasoning_requests")
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
        "group.id": f"reasoning-requests-observer-{int(time.time() * 1000)}",
        "auto.offset.reset": "latest",
        "enable.auto.commit": True,
    })
    c.subscribe([TOPIC])

    print(f"Reasoning requests observer started. topic={TOPIC}")
    print(f"Warmup={WARMUP_SEC}s, Measure={MEASURE_SEC}s, RUN_ID={TARGET_RUN_ID or 'ALL'}")

    start = time.time()
    measure_start = start + WARMUP_SEC
    measure_end = measure_start + MEASURE_SEC
    measure_start_ms = int(measure_start * 1000)
    measure_end_ms = int(measure_end * 1000)

    polled = 0
    accepted = 0

    unique_users = set()
    user_counts = Counter()
    semantic_counts = Counter()
    severity_counts = Counter()
    scheduler_version_counts = Counter()
    reasoning_route_counts = Counter()

    scheduler_delay_ms = []
    total_from_semantic_ms = []
    scheduler_transport_ms = []
    pipeline_cep_part_ms = []
    pipeline_rule_part_ms = []

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

        semantic_engine_ts_ms = safe_int(event.get("engine_ts_ms"))
        scheduler_ts_ms = safe_int(event.get("scheduler_ts_ms"))
        source_ksql_emit_ts_ms = safe_int(event.get("source_ksql_emit_ts_ms"))
        source_last_sent_at_ms = safe_int(event.get("source_last_sent_at_ms"))
        user_id = event.get("user_id")

        if semantic_engine_ts_ms is None or scheduler_ts_ms is None:
            continue

        in_window = measure_start_ms <= semantic_engine_ts_ms <= measure_end_ms
        if not in_window:
            continue

        accepted += 1

        scheduler_delay_ms.append(scheduler_ts_ms - semantic_engine_ts_ms)
        total_from_semantic_ms.append(now_ms - semantic_engine_ts_ms)
        scheduler_transport_ms.append(now_ms - scheduler_ts_ms)

        if source_last_sent_at_ms is not None and source_ksql_emit_ts_ms is not None:
            pipeline_cep_part_ms.append(source_ksql_emit_ts_ms - source_last_sent_at_ms)
            pipeline_rule_part_ms.append(semantic_engine_ts_ms - source_ksql_emit_ts_ms)

        if user_id:
            unique_users.add(user_id)
            user_counts[user_id] += 1

        semantic_counts[event.get("semantic_type", "UNKNOWN")] += 1
        severity_counts[event.get("severity", "UNKNOWN")] += 1
        scheduler_version_counts[event.get("scheduler_version", "UNKNOWN")] += 1
        reasoning_route_counts[event.get("reasoning_route", "UNKNOWN")] += 1

    c.close()

    throughput = accepted / max(MEASURE_SEC, 1)

    print()
    print(f"[{datetime.now().isoformat(timespec='seconds')}] Done. polled={polled}, accepted={accepted}")
    print(f"throughput={throughput:.2f} reasoning_requests/sec")
    print(f"unique_users={len(unique_users)}")

    report("requests_total_from_semantic(now-semantic_engine)", total_from_semantic_ms)
    report("requests_scheduler_delay(scheduler-semantic_engine)", scheduler_delay_ms)
    report("requests_scheduler_transport(now-scheduler)", scheduler_transport_ms)
    report("requests_pipeline_cep_part(ksql_emit-source_last_sent_at)", pipeline_cep_part_ms)
    report("requests_pipeline_rule_part(semantic_engine-source_ksql_emit)", pipeline_rule_part_ms)

    print("semantic_counts:", dict(semantic_counts.most_common()))
    print("severity_counts:", dict(severity_counts.most_common()))
    print("scheduler_version_counts:", dict(scheduler_version_counts.most_common()))
    print("reasoning_route_counts:", dict(reasoning_route_counts.most_common()))
    print(f"top_{TOP_N_USERS}_users:", dict(user_counts.most_common(TOP_N_USERS)))

    if accepted == 0:
        print("No reasoning requests observed in measurement window.")


if __name__ == "__main__":
    main()