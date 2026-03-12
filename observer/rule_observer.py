#!/usr/bin/env python3
import json
import time
import os
from datetime import datetime
from confluent_kafka import Consumer

BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("TOPIC", "semantic_events")
WARMUP_SEC = int(os.getenv("WARMUP_SEC", "5"))
MEASURE_SEC = int(os.getenv("MEASURE_SEC", "60"))
TARGET_RUN_ID = os.getenv("RUN_ID")

def percentile(data, p):
    if not data:
        return None
    data = sorted(data)
    k = int(len(data) * p / 100)
    return data[min(k, len(data)-1)]

def report(name, data):
    if not data:
        print(f"{name}: n=0")
        return
    print(
        f"{name}: n={len(data)} "
        f"mean={sum(data)/len(data):.1f}ms "
        f"p50={percentile(data,50)}ms "
        f"p95={percentile(data,95)}ms "
        f"max={max(data)}ms"
    )

def main():
    c = Consumer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": f"rule-observer-{int(time.time()*1000)}",
        "auto.offset.reset": "latest",
        "enable.auto.commit": True,
    })
    c.subscribe([TOPIC])

    print(f"Rule observer started. topic={TOPIC}")
    print(f"Warmup={WARMUP_SEC}s, Measure={MEASURE_SEC}s")

    start = time.time()
    measure_start = start + WARMUP_SEC
    measure_end = measure_start + MEASURE_SEC

    total_ms = []
    processing_ms = []
    transport_ms = []

    polled = 0
    accepted = 0

    rule_counts = {}
    semantic_counts = {}
    severity_counts = {}

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

        source_ksql_emit_ts_ms = event.get("source_ksql_emit_ts_ms")
        engine_ts_ms = event.get("engine_ts_ms")

        if source_ksql_emit_ts_ms is None or engine_ts_ms is None:
            continue

        source_ksql_emit_ts_ms = int(source_ksql_emit_ts_ms)
        engine_ts_ms = int(engine_ts_ms)

        if source_ksql_emit_ts_ms < measure_start * 1000:
            continue

        total_ms.append(now_ms - source_ksql_emit_ts_ms)
        processing_ms.append(engine_ts_ms - source_ksql_emit_ts_ms)
        transport_ms.append(now_ms - engine_ts_ms)

        rule_id = event.get("rule_id", "UNKNOWN")
        sem_type = event.get("semantic_type", "UNKNOWN")
        severity = event.get("severity", "UNKNOWN")

        rule_counts[rule_id] = rule_counts.get(rule_id, 0) + 1
        semantic_counts[sem_type] = semantic_counts.get(sem_type, 0) + 1
        severity_counts[severity] = severity_counts.get(severity, 0) + 1

        accepted += 1

    c.close()

    print()
    print(f"[{datetime.now().isoformat(timespec='seconds')}] Done. polled={polled}, accepted={accepted}")
    report("rule_total(now-source_ksql_emit)", total_ms)
    report("rule_processing(engine-source_ksql_emit)", processing_ms)
    report("rule_transport(now-engine)", transport_ms)

    print("rule_counts:", rule_counts)
    print("semantic_counts:", semantic_counts)
    print("severity_counts:", severity_counts)

if __name__ == "__main__":
    main()