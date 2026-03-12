#!/usr/bin/env python3
import json
import time
import os
from datetime import datetime
from confluent_kafka import Consumer

BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("TOPIC", "reasoning_events")
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
        "group.id": f"llm-observer-{int(time.time()*1000)}",
        "auto.offset.reset": "latest",
        "enable.auto.commit": True,
    })
    c.subscribe([TOPIC])

    print(f"LLM observer started. topic={TOPIC}")
    print(f"Warmup={WARMUP_SEC}s, Measure={MEASURE_SEC}s")

    start = time.time()
    measure_start = start + WARMUP_SEC
    measure_end = measure_start + MEASURE_SEC

    total_ms = []
    llm_ms = []

    polled = 0
    accepted = 0
    error_count = 0
    risk_counts = {}

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

        semantic_engine_ts_ms = event.get("semantic_engine_ts_ms")
        reasoning_ts_ms = event.get("reasoning_ts_ms")
        llm_latency_ms = event.get("llm_latency_ms")

        if semantic_engine_ts_ms is None or reasoning_ts_ms is None or llm_latency_ms is None:
            continue

        semantic_engine_ts_ms = int(semantic_engine_ts_ms)
        reasoning_ts_ms = int(reasoning_ts_ms)
        llm_latency_ms = int(llm_latency_ms)

        if semantic_engine_ts_ms < measure_start * 1000:
            continue

        total_ms.append(now_ms - semantic_engine_ts_ms)
        llm_ms.append(llm_latency_ms)

        risk = event.get("risk_level", "UNKNOWN")
        risk_counts[risk] = risk_counts.get(risk, 0) + 1

        if event.get("error"):
            error_count += 1

        accepted += 1

    c.close()

    print()
    print(f"[{datetime.now().isoformat(timespec='seconds')}] Done. polled={polled}, accepted={accepted}")
    report("reasoning_total(now-semantic_engine)", total_ms)
    report("llm_latency_ms", llm_ms)
    print("risk_counts:", risk_counts)
    print(f"error_count={error_count}")
    if accepted:
        print(f"error_rate={error_count/accepted:.3f}")

if __name__ == "__main__":
    main()