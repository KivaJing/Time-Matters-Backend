#!/usr/bin/env python3
import json
import time
import os
from datetime import datetime
from confluent_kafka import Consumer

BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("TOPIC", "complex_events_eval_w10_c50")

WARMUP_SEC = int(os.getenv("WARMUP_SEC", "2"))
MEASURE_SEC = int(os.getenv("MEASURE_SEC", "30"))

TARGET_RUN_ID = os.getenv("RUN_ID")

def pick(d, *keys):
    for k in keys:
        if isinstance(d, dict) and k in d and d[k] is not None:
            return d[k]
    return None

def parse_ksql_key(key_bytes):
    if not key_bytes:
        return {}

    s = key_bytes.decode("utf-8", errors="ignore")
    start = s.find("{")
    end = s.rfind("}")

    if start == -1 or end == -1:
        return {}

    try:
        return json.loads(s[start:end+1])
    except Exception:
        return {}

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
        "group.id": f"observer-{int(time.time()*1000)}",
        "auto.offset.reset": "latest",
        "enable.auto.commit": True,
    })

    c.subscribe([TOPIC])

    print(f"Observer started. topic={TOPIC}")
    print(f"Warmup={WARMUP_SEC}s, Measure={MEASURE_SEC}s")

    start = time.time()
    measure_start = start + WARMUP_SEC
    measure_end = measure_start + MEASURE_SEC

    e2e_total = []
    ksql_part = []
    consumer_part = []

    polled = 0
    accepted = 0

    scenario_counts = {}
    user_counts = {}

    while time.time() < measure_end:
        msg = c.poll(1.0)

        if msg is None:
            continue

        if msg.error():
            print("consumer error:", msg.error())
            continue

        polled += 1
        now_ms = int(time.time() * 1000)

        raw_key = msg.key()
        raw_val = msg.value()

        try:
            event = json.loads(raw_val.decode("utf-8"))
        except Exception:
            continue

        key_obj = parse_ksql_key(raw_key)

        run_id = pick(event, "run_id", "RUN_ID") or pick(key_obj, "run_id", "RUN_ID")
        user_id = pick(event, "user_id", "USER_ID") or pick(key_obj, "user_id", "USER_ID")

        if TARGET_RUN_ID and run_id != TARGET_RUN_ID:
            continue

        # Prefer sent_at based fields; fall back to older event-time fields
        source_ts = pick(
            event,
            "last_sent_at_ms",
            "LAST_SENT_AT_MS",
            "last_event_ts_ms",
            "LAST_EVENT_TS_MS",
        )
        emit_ts = pick(event, "ksql_emit_ts_ms", "KSQL_EMIT_TS_MS")

        if source_ts is None or emit_ts is None:
            continue

        source_ts = int(source_ts)
        emit_ts = int(emit_ts)

        if source_ts < measure_start * 1000:
            continue

        total = now_ms - source_ts
        ksql = emit_ts - source_ts
        consumer = now_ms - emit_ts

        e2e_total.append(total)
        ksql_part.append(ksql)
        consumer_part.append(consumer)

        latest_scenario = pick(event, "latest_scenario", "LATEST_SCENARIO", "scenario", "SCENARIO")
        if latest_scenario is None:
            latest_scenario = "UNKNOWN"
        scenario_counts[latest_scenario] = scenario_counts.get(latest_scenario, 0) + 1

        if user_id is None:
            user_id = "UNKNOWN"
        user_counts[user_id] = user_counts.get(user_id, 0) + 1

        accepted += 1

    c.close()

    print()
    print(f"[{datetime.now().isoformat(timespec='seconds')}] Done. polled={polled}, accepted={accepted}")

    report("E2E_total(now-source_ts)", e2e_total)
    report("ksql_part(emit-source_ts)", ksql_part)
    report("consumer_part(now-emit)", consumer_part)

    print("scenario_counts:", scenario_counts)
    print("user_counts:", user_counts)

    if accepted == 0:
        print("No CEP outputs observed in measurement window.")

if __name__ == "__main__":
    main()