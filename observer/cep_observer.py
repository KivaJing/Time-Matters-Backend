#!/usr/bin/env python3
import json
import time
import os
from datetime import datetime
from collections import Counter
from confluent_kafka import Consumer

BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("TOPIC", "risk_state_windows_new")

WARMUP_SEC = int(os.getenv("WARMUP_SEC", "5"))
MEASURE_SEC = int(os.getenv("MEASURE_SEC", "60"))
TARGET_RUN_ID = os.getenv("RUN_ID")

TOP_N_USERS = int(os.getenv("TOP_N_USERS", "10"))


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
        return json.loads(s[start:end + 1])
    except Exception:
        return {}


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


def report_num(name, data):
    if not data:
        print(f"{name}: n=0")
        return
    print(
        f"{name}: n={len(data)} "
        f"mean={sum(data)/len(data):.2f} "
        f"min={min(data):.2f} "
        f"p50={percentile(data, 50):.2f} "
        f"p95={percentile(data, 95):.2f} "
        f"max={max(data):.2f}"
    )


def main():
    c = Consumer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": f"cep-observer-{int(time.time() * 1000)}",
        "auto.offset.reset": "latest",
        "enable.auto.commit": True,
    })
    c.subscribe([TOPIC])

    print(f"CEP observer started. topic={TOPIC}")
    print(f"Warmup={WARMUP_SEC}s, Measure={MEASURE_SEC}s, RUN_ID={TARGET_RUN_ID or 'ALL'}")

    start = time.time()
    measure_start = start + WARMUP_SEC
    measure_end = measure_start + MEASURE_SEC

    e2e_total_ms = []
    ksql_part_ms = []
    observer_transport_ms = []
    ksql_reported_latency_ms = []
    event_count_per_window = []

    polled = 0
    accepted = 0

    scenario_counts = Counter()
    user_counts = Counter()
    unique_users = set()
    run_counts = Counter()

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

        key_obj = parse_ksql_key(msg.key())

        run_id = pick(event, "run_id", "RUN_ID") or pick(key_obj, "run_id", "RUN_ID")
        user_id = pick(event, "user_id", "USER_ID") or pick(key_obj, "user_id", "USER_ID")

        if TARGET_RUN_ID and run_id != TARGET_RUN_ID:
            continue

        source_ts = pick(
            event,
            "last_sent_at_ms",
            "LAST_SENT_AT_MS",
            "last_event_ts_ms",
            "LAST_EVENT_TS_MS",
        )
        emit_ts = pick(event, "ksql_emit_ts_ms", "KSQL_EMIT_TS_MS")
        reported_ksql_latency = pick(event, "ksql_latency_ms", "KSQL_LATENCY_MS")
        event_count = pick(event, "event_count", "EVENT_COUNT")
        latest_scenario = pick(event, "latest_scenario", "LATEST_SCENARIO", "scenario", "SCENARIO")

        if source_ts is None or emit_ts is None:
            continue

        source_ts = int(source_ts)
        emit_ts = int(emit_ts)

        if source_ts < int(measure_start * 1000):
            continue

        e2e_total_ms.append(now_ms - source_ts)
        ksql_part_ms.append(emit_ts - source_ts)
        observer_transport_ms.append(now_ms - emit_ts)

        if reported_ksql_latency is not None:
            ksql_reported_latency_ms.append(int(reported_ksql_latency))
        if event_count is not None:
            event_count_per_window.append(float(event_count))

        scenario_counts[latest_scenario or "UNKNOWN"] += 1
        user_counts[user_id or "UNKNOWN"] += 1
        run_counts[run_id or "UNKNOWN"] += 1
        if user_id:
            unique_users.add(user_id)

        accepted += 1

    c.close()

    elapsed = max(MEASURE_SEC, 1)
    throughput = accepted / elapsed

    print()
    print(f"[{datetime.now().isoformat(timespec='seconds')}] Done. polled={polled}, accepted={accepted}")
    print(f"throughput={throughput:.2f} windows/sec")
    print(f"unique_users={len(unique_users)}")

    report("cep_total(now-last_sent_at)", e2e_total_ms)
    report("cep_ksql_part(emit-last_sent_at)", ksql_part_ms)
    report("cep_observer_transport(now-emit)", observer_transport_ms)
    report("cep_ksql_reported_latency_ms", ksql_reported_latency_ms)
    report_num("event_count_per_window", event_count_per_window)

    print("scenario_counts:", dict(scenario_counts.most_common()))
    print("run_counts:", dict(run_counts.most_common()))
    print(f"top_{TOP_N_USERS}_users:", dict(user_counts.most_common(TOP_N_USERS)))

    if accepted == 0:
        print("No CEP outputs observed in measurement window.")


if __name__ == "__main__":
    main()