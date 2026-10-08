#!/usr/bin/env python3
# fast_llm_observer.py

import json
import builtins
import os
import time
from datetime import datetime
from statistics import mean
from collections import Counter

from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable


BOOTSTRAP = os.getenv("BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("TOPIC", "reasoning_events")
GROUP_ID = os.getenv("GROUP_ID", "fast-llm-observer")

RUN_ID_FILTER = os.getenv("RUN_ID", "").strip()
WARMUP_SEC = int(os.getenv("WARMUP_SEC", "10"))
MEASURE_SEC = int(os.getenv("MEASURE_SEC", "200"))



OUTPUT_FILE = os.getenv("OUTPUT_FILE", "fast_llm_reasoner_output.txt")


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


def build_consumer_with_retry(*args, retries: int = 60, sleep_sec: float = 2.0, **kwargs) -> KafkaConsumer:
    last = None
    for i in range(retries):
        try:
            return KafkaConsumer(*args, **kwargs)
        except NoBrokersAvailable as e:
            last = e
            log(f"[fast_llm_observer] Kafka consumer: no broker ({i+1}/{retries}), retry in {sleep_sec}s...", flush=True)
            time.sleep(sleep_sec)
    raise last


def pct(sorted_vals, p: float):
    if not sorted_vals:
        return None
    if len(sorted_vals) == 1:
        return sorted_vals[0]
    k = int(round((p / 100.0) * (len(sorted_vals) - 1)))
    return sorted_vals[max(0, min(k, len(sorted_vals) - 1))]


def fmt_stats(name: str, vals):
    if not vals:
        return f"{name}: n=0"
    arr = sorted(vals)
    return (
        f"{name}: n={len(arr)} "
        f"mean={mean(arr):.1f}ms "
        f"p50={pct(arr, 50):.0f}ms "
        f"p95={pct(arr, 95):.0f}ms "
        f"p99={pct(arr, 99):.0f}ms "
        f"max={max(arr):.0f}ms"
    )


def as_int(v):
    try:
        if v is None:
            return None
        return int(v)
    except Exception:
        return None


def is_fast_llm_event(evt: dict) -> bool:
    route = str(evt.get("reasoning_route", "") or "")
    mode = str(evt.get("reasoning_mode", "") or "")
    return route == "fast_llm" or mode.startswith("fast_llm")


def main():
    consumer = build_consumer_with_retry(
        TOPIC,
        bootstrap_servers=BOOTSTRAP,
        group_id=GROUP_ID,
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
        auto_offset_reset="latest",
        consumer_timeout_ms=1000,
    )

    log(f"Fast LLM observer started. topic={TOPIC}", flush=True)
    log(f"Warmup={WARMUP_SEC}s, Measure={MEASURE_SEC}s, RUN_ID={RUN_ID_FILTER or '*'}", flush=True)

    start_ts = time.time()
    warmup_end = start_ts + WARMUP_SEC
    measure_end = warmup_end + MEASURE_SEC

    polled = 0
    accepted = 0

    unique_users = set()
    top_users = Counter()

    route_counts = Counter()
    mode_counts = Counter()
    semantic_counts = Counter()
    severity_counts = Counter()
    decision_counts = Counter()
    model_counts = Counter()
    prompt_counts = Counter()

    fast_total_ms = []
    fast_model_ms = []
    fast_transport_ms = []

    pipeline_cep_ms = []
    pipeline_rule_ms = []
    pipeline_scheduler_ms = []
    pipeline_fast_llm_ms = []
    pipeline_total_ms = []

    try:
        while time.time() < measure_end:
            records = consumer.poll(timeout_ms=500, max_records=500)

            now_ms = int(time.time() * 1000)

            if not records:
                continue

            for _tp, msgs in records.items():
                for msg in msgs:
                    polled += 1
                    evt = msg.value or {}

                    if not is_fast_llm_event(evt):
                        continue

                    if RUN_ID_FILTER and str(evt.get("run_id")) != RUN_ID_FILTER:
                        continue

                    reasoning_ts_ms = as_int(evt.get("reasoning_ts_ms"))
                    scheduler_ts_ms = as_int(evt.get("scheduler_ts_ms"))
                    semantic_engine_ts_ms = as_int(evt.get("semantic_engine_ts_ms"))
                    source_ksql_emit_ts_ms = as_int(evt.get("source_ksql_emit_ts_ms"))
                    source_last_sent_at_ms = as_int(evt.get("source_last_sent_at_ms"))
                    llm_latency_ms = as_int(evt.get("llm_latency_ms"))

                    # Keep the same acceptance style as your other observers:
                    # require reasoning_ts_ms and scheduler_ts_ms and reasoning_ts within measurement window.
                    if reasoning_ts_ms is None or scheduler_ts_ms is None:
                        continue
                    if reasoning_ts_ms < int(warmup_end * 1000) or reasoning_ts_ms > int(measure_end * 1000):
                        continue

                    accepted += 1

                    user_id = str(evt.get("user_id") or "")
                    if user_id:
                        unique_users.add(user_id)
                        top_users[user_id] += 1

                    route_counts[str(evt.get("reasoning_route"))] += 1
                    mode_counts[str(evt.get("reasoning_mode"))] += 1
                    semantic_counts[str(evt.get("semantic_type"))] += 1
                    severity_counts[str(evt.get("severity"))] += 1
                    decision_counts[str(evt.get("decision"))] += 1
                    model_counts[str(evt.get("model"))] += 1
                    prompt_counts[str(evt.get("prompt_version"))] += 1

                    # Main fast-LLM latencies
                    fast_total_ms.append(now_ms - scheduler_ts_ms)

                    if llm_latency_ms is not None:
                        fast_model_ms.append(llm_latency_ms)

                    if reasoning_ts_ms is not None:
                        fast_transport_ms.append(now_ms - reasoning_ts_ms)

                    # Pipeline decomposition
                    if source_last_sent_at_ms is not None and source_ksql_emit_ts_ms is not None:
                        pipeline_cep_ms.append(source_ksql_emit_ts_ms - source_last_sent_at_ms)

                    if source_ksql_emit_ts_ms is not None and semantic_engine_ts_ms is not None:
                        pipeline_rule_ms.append(semantic_engine_ts_ms - source_ksql_emit_ts_ms)

                    if semantic_engine_ts_ms is not None and scheduler_ts_ms is not None:
                        pipeline_scheduler_ms.append(scheduler_ts_ms - semantic_engine_ts_ms)

                    if scheduler_ts_ms is not None and reasoning_ts_ms is not None:
                        pipeline_fast_llm_ms.append(reasoning_ts_ms - scheduler_ts_ms)

                    if source_last_sent_at_ms is not None:
                        pipeline_total_ms.append(now_ms - source_last_sent_at_ms)

    except KeyboardInterrupt:
        pass
    finally:
        try:
            consumer.close()
        except Exception:
            pass

    elapsed = max(1e-9, MEASURE_SEC)
    throughput = accepted / elapsed

    log("", flush=True)
    log(f"throughput={throughput:.2f} fast_reasoning_events/sec", flush=True)
    log(f"unique_users={len(unique_users)}", flush=True)

    log(fmt_stats("fast_llm_total(now-scheduler)", fast_total_ms), flush=True)
    log(fmt_stats("fast_llm_model_part(reasoning-scheduler)".replace("reasoning-scheduler", "llm_latency_reported"), fast_model_ms), flush=True)
    log(fmt_stats("fast_llm_transport(now-reasoning)", fast_transport_ms), flush=True)

    log(fmt_stats("fast_pipeline_cep_part(ksql_emit-source_last_sent_at)", pipeline_cep_ms), flush=True)
    log(fmt_stats("fast_pipeline_rule_part(semantic_engine-source_ksql_emit)", pipeline_rule_ms), flush=True)
    log(fmt_stats("fast_pipeline_scheduler_part(scheduler-semantic_engine)", pipeline_scheduler_ms), flush=True)
    log(fmt_stats("fast_pipeline_llm_part(reasoning-scheduler)", pipeline_fast_llm_ms), flush=True)
    log(fmt_stats("fast_pipeline_total(now-source_last_sent_at)", pipeline_total_ms), flush=True)

    log(f"semantic_counts: {dict(semantic_counts)}", flush=True)
    log(f"severity_counts: {dict(severity_counts)}", flush=True)
    log(f"decision_counts: {dict(decision_counts)}", flush=True)
    log(f"reasoning_route_counts: {dict(route_counts)}", flush=True)
    log(f"reasoning_mode_counts: {dict(mode_counts)}", flush=True)
    log(f"model_counts: {dict(model_counts)}", flush=True)
    log(f"prompt_version_counts: {dict(prompt_counts)}", flush=True)
    log(f"top_10_users: {dict(top_users.most_common(10))}", flush=True)


if __name__ == "__main__":
    main()