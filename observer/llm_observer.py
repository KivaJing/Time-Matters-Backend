#!/usr/bin/env python3
# llm_observer.py

import json
import time
import os
from datetime import datetime
from collections import Counter, defaultdict
from confluent_kafka import Consumer

BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("TOPIC", "reasoning_events")
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


def init_bucket():
    return {
        "accepted_completion": 0,
        "accepted_source": 0,
        "error_count_completion": 0,
        "error_count_source": 0,
        "unique_users_completion": set(),
        "unique_users_source": set(),
        "user_counts_completion": Counter(),
        "user_counts_source": Counter(),
        "risk_counts_completion": Counter(),
        "risk_counts_source": Counter(),
        "decision_counts_completion": Counter(),
        "decision_counts_source": Counter(),
        "intervention_counts_completion": Counter(),
        "intervention_counts_source": Counter(),
        "semantic_type_counts_completion": Counter(),
        "semantic_type_counts_source": Counter(),
        "reasoning_mode_counts_completion": Counter(),
        "reasoning_mode_counts_source": Counter(),
        "scheduler_version_counts_completion": Counter(),
        "scheduler_version_counts_source": Counter(),
        "prompt_version_counts_completion": Counter(),
        "prompt_version_counts_source": Counter(),
        "metrics_completion": defaultdict(list),
        "metrics_source": defaultdict(list),
    }


def update_bucket(bucket, event, now_ms, in_completion_window, in_source_window):
    user_id = event.get("user_id")
    risk_level = event.get("risk_level", "UNKNOWN")
    decision = event.get("decision", "UNKNOWN")
    intervention_type = event.get("intervention_type", "UNKNOWN")
    semantic_type = event.get("semantic_type", "UNKNOWN")
    reasoning_mode = event.get("reasoning_mode", "UNKNOWN")
    scheduler_version = event.get("scheduler_version", "UNKNOWN")
    prompt_version = event.get("prompt_version", "UNKNOWN")

    semantic_engine_ts_ms = safe_int(event.get("semantic_engine_ts_ms"))
    scheduler_ts_ms = safe_int(event.get("scheduler_ts_ms"))
    reasoning_ts_ms = safe_int(event.get("reasoning_ts_ms"))
    llm_latency_ms = safe_int(event.get("llm_latency_ms"))
    source_last_sent_at_ms = safe_int(event.get("source_last_sent_at_ms"))
    source_ksql_emit_ts_ms = safe_int(event.get("source_ksql_emit_ts_ms"))

    if in_completion_window:
        bucket["accepted_completion"] += 1

        bucket["risk_counts_completion"][risk_level] += 1
        bucket["decision_counts_completion"][decision] += 1
        bucket["intervention_counts_completion"][intervention_type] += 1
        bucket["semantic_type_counts_completion"][semantic_type] += 1
        bucket["reasoning_mode_counts_completion"][reasoning_mode] += 1
        bucket["scheduler_version_counts_completion"][scheduler_version] += 1
        bucket["prompt_version_counts_completion"][prompt_version] += 1

        if user_id:
            bucket["unique_users_completion"].add(user_id)
            bucket["user_counts_completion"][user_id] += 1

        if event.get("error"):
            bucket["error_count_completion"] += 1

        if semantic_engine_ts_ms is not None:
            bucket["metrics_completion"]["total_from_semantic_ms"].append(now_ms - semantic_engine_ts_ms)

        if llm_latency_ms is not None:
            bucket["metrics_completion"]["llm_only_latency_ms"].append(llm_latency_ms)

        if semantic_engine_ts_ms is not None and reasoning_ts_ms is not None:
            bucket["metrics_completion"]["semantic_to_reasoning_ms"].append(reasoning_ts_ms - semantic_engine_ts_ms)
            bucket["metrics_completion"]["reasoning_stage_total_ms"].append(reasoning_ts_ms - semantic_engine_ts_ms)

        if reasoning_ts_ms is not None:
            bucket["metrics_completion"]["post_reasoning_transport_ms"].append(now_ms - reasoning_ts_ms)

        if source_last_sent_at_ms is not None:
            bucket["metrics_completion"]["pipeline_e2e_ms"].append(now_ms - source_last_sent_at_ms)

        if source_last_sent_at_ms is not None and source_ksql_emit_ts_ms is not None:
            bucket["metrics_completion"]["pipeline_cep_part_ms"].append(
                source_ksql_emit_ts_ms - source_last_sent_at_ms
            )

        if source_ksql_emit_ts_ms is not None and semantic_engine_ts_ms is not None:
            bucket["metrics_completion"]["pipeline_rule_part_ms"].append(
                semantic_engine_ts_ms - source_ksql_emit_ts_ms
            )

        if scheduler_ts_ms is not None and semantic_engine_ts_ms is not None:
            bucket["metrics_completion"]["scheduler_delay_ms"].append(
                scheduler_ts_ms - semantic_engine_ts_ms
            )

        if scheduler_ts_ms is not None and reasoning_ts_ms is not None:
            bucket["metrics_completion"]["scheduler_to_reasoning_ms"].append(
                reasoning_ts_ms - scheduler_ts_ms
            )

        if scheduler_ts_ms is not None and reasoning_ts_ms is not None and llm_latency_ms is not None:
            bucket["metrics_completion"]["reasoner_queue_only_ms"].append(
                (reasoning_ts_ms - scheduler_ts_ms) - llm_latency_ms
            )

    if in_source_window:
        bucket["accepted_source"] += 1

        bucket["risk_counts_source"][risk_level] += 1
        bucket["decision_counts_source"][decision] += 1
        bucket["intervention_counts_source"][intervention_type] += 1
        bucket["semantic_type_counts_source"][semantic_type] += 1
        bucket["reasoning_mode_counts_source"][reasoning_mode] += 1
        bucket["scheduler_version_counts_source"][scheduler_version] += 1
        bucket["prompt_version_counts_source"][prompt_version] += 1

        if user_id:
            bucket["unique_users_source"].add(user_id)
            bucket["user_counts_source"][user_id] += 1

        if event.get("error"):
            bucket["error_count_source"] += 1

        if semantic_engine_ts_ms is not None:
            bucket["metrics_source"]["total_from_semantic_ms"].append(now_ms - semantic_engine_ts_ms)

        if llm_latency_ms is not None:
            bucket["metrics_source"]["llm_only_latency_ms"].append(llm_latency_ms)

        if semantic_engine_ts_ms is not None and reasoning_ts_ms is not None:
            bucket["metrics_source"]["semantic_to_reasoning_ms"].append(reasoning_ts_ms - semantic_engine_ts_ms)
            bucket["metrics_source"]["reasoning_stage_total_ms"].append(reasoning_ts_ms - semantic_engine_ts_ms)

        if reasoning_ts_ms is not None:
            bucket["metrics_source"]["post_reasoning_transport_ms"].append(now_ms - reasoning_ts_ms)

        if source_last_sent_at_ms is not None:
            bucket["metrics_source"]["pipeline_e2e_ms"].append(now_ms - source_last_sent_at_ms)

        if source_last_sent_at_ms is not None and source_ksql_emit_ts_ms is not None:
            bucket["metrics_source"]["pipeline_cep_part_ms"].append(
                source_ksql_emit_ts_ms - source_last_sent_at_ms
            )

        if source_ksql_emit_ts_ms is not None and semantic_engine_ts_ms is not None:
            bucket["metrics_source"]["pipeline_rule_part_ms"].append(
                semantic_engine_ts_ms - source_ksql_emit_ts_ms
            )

        if scheduler_ts_ms is not None and semantic_engine_ts_ms is not None:
            bucket["metrics_source"]["scheduler_delay_ms"].append(
                scheduler_ts_ms - semantic_engine_ts_ms
            )

        if scheduler_ts_ms is not None and reasoning_ts_ms is not None:
            bucket["metrics_source"]["scheduler_to_reasoning_ms"].append(
                reasoning_ts_ms - scheduler_ts_ms
            )

        if scheduler_ts_ms is not None and reasoning_ts_ms is not None and llm_latency_ms is not None:
            bucket["metrics_source"]["reasoner_queue_only_ms"].append(
                (reasoning_ts_ms - scheduler_ts_ms) - llm_latency_ms
            )


def print_bucket(title, bucket, elapsed):
    throughput_completion = bucket["accepted_completion"] / max(elapsed, 1)
    throughput_source = bucket["accepted_source"] / max(elapsed, 1)

    error_rate_completion = (
        bucket["error_count_completion"] / bucket["accepted_completion"]
        if bucket["accepted_completion"] else 0.0
    )
    error_rate_source = (
        bucket["error_count_source"] / bucket["accepted_source"]
        if bucket["accepted_source"] else 0.0
    )

    print(f"\n=== {title}: Completion-based (reasoning_ts_ms in window) ===")
    print(f"accepted_completion={bucket['accepted_completion']}")
    print(f"throughput_completion={throughput_completion:.2f} reasoning_events/sec")
    print(f"unique_users_completion={len(bucket['unique_users_completion'])}")
    print(f"error_count_completion={bucket['error_count_completion']}")
    print(f"error_rate_completion={error_rate_completion:.3f}")

    mc = bucket["metrics_completion"]
    report("completion_total_from_semantic(now-semantic_engine)", mc["total_from_semantic_ms"])
    report("completion_llm_only_latency_ms", mc["llm_only_latency_ms"])
    report("completion_semantic_to_reasoning(reasoning-semantic_engine)", mc["semantic_to_reasoning_ms"])
    report("completion_post_reasoning_transport(now-reasoning)", mc["post_reasoning_transport_ms"])
    report("completion_pipeline_e2e(now-source_last_sent_at)", mc["pipeline_e2e_ms"])
    report("completion_pipeline_cep_part(ksql_emit-source_last_sent_at)", mc["pipeline_cep_part_ms"])
    report("completion_pipeline_rule_part(semantic_engine-source_ksql_emit)", mc["pipeline_rule_part_ms"])
    report("completion_pipeline_reasoning_stage_total(reasoning-semantic_engine)", mc["reasoning_stage_total_ms"])
    report("completion_scheduler_delay(scheduler-semantic_engine)", mc["scheduler_delay_ms"])
    report("completion_scheduler_to_reasoning(reasoning-scheduler)", mc["scheduler_to_reasoning_ms"])
    report("completion_reasoner_queue_only((reasoning-scheduler)-llm_latency)", mc["reasoner_queue_only_ms"])

    print("completion_risk_counts:", dict(bucket["risk_counts_completion"].most_common()))
    print("completion_decision_counts:", dict(bucket["decision_counts_completion"].most_common()))
    print("completion_intervention_type_counts:", dict(bucket["intervention_counts_completion"].most_common()))
    print("completion_semantic_type_counts:", dict(bucket["semantic_type_counts_completion"].most_common()))
    print("completion_reasoning_mode_counts:", dict(bucket["reasoning_mode_counts_completion"].most_common()))
    print("completion_scheduler_version_counts:", dict(bucket["scheduler_version_counts_completion"].most_common()))
    print("completion_prompt_version_counts:", dict(bucket["prompt_version_counts_completion"].most_common()))
    print(f"completion_top_{TOP_N_USERS}_users:", dict(bucket["user_counts_completion"].most_common(TOP_N_USERS)))

    print(f"\n=== {title}: Source-aligned (semantic_engine_ts_ms in window) ===")
    print(f"accepted_source_aligned={bucket['accepted_source']}")
    print(f"throughput_source_aligned={throughput_source:.2f} reasoning_events/sec")
    print(f"unique_users_source_aligned={len(bucket['unique_users_source'])}")
    print(f"error_count_source_aligned={bucket['error_count_source']}")
    print(f"error_rate_source_aligned={error_rate_source:.3f}")

    ms = bucket["metrics_source"]
    report("source_total_from_semantic(now-semantic_engine)", ms["total_from_semantic_ms"])
    report("source_llm_only_latency_ms", ms["llm_only_latency_ms"])
    report("source_semantic_to_reasoning(reasoning-semantic_engine)", ms["semantic_to_reasoning_ms"])
    report("source_post_reasoning_transport(now-reasoning)", ms["post_reasoning_transport_ms"])
    report("source_pipeline_e2e(now-source_last_sent_at)", ms["pipeline_e2e_ms"])
    report("source_pipeline_cep_part(ksql_emit-source_last_sent_at)", ms["pipeline_cep_part_ms"])
    report("source_pipeline_rule_part(semantic_engine-source_ksql_emit)", ms["pipeline_rule_part_ms"])
    report("source_pipeline_reasoning_stage_total(reasoning-semantic_engine)", ms["reasoning_stage_total_ms"])
    report("source_scheduler_delay(scheduler-semantic_engine)", ms["scheduler_delay_ms"])
    report("source_scheduler_to_reasoning(reasoning-scheduler)", ms["scheduler_to_reasoning_ms"])
    report("source_reasoner_queue_only((reasoning-scheduler)-llm_latency)", ms["reasoner_queue_only_ms"])

    print("source_risk_counts:", dict(bucket["risk_counts_source"].most_common()))
    print("source_decision_counts:", dict(bucket["decision_counts_source"].most_common()))
    print("source_intervention_type_counts:", dict(bucket["intervention_counts_source"].most_common()))
    print("source_semantic_type_counts:", dict(bucket["semantic_type_counts_source"].most_common()))
    print("source_reasoning_mode_counts:", dict(bucket["reasoning_mode_counts_source"].most_common()))
    print("source_scheduler_version_counts:", dict(bucket["scheduler_version_counts_source"].most_common()))
    print("source_prompt_version_counts:", dict(bucket["prompt_version_counts_source"].most_common()))
    print(f"source_top_{TOP_N_USERS}_users:", dict(bucket["user_counts_source"].most_common(TOP_N_USERS)))


def main():
    c = Consumer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": f"llm-observer-{int(time.time() * 1000)}",
        "auto.offset.reset": "latest",
        "enable.auto.commit": True,
    })
    c.subscribe([TOPIC])

    print(f"LLM observer started. topic={TOPIC}")
    print(f"Warmup={WARMUP_SEC}s, Measure={MEASURE_SEC}s, RUN_ID={TARGET_RUN_ID or 'ALL'}")

    start = time.time()
    measure_start = start + WARMUP_SEC
    measure_end = measure_start + MEASURE_SEC
    measure_start_ms = int(measure_start * 1000)
    measure_end_ms = int(measure_end * 1000)

    polled = 0

    all_bucket = init_bucket()
    rule_bucket = init_bucket()
    llm_bucket = init_bucket()

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

        semantic_engine_ts_ms = safe_int(event.get("semantic_engine_ts_ms"))
        reasoning_ts_ms = safe_int(event.get("reasoning_ts_ms"))
        llm_latency_ms = safe_int(event.get("llm_latency_ms"))
        reasoning_mode = event.get("reasoning_mode", "UNKNOWN")

        if semantic_engine_ts_ms is None or reasoning_ts_ms is None or llm_latency_ms is None:
            continue

        in_completion_window = measure_start_ms <= reasoning_ts_ms <= measure_end_ms
        in_source_window = measure_start_ms <= semantic_engine_ts_ms <= measure_end_ms

        update_bucket(all_bucket, event, now_ms, in_completion_window, in_source_window)

        if reasoning_mode == "rule_based":
            update_bucket(rule_bucket, event, now_ms, in_completion_window, in_source_window)
        elif reasoning_mode == "llm":
            update_bucket(llm_bucket, event, now_ms, in_completion_window, in_source_window)

    c.close()

    print()
    print(f"[{datetime.now().isoformat(timespec='seconds')}] Done. polled={polled}")

    print_bucket("ALL reasoning events", all_bucket, MEASURE_SEC)
    print_bucket("RULE-BASED only", rule_bucket, MEASURE_SEC)
    print_bucket("LLM only", llm_bucket, MEASURE_SEC)

    if (
        all_bucket["accepted_completion"] == 0
        and all_bucket["accepted_source"] == 0
    ):
        print("\nNo reasoning events matched either completion-based or source-aligned window.")


if __name__ == "__main__":
    main()