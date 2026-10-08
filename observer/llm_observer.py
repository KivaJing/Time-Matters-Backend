#!/usr/bin/env python3
# llm_observer.py

import json
import os
import time
import builtins
from datetime import datetime
from collections import Counter, defaultdict

from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable


# ------------------------------------------------------------
# Configuration
# ------------------------------------------------------------

BOOTSTRAP_SERVERS = os.getenv(
    "BOOTSTRAP_SERVERS",
    os.getenv("BOOTSTRAP", "localhost:9092")
)

TOPIC = os.getenv("TOPIC", "reasoning_events")

WARMUP_SEC = int(os.getenv("WARMUP_SEC", "10"))
MEASURE_SEC = int(os.getenv("MEASURE_SEC", "200"))

TARGET_RUN_ID = os.getenv("RUN_ID", "").strip()

TOP_N_USERS = int(os.getenv("TOP_N_USERS", "10"))

OUTPUT_FILE = os.getenv(
    "OUTPUT_FILE",
    "llm_output.txt"
)


# ------------------------------------------------------------
# Logging
# ------------------------------------------------------------

def log(*args, **kwargs):
    builtins.print(*args, **kwargs)

    file_kwargs = {}

    for key in ("sep", "end", "flush"):
        if key in kwargs:
            file_kwargs[key] = kwargs[key]

    try:
        with open(OUTPUT_FILE, "a", encoding="utf-8") as f:
            builtins.print(*args, file=f, **file_kwargs)
    except Exception:
        # File logging must never stop the observer.
        pass


def log_session_start(name):
    try:
        with open(OUTPUT_FILE, "a", encoding="utf-8") as f:
            builtins.print(
                f"\n===== {name} started at "
                f"{datetime.now().isoformat(timespec='seconds')} =====",
                file=f,
            )
    except Exception:
        pass


# ------------------------------------------------------------
# Kafka
# ------------------------------------------------------------

def build_consumer_with_retry(
    *args,
    retries=60,
    sleep_sec=2.0,
    **kwargs
):
    last_error = None

    for i in range(retries):
        try:
            return KafkaConsumer(*args, **kwargs)

        except NoBrokersAvailable as e:
            last_error = e

            log(
                f"[llm_observer] Kafka broker unavailable "
                f"({i + 1}/{retries}); retrying in {sleep_sec}s...",
                flush=True,
            )

            time.sleep(sleep_sec)

    raise last_error


# ------------------------------------------------------------
# Helpers
# ------------------------------------------------------------

def safe_int(value):
    if value is None:
        return None

    try:
        return int(value)
    except Exception:
        return None


def percentile(data, p):
    if not data:
        return None

    values = sorted(data)

    if len(values) == 1:
        return values[0]

    k = int(round(
        (p / 100.0) * (len(values) - 1)
    ))

    k = max(
        0,
        min(k, len(values) - 1)
    )

    return values[k]


def report(name, data):
    if not data:
        log(f"{name}: n=0")
        return

    log(
        f"{name}: "
        f"n={len(data)} "
        f"mean={sum(data) / len(data):.1f}ms "
        f"p50={percentile(data, 50)}ms "
        f"p95={percentile(data, 95)}ms "
        f"p99={percentile(data, 99)}ms "
        f"max={max(data)}ms"
    )


# ------------------------------------------------------------
# Bucket
# ------------------------------------------------------------

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

        "reasoning_route_counts_completion": Counter(),
        "reasoning_route_counts_source": Counter(),

        "scheduler_version_counts_completion": Counter(),
        "scheduler_version_counts_source": Counter(),

        "prompt_version_counts_completion": Counter(),
        "prompt_version_counts_source": Counter(),

        "model_counts_completion": Counter(),
        "model_counts_source": Counter(),

        "metrics_completion": defaultdict(list),
        "metrics_source": defaultdict(list),
    }


def update_bucket(
    bucket,
    event,
    now_ms,
    in_completion_window,
    in_source_window,
):
    user_id = event.get("user_id")

    risk_level = str(
        event.get("risk_level", "UNKNOWN")
    )

    decision = str(
        event.get("decision", "UNKNOWN")
    )

    intervention_type = str(
        event.get("intervention_type", "UNKNOWN")
    )

    semantic_type = str(
        event.get("semantic_type", "UNKNOWN")
    )

    reasoning_mode = str(
        event.get("reasoning_mode", "UNKNOWN")
    )

    reasoning_route = str(
        event.get("reasoning_route", "UNKNOWN")
    )

    scheduler_version = str(
        event.get("scheduler_version", "UNKNOWN")
    )

    prompt_version = str(
        event.get("prompt_version", "UNKNOWN")
    )

    model = str(
        event.get("model", "NONE")
    )

    semantic_engine_ts_ms = safe_int(
        event.get("semantic_engine_ts_ms")
    )

    scheduler_ts_ms = safe_int(
        event.get("scheduler_ts_ms")
    )

    reasoning_ts_ms = safe_int(
        event.get("reasoning_ts_ms")
    )

    llm_latency_ms = safe_int(
        event.get("llm_latency_ms")
    )

    source_last_sent_at_ms = safe_int(
        event.get("source_last_sent_at_ms")
    )

    source_ksql_emit_ts_ms = safe_int(
        event.get("source_ksql_emit_ts_ms")
    )

    # --------------------------------------------------------
    # Completion-aligned bucket
    # --------------------------------------------------------

    if in_completion_window:
        bucket["accepted_completion"] += 1

        bucket["risk_counts_completion"][risk_level] += 1
        bucket["decision_counts_completion"][decision] += 1
        bucket["intervention_counts_completion"][intervention_type] += 1
        bucket["semantic_type_counts_completion"][semantic_type] += 1
        bucket["reasoning_mode_counts_completion"][reasoning_mode] += 1
        bucket["reasoning_route_counts_completion"][reasoning_route] += 1
        bucket["scheduler_version_counts_completion"][scheduler_version] += 1
        bucket["prompt_version_counts_completion"][prompt_version] += 1
        bucket["model_counts_completion"][model] += 1

        if user_id:
            bucket["unique_users_completion"].add(user_id)
            bucket["user_counts_completion"][user_id] += 1

        if event.get("error"):
            bucket["error_count_completion"] += 1

        if semantic_engine_ts_ms is not None:
            bucket["metrics_completion"][
                "total_from_semantic_ms"
            ].append(
                now_ms - semantic_engine_ts_ms
            )

        if llm_latency_ms is not None:
            bucket["metrics_completion"][
                "llm_only_latency_ms"
            ].append(
                llm_latency_ms
            )

        if (
            semantic_engine_ts_ms is not None
            and reasoning_ts_ms is not None
        ):
            latency = (
                reasoning_ts_ms
                - semantic_engine_ts_ms
            )

            bucket["metrics_completion"][
                "semantic_to_reasoning_ms"
            ].append(latency)

            bucket["metrics_completion"][
                "reasoning_stage_total_ms"
            ].append(latency)

        if reasoning_ts_ms is not None:
            bucket["metrics_completion"][
                "post_reasoning_transport_ms"
            ].append(
                now_ms - reasoning_ts_ms
            )

        if source_last_sent_at_ms is not None:
            bucket["metrics_completion"][
                "pipeline_e2e_ms"
            ].append(
                now_ms - source_last_sent_at_ms
            )

        if (
            source_last_sent_at_ms is not None
            and source_ksql_emit_ts_ms is not None
        ):
            bucket["metrics_completion"][
                "pipeline_cep_part_ms"
            ].append(
                source_ksql_emit_ts_ms
                - source_last_sent_at_ms
            )

        if (
            source_ksql_emit_ts_ms is not None
            and semantic_engine_ts_ms is not None
        ):
            bucket["metrics_completion"][
                "pipeline_rule_part_ms"
            ].append(
                semantic_engine_ts_ms
                - source_ksql_emit_ts_ms
            )

        if (
            scheduler_ts_ms is not None
            and semantic_engine_ts_ms is not None
        ):
            bucket["metrics_completion"][
                "scheduler_delay_ms"
            ].append(
                scheduler_ts_ms
                - semantic_engine_ts_ms
            )

        if (
            scheduler_ts_ms is not None
            and reasoning_ts_ms is not None
        ):
            bucket["metrics_completion"][
                "scheduler_to_reasoning_ms"
            ].append(
                reasoning_ts_ms
                - scheduler_ts_ms
            )

        if (
            scheduler_ts_ms is not None
            and reasoning_ts_ms is not None
            and llm_latency_ms is not None
        ):
            bucket["metrics_completion"][
                "reasoner_queue_only_ms"
            ].append(
                (
                    reasoning_ts_ms
                    - scheduler_ts_ms
                )
                - llm_latency_ms
            )

    # --------------------------------------------------------
    # Source-aligned bucket
    # --------------------------------------------------------

    if in_source_window:
        bucket["accepted_source"] += 1

        bucket["risk_counts_source"][risk_level] += 1
        bucket["decision_counts_source"][decision] += 1
        bucket["intervention_counts_source"][intervention_type] += 1
        bucket["semantic_type_counts_source"][semantic_type] += 1
        bucket["reasoning_mode_counts_source"][reasoning_mode] += 1
        bucket["reasoning_route_counts_source"][reasoning_route] += 1
        bucket["scheduler_version_counts_source"][scheduler_version] += 1
        bucket["prompt_version_counts_source"][prompt_version] += 1
        bucket["model_counts_source"][model] += 1

        if user_id:
            bucket["unique_users_source"].add(user_id)
            bucket["user_counts_source"][user_id] += 1

        if event.get("error"):
            bucket["error_count_source"] += 1

        if semantic_engine_ts_ms is not None:
            bucket["metrics_source"][
                "total_from_semantic_ms"
            ].append(
                now_ms - semantic_engine_ts_ms
            )

        if llm_latency_ms is not None:
            bucket["metrics_source"][
                "llm_only_latency_ms"
            ].append(
                llm_latency_ms
            )

        if (
            semantic_engine_ts_ms is not None
            and reasoning_ts_ms is not None
        ):
            latency = (
                reasoning_ts_ms
                - semantic_engine_ts_ms
            )

            bucket["metrics_source"][
                "semantic_to_reasoning_ms"
            ].append(latency)

            bucket["metrics_source"][
                "reasoning_stage_total_ms"
            ].append(latency)

        if reasoning_ts_ms is not None:
            bucket["metrics_source"][
                "post_reasoning_transport_ms"
            ].append(
                now_ms - reasoning_ts_ms
            )

        if source_last_sent_at_ms is not None:
            bucket["metrics_source"][
                "pipeline_e2e_ms"
            ].append(
                now_ms - source_last_sent_at_ms
            )

        if (
            source_last_sent_at_ms is not None
            and source_ksql_emit_ts_ms is not None
        ):
            bucket["metrics_source"][
                "pipeline_cep_part_ms"
            ].append(
                source_ksql_emit_ts_ms
                - source_last_sent_at_ms
            )

        if (
            source_ksql_emit_ts_ms is not None
            and semantic_engine_ts_ms is not None
        ):
            bucket["metrics_source"][
                "pipeline_rule_part_ms"
            ].append(
                semantic_engine_ts_ms
                - source_ksql_emit_ts_ms
            )

        if (
            scheduler_ts_ms is not None
            and semantic_engine_ts_ms is not None
        ):
            bucket["metrics_source"][
                "scheduler_delay_ms"
            ].append(
                scheduler_ts_ms
                - semantic_engine_ts_ms
            )

        if (
            scheduler_ts_ms is not None
            and reasoning_ts_ms is not None
        ):
            bucket["metrics_source"][
                "scheduler_to_reasoning_ms"
            ].append(
                reasoning_ts_ms
                - scheduler_ts_ms
            )

        if (
            scheduler_ts_ms is not None
            and reasoning_ts_ms is not None
            and llm_latency_ms is not None
        ):
            bucket["metrics_source"][
                "reasoner_queue_only_ms"
            ].append(
                (
                    reasoning_ts_ms
                    - scheduler_ts_ms
                )
                - llm_latency_ms
            )


# ------------------------------------------------------------
# Output
# ------------------------------------------------------------

def print_bucket(title, bucket, elapsed):
    throughput_completion = (
        bucket["accepted_completion"]
        / max(elapsed, 1)
    )

    throughput_source = (
        bucket["accepted_source"]
        / max(elapsed, 1)
    )

    if bucket["accepted_completion"]:
        error_rate_completion = (
            bucket["error_count_completion"]
            / bucket["accepted_completion"]
        )
    else:
        error_rate_completion = 0.0

    if bucket["accepted_source"]:
        error_rate_source = (
            bucket["error_count_source"]
            / bucket["accepted_source"]
        )
    else:
        error_rate_source = 0.0

    # --------------------------------------------------------
    # Completion aligned
    # --------------------------------------------------------

    log(
        f"\n=== {title}: "
        "Completion-based "
        "(reasoning_ts_ms in window) ==="
    )

    log(
        f"accepted_completion="
        f"{bucket['accepted_completion']}"
    )

    log(
        f"throughput_completion="
        f"{throughput_completion:.3f} "
        "reasoning_events/sec"
    )

    log(
        f"unique_users_completion="
        f"{len(bucket['unique_users_completion'])}"
    )

    log(
        f"error_count_completion="
        f"{bucket['error_count_completion']}"
    )

    log(
        f"error_rate_completion="
        f"{error_rate_completion:.3f}"
    )

    mc = bucket["metrics_completion"]

    report(
        "completion_total_from_semantic"
        "(now-semantic_engine)",
        mc["total_from_semantic_ms"],
    )

    report(
        "completion_llm_only_latency_ms",
        mc["llm_only_latency_ms"],
    )

    report(
        "completion_semantic_to_reasoning"
        "(reasoning-semantic_engine)",
        mc["semantic_to_reasoning_ms"],
    )

    report(
        "completion_post_reasoning_transport"
        "(now-reasoning)",
        mc["post_reasoning_transport_ms"],
    )

    report(
        "completion_pipeline_e2e"
        "(now-source_last_sent_at)",
        mc["pipeline_e2e_ms"],
    )

    report(
        "completion_pipeline_cep_part"
        "(ksql_emit-source_last_sent_at)",
        mc["pipeline_cep_part_ms"],
    )

    report(
        "completion_pipeline_rule_part"
        "(semantic_engine-source_ksql_emit)",
        mc["pipeline_rule_part_ms"],
    )

    report(
        "completion_pipeline_reasoning_stage_total"
        "(reasoning-semantic_engine)",
        mc["reasoning_stage_total_ms"],
    )

    report(
        "completion_scheduler_delay"
        "(scheduler-semantic_engine)",
        mc["scheduler_delay_ms"],
    )

    report(
        "completion_scheduler_to_reasoning"
        "(reasoning-scheduler)",
        mc["scheduler_to_reasoning_ms"],
    )

    report(
        "completion_reasoner_queue_only"
        "((reasoning-scheduler)-llm_latency)",
        mc["reasoner_queue_only_ms"],
    )

    log(
        "completion_risk_counts:",
        dict(
            bucket[
                "risk_counts_completion"
            ].most_common()
        ),
    )

    log(
        "completion_decision_counts:",
        dict(
            bucket[
                "decision_counts_completion"
            ].most_common()
        ),
    )

    log(
        "completion_intervention_type_counts:",
        dict(
            bucket[
                "intervention_counts_completion"
            ].most_common()
        ),
    )

    log(
        "completion_semantic_type_counts:",
        dict(
            bucket[
                "semantic_type_counts_completion"
            ].most_common()
        ),
    )

    log(
        "completion_reasoning_mode_counts:",
        dict(
            bucket[
                "reasoning_mode_counts_completion"
            ].most_common()
        ),
    )

    log(
        "completion_reasoning_route_counts:",
        dict(
            bucket[
                "reasoning_route_counts_completion"
            ].most_common()
        ),
    )

    log(
        "completion_model_counts:",
        dict(
            bucket[
                "model_counts_completion"
            ].most_common()
        ),
    )

    log(
        "completion_scheduler_version_counts:",
        dict(
            bucket[
                "scheduler_version_counts_completion"
            ].most_common()
        ),
    )

    log(
        "completion_prompt_version_counts:",
        dict(
            bucket[
                "prompt_version_counts_completion"
            ].most_common()
        ),
    )

    log(
        f"completion_top_{TOP_N_USERS}_users:",
        dict(
            bucket[
                "user_counts_completion"
            ].most_common(TOP_N_USERS)
        ),
    )

    # --------------------------------------------------------
    # Source aligned
    # --------------------------------------------------------

    log(
        f"\n=== {title}: "
        "Source-aligned "
        "(semantic_engine_ts_ms in window) ==="
    )

    log(
        f"accepted_source_aligned="
        f"{bucket['accepted_source']}"
    )

    log(
        f"throughput_source_aligned="
        f"{throughput_source:.3f} "
        "reasoning_events/sec"
    )

    log(
        f"unique_users_source_aligned="
        f"{len(bucket['unique_users_source'])}"
    )

    log(
        f"error_count_source_aligned="
        f"{bucket['error_count_source']}"
    )

    log(
        f"error_rate_source_aligned="
        f"{error_rate_source:.3f}"
    )

    ms = bucket["metrics_source"]

    report(
        "source_total_from_semantic"
        "(now-semantic_engine)",
        ms["total_from_semantic_ms"],
    )

    report(
        "source_llm_only_latency_ms",
        ms["llm_only_latency_ms"],
    )

    report(
        "source_semantic_to_reasoning"
        "(reasoning-semantic_engine)",
        ms["semantic_to_reasoning_ms"],
    )

    report(
        "source_post_reasoning_transport"
        "(now-reasoning)",
        ms["post_reasoning_transport_ms"],
    )

    report(
        "source_pipeline_e2e"
        "(now-source_last_sent_at)",
        ms["pipeline_e2e_ms"],
    )

    report(
        "source_pipeline_cep_part"
        "(ksql_emit-source_last_sent_at)",
        ms["pipeline_cep_part_ms"],
    )

    report(
        "source_pipeline_rule_part"
        "(semantic_engine-source_ksql_emit)",
        ms["pipeline_rule_part_ms"],
    )

    report(
        "source_pipeline_reasoning_stage_total"
        "(reasoning-semantic_engine)",
        ms["reasoning_stage_total_ms"],
    )

    report(
        "source_scheduler_delay"
        "(scheduler-semantic_engine)",
        ms["scheduler_delay_ms"],
    )

    report(
        "source_scheduler_to_reasoning"
        "(reasoning-scheduler)",
        ms["scheduler_to_reasoning_ms"],
    )

    report(
        "source_reasoner_queue_only"
        "((reasoning-scheduler)-llm_latency)",
        ms["reasoner_queue_only_ms"],
    )

    log(
        "source_risk_counts:",
        dict(
            bucket[
                "risk_counts_source"
            ].most_common()
        ),
    )

    log(
        "source_decision_counts:",
        dict(
            bucket[
                "decision_counts_source"
            ].most_common()
        ),
    )

    log(
        "source_intervention_type_counts:",
        dict(
            bucket[
                "intervention_counts_source"
            ].most_common()
        ),
    )

    log(
        "source_semantic_type_counts:",
        dict(
            bucket[
                "semantic_type_counts_source"
            ].most_common()
        ),
    )

    log(
        "source_reasoning_mode_counts:",
        dict(
            bucket[
                "reasoning_mode_counts_source"
            ].most_common()
        ),
    )

    log(
        "source_reasoning_route_counts:",
        dict(
            bucket[
                "reasoning_route_counts_source"
            ].most_common()
        ),
    )

    log(
        "source_model_counts:",
        dict(
            bucket[
                "model_counts_source"
            ].most_common()
        ),
    )

    log(
        "source_scheduler_version_counts:",
        dict(
            bucket[
                "scheduler_version_counts_source"
            ].most_common()
        ),
    )

    log(
        "source_prompt_version_counts:",
        dict(
            bucket[
                "prompt_version_counts_source"
            ].most_common()
        ),
    )

    log(
        f"source_top_{TOP_N_USERS}_users:",
        dict(
            bucket[
                "user_counts_source"
            ].most_common(TOP_N_USERS)
        ),
    )


# ------------------------------------------------------------
# Mode classification
# ------------------------------------------------------------

def classify_event(event):
    mode = str(
        event.get("reasoning_mode", "")
        or ""
    ).lower()

    route = str(
        event.get("reasoning_route", "")
        or ""
    ).lower()

    if (
        mode == "rule_based"
        or route == "rule_based"
    ):
        return "rule_based"

    # Includes:
    # llm
    # llm_fallback
    if (
        mode.startswith("llm")
        or route == "llm"
    ):
        return "llm"

    # Includes:
    # fast_llm
    # fast_llm_fallback
    if (
        mode.startswith("fast_llm")
        or route == "fast_llm"
    ):
        return "fast_llm"

    if (
        mode == "bypass_rule"
        or route == "bypass_rule"
    ):
        return "bypass_rule"

    return "other"


# ------------------------------------------------------------
# Main
# ------------------------------------------------------------

def main():
    group_id = (
        f"llm-observer-"
        f"{int(time.time() * 1000)}"
    )

    consumer = build_consumer_with_retry(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=group_id,

        enable_auto_commit=True,
        auto_offset_reset="latest",

        value_deserializer=lambda v: (
            json.loads(v.decode("utf-8"))
            if v
            else {}
        ),

        key_deserializer=lambda k: (
            k.decode("utf-8")
            if k
            else None
        ),

        consumer_timeout_ms=1000,
    )

    log_session_start(__file__)

    log(
        f"LLM observer started. "
        f"topic={TOPIC}"
    )

    log(
        f"Kafka={BOOTSTRAP_SERVERS}, "
        f"group_id={group_id}"
    )

    log(
        f"Warmup={WARMUP_SEC}s, "
        f"Measure={MEASURE_SEC}s, "
        f"RUN_ID={TARGET_RUN_ID or 'ALL'}"
    )

    start = time.time()

    measure_start = (
        start + WARMUP_SEC
    )

    measure_end = (
        measure_start + MEASURE_SEC
    )

    measure_start_ms = int(
        measure_start * 1000
    )

    measure_end_ms = int(
        measure_end * 1000
    )

    # --------------------------------------------------------
    # Buckets
    # --------------------------------------------------------

    all_bucket = init_bucket()

    rule_bucket = init_bucket()

    llm_bucket = init_bucket()

    fast_bucket = init_bucket()

    bypass_bucket = init_bucket()

    # --------------------------------------------------------
    # Diagnostics
    # --------------------------------------------------------

    polled = 0

    matched_run = 0

    valid_core_timestamps = 0

    missing_core_timestamps = 0

    outside_both_windows = 0

    classification_counts = Counter()

    missing_field_counts = Counter()

    # --------------------------------------------------------
    # Consume
    # --------------------------------------------------------

    try:
        while time.time() < measure_end:

            records = consumer.poll(
                timeout_ms=500,
                max_records=500,
            )

            if not records:
                continue

            now_ms = int(
                time.time() * 1000
            )

            for _tp, messages in records.items():

                for msg in messages:

                    polled += 1

                    event = (
                        msg.value
                        if isinstance(
                            msg.value,
                            dict
                        )
                        else {}
                    )

                    # ----------------------------------------
                    # Run filter
                    # ----------------------------------------

                    if (
                        TARGET_RUN_ID
                        and str(
                            event.get("run_id")
                        )
                        != TARGET_RUN_ID
                    ):
                        continue

                    matched_run += 1

                    # ----------------------------------------
                    # Core timestamps
                    # ----------------------------------------

                    semantic_engine_ts_ms = safe_int(
                        event.get(
                            "semantic_engine_ts_ms"
                        )
                    )

                    reasoning_ts_ms = safe_int(
                        event.get(
                            "reasoning_ts_ms"
                        )
                    )

                    # llm_latency is NOT a mandatory
                    # acceptance field anymore.
                    #
                    # Individual LLM-latency metrics simply
                    # ignore events where it is missing.

                    if semantic_engine_ts_ms is None:
                        missing_field_counts[
                            "semantic_engine_ts_ms"
                        ] += 1

                    if reasoning_ts_ms is None:
                        missing_field_counts[
                            "reasoning_ts_ms"
                        ] += 1

                    if (
                        semantic_engine_ts_ms is None
                        or reasoning_ts_ms is None
                    ):
                        missing_core_timestamps += 1
                        continue

                    valid_core_timestamps += 1

                    # ----------------------------------------
                    # Window alignment
                    # ----------------------------------------

                    in_completion_window = (
                        measure_start_ms
                        <= reasoning_ts_ms
                        <= measure_end_ms
                    )

                    in_source_window = (
                        measure_start_ms
                        <= semantic_engine_ts_ms
                        <= measure_end_ms
                    )

                    if (
                        not in_completion_window
                        and not in_source_window
                    ):
                        outside_both_windows += 1
                        continue

                    # ----------------------------------------
                    # ALL reasoning events
                    # ----------------------------------------

                    update_bucket(
                        all_bucket,
                        event,
                        now_ms,
                        in_completion_window,
                        in_source_window,
                    )

                    # ----------------------------------------
                    # Route / mode buckets
                    # ----------------------------------------

                    event_class = classify_event(
                        event
                    )

                    classification_counts[
                        event_class
                    ] += 1

                    if event_class == "rule_based":

                        update_bucket(
                            rule_bucket,
                            event,
                            now_ms,
                            in_completion_window,
                            in_source_window,
                        )

                    elif event_class == "llm":

                        update_bucket(
                            llm_bucket,
                            event,
                            now_ms,
                            in_completion_window,
                            in_source_window,
                        )

                    elif event_class == "fast_llm":

                        update_bucket(
                            fast_bucket,
                            event,
                            now_ms,
                            in_completion_window,
                            in_source_window,
                        )

                    elif event_class == "bypass_rule":

                        update_bucket(
                            bypass_bucket,
                            event,
                            now_ms,
                            in_completion_window,
                            in_source_window,
                        )

    except KeyboardInterrupt:
        pass

    finally:
        try:
            consumer.close()
        except Exception:
            pass

    # --------------------------------------------------------
    # Diagnostics
    # --------------------------------------------------------

    log()

    log(
        f"[{datetime.now().isoformat(timespec='seconds')}] "
        f"Done."
    )

    log(
        f"polled={polled}"
    )

    log(
        f"matched_run={matched_run}"
    )

    log(
        f"valid_core_timestamps="
        f"{valid_core_timestamps}"
    )

    log(
        f"missing_core_timestamps="
        f"{missing_core_timestamps}"
    )

    log(
        f"outside_both_windows="
        f"{outside_both_windows}"
    )

    log(
        "missing_field_counts:",
        dict(missing_field_counts),
    )

    log(
        "classification_counts:",
        dict(classification_counts),
    )

    # --------------------------------------------------------
    # Results
    # --------------------------------------------------------

    print_bucket(
        "ALL reasoning events",
        all_bucket,
        MEASURE_SEC,
    )

    print_bucket(
        "RULE-BASED only",
        rule_bucket,
        MEASURE_SEC,
    )

    print_bucket(
        "HIGH-CAPACITY LLM only",
        llm_bucket,
        MEASURE_SEC,
    )

    print_bucket(
        "FAST LLM only",
        fast_bucket,
        MEASURE_SEC,
    )

    print_bucket(
        "BYPASS RULE only",
        bypass_bucket,
        MEASURE_SEC,
    )

    # --------------------------------------------------------
    # Final validation
    # --------------------------------------------------------

    if (
        all_bucket["accepted_completion"] == 0
        and all_bucket["accepted_source"] == 0
    ):
        log()
        log(
            "WARNING: No reasoning events matched "
            "either measurement alignment."
        )

        log(
            "Check diagnostics above: "
            "polled, matched_run, "
            "missing_core_timestamps, and "
            "outside_both_windows."
        )


if __name__ == "__main__":
    main()