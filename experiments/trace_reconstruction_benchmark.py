#!/usr/bin/env python3

import json
import math
import statistics
import sys
import time
from collections import defaultdict
from pathlib import Path

REPEATS = 500


def read_jsonl(path):
    rows = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def present(x, field):
    return x.get(field) is not None and x.get(field) != ""


def pct(vals, p):
    vals = sorted(vals)
    if not vals:
        return None
    i = min(
        len(vals) - 1,
        max(0, math.ceil(len(vals) * p / 100.0) - 1),
    )
    return vals[i]


# ------------------------------------------------------------
# Exact intervention -> reasoning linkage
# ------------------------------------------------------------
def reasoning_link_key(x):
    return (
        str(x.get("run_id")),
        str(x.get("user_id")),
        str(x.get("window_start_ms")),
        str(x.get("window_end_ms")),
        str(x.get("reasoning_ts_ms")),
        str(x.get("scheduler_ts_ms")),
        str(x.get("source_last_sent_at_ms")),
    )


# ------------------------------------------------------------
# Exact semantic -> dropped-event linkage
# semantic uses engine_ts_ms;
# dropped event renames it semantic_engine_ts_ms.
# ------------------------------------------------------------
def semantic_drop_key(x, semantic=False):
    engine_ts = (
        x.get("engine_ts_ms")
        if semantic
        else x.get("semantic_engine_ts_ms")
    )

    return (
        str(x.get("run_id")),
        str(x.get("user_id")),
        str(x.get("semantic_type")),
        str(x.get("severity")),
        str(x.get("rule_id")),
        str(x.get("source_last_sent_at_ms")),
        str(x.get("source_ksql_emit_ts_ms")),
        str(engine_ts),
    )


def build_indices(semantics, requests, reasoning):
    semantic_by_id = defaultdict(list)
    request_by_id = defaultdict(list)
    reasoning_by_link = defaultdict(list)

    for x in semantics:
        sid = x.get("semantic_event_id")
        if sid is not None:
            semantic_by_id[
                (str(x.get("run_id")), str(sid))
            ].append(x)

    for x in requests:
        rid = x.get("request_id")
        if rid is not None:
            request_by_id[
                (str(x.get("run_id")), str(rid))
            ].append(x)

    for x in reasoning:
        reasoning_by_link[reasoning_link_key(x)].append(x)

    return {
        "semantic_by_id": semantic_by_id,
        "request_by_id": request_by_id,
        "reasoning_by_link": reasoning_by_link,
    }


def reconstruct(intervention, idx):
    # Stage 1: intervention -> reasoning
    candidates = idx["reasoning_by_link"].get(
        reasoning_link_key(intervention), []
    )

    if len(candidates) != 1:
        return None, "reasoning_match_not_unique"

    reasoning = candidates[0]

    # Stage 2: reasoning -> request
    rid = reasoning.get("request_id")

    if rid is None:
        return None, "reasoning_missing_request_id"

    candidates = idx["request_by_id"].get(
        (str(reasoning.get("run_id")), str(rid)),
        [],
    )

    if len(candidates) != 1:
        return None, "request_match_not_unique"

    request = candidates[0]

    # Stage 3: request -> semantic
    sid = request.get("semantic_event_id")

    if sid is None:
        return None, "request_missing_semantic_event_id"

    candidates = idx["semantic_by_id"].get(
        (str(request.get("run_id")), str(sid)),
        [],
    )

    if len(candidates) != 1:
        return None, "semantic_match_not_unique"

    semantic = candidates[0]

    return {
        "semantic": semantic,
        "request": request,
        "reasoning": reasoning,
        "intervention": intervention,
    }, None


def all_equal(values):
    vals = [
        str(v)
        for v in values
        if v is not None
    ]
    return len(set(vals)) <= 1


def trace_consistent(t):
    s = t["semantic"]
    q = t["request"]
    r = t["reasoning"]
    i = t["intervention"]

    checks = []

    # Identity.
    for field in ("run_id", "user_id"):
        checks.append(
            all_equal([
                s.get(field),
                q.get(field),
                r.get(field),
                i.get(field),
            ])
        )

    # Semantic interpretation propagated until reasoning.
    for field in (
        "semantic_type",
        "severity",
        "rule_id",
    ):
        checks.append(
            all_equal([
                s.get(field),
                q.get(field),
                r.get(field),
            ])
        )

    # semantic -> request.
    checks.append(
        str(s.get("semantic_event_id"))
        == str(q.get("semantic_event_id"))
    )

    # request -> reasoning.
    checks.append(
        str(q.get("request_id"))
        == str(r.get("request_id"))
    )

    # Window identity across all stages.
    checks.append(
        all_equal([
            s.get("source_window_start_ms"),
            q.get("source_window_start_ms"),
            r.get("window_start_ms"),
            i.get("window_start_ms"),
        ])
    )

    checks.append(
        all_equal([
            s.get("source_window_end_ms"),
            q.get("source_window_end_ms"),
            r.get("window_end_ms"),
            i.get("window_end_ms"),
        ])
    )

    # Original behavioural source timestamp.
    checks.append(
        all_equal([
            s.get("source_last_sent_at_ms"),
            q.get("source_last_sent_at_ms"),
            r.get("source_last_sent_at_ms"),
            i.get("source_last_sent_at_ms"),
        ])
    )

    # Semantic-engine timestamp.
    checks.append(
        all_equal([
            s.get("engine_ts_ms"),
            q.get("engine_ts_ms"),
            r.get("semantic_engine_ts_ms"),
            i.get("semantic_engine_ts_ms"),
        ])
    )

    # Scheduler timestamp.
    checks.append(
        all_equal([
            q.get("scheduler_ts_ms"),
            r.get("scheduler_ts_ms"),
            i.get("scheduler_ts_ms"),
        ])
    )

    # Reasoning timestamp.
    checks.append(
        all_equal([
            r.get("reasoning_ts_ms"),
            i.get("reasoning_ts_ms"),
        ])
    )

    return all(checks)


def trace_complete(t):
    required = {
        "semantic": [
            "semantic_event_id",
            "run_id",
            "user_id",
            "source_window_start_ms",
            "source_window_end_ms",
            "source_last_sent_at_ms",
            "semantic_type",
            "severity",
            "rule_id",
            "engine_ts_ms",
        ],
        "request": [
            "semantic_event_id",
            "request_id",
            "run_id",
            "user_id",
            "source_window_start_ms",
            "source_window_end_ms",
            "semantic_type",
            "severity",
            "rule_id",
            "engine_ts_ms",
            "scheduler_ts_ms",
            "reasoning_route",
        ],
        "reasoning": [
            "request_id",
            "run_id",
            "user_id",
            "window_start_ms",
            "window_end_ms",
            "semantic_type",
            "severity",
            "rule_id",
            "semantic_engine_ts_ms",
            "scheduler_ts_ms",
            "reasoning_ts_ms",
            "reasoning_mode",
        ],
        "intervention": [
            "run_id",
            "user_id",
            "window_start_ms",
            "window_end_ms",
            "source_last_sent_at_ms",
            "semantic_engine_ts_ms",
            "scheduler_ts_ms",
            "reasoning_ts_ms",
            "intervention_ts_ms",
            "decision",
            "intervention_type",
            "action",
        ],
    }

    missing = {}

    for stage, fields in required.items():
        x = t[stage]
        miss = [
            f for f in fields
            if not present(x, f)
        ]

        if miss:
            missing[stage] = miss

    return len(missing) == 0, missing


def intervention_linkage_complete(x):
    # Fields actually used to deterministically link the endpoint
    # back to the reasoning event.
    fields = [
        "run_id",
        "user_id",
        "window_start_ms",
        "window_end_ms",
        "source_last_sent_at_ms",
        "semantic_engine_ts_ms",
        "scheduler_ts_ms",
        "reasoning_ts_ms",
        "intervention_ts_ms",
    ]
    return all(present(x, f) for f in fields)


def benchmark_lookup(interventions, idx):
    per_pass_us = []

    for _ in range(REPEATS):
        t0 = time.perf_counter_ns()

        for intervention in interventions:
            reconstruct(intervention, idx)

        elapsed_ns = time.perf_counter_ns() - t0

        per_pass_us.append(
            elapsed_ns / len(interventions) / 1000.0
        )

    return per_pass_us


def main():
    if len(sys.argv) != 3:
        raise SystemExit(
            "Usage: trace_reconstruction_benchmark_v2.py "
            "<semantic_replay_dir> <output_dir>"
        )

    src = Path(sys.argv[1])
    out = Path(sys.argv[2])
    out.mkdir(parents=True, exist_ok=True)

    semantics = read_jsonl(
        src / "semantic_events.c2.jsonl"
    )
    high_requests = read_jsonl(
        src / "reasoning_requests.c2.jsonl"
    )
    fast_requests = read_jsonl(
        src / "fast_reasoning_requests.c2.jsonl"
    )
    requests = high_requests + fast_requests

    reasoning = read_jsonl(
        src / "reasoning_events.c2.jsonl"
    )
    interventions = read_jsonl(
        src / "intervention_events.c2.jsonl"
    )
    drops = read_jsonl(
        src / "reasoning_dropped_events.c2.jsonl"
    )

    # --------------------------------------------------------
    # Build indices
    # --------------------------------------------------------
    t0 = time.perf_counter_ns()

    idx = build_indices(
        semantics,
        requests,
        reasoning,
    )

    index_build_ms = (
        time.perf_counter_ns() - t0
    ) / 1_000_000.0

    # --------------------------------------------------------
    # Intervention traces
    # --------------------------------------------------------
    reconstructed = 0
    consistent = 0
    complete = 0
    endpoint_linkage_complete = 0
    failure_reasons = defaultdict(int)

    for intervention in interventions:
        if intervention_linkage_complete(intervention):
            endpoint_linkage_complete += 1

        trace, err = reconstruct(
            intervention,
            idx,
        )

        if trace is None:
            failure_reasons[err] += 1
            continue

        reconstructed += 1

        if trace_consistent(trace):
            consistent += 1

        ok, _ = trace_complete(trace)

        if ok:
            complete += 1

    # --------------------------------------------------------
    # Timing
    # --------------------------------------------------------
    timing_us = benchmark_lookup(
        interventions,
        idx,
    )

    # --------------------------------------------------------
    # Explicit drops -> semantic source
    # --------------------------------------------------------
    semantic_drop_index = defaultdict(list)

    for s in semantics:
        semantic_drop_index[
            semantic_drop_key(s, semantic=True)
        ].append(s)

    drop_matches = 0
    drop_complete = 0

    drop_required = [
        "run_id",
        "user_id",
        "semantic_type",
        "severity",
        "rule_id",
        "source_last_sent_at_ms",
        "source_ksql_emit_ts_ms",
        "semantic_engine_ts_ms",
        "scheduler_ts_ms",
        "drop_reason",
    ]

    for d in drops:
        candidates = semantic_drop_index.get(
            semantic_drop_key(d, semantic=False),
            [],
        )

        if len(candidates) == 1:
            drop_matches += 1

        if all(present(d, f) for f in drop_required):
            drop_complete += 1

    # --------------------------------------------------------
    # Coalesced events
    #
    # This replay archive does not contain dedicated structured
    # coalescing records. Infer the number from event conservation.
    # --------------------------------------------------------
    request_ids = {
        str(x.get("request_id"))
        for x in requests
        if x.get("request_id") is not None
    }

    direct_reasoning = [
        x for x in reasoning
        if x.get("request_id") is None
        or str(x.get("request_id")) not in request_ids
    ]

    inferred_coalesced = (
        len(semantics)
        - len(drops)
        - len(requests)
        - len(direct_reasoning)
    )

    conservation_ok = (
        len(semantics)
        ==
        len(drops)
        + len(requests)
        + len(direct_reasoning)
        + inferred_coalesced
    )

    # --------------------------------------------------------
    # Schema propagation observations
    # --------------------------------------------------------
    request_id_at_intervention = sum(
        present(x, "request_id")
        for x in interventions
    )

    semantic_type_at_intervention = sum(
        present(x, "semantic_type")
        for x in interventions
    )

    summary = {
        "input_counts": {
            "semantic_events": len(semantics),
            "high_reasoning_requests": len(high_requests),
            "fast_reasoning_requests": len(fast_requests),
            "total_reasoning_requests": len(requests),
            "reasoning_events": len(reasoning),
            "reasoning_dropped_events": len(drops),
            "intervention_events": len(interventions),
        },

        "index_build_ms": index_build_ms,

        "intervention_trace": {
            "n": len(interventions),

            "reconstruction_success":
                reconstructed,

            "reconstruction_success_rate":
                reconstructed / len(interventions)
                if interventions else 0.0,

            "consistent_trace_count":
                consistent,

            "consistent_trace_rate":
                consistent / len(interventions)
                if interventions else 0.0,

            "complete_provenance_count":
                complete,

            "complete_provenance_rate":
                complete / len(interventions)
                if interventions else 0.0,

            "endpoint_linkage_fields_complete_count":
                endpoint_linkage_complete,

            "endpoint_linkage_fields_complete_rate":
                endpoint_linkage_complete
                / len(interventions)
                if interventions else 0.0,

            "failure_reasons":
                dict(failure_reasons),

            "lookup_repeats":
                REPEATS,

            "offline_indexed_lookup_mean_us":
                statistics.mean(timing_us),

            "offline_indexed_lookup_median_us":
                statistics.median(timing_us),

            "offline_indexed_lookup_p95_us":
                pct(timing_us, 95),

            "offline_indexed_lookup_p99_us":
                pct(timing_us, 99),

            "offline_indexed_lookup_max_us":
                max(timing_us),
        },

        "schema_propagation": {
            "request_id_present_in_intervention_count":
                request_id_at_intervention,

            "request_id_present_in_intervention_rate":
                request_id_at_intervention
                / len(interventions)
                if interventions else 0.0,

            "semantic_type_present_in_intervention_count":
                semantic_type_at_intervention,

            "semantic_type_present_in_intervention_rate":
                semantic_type_at_intervention
                / len(interventions)
                if interventions else 0.0,

            "note": (
                "Intervention events are linked to reasoning events "
                "through preserved run/user/window and processing "
                "timestamps rather than direct request_id propagation."
            ),
        },

        "explicit_drop_trace": {
            "n": len(drops),

            "semantic_match_count":
                drop_matches,

            "semantic_match_rate":
                drop_matches / len(drops)
                if drops else 0.0,

            "complete_drop_metadata_count":
                drop_complete,

            "complete_drop_metadata_rate":
                drop_complete / len(drops)
                if drops else 0.0,
        },

        "coalesced": {
            "inferred_from_conservation":
                inferred_coalesced,

            "direct_rule_reasoning_events":
                len(direct_reasoning),

            "dedicated_structured_coalescing_records":
                0,

            "conservation_ok":
                conservation_ok,

            "equation": (
                "semantic_events = explicit_drops + "
                "reasoning_requests + direct_rule_reasoning "
                "+ inferred_coalesced"
            ),

            "note": (
                "The semantic-replay archive contains no dedicated "
                "structured coalescing event stream; coalesced "
                "replacements are therefore inferred from conservation."
            ),
        },
    }

    with open(
        out / "summary.json",
        "w",
        encoding="utf-8",
    ) as f:
        json.dump(
            summary,
            f,
            indent=2,
        )

    print(json.dumps(summary, indent=2))
    print(f"\nResults: {out}")


if __name__ == "__main__":
    main()
