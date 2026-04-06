#!/usr/bin/env python3
import argparse
import csv
import json
import os
import sys
import time
from collections import Counter, defaultdict
from typing import Dict, Iterable, List, Optional, Tuple

try:
    from confluent_kafka import Consumer
except Exception:
    Consumer = None


DEFAULT_BOOTSTRAP = os.getenv("BOOTSTRAP_SERVERS", "localhost:9092")
DEFAULT_TOPIC = os.getenv("TOPIC", "semantic_events")
DEFAULT_RUN_ID = os.getenv("RUN_ID", "")
DEFAULT_WARMUP_SEC = int(os.getenv("WARMUP_SEC", "5"))
DEFAULT_MEASURE_SEC = int(os.getenv("MEASURE_SEC", "60"))


def load_expectations(path: str) -> Dict[str, dict]:
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    return data.get("scenarios", {})


def safe_int(v):
    if v is None:
        return None
    try:
        return int(v)
    except Exception:
        return None


def read_jsonl(path: str, run_id: str = "") -> Iterable[dict]:
    with open(path, "r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                event = json.loads(line)
            except Exception as e:
                raise ValueError(f"Invalid JSON on line {line_no} of {path}: {e}")

            if run_id and event.get("run_id") != run_id:
                continue

            yield event

def consume_kafka(bootstrap: str, topic: str, run_id: str, warmup_sec: int, measure_sec: int) -> Iterable[dict]:
    if Consumer is None:
        raise RuntimeError("confluent_kafka is not installed in this environment.")

    consumer = Consumer({
        "bootstrap.servers": bootstrap,
        "group.id": f"semantic-correctness-eval-{int(time.time() * 1000)}",
        "auto.offset.reset": "latest",
        "enable.auto.commit": True,
    })
    consumer.subscribe([topic])

    start = time.time()
    measure_start = start + warmup_sec
    measure_end = measure_start + measure_sec
    measure_start_ms = int(measure_start * 1000)
    measure_end_ms = int(measure_end * 1000)

    try:
        while time.time() < measure_end:
            msg = consumer.poll(1.0)
            if msg is None or msg.error():
                continue
            try:
                event = json.loads(msg.value().decode("utf-8"))
            except Exception:
                continue

            if run_id and event.get("run_id") != run_id:
                continue

            source_ts = safe_int(event.get("source_ksql_emit_ts_ms"))
            if source_ts is None:
                continue
            if not (measure_start_ms <= source_ts <= measure_end_ms):
                continue
            yield event
    finally:
        consumer.close()


def collect(events: Iterable[dict], expectations: Dict[str, dict]) -> Tuple[dict, List[str]]:
    matrix = defaultdict(Counter)
    per_scenario_total = Counter()
    per_scenario_users = defaultdict(set)
    per_scenario_runs = defaultdict(set)
    per_scenario_examples = defaultdict(list)
    all_semantic_labels = set(["NONE"])
    unknown_scenarios = Counter()

    for event in events:
        scenario = event.get("source_latest_scenario") or event.get("scenario") or "UNKNOWN"
        semantic = event.get("semantic_type") or "UNKNOWN"
        user_id = event.get("user_id")
        run_id = event.get("run_id")

        matrix[scenario][semantic] += 1
        per_scenario_total[scenario] += 1
        all_semantic_labels.add(semantic)

        if user_id:
            per_scenario_users[scenario].add(user_id)
        if run_id:
            per_scenario_runs[scenario].add(run_id)

        if len(per_scenario_examples[scenario]) < 5:
            per_scenario_examples[scenario].append({
                "semantic_type": semantic,
                "severity": event.get("severity"),
                "rule_id": event.get("rule_id"),
                "user_id": user_id,
                "source_window_start_ms": event.get("source_window_start_ms"),
                "source_window_end_ms": event.get("source_window_end_ms"),
            })

        if scenario not in expectations:
            unknown_scenarios[scenario] += 1

    for scenario in expectations:
        matrix.setdefault(scenario, Counter())
        per_scenario_total.setdefault(scenario, 0)
        per_scenario_users.setdefault(scenario, set())
        per_scenario_runs.setdefault(scenario, set())
        per_scenario_examples.setdefault(scenario, [])

    labels_sorted = sorted(all_semantic_labels)
    summary = []
    for scenario in sorted(matrix.keys()):
        spec = expectations.get(scenario, {})
        expected = spec.get("expected_semantic", [])
        expected_primary = spec.get("expected_primary_semantic", "UNSPECIFIED")
        observed_counter = matrix[scenario]
        observed_total = sum(observed_counter.values())
        dominant_observed = "NONE"
        dominant_count = 0
        if observed_counter:
            dominant_observed, dominant_count = observed_counter.most_common(1)[0]

        false_positive_count = 0
        if expected_primary == "NONE":
            false_positive_count = observed_total

        matched = None
        if expected_primary == "REVIEW_NEEDED":
            matched = "REVIEW_NEEDED"
        elif expected_primary == "NONE":
            matched = "YES" if observed_total == 0 else "NO"
        else:
            matched = "YES" if dominant_observed in expected else "NO"

        purity = (dominant_count / observed_total) if observed_total else None

        summary.append({
            "scenario": scenario,
            "expected_primary_semantic": expected_primary,
            "expected_semantic": "; ".join(expected) if expected else "NONE",
            "observed_total": observed_total,
            "dominant_observed_semantic": dominant_observed,
            "dominant_observed_count": dominant_count,
            "dominant_observed_ratio": round(purity, 4) if purity is not None else "",
            "match": matched,
            "false_positive_count": false_positive_count,
            "unique_users": len(per_scenario_users[scenario]),
            "run_ids": "; ".join(sorted(per_scenario_runs[scenario])),
            "notes": spec.get("notes", ""),
        })

    return {
        "matrix": matrix,
        "summary": summary,
        "labels_sorted": labels_sorted,
        "unknown_scenarios": unknown_scenarios,
        "examples": per_scenario_examples,
    }, labels_sorted


def write_confusion_csv(path: str, matrix: dict, labels_sorted: List[str], scenario_order: List[str]) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["expected_scenario", *labels_sorted])
        for scenario in scenario_order:
            row = [scenario]
            counts = matrix.get(scenario, Counter())
            row.extend(counts.get(label, 0) for label in labels_sorted)
            writer.writerow(row)


def write_summary_csv(path: str, summary: List[dict]) -> None:
    if not summary:
        return
    fieldnames = list(summary[0].keys())
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(summary)


def print_report(results: dict, labels_sorted: List[str]) -> None:
    summary = results["summary"]
    matrix = results["matrix"]
    unknown_scenarios = results["unknown_scenarios"]

    print("\nScenario-level semantic correctness summary\n")
    for row in summary:
        scenario = row["scenario"]
        observed_dist = dict(matrix.get(scenario, Counter()).most_common())
        print(f"Scenario: {scenario}")
        print(f"  expected_primary_semantic: {row['expected_primary_semantic']}")
        print(f"  expected_semantic: {row['expected_semantic']}")
        print(f"  observed_total: {row['observed_total']}")
        print(f"  dominant_observed_semantic: {row['dominant_observed_semantic']}")
        print(f"  dominant_observed_count: {row['dominant_observed_count']}")
        print(f"  dominant_observed_ratio: {row['dominant_observed_ratio']}")
        print(f"  match: {row['match']}")
        if row["false_positive_count"]:
            print(f"  false_positive_count: {row['false_positive_count']}")
        print(f"  observed_distribution: {observed_dist}")
        print()

    print("Confusion matrix labels:", labels_sorted)
    if unknown_scenarios:
        print("Unknown scenarios found in data:", dict(unknown_scenarios))


def main():
    parser = argparse.ArgumentParser(description="Evaluate scenario-to-semantic functional correctness.")
    parser.add_argument("--expectations", required=True, help="Path to scenario_expectations.json")
    parser.add_argument("--input-jsonl", help="Read semantic_events from a JSONL file instead of Kafka")
    parser.add_argument("--bootstrap-servers", default=DEFAULT_BOOTSTRAP)
    parser.add_argument("--topic", default=DEFAULT_TOPIC)
    parser.add_argument("--run-id", default=DEFAULT_RUN_ID)
    parser.add_argument("--warmup-sec", type=int, default=DEFAULT_WARMUP_SEC)
    parser.add_argument("--measure-sec", type=int, default=DEFAULT_MEASURE_SEC)
    parser.add_argument("--output-dir", default=".")
    args = parser.parse_args()

    expectations = load_expectations(args.expectations)

    if args.input_jsonl:
        events = list(read_jsonl(args.input_jsonl, run_id=args.run_id))
    else:
        events = list(consume_kafka(
            bootstrap=args.bootstrap_servers,
            topic=args.topic,
            run_id=args.run_id,
            warmup_sec=args.warmup_sec,
            measure_sec=args.measure_sec,
        ))

    results, labels_sorted = collect(events, expectations)
    os.makedirs(args.output_dir, exist_ok=True)

    scenario_order = sorted(results["matrix"].keys())
    confusion_csv = os.path.join(args.output_dir, "semantic_confusion_matrix.csv")
    summary_csv = os.path.join(args.output_dir, "semantic_correctness_summary.csv")

    write_confusion_csv(confusion_csv, results["matrix"], labels_sorted, scenario_order)
    write_summary_csv(summary_csv, results["summary"])
    print_report(results, labels_sorted)

    print(f"Wrote: {confusion_csv}")
    print(f"Wrote: {summary_csv}")


if __name__ == "__main__":
    main()
