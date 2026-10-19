#!/usr/bin/env python3

import copy
import csv
import json
import math
import random
import statistics
import sys
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "rule_engine"))

from rules import evaluate_rules
from state_store import StateStore


CONFIG_PATH = ROOT / "rule_engine" / "config.json"

with CONFIG_PATH.open(encoding="utf-8") as f:
    BASE_CONFIG = json.load(f)

SEEDS = [101, 202, 303]
CASES_PER_CLASS_PER_SEED = 50
BOOTSTRAP_REPS = 2000

TARGET_LABELS = [
    "NONE",
    "RISING_STRESS_TRAJECTORY",
    "RISING_CRAVING_TRAJECTORY",
    "COMBINED_RELAPSE_RISK",
]

ALL_CONFUSION_LABELS = TARGET_LABELS + ["OTHER"]


def clamp(x, lo=0.0, hi=10.0):
    return max(lo, min(hi, float(x)))


def make_window(
    run_id,
    user_id,
    idx,
    craving,
    stress,
    motivation,
    efficacy,
    receptivity=1,
):
    start = idx * 5000

    return {
        "run_id": run_id,
        "user_id": user_id,
        "window_start_ms": start,
        "window_end_ms": start + 25000,
        "event_count": 3,

        "craving_avg": clamp(craving),
        "stress_avg": clamp(stress),
        "motivation_avg": clamp(motivation),
        "self_efficacy_avg": clamp(efficacy),

        "craving_max": clamp(craving),
        "stress_max": clamp(stress),
        "motivation_min": clamp(motivation),
        "self_efficacy_min": clamp(efficacy),

        "latest_receptivity": int(receptivity),
        "latest_strategy_use": 2,
        "latest_is_moving": 0,
        "latest_in_meeting": 0,
        "latest_location_type": "home",
        "ksql_latency_ms": 0,
    }


def build_case(label, seed, case_no):
    rng = random.Random(seed * 100000 + case_no * 97 + TARGET_LABELS.index(label))

    run_id = f"correctness_v2_{seed}"
    user_id = f"{label.lower()}_{case_no}"

    windows = []

    if label == "NONE":
        craving = rng.uniform(1.5, 2.8)
        stress = rng.uniform(1.5, 2.8)
        motivation = rng.uniform(7.2, 9.0)
        efficacy = rng.uniform(7.2, 9.0)

        for i in range(5):
            windows.append(
                make_window(
                    run_id,
                    user_id,
                    i,
                    craving + rng.uniform(-0.15, 0.15),
                    stress + rng.uniform(-0.15, 0.15),
                    motivation + rng.uniform(-0.15, 0.15),
                    efficacy + rng.uniform(-0.15, 0.15),
                )
            )

    elif label == "RISING_STRESS_TRAJECTORY":
        craving = rng.uniform(1.8, 3.0)
        motivation = rng.uniform(6.8, 8.5)
        efficacy = rng.uniform(6.8, 8.5)

        start = rng.uniform(1.8, 2.3)
        inc = rng.uniform(1.10, 1.25)

        stress_values = [
            start + i * inc
            for i in range(5)
        ]

        for i, stress in enumerate(stress_values):
            windows.append(
                make_window(
                    run_id,
                    user_id,
                    i,
                    craving + rng.uniform(-0.10, 0.10),
                    stress,
                    motivation + rng.uniform(-0.10, 0.10),
                    efficacy + rng.uniform(-0.10, 0.10),
                )
            )

    elif label == "RISING_CRAVING_TRAJECTORY":
        stress = rng.uniform(1.8, 3.0)
        motivation = rng.uniform(6.8, 8.5)
        efficacy = rng.uniform(6.8, 8.5)

        start = rng.uniform(1.8, 2.3)
        inc = rng.uniform(1.10, 1.25)

        craving_values = [
            start + i * inc
            for i in range(5)
        ]

        for i, craving in enumerate(craving_values):
            windows.append(
                make_window(
                    run_id,
                    user_id,
                    i,
                    craving,
                    stress + rng.uniform(-0.10, 0.10),
                    motivation + rng.uniform(-0.10, 0.10),
                    efficacy + rng.uniform(-0.10, 0.10),
                )
            )

    elif label == "COMBINED_RELAPSE_RISK":
        craving = rng.uniform(7.6, 9.3)
        stress = rng.uniform(7.6, 9.3)
        motivation = rng.uniform(1.5, 2.7)
        efficacy = rng.uniform(1.5, 2.7)

        # Earlier safe windows avoid defining the case only through
        # a pre-existing combined state.
        for i in range(3):
            windows.append(
                make_window(
                    run_id,
                    user_id,
                    i,
                    rng.uniform(2.0, 3.5),
                    rng.uniform(2.0, 3.5),
                    rng.uniform(6.0, 8.0),
                    rng.uniform(6.0, 8.0),
                )
            )

        for i in range(3, 5):
            windows.append(
                make_window(
                    run_id,
                    user_id,
                    i,
                    craving + rng.uniform(-0.20, 0.20),
                    stress + rng.uniform(-0.20, 0.20),
                    motivation + rng.uniform(-0.15, 0.15),
                    efficacy + rng.uniform(-0.15, 0.15),
                )
            )

    else:
        raise ValueError(label)

    return {
        "seed": seed,
        "case_no": case_no,
        "expected": label,
        "windows": windows,
    }


def build_clean_cases():
    cases = []

    for seed in SEEDS:
        for label in TARGET_LABELS:
            for i in range(CASES_PER_CLASS_PER_SEED):
                cases.append(build_case(label, seed, i))

    return cases


def perturb_noise(case, sd=0.35):
    x = copy.deepcopy(case)
    rng = random.Random(case["seed"] * 300000 + case["case_no"] * 31)

    for w in x["windows"]:
        for avg_field, extreme_field in [
            ("craving_avg", "craving_max"),
            ("stress_avg", "stress_max"),
            ("motivation_avg", "motivation_min"),
            ("self_efficacy_avg", "self_efficacy_min"),
        ]:
            value = clamp(w[avg_field] + rng.gauss(0.0, sd))
            w[avg_field] = value
            w[extreme_field] = value

    return x


def perturb_missing_window(case):
    x = copy.deepcopy(case)
    rng = random.Random(case["seed"] * 500000 + case["case_no"] * 43)

    # Keep the final window, but omit one earlier finalized observation.
    # This represents 20% window-level missingness in a five-window sequence.
    idx = rng.choice([0, 1, 2, 3])
    del x["windows"][idx]

    return x


def perturb_delay(case):
    x = copy.deepcopy(case)
    rng = random.Random(case["seed"] * 700000 + case["case_no"] * 53)

    # Swap one adjacent pair near the end to simulate delayed/out-of-order
    # delivery while retaining all measurements.
    pair_start = rng.choice([2, 3])

    if pair_start + 1 < len(x["windows"]):
        x["windows"][pair_start], x["windows"][pair_start + 1] = (
            x["windows"][pair_start + 1],
            x["windows"][pair_start],
        )

    return x


def scaled_config(factor):
    cfg = copy.deepcopy(BASE_CONFIG)

    for key in cfg["thresholds"]:
        cfg["thresholds"][key] = float(cfg["thresholds"][key]) * factor

    return cfg


def evaluate_case(case, config):
    state = StateStore(config)
    final_result = None

    for window in case["windows"]:
        final_result = evaluate_rules(window, config, state)

    observed = (
        final_result["semantic_type"]
        if final_result
        else "NONE"
    )

    candidates = []

    if final_result:
        candidates = [
            c["semantic_type"]
            for c in final_result.get("all_candidates", [])
        ]

    return observed, candidates


def normalize_observed(label):
    return label if label in TARGET_LABELS else "OTHER"


def metrics(rows):
    total = len(rows)

    correct = sum(
        r["expected"] == r["observed_norm"]
        for r in rows
    )

    accuracy = correct / total if total else 0.0

    per_class = {}

    for label in TARGET_LABELS:
        tp = sum(
            r["expected"] == label
            and r["observed_norm"] == label
            for r in rows
        )

        fp = sum(
            r["expected"] != label
            and r["observed_norm"] == label
            for r in rows
        )

        fn = sum(
            r["expected"] == label
            and r["observed_norm"] != label
            for r in rows
        )

        precision = tp / (tp + fp) if tp + fp else 0.0
        recall = tp / (tp + fn) if tp + fn else 0.0
        f1 = (
            2 * precision * recall / (precision + recall)
            if precision + recall
            else 0.0
        )

        per_class[label] = {
            "tp": tp,
            "fp": fp,
            "fn": fn,
            "precision": precision,
            "recall": recall,
            "f1": f1,
        }

    macro_precision = statistics.mean(
        x["precision"] for x in per_class.values()
    )

    macro_recall = statistics.mean(
        x["recall"] for x in per_class.values()
    )

    macro_f1 = statistics.mean(
        x["f1"] for x in per_class.values()
    )

    baseline_rows = [
        r for r in rows
        if r["expected"] == "NONE"
    ]

    baseline_fp_rate = (
        sum(r["observed_norm"] != "NONE" for r in baseline_rows)
        / len(baseline_rows)
        if baseline_rows
        else 0.0
    )

    return {
        "n": total,
        "accuracy": accuracy,
        "balanced_accuracy": macro_recall,
        "macro_precision": macro_precision,
        "macro_recall": macro_recall,
        "macro_f1": macro_f1,
        "baseline_false_positive_rate": baseline_fp_rate,
        "per_class": per_class,
    }


def confusion_matrix(rows):
    matrix = {
        expected: {
            observed: 0
            for observed in ALL_CONFUSION_LABELS
        }
        for expected in TARGET_LABELS
    }

    for r in rows:
        matrix[r["expected"]][r["observed_norm"]] += 1

    return matrix


def percentile(vals, p):
    vals = sorted(vals)

    if not vals:
        return None

    pos = (len(vals) - 1) * p
    lo = int(math.floor(pos))
    hi = int(math.ceil(pos))

    if lo == hi:
        return vals[lo]

    frac = pos - lo
    return vals[lo] * (1 - frac) + vals[hi] * frac


def bootstrap_ci(rows, reps=BOOTSTRAP_REPS):
    rng = random.Random(20261019)

    aggregate = {
        "accuracy": [],
        "balanced_accuracy": [],
        "macro_f1": [],
        "baseline_false_positive_rate": [],
    }

    class_f1 = {
        label: []
        for label in TARGET_LABELS
    }

    n = len(rows)

    for _ in range(reps):
        sample = [
            rows[rng.randrange(n)]
            for _ in range(n)
        ]

        m = metrics(sample)

        for key in aggregate:
            aggregate[key].append(m[key])

        for label in TARGET_LABELS:
            class_f1[label].append(
                m["per_class"][label]["f1"]
            )

    ci = {}

    for key, vals in aggregate.items():
        ci[key] = {
            "low": percentile(vals, 0.025),
            "high": percentile(vals, 0.975),
        }

    ci["per_class_f1"] = {}

    for label, vals in class_f1.items():
        ci["per_class_f1"][label] = {
            "low": percentile(vals, 0.025),
            "high": percentile(vals, 0.975),
        }

    return ci


def run_dataset(name, cases, config):
    rows = []

    for case in cases:
        observed, candidates = evaluate_case(case, config)

        rows.append({
            "condition": name,
            "seed": case["seed"],
            "case_no": case["case_no"],
            "expected": case["expected"],
            "observed": observed,
            "observed_norm": normalize_observed(observed),
            "candidates": "|".join(candidates),
        })

    m = metrics(rows)
    matrix = confusion_matrix(rows)
    ci = bootstrap_ci(rows)

    return rows, m, matrix, ci


def build_ambiguous_cases():
    cases = []

    for seed in SEEDS:
        rng = random.Random(seed * 900001)

        for case_no in range(50):
            run_id = f"ambiguous_{seed}"
            user_id = f"ambiguous_{case_no}"

            start_c = rng.uniform(1.8, 2.3)
            start_s = rng.uniform(1.8, 2.3)
            inc_c = rng.uniform(1.10, 1.25)
            inc_s = rng.uniform(1.10, 1.25)

            windows = []

            for i in range(5):
                windows.append(
                    make_window(
                        run_id,
                        user_id,
                        i,
                        start_c + i * inc_c,
                        start_s + i * inc_s,
                        rng.uniform(6.8, 8.3),
                        rng.uniform(6.8, 8.3),
                    )
                )

            cases.append({
                "seed": seed,
                "case_no": case_no,
                "windows": windows,
            })

    return cases


def evaluate_ambiguous(cases):
    passed = 0
    rows = []

    required = {
        "RISING_STRESS_TRAJECTORY",
        "RISING_CRAVING_TRAJECTORY",
    }

    for case in cases:
        state = StateStore(BASE_CONFIG)
        result = None

        for w in case["windows"]:
            result = evaluate_rules(w, BASE_CONFIG, state)

        candidates = set()

        if result:
            candidates = {
                c["semantic_type"]
                for c in result.get("all_candidates", [])
            }

        ok = required.issubset(candidates)
        passed += int(ok)

        rows.append({
            "seed": case["seed"],
            "case_no": case["case_no"],
            "dominant": result["semantic_type"] if result else "NONE",
            "candidates": "|".join(sorted(candidates)),
            "acceptable_set_detected": ok,
        })

    return rows, {
        "n": len(rows),
        "acceptable_candidate_set_accuracy": passed / len(rows),
    }


def write_confusion(path, matrix):
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)

        writer.writerow(
            ["expected\\observed"] + ALL_CONFUSION_LABELS
        )

        for expected in TARGET_LABELS:
            writer.writerow(
                [expected]
                + [
                    matrix[expected][observed]
                    for observed in ALL_CONFUSION_LABELS
                ]
            )


def main():
    if len(sys.argv) != 2:
        raise SystemExit(
            "Usage: correctness_v2.py <output_dir>"
        )

    out_dir = Path(sys.argv[1])
    out_dir.mkdir(parents=True, exist_ok=True)

    clean_cases = build_clean_cases()

    datasets = {
        "clean": (
            clean_cases,
            BASE_CONFIG,
        ),
        "noise_sd_0.35": (
            [perturb_noise(c) for c in clean_cases],
            BASE_CONFIG,
        ),
        "missing_one_of_five_windows": (
            [perturb_missing_window(c) for c in clean_cases],
            BASE_CONFIG,
        ),
        "delayed_adjacent_window": (
            [perturb_delay(c) for c in clean_cases],
            BASE_CONFIG,
        ),
        "threshold_0.90": (
            clean_cases,
            scaled_config(0.90),
        ),
        "threshold_1.00": (
            clean_cases,
            scaled_config(1.00),
        ),
        "threshold_1.10": (
            clean_cases,
            scaled_config(1.10),
        ),
    }

    full_summary = {
        "method": {
            "seeds": SEEDS,
            "cases_per_class_per_seed": CASES_PER_CLASS_PER_SEED,
            "target_labels": TARGET_LABELS,
            "bootstrap_reps": BOOTSTRAP_REPS,
            "base_thresholds": BASE_CONFIG["thresholds"],
        },
        "conditions": {},
    }

    all_rows = []

    for name, (cases, cfg) in datasets.items():
        print(f"\n===== {name} =====")

        rows, m, matrix, ci = run_dataset(
            name,
            cases,
            cfg,
        )

        all_rows.extend(rows)

        full_summary["conditions"][name] = {
            "metrics": m,
            "bootstrap_95ci": ci,
            "thresholds": cfg["thresholds"],
        }

        write_confusion(
            out_dir / f"confusion_{name}.csv",
            matrix,
        )

        print(
            f"n={m['n']} "
            f"accuracy={m['accuracy']:.4f} "
            f"balanced_accuracy={m['balanced_accuracy']:.4f} "
            f"macro_f1={m['macro_f1']:.4f} "
            f"baseline_FPR={m['baseline_false_positive_rate']:.4f}"
        )

        print(
            "95% CI macro_f1="
            f"[{ci['macro_f1']['low']:.4f}, "
            f"{ci['macro_f1']['high']:.4f}]"
        )

        for label in TARGET_LABELS:
            pm = m["per_class"][label]

            print(
                f"  {label}: "
                f"P={pm['precision']:.4f} "
                f"R={pm['recall']:.4f} "
                f"F1={pm['f1']:.4f}"
            )

    ambiguous_rows, ambiguous_summary = evaluate_ambiguous(
        build_ambiguous_cases()
    )

    full_summary["ambiguous_overlap"] = ambiguous_summary

    print("\n===== ambiguous_overlap =====")
    print(
        f"n={ambiguous_summary['n']} "
        f"acceptable_candidate_set_accuracy="
        f"{ambiguous_summary['acceptable_candidate_set_accuracy']:.4f}"
    )

    result_fields = [
        "condition",
        "seed",
        "case_no",
        "expected",
        "observed",
        "observed_norm",
        "candidates",
    ]

    with (out_dir / "case_results.csv").open(
        "w",
        newline="",
        encoding="utf-8",
    ) as f:
        writer = csv.DictWriter(
            f,
            fieldnames=result_fields,
        )
        writer.writeheader()
        writer.writerows(all_rows)

    with (out_dir / "ambiguous_results.csv").open(
        "w",
        newline="",
        encoding="utf-8",
    ) as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "seed",
                "case_no",
                "dominant",
                "candidates",
                "acceptable_set_detected",
            ],
        )
        writer.writeheader()
        writer.writerows(ambiguous_rows)

    with (out_dir / "summary.json").open(
        "w",
        encoding="utf-8",
    ) as f:
        json.dump(
            full_summary,
            f,
            indent=2,
        )

    print(f"\nResults: {out_dir}")
    print(f"Summary: {out_dir / 'summary.json'}")


if __name__ == "__main__":
    main()
