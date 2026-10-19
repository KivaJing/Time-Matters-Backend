#!/usr/bin/env python3

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "rule_engine"))

from rules import evaluate_rules
from state_store import StateStore


CONFIG_PATH = ROOT / "rule_engine" / "config.json"

with open(CONFIG_PATH, encoding="utf-8") as f:
    CONFIG = json.load(f)

TH = CONFIG["thresholds"]


def make_window(
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
        "run_id": "correctness_v2_smoke",
        "user_id": user_id,
        "window_start_ms": start,
        "window_end_ms": start + 25000,
        "event_count": 3,
        "craving_avg": float(craving),
        "stress_avg": float(stress),
        "motivation_avg": float(motivation),
        "self_efficacy_avg": float(efficacy),
        "craving_max": float(craving),
        "stress_max": float(stress),
        "motivation_min": float(motivation),
        "self_efficacy_min": float(efficacy),
        "latest_receptivity": int(receptivity),
        "latest_strategy_use": 2,
        "latest_is_moving": 0,
        "latest_in_meeting": 0,
        "latest_location_type": "home",
        "ksql_latency_ms": 0,
    }


def run_sequence(name, windows, expected_primary, expected_candidates=None):
    state = StateStore(CONFIG)

    outputs = []

    for w in windows:
        result = evaluate_rules(w, CONFIG, state)
        outputs.append(result)

    final = outputs[-1]

    observed = final["semantic_type"] if final else "NONE"

    candidates = set()
    if final:
        candidates = {
            x["semantic_type"]
            for x in final.get("all_candidates", [])
        }

    primary_ok = observed == expected_primary

    candidates_ok = True
    if expected_candidates is not None:
        candidates_ok = set(expected_candidates).issubset(candidates)

    ok = primary_ok and candidates_ok

    print(f"\n===== {name} =====")
    print("expected_primary =", expected_primary)
    print("observed_primary =", observed)
    print("candidates       =", sorted(candidates))
    print("PASS             =", ok)

    return ok


def main():
    ch = TH["craving_high"]
    sh = TH["stress_high"]
    ml = TH["motivation_low"]
    el = TH["self_efficacy_low"]

    safe_craving = max(1.0, ch - 3.0)
    safe_stress = max(1.0, sh - 3.0)
    safe_motivation = ml + 3.0
    safe_efficacy = el + 3.0

    tests = []

    # 1. Stable baseline: no semantic event.
    baseline = [
        make_window(
            "baseline",
            i,
            safe_craving,
            safe_stress,
            safe_motivation,
            safe_efficacy,
        )
        for i in range(3)
    ]

    tests.append(
        run_sequence(
            "stable_baseline",
            baseline,
            "NONE",
        )
    )

    # 2. Isolated stress trajectory.
    stress_values = [
        sh - 3.2,
        sh - 2.0,
        sh - 0.8,
    ]

    stress_seq = [
        make_window(
            "stress_only",
            i,
            safe_craving,
            stress_values[i],
            safe_motivation,
            safe_efficacy,
        )
        for i in range(3)
    ]

    tests.append(
        run_sequence(
            "isolated_rising_stress",
            stress_seq,
            "RISING_STRESS_TRAJECTORY",
        )
    )

    # 3. Isolated craving trajectory.
    craving_values = [
        ch - 3.2,
        ch - 2.0,
        ch - 0.8,
    ]

    craving_seq = [
        make_window(
            "craving_only",
            i,
            craving_values[i],
            safe_stress,
            safe_motivation,
            safe_efficacy,
        )
        for i in range(3)
    ]

    tests.append(
        run_sequence(
            "isolated_rising_craving",
            craving_seq,
            "RISING_CRAVING_TRAJECTORY",
        )
    )

    # 4. Combined risk.
    combined = [
        make_window(
            "combined",
            0,
            ch + 1.0,
            sh + 1.0,
            ml - 0.5,
            el - 0.5,
        )
    ]

    tests.append(
        run_sequence(
            "combined_relapse_risk",
            combined,
            "COMBINED_RELAPSE_RISK",
        )
    )

    # 5. Ambiguous trajectory:
    # stress and craving both rise simultaneously.
    ambiguous = [
        make_window(
            "ambiguous",
            i,
            craving_values[i],
            stress_values[i],
            safe_motivation,
            safe_efficacy,
        )
        for i in range(3)
    ]

    tests.append(
        run_sequence(
            "ambiguous_dual_trajectory",
            ambiguous,
            "RISING_STRESS_TRAJECTORY",
            expected_candidates={
                "RISING_STRESS_TRAJECTORY",
                "RISING_CRAVING_TRAJECTORY",
            },
        )
    )

    print("\n================================")
    print(f"passed={sum(tests)}/{len(tests)}")

    if not all(tests):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
