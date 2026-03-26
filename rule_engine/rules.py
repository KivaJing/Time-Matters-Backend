# rules.py

from typing import Dict, Any, List, Optional

RULE_PRIORITY = {
    "LOW_RECEPTIVITY_CONTEXT": 100,
    "COMBINED_RELAPSE_RISK": 90,
    "RISING_STRESS_TRAJECTORY": 80,
    "RISING_CRAVING_TRAJECTORY": 80,
    "SUSTAINED_ELEVATED_CRAVING": 70,
    "ELEVATED_CRAVING": 60,
    "ELEVATED_STRESS": 60,
    "LOW_MOTIVATION": 50,
    "LOW_SELF_EFFICACY": 50,
}

SEVERITY_RANK = {
    "LOW": 1,
    "MEDIUM": 2,
    "HIGH": 3,
    "VERY_HIGH": 4,
}


def _candidate(
    semantic_type: str,
    severity: str,
    rule_id: str,
    explanation: str,
) -> Dict[str, Any]:
    return {
        "semantic_type": semantic_type,
        "severity": severity,
        "rule_id": rule_id,
        "explanation": explanation,
    }


def _sort_key(item: Dict[str, Any]):
    return (
        RULE_PRIORITY.get(item["semantic_type"], 0),
        SEVERITY_RANK.get(item["severity"], 0),
    )


def evaluate_rules(event: Dict[str, Any], config: Dict[str, Any], state_store) -> Optional[Dict[str, Any]]:
    th = config["thresholds"]

    craving_avg = float(event.get("craving_avg", 0) or 0)
    stress_avg = float(event.get("stress_avg", 0) or 0)
    motivation_avg = float(event.get("motivation_avg", 0) or 0)
    self_efficacy_avg = float(event.get("self_efficacy_avg", 0) or 0)
    craving_max = float(event.get("craving_max", 0) or 0)
    stress_max = float(event.get("stress_max", 0) or 0)
    latest_receptivity = int(event.get("latest_receptivity", 1) or 0)
    user_id = str(event["user_id"])

    state_store.add_history(user_id, event)

    candidates: List[Dict[str, Any]] = []

    # 1) Context / suppression signal
    if latest_receptivity == 0:
        candidates.append(
            _candidate(
                "LOW_RECEPTIVITY_CONTEXT",
                "LOW",
                "R0",
                "Current context suggests low receptivity to intervention.",
            )
        )

    # 2) Combined high-risk state
    if (
        craving_avg >= th["craving_high"]
        and stress_avg >= th["stress_high"]
        and motivation_avg <= th["motivation_low"]
        and self_efficacy_avg <= th["self_efficacy_low"]
    ):
        severity = "VERY_HIGH" if (craving_avg + stress_avg) >= th["combined_high_sum"] else "HIGH"
        candidates.append(
            _candidate(
                "COMBINED_RELAPSE_RISK",
                severity,
                "R1",
                "Craving and stress are elevated while motivation and self-efficacy are low.",
            )
        )

    # 3) Trajectories
    if state_store.is_rising(user_id, "stress_avg", windows=3, delta=2.0):
        candidates.append(
            _candidate(
                "RISING_STRESS_TRAJECTORY",
                "HIGH",
                "R2",
                "Stress increased across 3 consecutive windows.",
            )
        )

    if state_store.is_rising(user_id, "craving_avg", windows=3, delta=2.0):
        candidates.append(
            _candidate(
                "RISING_CRAVING_TRAJECTORY",
                "HIGH",
                "R3",
                "Craving increased across 3 consecutive windows.",
            )
        )

    if state_store.is_sustained_high(user_id, "craving_avg", windows=3, threshold=th["craving_high"]):
        candidates.append(
            _candidate(
                "SUSTAINED_ELEVATED_CRAVING",
                "HIGH",
                "R4",
                "Craving remained elevated across 3 windows.",
            )
        )

    # 4) Single-window threshold rules
    if craving_avg >= th["craving_high"] or craving_max >= th["craving_very_high"]:
        severity = (
            "HIGH"
            if (craving_avg >= th["craving_very_high"] or craving_max >= th["craving_very_high"])
            else "MEDIUM"
        )
        candidates.append(
            _candidate(
                "ELEVATED_CRAVING",
                severity,
                "R5",
                f"Craving is elevated (avg={craving_avg:.2f}, max={craving_max:.2f}).",
            )
        )

    if stress_avg >= th["stress_high"] or stress_max >= th["stress_very_high"]:
        severity = (
            "HIGH"
            if (stress_avg >= th["stress_very_high"] or stress_max >= th["stress_very_high"])
            else "MEDIUM"
        )
        candidates.append(
            _candidate(
                "ELEVATED_STRESS",
                severity,
                "R6",
                f"Stress is elevated (avg={stress_avg:.2f}, max={stress_max:.2f}).",
            )
        )

    if motivation_avg <= th["motivation_low"]:
        severity = "HIGH" if motivation_avg <= th["motivation_very_low"] else "MEDIUM"
        candidates.append(
            _candidate(
                "LOW_MOTIVATION",
                severity,
                "R7",
                f"Motivation is low (avg={motivation_avg:.2f}).",
            )
        )

    if self_efficacy_avg <= th["self_efficacy_low"]:
        severity = "HIGH" if self_efficacy_avg <= th["self_efficacy_very_low"] else "MEDIUM"
        candidates.append(
            _candidate(
                "LOW_SELF_EFFICACY",
                severity,
                "R8",
                f"Self-efficacy is low (avg={self_efficacy_avg:.2f}).",
            )
        )

    if not candidates:
        return None

    candidates.sort(key=_sort_key, reverse=True)
    return candidates[0]