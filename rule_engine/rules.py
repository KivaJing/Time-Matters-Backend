def evaluate_rules(event, config, state_store):
    th = config["thresholds"]

    craving_avg = float(event.get("craving_avg", 0) or 0)
    stress_avg = float(event.get("stress_avg", 0) or 0)
    motivation_avg = float(event.get("motivation_avg", 0) or 0)
    self_efficacy_avg = float(event.get("self_efficacy_avg", 0) or 0)
    craving_max = float(event.get("craving_max", 0) or 0)
    stress_max = float(event.get("stress_max", 0) or 0)
    latest_receptivity = int(event.get("latest_receptivity", 1) or 0)
    user_id = event["user_id"]

    state_store.add_history(user_id, event)

    # Context / suppression signal
    if latest_receptivity == 0:
        return {
            "semantic_type": "LOW_RECEPTIVITY_CONTEXT",
            "severity": "LOW",
            "rule_id": "R0",
            "explanation": "Current context suggests low receptivity to intervention.",
        }

    # Combined high-risk state
    if (
        craving_avg >= th["craving_high"]
        and stress_avg >= th["stress_high"]
        and motivation_avg <= th["motivation_low"]
        and self_efficacy_avg <= th["self_efficacy_low"]
    ):
        severity = "VERY_HIGH" if (craving_avg + stress_avg) >= th["combined_high_sum"] else "HIGH"
        return {
            "semantic_type": "COMBINED_RELAPSE_RISK",
            "severity": severity,
            "rule_id": "R1",
            "explanation": "Craving and stress are elevated while motivation and self-efficacy are low.",
        }

    # Sustained trajectories
    if state_store.is_rising(user_id, "stress_avg", windows=3, delta=2.0):
        return {
            "semantic_type": "RISING_STRESS_TRAJECTORY",
            "severity": "HIGH",
            "rule_id": "R2",
            "explanation": "Stress increased across 3 consecutive windows.",
        }

    if state_store.is_rising(user_id, "craving_avg", windows=3, delta=2.0):
        return {
            "semantic_type": "RISING_CRAVING_TRAJECTORY",
            "severity": "HIGH",
            "rule_id": "R3",
            "explanation": "Craving increased across 3 consecutive windows.",
        }

    if state_store.is_sustained_high(user_id, "craving_avg", windows=3, threshold=th["craving_high"]):
        return {
            "semantic_type": "SUSTAINED_ELEVATED_CRAVING",
            "severity": "HIGH",
            "rule_id": "R4",
            "explanation": "Craving remained elevated across 3 windows.",
        }

    # Single-window threshold rules
    if craving_avg >= th["craving_high"] or craving_max >= th["craving_very_high"]:
        return {
            "semantic_type": "ELEVATED_CRAVING",
            "severity": "HIGH" if craving_avg >= th["craving_very_high"] or craving_max >= th["craving_very_high"] else "MEDIUM",
            "rule_id": "R5",
            "explanation": f"Craving is elevated (avg={craving_avg:.2f}, max={craving_max:.2f}).",
        }

    if stress_avg >= th["stress_high"] or stress_max >= th["stress_very_high"]:
        return {
            "semantic_type": "ELEVATED_STRESS",
            "severity": "HIGH" if stress_avg >= th["stress_very_high"] or stress_max >= th["stress_very_high"] else "MEDIUM",
            "rule_id": "R6",
            "explanation": f"Stress is elevated (avg={stress_avg:.2f}, max={stress_max:.2f}).",
        }

    if motivation_avg <= th["motivation_low"]:
        return {
            "semantic_type": "LOW_MOTIVATION",
            "severity": "HIGH" if motivation_avg <= th["motivation_very_low"] else "MEDIUM",
            "rule_id": "R7",
            "explanation": f"Motivation is low (avg={motivation_avg:.2f}).",
        }

    if self_efficacy_avg <= th["self_efficacy_low"]:
        return {
            "semantic_type": "LOW_SELF_EFFICACY",
            "severity": "HIGH" if self_efficacy_avg <= th["self_efficacy_very_low"] else "MEDIUM",
            "rule_id": "R8",
            "explanation": f"Self-efficacy is low (avg={self_efficacy_avg:.2f}).",
        }

    return None
