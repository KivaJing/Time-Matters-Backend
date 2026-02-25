def evaluate_rules(event, config, state_store):
    span = event["max_rt"] - event["min_rt"]

    thresholds = config["thresholds"]

    # Rule 1: High Instability
    if span >= thresholds["rt_high_span"]:
        return {
            "semantic_type": "INSTABILITY_SPIKE",
            "severity": "HIGH",
            "rule_id": "R1",
            "explanation": f"RT span {span}ms exceeds high threshold"
        }

    # Rule 2: Moderate Instability
    if span >= thresholds["rt_moderate_span"]:
        return {
            "semantic_type": "INSTABILITY_RISE",
            "severity": "MEDIUM",
            "rule_id": "R2",
            "explanation": f"RT span {span}ms exceeds moderate threshold"
        }

    return None