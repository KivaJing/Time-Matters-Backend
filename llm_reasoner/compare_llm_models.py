#!/usr/bin/env python3
# compare_llm_models.py

import argparse
import json
import statistics
import time
from pathlib import Path
from typing import Any, Dict, List, Tuple

import requests

def load_events(path: str) -> List[Dict[str, Any]]:
    p = Path(path)
    text = p.read_text(encoding="utf-8").strip()
    if not text:
        return []

    if text.startswith("["):
        data = json.loads(text)
        if not isinstance(data, list):
            raise ValueError("JSON file must contain a list of request events")
        return data

    events = []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        events.append(json.loads(line))
    return events


def build_prompt(request_event: Dict[str, Any]) -> Tuple[str, str]:
    system = (
        "You are a relapse-risk reasoning assistant for smoking cessation support. "
        "You assess momentary lapse/relapse risk from recent semantic risk signals. "
        "You MUST output ONLY a single valid JSON object, with no markdown, no code fences, and no extra text. "
        "Do NOT fabricate facts not present in the input. If information is missing, use null or a conservative default. "
        "The JSON must contain all required fields exactly as specified."
    )

    user = {
        "task": (
            "Based on the semantic risk event, estimate current lapse risk, intervention urgency, "
            "and whether the system should monitor only, deliver a coping message, or suppress intervention temporarily."
        ),
        "input": request_event,
        "required_output_json_schema": {
            "risk_level": "One of: LOW, MODERATE, HIGH, VERY_HIGH, UNKNOWN",
            "urgency": "Integer 0-100",
            "decision": "One of: MONITOR, INTERVENE, SUPPRESS",
            "intervention_type": "Short label such as craving_coping, stress_regulation, motivation_support, efficacy_support, none",
            "recommendations": "Array of short actionable sentences",
            "rationale": "1-2 short sentences grounded in the input",
            "confidence": "Float 0.0-1.0",
        },
    }
    return system, json.dumps(user, ensure_ascii=False)


def extract_json_object(text: str) -> Dict[str, Any]:
    text = text.strip()

    try:
        return json.loads(text)
    except Exception:
        pass

    if text.startswith("```"):
        lines = text.splitlines()
        if len(lines) >= 3:
            text2 = "\n".join(lines[1:-1]).strip()
            try:
                return json.loads(text2)
            except Exception:
                pass

    start = text.find("{")
    end = text.rfind("}")
    if start != -1 and end != -1 and end > start:
        return json.loads(text[start:end + 1])

    raise ValueError("No valid JSON object found in model response")


def normalize_result(result: Dict[str, Any]) -> Dict[str, Any]:
    risk_level = str(result.get("risk_level", "UNKNOWN")).upper()
    if risk_level not in {"LOW", "MODERATE", "HIGH", "VERY_HIGH", "UNKNOWN"}:
        risk_level = "UNKNOWN"

    decision = str(result.get("decision", "MONITOR")).upper()
    if decision not in {"MONITOR", "INTERVENE", "SUPPRESS"}:
        decision = "MONITOR"

    intervention_type = str(result.get("intervention_type", "none") or "none").strip()

    recommendations = result.get("recommendations", [])
    if not isinstance(recommendations, list):
        recommendations = [str(recommendations)]
    recommendations = [str(x) for x in recommendations[:1]]

    rationale = result.get("rationale", "")
    if isinstance(rationale, list):
        rationale = " ".join(str(x) for x in rationale)
    rationale = str(rationale).strip()

    try:
        urgency = int(result.get("urgency", 0))
    except Exception:
        urgency = 0
    urgency = max(0, min(100, urgency))

    try:
        confidence = float(result.get("confidence", 0.0))
    except Exception:
        confidence = 0.0
    confidence = max(0.0, min(1.0, confidence))

    return {
        "risk_level": risk_level,
        "urgency": urgency,
        "decision": decision,
        "intervention_type": intervention_type,
        "recommendations": recommendations,
        "rationale": rationale,
        "confidence": confidence,
    }


def call_ollama_chat(
    url: str,
    model: str,
    request_event: Dict[str, Any],
    timeout_sec: float,
    temperature: float,
    num_predict: int | None,
) -> Dict[str, Any]:
    system, user = build_prompt(request_event)

    payload: Dict[str, Any] = {
        "model": model,
        "stream": False,
        "format": "json",
        "options": {
            "temperature": temperature,
        },
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
    }

    if num_predict is not None:
        payload["options"]["num_predict"] = num_predict

    t0 = time.time()
    r = requests.post(url, json=payload, timeout=(5, timeout_sec))
    latency_ms = int((time.time() - t0) * 1000)
    r.raise_for_status()

    data = r.json()
    text = data["message"]["content"].strip()

    print("\n===== MODEL =====")
    print(model)
    print("\n===== RAW CONTENT =====")
    print(repr(text))
    print("\n===== RAW CONTENT PREVIEW =====")
    print(text[:2000])

    parsed = extract_json_object(text)
    norm = normalize_result(parsed)

    return {
        "latency_ms": latency_ms,
        "raw_text": text,
        "parsed": norm,
    }

def compare_outputs(a: Dict[str, Any], b: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "risk_level_match": a["risk_level"] == b["risk_level"],
        "decision_match": a["decision"] == b["decision"],
        "intervention_type_match": a["intervention_type"] == b["intervention_type"],
        "urgency_abs_diff": abs(a["urgency"] - b["urgency"]),
        "confidence_abs_diff": abs(a["confidence"] - b["confidence"]),
    }


def pct(n: int, d: int) -> float:
    return 0.0 if d == 0 else 100.0 * n / d


def mean_or_none(xs: List[float]) -> float | None:
    return None if not xs else sum(xs) / len(xs)


def p50(xs: List[float]) -> float | None:
    return None if not xs else statistics.median(xs)


def p95(xs: List[float]) -> float | None:
    if not xs:
        return None
    ys = sorted(xs)
    k = int(0.95 * (len(ys) - 1))
    return ys[k]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="JSONL or JSON array of reasoning_request events")
    ap.add_argument("--ollama-url", default="http://localhost:11434/api/chat")
    ap.add_argument("--baseline-model", required=True)
    ap.add_argument("--candidate-model", required=True)
    ap.add_argument("--timeout-sec", type=float, default=60.0)
    ap.add_argument("--temperature", type=float, default=0.2)
    ap.add_argument("--num-predict", type=int, default=None)
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--out", default="model_compare_results.json")
    args = ap.parse_args()

    events = load_events(args.input)
    if args.limit is not None:
        events = events[: args.limit]

    if not events:
        raise ValueError("No events found in input file")

    rows = []
    baseline_lat = []
    candidate_lat = []
    ok_pairs = 0
    parse_or_call_fail_baseline = 0
    parse_or_call_fail_candidate = 0

    risk_matches = 0
    decision_matches = 0
    intervention_matches = 0
    urgency_diffs: List[float] = []
    confidence_diffs: List[float] = []

    for idx, ev in enumerate(events, start=1):
        row: Dict[str, Any] = {
            "index": idx,
            "user_id": ev.get("user_id"),
            "semantic_type": ev.get("semantic_type"),
            "severity": ev.get("severity"),
        }

        baseline_res = None
        candidate_res = None
        baseline_err = None
        candidate_err = None

        try:
            baseline_res = call_ollama_chat(
                url=args.ollama_url,
                model=args.baseline_model,
                request_event=ev,
                timeout_sec=args.timeout_sec,
                temperature=args.temperature,
                num_predict=args.num_predict,
            )
            baseline_lat.append(baseline_res["latency_ms"])
            row["baseline"] = baseline_res
        except Exception as e:
            baseline_err = repr(e)
            parse_or_call_fail_baseline += 1
            row["baseline_error"] = baseline_err
            print(f"BASELINE ERROR [{idx}]: {baseline_err}")

        try:
            candidate_res = call_ollama_chat(
                url=args.ollama_url,
                model=args.candidate_model,
                request_event=ev,
                timeout_sec=args.timeout_sec,
                temperature=args.temperature,
                num_predict=args.num_predict,
            )
            candidate_lat.append(candidate_res["latency_ms"])
            row["candidate"] = candidate_res
        except Exception as e:
            candidate_err = repr(e)
            parse_or_call_fail_candidate += 1
            row["candidate_error"] = candidate_err
            print(f"CANDIDATE ERROR [{idx}]: {candidate_err}")

        if baseline_res and candidate_res:
            ok_pairs += 1
            cmp_res = compare_outputs(
                baseline_res["parsed"],
                candidate_res["parsed"],
            )
            row["comparison"] = cmp_res

            if cmp_res["risk_level_match"]:
                risk_matches += 1
            if cmp_res["decision_match"]:
                decision_matches += 1
            if cmp_res["intervention_type_match"]:
                intervention_matches += 1

            urgency_diffs.append(cmp_res["urgency_abs_diff"])
            confidence_diffs.append(cmp_res["confidence_abs_diff"])

        rows.append(row)

        print(
            f"[{idx}/{len(events)}] "
            f"semantic={ev.get('semantic_type')} severity={ev.get('severity')} "
            f"baseline_ms={baseline_res['latency_ms'] if baseline_res else 'ERR'} "
            f"candidate_ms={candidate_res['latency_ms'] if candidate_res else 'ERR'}"
        )

    summary = {
        "input_file": args.input,
        "num_events": len(events),
        "baseline_model": args.baseline_model,
        "candidate_model": args.candidate_model,
        "baseline_latency_ms": {
            "mean": mean_or_none(baseline_lat),
            "p50": p50(baseline_lat),
            "p95": p95(baseline_lat),
        },
        "candidate_latency_ms": {
            "mean": mean_or_none(candidate_lat),
            "p50": p50(candidate_lat),
            "p95": p95(candidate_lat),
        },
        "latency_speedup_ratio_mean": (
            None if not baseline_lat or not candidate_lat
            else (mean_or_none(baseline_lat) / mean_or_none(candidate_lat))
        ),
        "call_or_parse_failures": {
            "baseline": parse_or_call_fail_baseline,
            "candidate": parse_or_call_fail_candidate,
        },
        "pairwise_quality": {
            "valid_pairs": ok_pairs,
            "risk_level_match_rate_pct": pct(risk_matches, ok_pairs),
            "decision_match_rate_pct": pct(decision_matches, ok_pairs),
            "intervention_type_match_rate_pct": pct(intervention_matches, ok_pairs),
            "urgency_abs_diff_mean": mean_or_none(urgency_diffs),
            "confidence_abs_diff_mean": mean_or_none(confidence_diffs),
        },
    }

    # A simple pass heuristic you can tune
    pass_flag = False
    if ok_pairs > 0 and summary["candidate_latency_ms"]["mean"] is not None:
        pass_flag = (
            summary["pairwise_quality"]["risk_level_match_rate_pct"] >= 95.0
            and summary["pairwise_quality"]["decision_match_rate_pct"] >= 95.0
            and summary["pairwise_quality"]["intervention_type_match_rate_pct"] >= 85.0
            and (summary["pairwise_quality"]["urgency_abs_diff_mean"] or 999) <= 8.0
            and summary["latency_speedup_ratio_mean"] is not None
            and summary["latency_speedup_ratio_mean"] >= 1.5
            and parse_or_call_fail_candidate == 0
        )

    output = {
        "summary": summary,
        "candidate_passed_simple_gate": pass_flag,
        "results": rows,
    }

    Path(args.out).write_text(json.dumps(output, ensure_ascii=False, indent=2), encoding="utf-8")

    print("\n=== SUMMARY ===")
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    print(f"\ncandidate_passed_simple_gate={pass_flag}")
    print(f"saved_to={args.out}")


if __name__ == "__main__":
    main()