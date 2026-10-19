#!/usr/bin/env python3

import csv
import json
import math
import statistics
import sys
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

WARMUPS = 5
SEQUENTIAL_REQUESTS = 20
PARALLEL_REQUESTS = 20

FAST_URL = "http://localhost:11435/api/generate"
HIGH_URL = "http://localhost:11434/api/chat"

FAST_MODEL = "granite3-moe:1b"
HIGH_MODEL = "llama3.2:3b"

FAST_PROMPT = (
    "Return exactly one JSON object only.\n"
    'Schema: {"urgency": <0-100 int>, '
    '"intervention_type": "none|stress_regulation|craving_coping"}\n'
    "Use higher urgency for stronger or converging evidence.\n"
    "Use lower urgency for weak or ambiguous evidence.\n"
    "No markdown. No explanation.\n"
    "semantic_type=ELEVATED_STRESS\n"
    "severity=HIGH\n"
    "craving_score=6.8\n"
    "stress_score=8.4\n"
    "negative_affect_score=7.1\n"
    "recent_lapse=false\n"
)

HIGH_SYSTEM = (
    "You are a relapse-risk reasoning assistant for smoking cessation support. "
    "Return exactly one valid JSON object only. "
    "Do not output markdown, code fences, or any extra text. "
    "Do not include explanations outside the JSON."
)

HIGH_USER = json.dumps({
    "task": (
        "Estimate intervention urgency and whether the system should MONITOR, "
        "INTERVENE, or SUPPRESS based on the semantic risk event."
    ),
    "input": {
        "semantic_type": "COMBINED_RELAPSE_RISK",
        "severity": "VERY_HIGH",
        "rule_id": "R1",
        "evidence": {
            "event_count": 3,
            "craving_avg": 8.2,
            "stress_avg": 8.1,
            "motivation_avg": 2.4,
            "self_efficacy_avg": 2.2,
            "craving_max": 9.0,
            "stress_max": 8.8,
            "latest_receptivity": 1
        }
    },
    "required_output_json_schema": {
        "urgency": "Integer 0-100",
        "decision": "One of: MONITOR, INTERVENE, SUPPRESS",
        "confidence": "Float 0.0-1.0"
    }
}, separators=(",", ":"))

SPECS = {
    "fast": {
        "url": FAST_URL,
        "model": FAST_MODEL,
        "parallelism": 3,
        "payload": {
            "model": FAST_MODEL,
            "prompt": FAST_PROMPT,
            "stream": False,
            "format": "json",
            "options": {
                "temperature": 0,
                "num_predict": 32,
                "num_ctx": 256
            }
        }
    },
    "high": {
        "url": HIGH_URL,
        "model": HIGH_MODEL,
        "parallelism": 2,
        "payload": {
            "model": HIGH_MODEL,
            "stream": False,
            "format": "json",
            "options": {
                "temperature": 0,
                "num_predict": 100
            },
            "messages": [
                {"role": "system", "content": HIGH_SYSTEM},
                {"role": "user", "content": HIGH_USER}
            ]
        }
    }
}


def post_json(url, payload):
    body = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST"
    )

    t0 = time.perf_counter()
    with urllib.request.urlopen(req, timeout=120) as resp:
        raw = resp.read()
    wall_ms = (time.perf_counter() - t0) * 1000.0

    data = json.loads(raw.decode("utf-8"))
    return data, wall_ms


def ns_to_ms(x):
    return float(x or 0) / 1_000_000.0


def tokens_per_sec(count, duration_ns):
    count = int(count or 0)
    duration_ns = int(duration_ns or 0)
    if count <= 0 or duration_ns <= 0:
        return None
    return count / (duration_ns / 1_000_000_000.0)


def percentile(vals, p):
    if not vals:
        return None
    vals = sorted(vals)
    k = max(0, min(len(vals) - 1, math.ceil((p / 100.0) * len(vals)) - 1))
    return vals[k]


def make_row(model_name, model_tag, phase, idx, result, wall_ms):
    return {
        "model_name": model_name,
        "model_tag": model_tag,
        "phase": phase,
        "request": idx,
        "wall_ms": wall_ms,
        "total_duration_ms": ns_to_ms(result.get("total_duration")),
        "load_duration_ms": ns_to_ms(result.get("load_duration")),
        "prompt_eval_count": int(result.get("prompt_eval_count") or 0),
        "prompt_eval_ms": ns_to_ms(result.get("prompt_eval_duration")),
        "prompt_tokens_per_sec": tokens_per_sec(
            result.get("prompt_eval_count"),
            result.get("prompt_eval_duration")
        ),
        "eval_count": int(result.get("eval_count") or 0),
        "eval_ms": ns_to_ms(result.get("eval_duration")),
        "generation_tokens_per_sec": tokens_per_sec(
            result.get("eval_count"),
            result.get("eval_duration")
        ),
        "done_reason": result.get("done_reason", "")
    }


def summary_for_rows(rows):
    gen = [
        r["generation_tokens_per_sec"]
        for r in rows
        if r["generation_tokens_per_sec"] is not None
    ]
    prompt = [
        r["prompt_tokens_per_sec"]
        for r in rows
        if r["prompt_tokens_per_sec"] is not None
    ]
    wall = [r["wall_ms"] for r in rows]
    eval_counts = [r["eval_count"] for r in rows]

    return {
        "n": len(rows),
        "generation_tps_mean": statistics.mean(gen) if gen else None,
        "generation_tps_sd": statistics.stdev(gen) if len(gen) > 1 else 0.0,
        "generation_tps_median": statistics.median(gen) if gen else None,
        "generation_tps_p95": percentile(gen, 95),
        "prompt_tps_mean": statistics.mean(prompt) if prompt else None,
        "wall_ms_mean": statistics.mean(wall) if wall else None,
        "wall_ms_p95": percentile(wall, 95),
        "eval_count_mean": statistics.mean(eval_counts) if eval_counts else None
    }


def run_model(name, spec, out_dir):
    print(f"\n===== {name.upper()} : {spec['model']} =====", flush=True)

    raw_path = out_dir / f"{name}_raw.jsonl"

    # Warm-up phase: not included in statistics.
    print(f"Warm-up: {WARMUPS} requests", flush=True)
    for i in range(1, WARMUPS + 1):
        result, wall_ms = post_json(spec["url"], spec["payload"])
        print(
            f"  warmup {i}/{WARMUPS}: "
            f"eval_count={result.get('eval_count')} wall={wall_ms:.1f} ms",
            flush=True
        )

    # Sequential steady-state measurements.
    sequential_rows = []
    print(f"Sequential measurement: {SEQUENTIAL_REQUESTS} requests", flush=True)

    with raw_path.open("w", encoding="utf-8") as rawf:
        for i in range(1, SEQUENTIAL_REQUESTS + 1):
            result, wall_ms = post_json(spec["url"], spec["payload"])
            row = make_row(name, spec["model"], "sequential", i, result, wall_ms)
            sequential_rows.append(row)

            rawf.write(json.dumps({
                "phase": "sequential",
                "request": i,
                "wall_ms": wall_ms,
                "response": result
            }, ensure_ascii=False) + "\n")

            print(
                f"  seq {i:02d}: "
                f"eval={row['eval_count']} tokens "
                f"gen={row['generation_tokens_per_sec']:.2f} tok/s "
                f"wall={wall_ms:.1f} ms",
                flush=True
            )

        # Configured-parallelism serving test.
        print(
            f"Parallel measurement: {PARALLEL_REQUESTS} requests "
            f"at concurrency={spec['parallelism']}",
            flush=True
        )

        parallel_rows = []
        phase_start = time.perf_counter()

        with ThreadPoolExecutor(max_workers=spec["parallelism"]) as ex:
            futures = {
                ex.submit(post_json, spec["url"], spec["payload"]): i
                for i in range(1, PARALLEL_REQUESTS + 1)
            }

            for fut in as_completed(futures):
                i = futures[fut]
                result, wall_ms = fut.result()
                row = make_row(name, spec["model"], "parallel", i, result, wall_ms)
                parallel_rows.append(row)

                rawf.write(json.dumps({
                    "phase": "parallel",
                    "request": i,
                    "wall_ms": wall_ms,
                    "response": result
                }, ensure_ascii=False) + "\n")

        phase_elapsed = time.perf_counter() - phase_start
        parallel_rows.sort(key=lambda x: x["request"])

    all_rows = sequential_rows + parallel_rows

    total_parallel_tokens = sum(r["eval_count"] for r in parallel_rows)
    aggregate_parallel_tps = (
        total_parallel_tokens / phase_elapsed
        if phase_elapsed > 0 else None
    )

    return {
        "rows": all_rows,
        "sequential": summary_for_rows(sequential_rows),
        "parallel": summary_for_rows(parallel_rows),
        "parallel_phase_elapsed_sec": phase_elapsed,
        "parallel_total_generated_tokens": total_parallel_tokens,
        "parallel_aggregate_generation_tps": aggregate_parallel_tps
    }


def main():
    if len(sys.argv) != 2:
        raise SystemExit("Usage: benchmark_ollama_tokens.py <output_dir>")

    out_dir = Path(sys.argv[1])
    out_dir.mkdir(parents=True, exist_ok=True)

    all_rows = []
    final_summary = {}

    # Run one model at a time so the two Ollama containers do not
    # intentionally compete during the benchmark.
    for name in ("fast", "high"):
        result = run_model(name, SPECS[name], out_dir)
        all_rows.extend(result["rows"])
        final_summary[name] = {
            k: v for k, v in result.items() if k != "rows"
        }

    csv_path = out_dir / "token_benchmark.csv"
    fields = [
        "model_name",
        "model_tag",
        "phase",
        "request",
        "wall_ms",
        "total_duration_ms",
        "load_duration_ms",
        "prompt_eval_count",
        "prompt_eval_ms",
        "prompt_tokens_per_sec",
        "eval_count",
        "eval_ms",
        "generation_tokens_per_sec",
        "done_reason"
    ]

    with csv_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(all_rows)

    with (out_dir / "summary.json").open("w", encoding="utf-8") as f:
        json.dump(final_summary, f, indent=2)

    print("\n================ FINAL SUMMARY ================")
    for name in ("fast", "high"):
        s = final_summary[name]
        seq = s["sequential"]
        par = s["parallel"]

        print(f"\n{name.upper()}  {SPECS[name]['model']}")
        print(
            f"Sequential generation throughput: "
            f"{seq['generation_tps_mean']:.2f} tok/s "
            f"(SD {seq['generation_tps_sd']:.2f}, "
            f"P95 {seq['generation_tps_p95']:.2f})"
        )
        print(
            f"Sequential wall latency: "
            f"{seq['wall_ms_mean']:.1f} ms "
            f"(P95 {seq['wall_ms_p95']:.1f})"
        )
        print(
            f"Configured-parallel aggregate generation throughput: "
            f"{s['parallel_aggregate_generation_tps']:.2f} tok/s "
            f"(concurrency={SPECS[name]['parallelism']})"
        )
        print(
            f"Parallel request wall latency: "
            f"{par['wall_ms_mean']:.1f} ms "
            f"(P95 {par['wall_ms_p95']:.1f})"
        )

    print(f"\nCSV:     {csv_path}")
    print(f"Summary: {out_dir / 'summary.json'}")


if __name__ == "__main__":
    main()
