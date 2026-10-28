#!/usr/bin/env python3
"""Post-hoc analysis of the archived formal scaling experiment.

This script reads the frozen scaling archive used by the manuscript and derives:
  * dominant semantic-event distribution by user load;
  * severity-conditioned event-level completion coverage;
  * scheduler outcome/drop-reason decomposition by severity and load;
  * episode-level completion coverage;
  * first-seen vs latest-window trajectory diagnostics; and
  * raw mixed-risk phase values.

The runtime experiments are not re-executed. Formal 200-s measurement windows are
reconstructed from the archived per-run reasoning-request observer logs. Global
Kafka topic dumps are filtered to those formal windows before aggregation.
"""

from __future__ import annotations

import argparse
import collections
import csv
import json
import math
import re
import tarfile
import tempfile
from datetime import datetime
from pathlib import Path
from statistics import mean, stdev

LOADS = (10, 50, 100, 150, 200)
SEVERITIES = ("HIGH", "MEDIUM")
FORMAL_MEASURE_MS = 200_000
T95_DF4 = 2.7764451051977987  # two-sided 95% Student-t critical value, df=4


def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("archive", type=Path, help="scaling_20261008_144334.tar.gz")
    ap.add_argument("--output", type=Path, default=Path("scaling_archive_analysis"))
    return ap.parse_args()


def parse_run_id(run_id: str):
    m = re.fullmatch(r"scaling_u(\d+)_r(\d+)", run_id)
    if not m:
        raise ValueError(f"Unexpected run_id: {run_id}")
    return int(m.group(1)), int(m.group(2))


def read_json_kv(path: Path):
    out = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            _, payload = line.rstrip("\n").split("\t", 1)
            out.append(json.loads(payload))
    return out


def read_binary_key_json_kv(path: Path):
    """Parse kcat output with a JSON key followed by binary window-key bytes."""
    out = []
    with path.open("rb") as f:
        for line in f:
            if not line.strip() or b"\t" not in line:
                continue
            key_bytes, value_bytes = line.rstrip(b"\n").split(b"\t", 1)
            end = key_bytes.find(b"}")
            if end < 0:
                continue
            try:
                key = json.loads(key_bytes[: end + 1].decode("utf-8"))
                value = json.loads(value_bytes.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError):
                continue
            out.append({**key, **value})
    return out


def write_csv(path: Path, rows, fieldnames=None):
    rows = list(rows)
    if fieldnames is None:
        fieldnames = list(rows[0].keys()) if rows else []
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)


def mean_ci95(values):
    values = list(values)
    m = mean(values)
    if len(values) < 2:
        return m, m, m
    s = stdev(values)
    if s == 0:
        return m, m, m
    half = T95_DF4 * s / math.sqrt(len(values)) if len(values) == 5 else float("nan")
    return m, m - half, m + half


def max_consecutive_ge7(window_map):
    groups = collections.defaultdict(list)
    for (rid, uid, _, _), x in window_map.items():
        groups[(rid, uid)].append(x)
    maxima = {}
    for key, arr in groups.items():
        arr.sort(key=lambda x: x["WINDOW_START_MS"])
        best = cur = 0
        prev = None
        for x in arr:
            ws = x["WINDOW_START_MS"]
            if x["CRAVING_AVG"] >= 7.0:
                cur = cur + 1 if cur and prev is not None and ws - prev == 5000 else 1
            else:
                cur = 0
            best = max(best, cur)
            prev = ws
        maxima[key] = best
    return maxima


def rise3_hits(window_map, variable):
    groups = collections.defaultdict(list)
    for (rid, uid, _, _), x in window_map.items():
        groups[(rid, uid)].append(x)
    hits = []
    for key, arr in groups.items():
        arr.sort(key=lambda x: x["WINDOW_START_MS"])
        for i in range(2, len(arr)):
            a, b, c = arr[i - 2 : i + 1]
            if b["WINDOW_START_MS"] - a["WINDOW_START_MS"] != 5000:
                continue
            if c["WINDOW_START_MS"] - b["WINDOW_START_MS"] != 5000:
                continue
            va, vb, vc = a[variable], b[variable], c[variable]
            if va < vb < vc and vc - va >= 2.0:
                hits.append((key, a["WINDOW_START_MS"], va, vb, vc))
    return hits


def main():
    args = parse_args()
    args.output.mkdir(parents=True, exist_ok=True)

    with tempfile.TemporaryDirectory(prefix="scaling_archive_") as td:
        td = Path(td)
        with tarfile.open(args.archive, "r:gz") as t:
            t.extractall(td, filter="data")
        roots = [p for p in td.iterdir() if p.is_dir()]
        if len(roots) != 1:
            raise RuntimeError("Expected exactly one top-level archive directory")
        root = roots[0]
        topics = root / "topics"

        runs_root = td / "nested_runs"
        runs_root.mkdir()
        with tarfile.open(root / "scaling_runs.tar.gz", "r:gz") as t:
            t.extractall(runs_root, filter="data")
        runs_dir = runs_root / "runs"

        measure = {}
        for run_dir in sorted(runs_dir.iterdir()):
            if not run_dir.is_dir():
                continue
            run_id = run_dir.name
            observer = (run_dir / "reasoning_requests_observer.log").read_text(encoding="utf-8")
            m = re.search(r"\[(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})\] Done", observer)
            if not m:
                raise RuntimeError(f"No observer end timestamp for {run_id}")
            offset_dt = datetime.fromisoformat((run_dir / "end_time.txt").read_text().strip())
            local_done = datetime.fromisoformat(m.group(1)).replace(tzinfo=offset_dt.tzinfo)
            end_ms = int(local_done.timestamp() * 1000)
            measure[run_id] = (end_ms - FORMAL_MEASURE_MS, end_ms)

        def formal(run_id, ts):
            lo, hi = measure[run_id]
            return lo <= int(ts) <= hi

        semantic_all = read_json_kv(topics / "semantic_events.kv")
        drops_all = read_json_kv(topics / "reasoning_dropped_events.kv")
        high_requests_all = read_json_kv(topics / "reasoning_requests.kv")
        fast_requests_all = read_json_kv(topics / "fast_reasoning_requests.kv")
        interventions_all = read_json_kv(topics / "intervention_events.kv")
        raw_all = read_json_kv(topics / "raw_state_events.kv")
        windows_all = read_binary_key_json_kv(topics / "risk_state_windows_new.kv")

        semantic = [x for x in semantic_all if formal(x["run_id"], x["engine_ts_ms"])]
        drops = [x for x in drops_all if formal(x["run_id"], x["scheduler_ts_ms"])]
        high_requests = [x for x in high_requests_all if formal(x["run_id"], x["scheduler_ts_ms"])]
        fast_requests = [x for x in fast_requests_all if formal(x["run_id"], x["scheduler_ts_ms"])]
        interventions = [x for x in interventions_all if formal(x["run_id"], x["intervention_ts_ms"])]
        raw = [x for x in raw_all if formal(x["run_id"], x["sent_at_ms"])]
        windows = [x for x in windows_all if formal(x["RUN_ID"], x["KSQL_EMIT_TS_MS"])]
        admitted = high_requests + fast_requests
        admitted_ids = {x["semantic_event_id"] for x in admitted}

        # Semantic distribution.
        semantic_rows = []
        for load in LOADS:
            xs = [x for x in semantic if parse_run_id(x["run_id"])[0] == load]
            ct = collections.Counter(x["semantic_type"] for x in xs)
            sev = collections.Counter(x["severity"] for x in xs)
            row = {"users": load, "semantic_total": len(xs)}
            for name in [
                "ELEVATED_STRESS", "ELEVATED_CRAVING", "COMBINED_RELAPSE_RISK",
                "RISING_STRESS_TRAJECTORY", "RISING_CRAVING_TRAJECTORY",
                "SUSTAINED_ELEVATED_CRAVING",
            ]:
                row[name] = ct[name]
            row["HIGH"] = sev["HIGH"]
            row["MEDIUM"] = sev["MEDIUM"]
            semantic_rows.append(row)
        write_csv(args.output / "semantic_event_type_distribution_by_load.csv", semantic_rows)

        # Severity outcomes and explicit drop reasons.
        outcome_rows = []
        for load in LOADS:
            for severity in SEVERITIES:
                sem = [x for x in semantic if parse_run_id(x["run_id"])[0] == load and x["severity"] == severity]
                dr = [x for x in drops if parse_run_id(x["run_id"])[0] == load and x["severity"] == severity]
                ad = [x for x in admitted if parse_run_id(x["run_id"])[0] == load and x["severity"] == severity]
                reasons = collections.Counter(x["drop_reason"] for x in dr)
                coalesced = len(sem) - len(dr) - len(ad)
                outcome_rows.append({
                    "users": load,
                    "severity": severity,
                    "semantic_events": len(sem),
                    "admitted_completed": len(ad),
                    "explicit_drops": len(dr),
                    "coalesced_replacements": coalesced,
                    "global_rate_limit": reasons["global_rate_limit"],
                    "fast_global_rate_limit": reasons["fast_global_rate_limit"],
                    "medium_under_pressure": reasons["medium_under_pressure"],
                    "completion_pct": round(100 * len(ad) / len(sem), 6),
                    "explicit_drop_pct": round(100 * len(dr) / len(sem), 6),
                    "coalesced_pct": round(100 * coalesced / len(sem), 6),
                })
        write_csv(args.output / "severity_outcome_and_drop_reason_by_load.csv", outcome_rows)

        # Run-level event coverage and 95% CI summary.
        run_cov = []
        for run_id in sorted(measure):
            load, run = parse_run_id(run_id)
            for severity in SEVERITIES:
                n = sum(x["run_id"] == run_id and x["severity"] == severity for x in semantic)
                a = sum(x["run_id"] == run_id and x["severity"] == severity for x in admitted)
                run_cov.append({"run_id": run_id, "users": load, "run": run, "severity": severity,
                                "semantic_events": n, "completed": a,
                                "coverage_pct": 100 * a / n if n else 0.0})
        write_csv(args.output / "event_coverage_by_run.csv", run_cov)
        cov_summary = []
        for load in LOADS:
            for severity in SEVERITIES:
                vals = [x["coverage_pct"] for x in run_cov if x["users"] == load and x["severity"] == severity]
                m, lo, hi = mean_ci95(vals)
                cov_summary.append({"users": load, "severity": severity,
                                    "run_mean_coverage_pct": round(m, 6),
                                    "ci95_low_pct": round(lo, 6),
                                    "ci95_high_pct": round(hi, 6)})
        write_csv(args.output / "event_coverage_ci_by_load.csv", cov_summary)

        # Episode-level coverage. Episode = maximal same type/severity sequence for one user
        # with no inter-event gap > 30 s within a formal run.
        grouped = collections.defaultdict(list)
        for x in semantic:
            grouped[(x["run_id"], x["user_id"])].append(x)
        episodes = []
        for (run_id, user_id), arr in grouped.items():
            arr.sort(key=lambda x: (x["source_last_sent_at_ms"], x["engine_ts_ms"], x["source_window_start_ms"]))
            current = None
            for x in arr:
                t = x["source_last_sent_at_ms"]
                new_episode = (
                    current is None
                    or x["semantic_type"] != current["semantic_type"]
                    or x["severity"] != current["severity"]
                    or t - current["last_t"] > 30_000
                )
                if new_episode:
                    if current is not None:
                        episodes.append(current)
                    current = {"run_id": run_id, "user_id": user_id,
                               "semantic_type": x["semantic_type"], "severity": x["severity"],
                               "start_t": t, "last_t": t, "event_count": 0, "completed": False}
                current["last_t"] = t
                current["event_count"] += 1
                current["completed"] = current["completed"] or x["semantic_event_id"] in admitted_ids
            if current is not None:
                episodes.append(current)

        episode_rows = []
        for load in LOADS:
            for severity in SEVERITIES:
                ep = [x for x in episodes if parse_run_id(x["run_id"])[0] == load and x["severity"] == severity]
                completed = sum(bool(x["completed"]) for x in ep)
                episode_rows.append({"users": load, "severity": severity,
                                     "episodes": len(ep), "completed_episodes": completed,
                                     "episode_coverage_pct": round(100 * completed / len(ep), 6)})
        write_csv(args.output / "episode_coverage_by_load.csv", episode_rows)

        # Window-history diagnostics.
        first, latest = {}, {}
        for x in windows:
            key = (x["RUN_ID"], x["USER_ID"], x["WINDOW_START_MS"], x["WINDOW_END_MS"])
            if key not in first or x["KSQL_EMIT_TS_MS"] < first[key]["KSQL_EMIT_TS_MS"]:
                first[key] = x
            if key not in latest or x["KSQL_EMIT_TS_MS"] > latest[key]["KSQL_EMIT_TS_MS"]:
                latest[key] = x

        diag_rows = []
        for label, mapping in (("first_seen", first), ("latest_changelog", latest)):
            stress_hits = rise3_hits(mapping, "STRESS_AVG")
            craving_hits = rise3_hits(mapping, "CRAVING_AVG")
            sustained = max_consecutive_ge7(mapping)
            for load in LOADS:
                stress_l = [x for x in stress_hits if parse_run_id(x[0][0])[0] == load]
                craving_l = [x for x in craving_hits if parse_run_id(x[0][0])[0] == load]
                userruns = [(rid, uid) for (rid, uid) in sustained if parse_run_id(rid)[0] == load]
                diag_rows.append({
                    "window_state": label,
                    "users": load,
                    "stress_rise3_eligible_triples": len(stress_l),
                    "stress_rise3_user_runs": len({x[0] for x in stress_l}),
                    "craving_rise3_eligible_triples": len(craving_l),
                    "craving_rise3_user_runs": len({x[0] for x in craving_l}),
                    "user_runs_with_3_consecutive_craving_avg_ge7": sum(sustained[k] >= 3 for k in userruns),
                    "max_consecutive_craving_avg_ge7": max((sustained[k] for k in userruns), default=0),
                })
        write_csv(args.output / "trajectory_window_diagnostic.csv", diag_rows)

        # Raw mixed-risk phase values.
        phase_rows = []
        for phase_step in range(12):
            xs = [x for x in raw if int(x["logical_step"]) % 12 == phase_step]
            if not xs:
                continue
            scenario = collections.Counter(x["scenario"] for x in xs).most_common(1)[0][0]
            craving = [x["craving"] for x in xs]
            stress = [x["stress"] for x in xs]
            phase_rows.append({
                "phase_step": phase_step,
                "scenario": scenario,
                "n": len(xs),
                "craving_mean": round(mean(craving), 6),
                "craving_min": min(craving), "craving_max": max(craving),
                "stress_mean": round(mean(stress), 6),
                "stress_min": min(stress), "stress_max": max(stress),
            })
        write_csv(args.output / "raw_phase_values.csv", phase_rows)

        # Validation and concise summary.
        summary = {
            "formal_semantic_events": len(semantic),
            "formal_explicit_drops": len(drops),
            "formal_fast_requests": len(fast_requests),
            "formal_high_capacity_requests": len(high_requests),
            "formal_interventions": len(interventions),
            "admitted_equals_interventions": len(admitted) == len(interventions),
            "drop_reasons": dict(collections.Counter(x["drop_reason"] for x in drops)),
            "dominant_semantic_types": dict(collections.Counter(x["semantic_type"] for x in semantic)),
            "severity_counts": dict(collections.Counter(x["severity"] for x in semantic)),
        }
        (args.output / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
        print(json.dumps(summary, indent=2))
        print(f"Wrote analysis to {args.output}")


if __name__ == "__main__":
    main()
