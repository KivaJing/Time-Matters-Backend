#!/usr/bin/env python3
import argparse
import json
from collections import Counter, defaultdict
from typing import Dict, List, Tuple, Any


def read_jsonl(path: str):
    with open(path, "r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except Exception as e:
                raise ValueError(f"Invalid JSON on line {line_no}: {e}")


def parse_csv_arg(s: str) -> List[str]:
    return [x.strip() for x in s.split(",") if x.strip()]


def event_order_key(evt: dict) -> Tuple:
    """
    Sort events within a run/user so repeated runs can be aligned by ordinal position.
    Prefer source/window timestamps when present.
    """
    return (
        as_int(evt.get("source_window_start_ms")),
        as_int(evt.get("window_start_ms")),
        as_int(evt.get("source_window_end_ms")),
        as_int(evt.get("window_end_ms")),
        as_int(evt.get("source_ksql_emit_ts_ms")),
        as_int(evt.get("semantic_engine_ts_ms")),
        as_int(evt.get("scheduler_ts_ms")),
        as_int(evt.get("reasoning_ts_ms")),
        as_int(evt.get("intervention_ts_ms")),
        str(evt.get("semantic_type") or ""),
        str(evt.get("rule_id") or ""),
    )


def as_int(v: Any):
    if v is None:
        return 10**18
    try:
        return int(v)
    except Exception:
        return 10**18


def matches_filters(evt: dict, filters: List[Tuple[str, set]]) -> bool:
    for field, allowed in filters:
        val = evt.get(field)
        if str(val) not in allowed:
            return False
    return True


def load_events(
    path: str,
    run_ids: List[str],
    user_field: str,
    filters: List[Tuple[str, set]],
) -> Dict[str, Dict[str, List[dict]]]:
    data = {rid: defaultdict(list) for rid in run_ids}
    run_id_set = set(run_ids)

    for evt in read_jsonl(path):
        rid = evt.get("run_id")
        if rid not in run_id_set:
            continue
        if not matches_filters(evt, filters):
            continue

        user_id = str(evt.get(user_field) or "UNKNOWN")
        data[rid][user_id].append(evt)

    for rid in run_ids:
        for user_id in data[rid]:
            data[rid][user_id].sort(key=event_order_key)

    return data


def compare_runs(
    data: Dict[str, Dict[str, List[dict]]],
    run_ids: List[str],
    compare_fields: List[str],
):
    """
    Align by (user_id, ordinal index in sorted sequence).
    This works well when repeated replays use the same input order and same seed/config.
    """
    all_users = sorted(set().union(*[set(data[rid].keys()) for rid in run_ids]))

    field_total = Counter()
    field_match = Counter()

    exact_total = 0
    exact_match = 0

    presence_total = 0
    presence_match = 0

    mismatch_examples = []

    per_user_stats = []

    for user_id in all_users:
        seqs = {rid: data[rid].get(user_id, []) for rid in run_ids}
        lengths = {rid: len(seqs[rid]) for rid in run_ids}
        max_len = max(lengths.values()) if lengths else 0

        user_presence_match = 0
        user_exact_match = 0
        user_total = max_len

        for idx in range(max_len):
            row = {rid: (seqs[rid][idx] if idx < len(seqs[rid]) else None) for rid in run_ids}

            # Presence consistency
            presence_total += 1
            present_pattern = [row[rid] is not None for rid in run_ids]
            all_present = all(present_pattern)
            all_absent = not any(present_pattern)
            if all_present or all_absent:
                presence_match += 1
                user_presence_match += 1

            # Only compare fields when all runs have the aligned event
            if not all_present:
                if len(mismatch_examples) < 20:
                    mismatch_examples.append({
                        "user_id": user_id,
                        "ordinal": idx,
                        "type": "presence_mismatch",
                        "presence": {rid: (row[rid] is not None) for rid in run_ids},
                    })
                continue

            exact_total += 1
            user_all_fields_match = True

            for field in compare_fields:
                vals = [row[rid].get(field) for rid in run_ids]
                field_total[field] += 1
                if all(v == vals[0] for v in vals[1:]):
                    field_match[field] += 1
                else:
                    user_all_fields_match = False
                    if len(mismatch_examples) < 20:
                        mismatch_examples.append({
                            "user_id": user_id,
                            "ordinal": idx,
                            "type": "field_mismatch",
                            "field": field,
                            "values": {rid: row[rid].get(field) for rid in run_ids},
                            "semantic_type": row[run_ids[0]].get("semantic_type"),
                        })

            if user_all_fields_match:
                exact_match += 1
                user_exact_match += 1

        per_user_stats.append({
            "user_id": user_id,
            "events_max_aligned": user_total,
            "run_counts": lengths,
            "presence_match_rate": round(user_presence_match / user_total, 4) if user_total else 1.0,
            "exact_match_rate": round(user_exact_match / user_total, 4) if user_total else 1.0,
        })

    return {
        "field_total": field_total,
        "field_match": field_match,
        "exact_total": exact_total,
        "exact_match": exact_match,
        "presence_total": presence_total,
        "presence_match": presence_match,
        "mismatch_examples": mismatch_examples,
        "per_user_stats": per_user_stats,
        "run_event_counts": {
            rid: sum(len(v) for v in data[rid].values()) for rid in run_ids
        },
    }


def print_report(results, run_ids, compare_fields):
    print("\n=== Repeated-run consistency report ===\n")
    print("Runs compared:", ", ".join(run_ids))
    print("Run event counts:")
    for rid, n in results["run_event_counts"].items():
        print(f"  {rid}: {n}")

    print()
    presence_rate = (
        results["presence_match"] / results["presence_total"]
        if results["presence_total"] else 0.0
    )
    exact_rate = (
        results["exact_match"] / results["exact_total"]
        if results["exact_total"] else 0.0
    )

    print(f"Aligned positions: {results['presence_total']}")
    print(f"Presence consistency: {results['presence_match']}/{results['presence_total']} = {presence_rate:.4%}")
    print(f"Exact all-field consistency: {results['exact_match']}/{results['exact_total']} = {exact_rate:.4%}")

    print("\nField-wise consistency:")
    for field in compare_fields:
        total = results["field_total"][field]
        match = results["field_match"][field]
        rate = (match / total) if total else 0.0
        print(f"  {field}: {match}/{total} = {rate:.4%}")

    if results["mismatch_examples"]:
        print("\nMismatch examples:")
        for ex in results["mismatch_examples"][:10]:
            print(json.dumps(ex, ensure_ascii=False))
    else:
        print("\nNo mismatches found.")


def main():
    parser = argparse.ArgumentParser(description="Compare repeated runs for consistency/determinism.")
    parser.add_argument("--input-jsonl", required=True, help="Combined JSONL containing multiple run_ids")
    parser.add_argument("--run-ids", required=True, help="Comma-separated run_ids to compare")
    parser.add_argument(
        "--fields",
        default="semantic_type,decision,intervention_type,risk_level",
        help="Comma-separated fields to compare"
    )
    parser.add_argument(
        "--user-field",
        default="user_id",
        help="User identifier field for alignment"
    )
    parser.add_argument(
        "--filter",
        action="append",
        default=[],
        help='Optional filter in form field=value1,value2 . Example: --filter reasoning_mode=rule_based'
    )
    args = parser.parse_args()

    run_ids = parse_csv_arg(args.run_ids)
    compare_fields = parse_csv_arg(args.fields)

    filters = []
    for item in args.filter:
        if "=" not in item:
            raise ValueError(f"Invalid --filter: {item}")
        field, values = item.split("=", 1)
        filters.append((field.strip(), set(parse_csv_arg(values))))

    data = load_events(
        path=args.input_jsonl,
        run_ids=run_ids,
        user_field=args.user_field,
        filters=filters,
    )

    results = compare_runs(
        data=data,
        run_ids=run_ids,
        compare_fields=compare_fields,
    )

    print_report(results, run_ids, compare_fields)


if __name__ == "__main__":
    main()