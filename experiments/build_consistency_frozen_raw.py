import json
import os
from pathlib import Path

base = Path(os.environ["CONS"])
src = base / "source" / "raw_all.kv"
dst = base / "source" / "frozen_raw.jsonl"

n = 0

with src.open("r", encoding="utf-8", errors="replace") as f, \
     dst.open("w", encoding="utf-8") as out:

    for line in f:
        line = line.rstrip("\n")
        _, sep, value = line.partition("\t")
        if not sep:
            value = line

        try:
            evt = json.loads(value)
        except Exception:
            continue

        if evt.get("run_id") != "consistency_source":
            continue

        out.write(json.dumps(evt, ensure_ascii=False) + "\n")
        n += 1

print("frozen events =", n)
