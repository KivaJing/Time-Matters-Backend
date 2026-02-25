#!/usr/bin/env python3
import json
import time
from datetime import datetime
from confluent_kafka import Consumer

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "complex_events_eval"

def pick(d, *keys, default=None):
    for k in keys:
        if isinstance(d, dict) and k in d and d[k] is not None:
            return d[k]
    return default

def parse_ksql_key(key_bytes: bytes) -> dict:
    if not key_bytes:
        return {}

    s = key_bytes.decode("utf-8", errors="ignore").strip()

    # Find JSON object bounds
    start = s.find("{")
    end = s.rfind("}")
    if start == -1 or end == -1 or end <= start:
        return {"__raw_key": s}

    json_part = s[start : end + 1]
    try:
        obj = json.loads(json_part)
        return obj if isinstance(obj, dict) else {"__raw_key": s}
    except Exception:
        return {"__raw_key": s, "__json_part": json_part}

def main():
    c = Consumer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": f"observer-{int(time.time())}",
        "auto.offset.reset": "latest",
        "enable.auto.commit": True,
    })

    c.subscribe([TOPIC])
    print("Observer started, listening to:", TOPIC)

    try:
        while True:
            msg = c.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print("consumer error:", msg.error())
                continue

            now_ms = int(time.time() * 1000)

            raw_key = msg.key()
            raw_val = msg.value()

            # ---- decode VALUE (pure JSON) ----
            event = {}
            if raw_val:
                try:
                    event = json.loads(raw_val.decode("utf-8"))
                except Exception as e:
                    print("VALUE json decode failed:", e, "raw_val_head=", raw_val[:200])
                    continue

            # ---- decode KEY (JSON prefix + binary suffix) ----
            key_obj = parse_ksql_key(raw_key)

            run_id  = pick(event, "run_id", "RUN_ID")  or pick(key_obj, "run_id", "RUN_ID")
            user_id = pick(event, "user_id", "USER_ID") or pick(key_obj, "user_id", "USER_ID")

            last_event_ts = pick(event, "last_event_ts_ms", "LAST_EVENT_TS_MS", "last_event_ts", "LAST_EVENT_TS")
            ksql_emit_ts  = pick(event, "ksql_emit_ts_ms", "KSQL_EMIT_TS_MS")
            event_count   = pick(event, "event_count", "EVENT_COUNT")

            if last_event_ts is None:
                # print("missing last_event_ts, key_obj=", key_obj, "value=", event)
                continue

            e2e_total_ms = now_ms - int(last_event_ts)

            e2e_ksql_ms = None
            e2e_kafka_consumer_ms = None
            if ksql_emit_ts is not None:
                ksql_emit_ts = int(ksql_emit_ts)
                e2e_ksql_ms = ksql_emit_ts - int(last_event_ts)
                e2e_kafka_consumer_ms = now_ms - ksql_emit_ts

            ts = datetime.now().isoformat(timespec="seconds")

            if e2e_ksql_ms is None:
                print(f"[{ts}] run_id={run_id} user_id={user_id} count={event_count} E2E_total_ms={e2e_total_ms}")
            else:
                print(
                    f"[{ts}] run_id={run_id} user_id={user_id} count={event_count} "
                    f"E2E_total_ms={e2e_total_ms} E2E_ksql_ms={e2e_ksql_ms} "
                    f"E2E_kafka_consumer_ms={e2e_kafka_consumer_ms}"
                )
    finally:
        c.close()

if __name__ == "__main__":
    main()
