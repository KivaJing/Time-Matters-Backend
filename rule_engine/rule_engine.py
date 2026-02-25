import json
import time
import uuid
import os
from kafka import KafkaConsumer, KafkaProducer

from rules import evaluate_rules
from state_store import StateStore

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

def build_semantic_event(event: dict, rule_result: dict) -> dict:
    max_rt = event.get("max_rt", 0)
    min_rt = event.get("min_rt", 0)

    return {
        "semantic_event_id": str(uuid.uuid4()),
        "run_id": event.get("run_id"),
        "user_id": event.get("user_id"),
        "source_window_start_ms": event.get("window_start_ms"),
        "source_window_end_ms": event.get("window_end_ms"),
        "semantic_type": rule_result.get("semantic_type"),
        "severity": rule_result.get("severity"),
        "rule_id": rule_result.get("rule_id"),
        "engine_ts_ms": int(time.time() * 1000),
        "explanation": rule_result.get("explanation", ""),
        "evidence": {
            "event_count": event.get("event_count"),
            "max_rt": max_rt,
            "min_rt": min_rt,
            "span": max_rt - min_rt,
            "ksql_latency_ms": event.get("ksql_latency_ms"),
        },
    }

def parse_key_json_bytes(key_bytes) -> dict:
    if not key_bytes:
        return {}
    key_bytes = key_bytes.split(b"\x00", 1)[0]
    try:
        return json.loads(key_bytes.decode("utf-8"))
    except Exception:
        return {}

def lower_keys(d):
    return {k.lower(): v for k, v in d.items()} if isinstance(d, dict) else {}

def main():
    # 1) load config
    config_path = os.path.join(BASE_DIR, "config.json")
    with open(config_path, "r", encoding="utf-8") as f:
        config = json.load(f)

    kcfg = config["kafka"]

    in_topic = kcfg.get("input_topic", "complex_events_eval")
    out_topic = kcfg.get("output_topic", "semantic_events")

    # 2) init kafka (manual commit for correctness)
    consumer = KafkaConsumer(
        in_topic,
        bootstrap_servers=kcfg.get("bootstrap_servers", "localhost:9092"),
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        group_id=kcfg.get("group_id", "rule-engine-group"),
        key_deserializer=lambda m: m,  # keep bytes
        value_deserializer=lambda m: json.loads(m.decode("utf-8")) if m else None,
    )

    producer = KafkaProducer(
        bootstrap_servers=kcfg.get("bootstrap_servers", "localhost:9092"),
        key_serializer=lambda k: (k or "").encode("utf-8"),
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
    )

    # 3) init state store
    state = StateStore(config)

    print(f"Rule engine started... consuming={in_topic} producing={out_topic}")

    for msg in consumer:
        try:
            k = lower_keys(parse_key_json_bytes(msg.key))
            v = lower_keys(msg.value or {})
            event = {**v, **k}

            # Required fields for rule evaluation + semantic event emission
            required = [
                "run_id",
                "user_id",
                "window_start_ms",
                "window_end_ms",
                "event_count",
                "max_rt",
                "min_rt",
                "ksql_latency_ms",
            ]
            missing = [f for f in required if f not in event or event.get(f) is None]
            if missing:
                print("WARN missing fields:", missing, "raw_key_bytes=", msg.key)
                continue

            # A) dedup (ksql EMIT CHANGES can repeat the same window)
            if state.is_duplicate(event):
                # print("SKIP duplicate:", event.get("run_id"), event.get("user_id"), event.get("window_end_ms"))
                continue

            # B) evaluate rules
            rule_result = evaluate_rules(event, config, state)
            if not rule_result:
                continue

            # C) cooldown (per user + semantic_type)
            user_id = event["user_id"]
            sem_type = rule_result["semantic_type"]
            if state.in_cooldown(user_id, sem_type):
                continue

            # D) emit semantic event
            semantic_event = build_semantic_event(event, rule_result)

            producer.send(
                out_topic,
                key=user_id,
                value=semantic_event,
            )
            producer.flush()

            state.update_emit_time(user_id, sem_type)

            print("EMIT:", semantic_event)

        except Exception as e:
            print("ERROR processing message:", repr(e))
        finally:
            try:
                consumer.commit()
            except Exception as e:
                print("ERROR commit:", repr(e))


if __name__ == "__main__":
    main()