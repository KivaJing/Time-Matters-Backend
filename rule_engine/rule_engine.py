import json
import os
import time
import uuid
from typing import Dict, Any

from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable, KafkaError

from rules import evaluate_rules
from state_store import StateStore

BASE_DIR = os.path.dirname(os.path.abspath(__file__))


def build_semantic_event(event, rule_result):
    max_rt = event.get("max_rt", 0) or 0
    min_rt = event.get("min_rt", 0) or 0
    now_ms = int(time.time() * 1000)

    return {
        "semantic_event_id": str(uuid.uuid4()),
        "run_id": event.get("run_id"),
        "user_id": event.get("user_id"),

        "source_window_start_ms": event.get("window_start_ms"),
        "source_window_end_ms": event.get("window_end_ms"),

        "source_last_sent_at_ms": event.get("last_sent_at_ms"),
        "source_last_event_ts_ms": event.get("last_event_ts_ms"),
        "source_ksql_emit_ts_ms": event.get("ksql_emit_ts_ms"),
        "source_latest_scenario": event.get("latest_scenario"),

        "semantic_type": rule_result.get("semantic_type"),
        "severity": rule_result.get("severity"),
        "rule_id": rule_result.get("rule_id"),

        "engine_ts_ms": now_ms,

        "explanation": rule_result.get("explanation", ""),
        "evidence": {
            "event_count": event.get("event_count"),
            "max_rt": max_rt,
            "min_rt": min_rt,
            "span": max_rt - min_rt,
            "ksql_latency_ms": event.get("ksql_latency_ms"),
        },
    }

def parse_key_json_bytes(key_bytes: bytes | None) -> Dict[str, Any]:
    if not key_bytes:
        return {}

    key_bytes = key_bytes.split(b"\x00", 1)[0]

    last_brace = key_bytes.rfind(b"}")
    if last_brace != -1:
        key_bytes = key_bytes[: last_brace + 1]

    try:
        return json.loads(key_bytes.decode("utf-8"))
    except Exception:
        return {}


def lower_keys(d: Any) -> Any:
    if not isinstance(d, dict):
        return d
    return {str(k).lower(): v for k, v in d.items()}


def build_consumer_with_retry(*args, retries: int = 60, sleep_sec: float = 2.0, **kwargs) -> KafkaConsumer:
    last = None
    for i in range(retries):
        try:
            return KafkaConsumer(*args, **kwargs)
        except NoBrokersAvailable as e:
            last = e
            print(f"[rule_engine] Kafka consumer: no broker ({i+1}/{retries}), retry in {sleep_sec}s...")
            time.sleep(sleep_sec)
    raise last


def build_producer_with_retry(retries: int = 60, sleep_sec: float = 2.0, **kwargs) -> KafkaProducer:
    last = None
    for i in range(retries):
        try:
            return KafkaProducer(**kwargs)
        except NoBrokersAvailable as e:
            last = e
            print(f"[rule_engine] Kafka producer: no broker ({i+1}/{retries}), retry in {sleep_sec}s...")
            time.sleep(sleep_sec)
    raise last


def main():
    # 1) load config
    config_path = os.path.join(BASE_DIR, "config.json")
    with open(config_path, "r", encoding="utf-8") as f:
        config = json.load(f)

    kcfg = config.get("kafka", {})

    # ---- environment override (docker-friendly) ----
    bootstrap = os.getenv("BOOTSTRAP_SERVERS", kcfg.get("bootstrap_servers", "localhost:9092"))
    in_topic = os.getenv("INPUT_TOPIC", kcfg.get("input_topic", "complex_events_eval"))
    out_topic = os.getenv("OUTPUT_TOPIC", kcfg.get("output_topic", "semantic_events"))
    group_id = os.getenv("GROUP_ID", kcfg.get("group_id", "rule-engine-group"))

    # 2) init kafka (manual commit)
    consumer = build_consumer_with_retry(
        in_topic,
        bootstrap_servers=bootstrap,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        group_id=group_id,
        key_deserializer=lambda m: m,  # keep bytes
        value_deserializer=lambda m: json.loads(m.decode("utf-8")) if m else None,
        # NOTE: poll loop controls waiting; keep consumer alive forever
        consumer_timeout_ms=0,
    )

    producer = build_producer_with_retry(
        bootstrap_servers=bootstrap,
        key_serializer=lambda k: (k or "").encode("utf-8"),
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        acks="all",
        retries=5,
        linger_ms=20,
    )

    # 3) init state store
    state = StateStore(config)

    print(
        "[rule_engine] started"
        f" bootstrap={bootstrap} consuming={in_topic} producing={out_topic} group_id={group_id}",
        flush=True
    )

    try:
        while True:
            # poll returns {TopicPartition: [ConsumerRecord, ...], ...}
            records = consumer.poll(timeout_ms=1000, max_records=200)

            if not records:
                continue

            for tp, msgs in records.items():
                for msg in msgs:
                    try:
                        k = lower_keys(parse_key_json_bytes(msg.key))
                        v = lower_keys(msg.value or {})
                        event = {**v, **k}

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
                        missing = [f for f in required if event.get(f) is None]
                        if missing:
                            print("[rule_engine] WARN missing fields:", missing, "raw_key_bytes=", msg.key)
                            # do not commit: let it be retried if upstream fixes schema
                            continue

                        # A) dedup (ksql EMIT CHANGES can repeat the same window)
                        if state.is_duplicate(event):
                            consumer.commit()
                            continue

                        # B) evaluate rules
                        rule_result = evaluate_rules(event, config, state)
                        if not rule_result:
                            consumer.commit()
                            continue

                        # C) cooldown
                        user_id = event["user_id"]
                        sem_type = rule_result["semantic_type"]
                        if state.in_cooldown(user_id, sem_type):
                            consumer.commit()
                            continue

                        # D) emit semantic event (correctness-first)
                        semantic_event = build_semantic_event(event, rule_result)

                        future = producer.send(out_topic, key=user_id, value=semantic_event)
                        future.get(timeout=10)  # wait for broker ack

                        state.update_emit_time(user_id, sem_type)

                        consumer.commit()

                        print("[rule_engine] EMIT:", semantic_event, flush=True)

                    except KafkaError as e:
                        # Do not commit; message will be retried
                        print("[rule_engine] KAFKA ERROR:", repr(e))
                        time.sleep(1)

                    except Exception as e:
                        print("[rule_engine] ERROR processing message:", repr(e))
                        time.sleep(0.2)

    except KeyboardInterrupt:
        print("[rule_engine] stopping (KeyboardInterrupt)")

    finally:
        try:
            producer.flush(10)
        except Exception:
            pass
        try:
            producer.close()
        except Exception:
            pass
        try:
            consumer.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()