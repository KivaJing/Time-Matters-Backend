#!/usr/bin/env python3
# rule_engine.py

import json
import os
import time
import uuid
from typing import Dict, Any, List

from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable, KafkaError

from rules import evaluate_rules
from state_store import StateStore

BASE_DIR = os.path.dirname(os.path.abspath(__file__))


def build_semantic_event(event: Dict[str, Any], rule_result: Dict[str, Any]) -> Dict[str, Any]:
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
        "routing_hint": rule_result.get("routing_hint", "rule_based"),
        "fast_llm_candidate": bool(rule_result.get("fast_llm_candidate", False)),
        "all_candidates": rule_result.get("all_candidates", []),
        "evidence": {
            "event_count": event.get("event_count"),
            "craving_avg": event.get("craving_avg"),
            "stress_avg": event.get("stress_avg"),
            "motivation_avg": event.get("motivation_avg"),
            "self_efficacy_avg": event.get("self_efficacy_avg"),
            "motivation_min": event.get("motivation_min"),
            "self_efficacy_min": event.get("self_efficacy_min"),
            "craving_max": event.get("craving_max"),
            "stress_max": event.get("stress_max"),
            "latest_strategy_use": event.get("latest_strategy_use"),
            "latest_receptivity": event.get("latest_receptivity"),
            "latest_is_moving": event.get("latest_is_moving"),
            "latest_in_meeting": event.get("latest_in_meeting"),
            "latest_location_type": event.get("latest_location_type"),
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
            print(
                f"[rule_engine] Kafka consumer: no broker ({i+1}/{retries}), retry in {sleep_sec}s...",
                flush=True,
            )
            time.sleep(sleep_sec)
    raise last


def build_producer_with_retry(retries: int = 60, sleep_sec: float = 2.0, **kwargs) -> KafkaProducer:
    last = None
    for i in range(retries):
        try:
            return KafkaProducer(**kwargs)
        except NoBrokersAvailable as e:
            last = e
            print(
                f"[rule_engine] Kafka producer: no broker ({i+1}/{retries}), retry in {sleep_sec}s...",
                flush=True,
            )
            time.sleep(sleep_sec)
    raise last


def main():
    config_path = os.path.join(BASE_DIR, "config.json")
    with open(config_path, "r", encoding="utf-8") as f:
        config = json.load(f)

    kcfg = config.get("kafka", {})
    bootstrap = os.getenv("BOOTSTRAP_SERVERS", kcfg.get("bootstrap_servers", "localhost:9092"))
    in_topic = os.getenv("INPUT_TOPIC", kcfg.get("input_topic", "risk_state_windows_new"))
    out_topic = os.getenv("OUTPUT_TOPIC", kcfg.get("output_topic", "semantic_events"))
    group_id = os.getenv("GROUP_ID", kcfg.get("group_id", "rule-engine"))
    emit_same_state_after_sec = float(
        os.getenv(
            "EMIT_SAME_STATE_AFTER_SEC",
            config.get("rule_engine", {}).get("emit_same_state_after_sec", 30.0),
        )
    )

    consumer = build_consumer_with_retry(
        in_topic,
        bootstrap_servers=bootstrap,
        auto_offset_reset="latest",   # changed
        enable_auto_commit=False,
        group_id=group_id,
        key_deserializer=lambda m: m,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")) if m else None,
        consumer_timeout_ms=0,
        max_poll_records=500,         # changed
    )

    producer = build_producer_with_retry(
        bootstrap_servers=bootstrap,
        key_serializer=lambda k: (k or "").encode("utf-8"),
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        acks="all",
        retries=5,
        linger_ms=20,
    )

    state = StateStore(config)

    print(
        "[rule_engine] started"
        f" bootstrap={bootstrap} consuming={in_topic} producing={out_topic} group_id={group_id}"
        f" emit_same_state_after_sec={emit_same_state_after_sec}",
        flush=True,
    )

    try:
        while True:
            records = consumer.poll(timeout_ms=500, max_records=500)
            if not records:
                continue

            sent_futures: List[Any] = []
            processed_count = 0

            try:
                for tp, msgs in records.items():
                    for msg in msgs:
                        k = lower_keys(parse_key_json_bytes(msg.key))
                        v = lower_keys(msg.value or {})
                        event = {**v, **k}

                        required = [
                            "run_id",
                            "user_id",
                            "window_start_ms",
                            "window_end_ms",
                            "event_count",
                            "craving_avg",
                            "stress_avg",
                            "motivation_avg",
                            "self_efficacy_avg",
                            "latest_receptivity",
                            "ksql_latency_ms",
                        ]
                        missing = [f for f in required if event.get(f) is None]
                        if missing:
                            continue

                        if state.is_duplicate(event):
                            processed_count += 1
                            continue

                        rule_result = evaluate_rules(event, config, state)
                        if not rule_result:
                            processed_count += 1
                            continue

                        run_id = str(event["run_id"])
                        user_id = str(event["user_id"])
                        sem_type = str(rule_result["semantic_type"])
                        severity = str(rule_result["severity"])

                        if not state.should_emit_on_state_change(
                            run_id=run_id,
                            user_id=user_id,
                            semantic_type=sem_type,
                            severity=severity,
                            max_same_state_silence_sec=emit_same_state_after_sec,
                        ):
                            processed_count += 1
                            continue

                        semantic_event = build_semantic_event(event, rule_result)
                        fut = producer.send(out_topic, key=user_id, value=semantic_event)
                        sent_futures.append((fut, run_id, user_id, sem_type, severity, semantic_event))
                        processed_count += 1

                for fut, run_id, user_id, sem_type, severity, semantic_event in sent_futures:
                    fut.get(timeout=10)
                    state.update_selected_state(
                        run_id=run_id,
                        user_id=user_id,
                        semantic_type=sem_type,
                        severity=severity,
                    )

                consumer.commit()

            except KafkaError as e:
                print("[rule_engine] KAFKA ERROR:", repr(e), flush=True)
                time.sleep(1)
            except Exception as e:
                print("[rule_engine] ERROR processing batch:", repr(e), flush=True)
                time.sleep(0.2)

    except KeyboardInterrupt:
        print("[rule_engine] stopping (KeyboardInterrupt)", flush=True)
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