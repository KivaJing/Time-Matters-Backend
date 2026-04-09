#!/usr/bin/env python3
import json
import os
import time
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable, KafkaError

BOOTSTRAP = os.getenv("BOOTSTRAP", "localhost:9092")
IN_TOPIC = os.getenv("IN_TOPIC", "reasoning_events")
OUT_TOPIC = os.getenv("OUT_TOPIC", "intervention_events")
GROUP_ID = os.getenv("GROUP_ID", "intervention-engine")
STATE_FILE = os.getenv("STATE_FILE", "./intervention_state.json")


def build_consumer_with_retry(*args, retries: int = 60, sleep_sec: float = 2.0, **kwargs) -> KafkaConsumer:
    last = None
    for i in range(retries):
        try:
            return KafkaConsumer(*args, **kwargs)
        except NoBrokersAvailable as e:
            last = e
            print(f"[intervention_engine] Kafka consumer: no broker ({i+1}/{retries}), retry in {sleep_sec}s...")
            time.sleep(sleep_sec)
    raise last


def build_producer_with_retry(retries: int = 60, sleep_sec: float = 2.0, **kwargs) -> KafkaProducer:
    last = None
    for i in range(retries):
        try:
            return KafkaProducer(**kwargs)
        except NoBrokersAvailable as e:
            last = e
            print(f"[intervention_engine] Kafka producer: no broker ({i+1}/{retries}), retry in {sleep_sec}s...")
            time.sleep(sleep_sec)
    raise last


def load_state():
    if not os.path.exists(STATE_FILE):
        return {"users": {}}
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"users": {}}


def get_user_state(user_id: str):
    st = load_state()
    return (st.get("users") or {}).get(user_id, {})


def decide_action(evt: dict) -> dict:
    decision = evt.get("decision", "MONITOR")
    urgency = int(evt.get("urgency", 0) or 0)
    evidence = evt.get("evidence", {}) or {}
    intervention_type = evt.get("intervention_type", "none")
    user_id = evt.get("user_id")

    receptivity = int(evidence.get("latest_receptivity", 1) or 0)
    is_moving = int(evidence.get("latest_is_moving", 0) or 0)
    in_meeting = int(evidence.get("latest_in_meeting", 0) or 0)

    action = "MONITOR_ONLY"
    suppression_reason = None
    message = None

    now_ms = int(time.time() * 1000)
    user_state = get_user_state(user_id)
    cooldown_until_ms = int(user_state.get("cooldown_until_ms", 0) or 0)

    if cooldown_until_ms > now_ms:
        action = "SUPPRESS_INTERVENTION"
        suppression_reason = "user_cooldown"
    elif decision == "SUPPRESS" or receptivity == 0:
        action = "SUPPRESS_INTERVENTION"
        suppression_reason = "low_receptivity"
    elif in_meeting == 1:
        action = "SUPPRESS_INTERVENTION"
        suppression_reason = "in_meeting"
    elif is_moving == 1 and urgency < 80:
        action = "MONITOR_ONLY"
        suppression_reason = "moving_context"
    elif decision == "INTERVENE" and urgency >= 60:
        action = "SEND_COPING_MESSAGE"
        recs = evt.get("recommendations", [])
        message = recs[0] if recs else "Pause, notice the urge, and try one regulation strategy now."

    return {
        "run_id": evt.get("run_id"),
        "user_id": user_id,
        "window_start_ms": evt.get("window_start_ms"),
        "window_end_ms": evt.get("window_end_ms"),
        "reasoning_ts_ms": evt.get("reasoning_ts_ms"),
        "intervention_ts_ms": int(time.time() * 1000),
        "risk_level": evt.get("risk_level"),
        "urgency": urgency,
        "decision": decision,
        "intervention_type": intervention_type,
        "action": action,
        "suppression_reason": suppression_reason,
        "message": message,
        "source_last_sent_at_ms": evt.get("source_last_sent_at_ms"),
        "source_ksql_emit_ts_ms": evt.get("source_ksql_emit_ts_ms"),
        "semantic_engine_ts_ms": evt.get("semantic_engine_ts_ms"),
        "scheduler_ts_ms": evt.get("scheduler_ts_ms"),
    }


def main():
    consumer = build_consumer_with_retry(
        IN_TOPIC,
        bootstrap_servers=BOOTSTRAP,
        group_id=GROUP_ID,
        enable_auto_commit=False,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
        auto_offset_reset="latest",
    )

    producer = build_producer_with_retry(
        bootstrap_servers=BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        linger_ms=20,
        retries=3,
        acks="all",
    )

    print(f"[intervention_engine] started bootstrap={BOOTSTRAP} consuming={IN_TOPIC} producing={OUT_TOPIC}", flush=True)

    try:
        while True:
            records = consumer.poll(timeout_ms=1000, max_records=100)
            if not records:
                continue

            for tp, msgs in records.items():
                for msg in msgs:
                    try:
                        evt = msg.value
                        key = msg.key or ""
                        out = decide_action(evt)
                        fut = producer.send(OUT_TOPIC, key=key, value=out)
                        fut.get(timeout=10)
                        consumer.commit()
                        print(
                            f"[intervention_engine] emitted intervention_event user_id={out.get('user_id')} action={out.get('action')} suppression_reason={out.get('suppression_reason')}",
                            flush=True,
                        )
                    except KafkaError as e:
                        print("[intervention_engine] KAFKA ERROR:", repr(e))
                        time.sleep(1)
                    except Exception as e:
                        print("[intervention_engine] ERROR:", repr(e))
                        time.sleep(0.2)

    except KeyboardInterrupt:
        print("[intervention_engine] stopping (KeyboardInterrupt)")
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