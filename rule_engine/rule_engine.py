import json
import time
import uuid
from kafka import KafkaConsumer, KafkaProducer

from rules import evaluate_rules
from state_store import StateStore

def build_semantic_event(event, rule_result):
    return {
        "semantic_event_id": str(uuid.uuid4()),
        "run_id": event["run_id"],
        "user_id": event["user_id"],
        "source_window_start_ms": event["window_start_ms"],
        "source_window_end_ms": event["window_end_ms"],
        "semantic_type": rule_result["semantic_type"],
        "severity": rule_result["severity"],
        "rule_id": rule_result["rule_id"],
        "engine_ts_ms": int(time.time() * 1000),
        "explanation": rule_result.get("explanation", ""),
        "evidence": {
            "event_count": event["event_count"],
            "max_rt": event["max_rt"],
            "min_rt": event["min_rt"],
            "span": event["max_rt"] - event["min_rt"],
            "ksql_latency_ms": event["ksql_latency_ms"]
        }
    }

# 1) load config
with open("config.json", "r", encoding="utf-8") as f:
    config = json.load(f)

kcfg = config["kafka"]

# 2) init kafka
consumer = KafkaConsumer(
    "complex_events_eval",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="rule-engine-group",
    key_deserializer=lambda m: m.decode("utf-8", errors="ignore") if m else None,
    value_deserializer=lambda m: json.loads(m.decode("utf-8")) if m else None,
)

producer = KafkaProducer(
    bootstrap_servers=kcfg["bootstrap_servers"],
    key_serializer=lambda k: k.encode("utf-8"),
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def parse_key_json(key_str: str) -> dict:
    if not key_str:
        return {}
    i = key_str.find("{")
    j = key_str.rfind("}")
    if i == -1 or j == -1 or j <= i:
        return {}
    try:
        return json.loads(key_str[i:j+1])
    except Exception:
        return {}

def lower_keys(d):
    return {k.lower(): v for k, v in d.items()} if isinstance(d, dict) else {}

# 3) init state store
state = StateStore(config)

print("Rule engine started...")

for msg in consumer:
    k = lower_keys(parse_key_json(msg.key))
    v = lower_keys(msg.value or {})
    event = {**v, **k} 

    # A) dedup (EMIT CHANGES replicates)
    if state.is_duplicate(event):
        continue

    # B) evaluate rules
    rule_result = evaluate_rules(event, config, state)
    if not rule_result:
        continue

    # C) cooldown 
    user_id = event["user_id"]
    sem_type = rule_result["semantic_type"]
    if state.in_cooldown(user_id, sem_type):
        continue

    # D) emit semantic event
    semantic_event = build_semantic_event(event, rule_result)

    producer.send(
        kcfg["output_topic"],
        key=event["user_id"],  
        value=semantic_event
    )
    producer.flush()

    state.update_emit_time(user_id, sem_type)

    print("EMIT:", semantic_event)