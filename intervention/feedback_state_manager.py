#!/usr/bin/env python3
import json
import os
import time
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

BOOTSTRAP = os.getenv("BOOTSTRAP", "localhost:9092")
FEEDBACK_TOPIC = os.getenv("FEEDBACK_TOPIC", "intervention_feedback")
GROUP_ID = os.getenv("GROUP_ID", "feedback-state-manager")
STATE_FILE = os.getenv("STATE_FILE", "./intervention_state.json")

COOLDOWN_AFTER_DISMISS_SEC = int(os.getenv("COOLDOWN_AFTER_DISMISS_SEC", "60"))
COOLDOWN_AFTER_COMPLETE_SEC = int(os.getenv("COOLDOWN_AFTER_COMPLETE_SEC", "180"))

state = {"users": {}}


def build_consumer_with_retry(*args, retries: int = 60, sleep_sec: float = 2.0, **kwargs):
    last = None
    for i in range(retries):
        try:
            return KafkaConsumer(*args, **kwargs)
        except NoBrokersAvailable as e:
            last = e
            print(f"[fsm] no broker ({i+1}/{retries}), retry in {sleep_sec}s...", flush=True)
            time.sleep(sleep_sec)
    raise last


def load_state():
    global state
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                state = json.load(f)
        except Exception:
            state = {"users": {}}


def save_state():
    tmp = STATE_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)
    os.replace(tmp, STATE_FILE)


def update_user(feedback: dict):
    user_id = feedback.get("user_id")
    if not user_id:
        return

    now_ms = int(time.time() * 1000)
    action = feedback.get("action")

    user = state["users"].setdefault(user_id, {})
    user["last_feedback_action"] = action
    user["last_feedback_ts_ms"] = feedback.get("feedback_ts_ms", now_ms)
    user["last_intervention_ts_ms"] = feedback.get("intervention_ts_ms")
    user["last_intervention_type"] = feedback.get("intervention_type")
    user["last_run_id"] = feedback.get("run_id")

    # new tracing fields
    user["last_event_id"] = feedback.get("event_id")
    user["last_bridge_sent_ts_ms"] = feedback.get("bridge_sent_ts_ms")
    user["last_bridge_feedback_received_ts_ms"] = feedback.get("bridge_feedback_received_ts_ms")
    user["last_unity_received_ts_ms"] = feedback.get("unity_received_ts_ms")
    user["last_unity_rendered_ts_ms"] = feedback.get("unity_rendered_ts_ms")
    user["last_unity_render_ms"] = feedback.get("unity_render_ms")
    user["last_bridge_roundtrip_ms"] = feedback.get("bridge_roundtrip_ms")

    if action == "intervention_dismissed":
        user["cooldown_until_ms"] = now_ms + COOLDOWN_AFTER_DISMISS_SEC * 1000
    elif action == "intervention_completed":
        user["cooldown_until_ms"] = now_ms + COOLDOWN_AFTER_COMPLETE_SEC * 1000
    elif action == "intervention_started":
        user.setdefault("cooldown_until_ms", 0)

    print(
        f"[fsm] user_id={user_id} event_id={user.get('last_event_id')} "
        f"action={action} unity_render_ms={user.get('last_unity_render_ms')} "
        f"bridge_roundtrip_ms={user.get('last_bridge_roundtrip_ms')} "
        f"cooldown_until_ms={user.get('cooldown_until_ms', 0)}",
        flush=True,
    )

def main():
    load_state()

    consumer = build_consumer_with_retry(
        FEEDBACK_TOPIC,
        bootstrap_servers=BOOTSTRAP,
        group_id=GROUP_ID,
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
        auto_offset_reset="latest",
    )

    print(f"[fsm] started bootstrap={BOOTSTRAP} topic={FEEDBACK_TOPIC} state_file={STATE_FILE}", flush=True)

    while True:
        records = consumer.poll(timeout_ms=1000, max_records=100)
        if not records:
            continue

        for _, msgs in records.items():
            for msg in msgs:
                try:
                    feedback = msg.value
                    if feedback.get("type") != "intervention_feedback":
                        continue
                    update_user(feedback)
                    save_state()
                except Exception as e:
                    print(f"[fsm] error: {e!r}", flush=True)
                    time.sleep(0.2)


if __name__ == "__main__":
    main()