#!/usr/bin/env python3
import json
import os
import time
from collections import deque
from typing import Any

import pandas as pd
import streamlit as st
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

BOOTSTRAP = os.getenv("BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("TOPIC", "intervention_events")
GROUP_ID = os.getenv("GROUP_ID", f"demo-dashboard-{int(time.time())}")
MAX_HISTORY = int(os.getenv("MAX_HISTORY", "100"))

RISK_ORDER = {
    "UNKNOWN": 0,
    "LOW": 1,
    "MODERATE": 2,
    "HIGH": 3,
    "VERY_HIGH": 4,
    "CRITICAL": 5,
}

ACTION_LABELS = {
    "MONITOR_ONLY": "Monitor only",
    "SEND_COPING_MESSAGE": "Send coping message",
    "SUPPRESS_INTERVENTION": "Suppress intervention",
}


def init_state():
    if "consumer" not in st.session_state:
        st.session_state.consumer = None
    if "latest_by_user" not in st.session_state:
        st.session_state.latest_by_user = {}
    if "event_history" not in st.session_state:
        st.session_state.event_history = deque(maxlen=MAX_HISTORY)
    if "consumer_connected" not in st.session_state:
        st.session_state.consumer_connected = False
    if "consumer_error" not in st.session_state:
        st.session_state.consumer_error = None
    if "messages_seen" not in st.session_state:
        st.session_state.messages_seen = 0


def risk_badge_html(risk_level: str) -> str:
    risk = (risk_level or "UNKNOWN").upper()
    color_map = {
        "LOW": "#d1fae5",
        "MODERATE": "#fef3c7",
        "HIGH": "#fed7aa",
        "VERY_HIGH": "#fecaca",
        "CRITICAL": "#fca5a5",
        "UNKNOWN": "#e5e7eb",
    }
    text_map = {
        "LOW": "#065f46",
        "MODERATE": "#92400e",
        "HIGH": "#9a3412",
        "VERY_HIGH": "#991b1b",
        "CRITICAL": "#7f1d1d",
        "UNKNOWN": "#374151",
    }
    bg = color_map.get(risk, "#e5e7eb")
    fg = text_map.get(risk, "#374151")
    return f"""
    <div style="
        display:inline-block;
        padding:0.35rem 0.7rem;
        border-radius:999px;
        background:{bg};
        color:{fg};
        font-weight:600;
        font-size:0.9rem;
    ">
        {risk}
    </div>
    """


def action_badge_html(action: str) -> str:
    action = action or "MONITOR_ONLY"
    color_map = {
        "MONITOR_ONLY": ("#dbeafe", "#1d4ed8"),
        "SEND_COPING_MESSAGE": ("#dcfce7", "#166534"),
        "SUPPRESS_INTERVENTION": ("#f3f4f6", "#374151"),
    }
    bg, fg = color_map.get(action, ("#f3f4f6", "#374151"))
    label = ACTION_LABELS.get(action, action)
    return f"""
    <div style="
        display:inline-block;
        padding:0.35rem 0.7rem;
        border-radius:999px;
        background:{bg};
        color:{fg};
        font-weight:600;
        font-size:0.9rem;
    ">
        {label}
    </div>
    """


def format_ts(ms: Any) -> str:
    if ms is None:
        return "-"
    try:
        return time.strftime("%H:%M:%S", time.localtime(int(ms) / 1000))
    except Exception:
        return "-"


def get_consumer():
    if st.session_state.consumer is not None:
        return st.session_state.consumer

    try:
        consumer = KafkaConsumer(
            TOPIC,
            bootstrap_servers=BOOTSTRAP,
            group_id=GROUP_ID,
            enable_auto_commit=True,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            key_deserializer=lambda k: k.decode("utf-8") if k else None,
            auto_offset_reset="earliest",
        )
        st.session_state.consumer = consumer
        st.session_state.consumer_connected = True
        st.session_state.consumer_error = None
        return consumer
    except NoBrokersAvailable as e:
        st.session_state.consumer_connected = False
        st.session_state.consumer_error = repr(e)
        return None
    except Exception as e:
        st.session_state.consumer_connected = False
        st.session_state.consumer_error = repr(e)
        return None


def poll_messages():
    consumer = get_consumer()
    if consumer is None:
        return

    try:
        records = consumer.poll(timeout_ms=1000, max_records=200)
        if not records:
            return

        for _, msgs in records.items():
            for msg in msgs:
                evt = msg.value or {}
                user_id = evt.get("user_id", "unknown")
                st.session_state.latest_by_user[user_id] = evt
                st.session_state.event_history.appendleft(evt)
                st.session_state.messages_seen += 1

        st.session_state.consumer_connected = True
        st.session_state.consumer_error = None
    except Exception as e:
        st.session_state.consumer_connected = False
        st.session_state.consumer_error = repr(e)


def make_summary():
    users = list(st.session_state.latest_by_user.values())
    total_users = len(users)
    high_risk = sum(1 for u in users if RISK_ORDER.get((u.get("risk_level") or "UNKNOWN").upper(), 0) >= 3)
    sent_msgs = sum(1 for u in users if u.get("action") == "SEND_COPING_MESSAGE")
    suppressed = sum(1 for u in users if u.get("action") == "SUPPRESS_INTERVENTION")
    return total_users, high_risk, sent_msgs, suppressed


def build_table_df() -> pd.DataFrame:
    users = list(st.session_state.latest_by_user.values())

    rows = []
    for evt in users:
        rows.append({
            "user_id": evt.get("user_id"),
            "risk_level": evt.get("risk_level"),
            "urgency": evt.get("urgency"),
            "decision": evt.get("decision"),
            "action": evt.get("action"),
            "intervention_type": evt.get("intervention_type"),
            "suppression_reason": evt.get("suppression_reason"),
            "message": evt.get("message"),
            "updated_at": format_ts(evt.get("intervention_ts_ms")),
        })

    if not rows:
        return pd.DataFrame(columns=[
            "user_id", "risk_level", "urgency", "decision", "action",
            "intervention_type", "suppression_reason", "message", "updated_at"
        ])

    df = pd.DataFrame(rows)
    df["risk_rank"] = df["risk_level"].map(lambda x: RISK_ORDER.get((x or "UNKNOWN").upper(), 0))
    df = df.sort_values(["risk_rank", "urgency"], ascending=[False, False]).drop(columns=["risk_rank"])
    return df.reset_index(drop=True)


def render_user_cards():
    users = list(st.session_state.latest_by_user.values())
    users = sorted(
        users,
        key=lambda u: (
            -RISK_ORDER.get((u.get("risk_level") or "UNKNOWN").upper(), 0),
            -(u.get("urgency") or 0),
        ),
    )

    if not users:
        st.info("No intervention events received yet.")
        return

    cols = st.columns(3)
    for idx, evt in enumerate(users):
        col = cols[idx % 3]
        with col:
            risk_html = risk_badge_html(evt.get("risk_level", "UNKNOWN"))
            action_html = action_badge_html(evt.get("action", "MONITOR_ONLY"))

            st.markdown(
                f"""
                <div style="
                    border:1px solid #e5e7eb;
                    border-radius:18px;
                    padding:1rem;
                    margin-bottom:1rem;
                    background:white;
                    box-shadow:0 2px 8px rgba(0,0,0,0.04);
                    min-height:280px;
                ">
                    <div style="font-size:1.15rem;font-weight:700;margin-bottom:0.5rem;">
                        {evt.get("user_id", "-")}
                    </div>
                    <div style="margin-bottom:0.5rem;">{risk_html}</div>
                    <div style="margin-bottom:0.5rem;">{action_html}</div>
                    <div><b>Urgency:</b> {evt.get("urgency", "-")}</div>
                    <div><b>Decision:</b> {evt.get("decision", "-")}</div>
                    <div><b>Type:</b> {evt.get("intervention_type", "-")}</div>
                    <div><b>Reason:</b> {evt.get("suppression_reason") or "-"}</div>
                    <div><b>Updated:</b> {format_ts(evt.get("intervention_ts_ms"))}</div>
                    <div style="margin-top:0.75rem;padding:0.75rem;border-radius:12px;background:#f9fafb;">
                        <b>Message</b><br/>
                        {evt.get("message") or "—"}
                    </div>
                </div>
                """,
                unsafe_allow_html=True,
            )


def render_recent_events():
    events = list(st.session_state.event_history)[:12]

    rows = []
    for evt in events:
        rows.append({
            "time": format_ts(evt.get("intervention_ts_ms")),
            "user_id": evt.get("user_id"),
            "risk_level": evt.get("risk_level"),
            "urgency": evt.get("urgency"),
            "decision": evt.get("decision"),
            "action": evt.get("action"),
            "message": evt.get("message") or "",
        })

    df = pd.DataFrame(rows)
    st.dataframe(df, width="stretch", hide_index=True)


def render_status():
    if st.session_state.consumer_connected:
        st.success(f"Connected to Kafka. Messages seen: {st.session_state.messages_seen}")
    else:
        st.error(f"Kafka not connected. Last error: {st.session_state.consumer_error or '-'}")

    st.caption(f"Bootstrap: {BOOTSTRAP} | Topic: {TOPIC} | Group ID: {GROUP_ID}")


def main():
    st.set_page_config(page_title="Relapse Risk Demo Dashboard", page_icon="📊", layout="wide")
    init_state()
    poll_messages()

    st.title("Relapse Risk Demo Dashboard")
    render_status()

    total_users, high_risk, sent_msgs, suppressed = make_summary()
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Active users", total_users)
    c2.metric("High-risk users", high_risk)
    c3.metric("Messages sent", sent_msgs)
    c4.metric("Suppressed", suppressed)

    st.markdown("### Current user state")
    render_user_cards()

    st.markdown("### Current user table")
    st.dataframe(build_table_df(), width="stretch", hide_index=True)

    st.markdown("### Recent intervention events")
    render_recent_events()

    time.sleep(2)
    st.rerun()


if __name__ == "__main__":
    main()