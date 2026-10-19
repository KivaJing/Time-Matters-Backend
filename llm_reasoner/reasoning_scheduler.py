#!/usr/bin/env python3
# reasoning_scheduler.py

import json
import os
import time
from collections import deque
from typing import Dict, Any, Tuple, List

from kafka import KafkaConsumer, KafkaProducer
from kafka.structs import TopicPartition, OffsetAndMetadata
from kafka.errors import NoBrokersAvailable


BOOTSTRAP = os.getenv("BOOTSTRAP", "localhost:9092")

IN_TOPIC = os.getenv("IN_TOPIC", "semantic_events")
OUT_REQUEST_TOPIC = os.getenv("OUT_REQUEST_TOPIC", "reasoning_requests")
OUT_FAST_REQUEST_TOPIC = os.getenv("OUT_FAST_REQUEST_TOPIC", "fast_reasoning_requests")
OUT_REASONING_TOPIC = os.getenv("OUT_REASONING_TOPIC", "reasoning_events")
OUT_DROPPED_TOPIC = os.getenv("OUT_DROPPED_TOPIC", "reasoning_dropped_events")

GROUP_ID = os.getenv("GROUP_ID", "reasoning-scheduler")

COALESCE_SEC = float(os.getenv("COALESCE_SEC", "0.2"))
MAX_PENDING = int(os.getenv("MAX_PENDING", "256"))
MAX_PENDING_PER_USER = int(os.getenv("MAX_PENDING_PER_USER", "3"))

ENABLE_PRIORITY_FILTER = os.getenv("ENABLE_PRIORITY_FILTER", "true").lower() == "true"
DROP_MEDIUM_WHEN_PENDING_GE = int(os.getenv("DROP_MEDIUM_WHEN_PENDING_GE", "64"))
DROP_LOW_ALWAYS = os.getenv("DROP_LOW_ALWAYS", "true").lower() == "true"

ENABLE_DROPPED_TOPIC = os.getenv("ENABLE_DROPPED_TOPIC", "true").lower() == "true"

ENABLE_USER_COOLDOWN = os.getenv("ENABLE_USER_COOLDOWN", "true").lower() == "true"
USER_COOLDOWN_SEC = float(os.getenv("USER_COOLDOWN_SEC", "2"))

MAX_LLM_REQUESTS_PER_SEC = float(os.getenv("MAX_LLM_REQUESTS_PER_SEC", "4"))
MAX_FAST_LLM_REQUESTS_PER_SEC = float(os.getenv("MAX_FAST_LLM_REQUESTS_PER_SEC", "6"))

SCHEDULER_VERSION = os.getenv("SCHEDULER_VERSION", "v4_scored")

FORCE_ROUTE = os.getenv("FORCE_ROUTE", "").strip().lower()
DISABLE_LOAD_CONTROL = (
    os.getenv("DISABLE_LOAD_CONTROL", "false").strip().lower() == "true"
)

VALID_FORCE_ROUTES = {"", "rule_based", "fast_llm", "llm"}

if FORCE_ROUTE not in VALID_FORCE_ROUTES:
    raise ValueError(
        f"Invalid FORCE_ROUTE={FORCE_ROUTE!r}; "
        f"expected one of {sorted(VALID_FORCE_ROUTES)}"
    )

def build_consumer_with_retry(*args, retries: int = 60, sleep_sec: float = 2.0, **kwargs) -> KafkaConsumer:
    last = None
    for i in range(retries):
        try:
            return KafkaConsumer(*args, **kwargs)
        except NoBrokersAvailable as e:
            last = e
            print(
                f"[reasoning_scheduler] Kafka consumer: no broker ({i+1}/{retries}), retry in {sleep_sec}s...",
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
                f"[reasoning_scheduler] Kafka producer: no broker ({i+1}/{retries}), retry in {sleep_sec}s...",
                flush=True,
            )
            time.sleep(sleep_sec)
    raise last


class CommitTracker:
    def __init__(self, consumer: KafkaConsumer):
        self.consumer = consumer
        self.next_commit_offset: Dict[Tuple[str, int], int] = {}
        self.completed_offsets: Dict[Tuple[str, int], set] = {}

    def register_seen(self, topic: str, partition: int, offset: int) -> None:
        key = (topic, partition)
        if key not in self.next_commit_offset:
            self.next_commit_offset[key] = offset
            self.completed_offsets[key] = set()

    def mark_done(self, topic: str, partition: int, offset: int) -> None:
        key = (topic, partition)
        if key not in self.next_commit_offset:
            self.next_commit_offset[key] = offset
            self.completed_offsets[key] = set()
        self.completed_offsets[key].add(offset)

    def _make_offset_meta(self, next_offset: int):
        try:
            return OffsetAndMetadata(next_offset, None, -1)
        except TypeError:
            return OffsetAndMetadata(next_offset, None)

    def commit_contiguous(self) -> None:
        commits = {}

        for (topic, partition), next_offset in list(self.next_commit_offset.items()):
            done_set = self.completed_offsets[(topic, partition)]
            advanced = False

            while next_offset in done_set:
                done_set.remove(next_offset)
                next_offset += 1
                advanced = True

            if advanced:
                self.next_commit_offset[(topic, partition)] = next_offset
                commits[(topic, partition)] = next_offset

        if not commits:
            return

        commit_map = {}
        for (topic, partition), next_offset in commits.items():
            tp = TopicPartition(topic, partition)
            commit_map[tp] = self._make_offset_meta(next_offset)

        self.consumer.commit(offsets=commit_map)

        for (topic, partition), next_offset in commits.items():
            print(
                f"[reasoning_scheduler] committed topic={topic} partition={partition} next_offset={next_offset}",
                flush=True,
            )


def make_coalesce_key(semantic_event: Dict[str, Any]) -> Tuple[str, str, str, str]:
    return (
        str(semantic_event.get("run_id")),
        str(semantic_event.get("user_id")),
        str(semantic_event.get("semantic_type")),
        str(semantic_event.get("severity")),
    )


def pending_count_for_user(
    pending_coalesce: Dict[Tuple[str, str, str, str], Dict[str, Any]],
    user_id: str,
) -> int:
    n = 0
    for _, entry in pending_coalesce.items():
        if str(entry["semantic_event"].get("user_id")) == str(user_id):
            n += 1
    return n


def user_in_cooldown(user_id: str, last_enqueued_by_user: Dict[str, float]) -> bool:
    if DISABLE_LOAD_CONTROL:
        return False

    if not ENABLE_USER_COOLDOWN:
        return False

    last_ts = last_enqueued_by_user.get(str(user_id))
    if last_ts is None:
        return False

    return (time.time() - last_ts) < USER_COOLDOWN_SEC

def admission_allowed(recent_enqueues: deque, max_rate_per_sec: float) -> bool:
    if DISABLE_LOAD_CONTROL:
        return True

    now = time.time()
    while recent_enqueues and (now - recent_enqueues[0]) > 1.0:
        recent_enqueues.popleft()
    return len(recent_enqueues) < max_rate_per_sec


def build_reasoning_output_base(semantic_event: Dict[str, Any], llm_ms: int = 0) -> Dict[str, Any]:
    return {
        "run_id": semantic_event.get("run_id"),
        "user_id": semantic_event.get("user_id"),
        "window_start_ms": semantic_event.get("source_window_start_ms"),
        "window_end_ms": semantic_event.get("source_window_end_ms"),
        "semantic_type": semantic_event.get("semantic_type"),
        "severity": semantic_event.get("severity"),
        "rule_id": semantic_event.get("rule_id"),
        "source_last_sent_at_ms": semantic_event.get("source_last_sent_at_ms"),
        "source_last_event_ts_ms": semantic_event.get("source_last_event_ts_ms"),
        "source_ksql_emit_ts_ms": semantic_event.get("source_ksql_emit_ts_ms"),
        "semantic_engine_ts_ms": semantic_event.get("engine_ts_ms"),
        "reasoning_ts_ms": int(time.time() * 1000),
        "model": None,
        "prompt_version": None,
        "schema_version": "v2",
        "llm_latency_ms": llm_ms,
        "evidence": semantic_event.get("evidence", {}),
        "scheduler_version": SCHEDULER_VERSION,
    }


def build_bypass_output(semantic_event: Dict[str, Any]) -> Dict[str, Any] | None:
    semantic_type = semantic_event.get("semantic_type")
    out = build_reasoning_output_base(semantic_event, llm_ms=0)

    if semantic_type == "LOW_RECEPTIVITY_CONTEXT":
        out.update({
            "risk_level": "MODERATE",
            "urgency": 20,
            "decision": "SUPPRESS",
            "intervention_type": "none",
            "recommendations": [],
            "rationale": "Intervention is suppressed because receptivity is low in the current context.",
            "confidence": 1.0,
            "error": None,
            "reasoning_mode": "bypass_rule",
            "reasoning_route": "bypass_rule",
            "bypass_rule": "LOW_RECEPTIVITY_CONTEXT",
        })
        return out

    return None


def build_rule_based_reasoning_output(semantic_event: Dict[str, Any]) -> Dict[str, Any]:
    semantic_type = str(semantic_event.get("semantic_type") or "")
    severity = str(semantic_event.get("severity") or "UNKNOWN").upper()

    out = build_reasoning_output_base(semantic_event, llm_ms=0)

    if semantic_type == "RISING_STRESS_TRAJECTORY":
        out.update({
            "risk_level": "HIGH" if severity in {"HIGH", "VERY_HIGH"} else "MODERATE",
            "urgency": 70 if severity in {"HIGH", "VERY_HIGH"} else 55,
            "decision": "INTERVENE",
            "intervention_type": "stress_regulation",
            "recommendations": ["Try a brief grounding or breathing exercise now."],
            "rationale": "Stress has risen across consecutive windows, suggesting increasing momentary risk.",
            "confidence": 0.9,
            "error": None,
            "reasoning_mode": "rule_based",
            "reasoning_route": "rule_based",
            "bypass_rule": "DIRECT_STRESS_RULE",
        })
        return out

    if semantic_type == "RISING_CRAVING_TRAJECTORY":
        out.update({
            "risk_level": "HIGH" if severity in {"HIGH", "VERY_HIGH"} else "MODERATE",
            "urgency": 70 if severity in {"HIGH", "VERY_HIGH"} else 55,
            "decision": "INTERVENE",
            "intervention_type": "craving_coping",
            "recommendations": ["Use a coping strategy now to interrupt rising craving."],
            "rationale": "Craving has risen across consecutive windows, suggesting increasing momentary risk.",
            "confidence": 0.9,
            "error": None,
            "reasoning_mode": "rule_based",
            "reasoning_route": "rule_based",
            "bypass_rule": "DIRECT_CRAVING_RULE",
        })
        return out

    if semantic_type == "SUSTAINED_ELEVATED_CRAVING":
        out.update({
            "risk_level": "HIGH" if severity in {"HIGH", "VERY_HIGH"} else "MODERATE",
            "urgency": 70 if severity in {"HIGH", "VERY_HIGH"} else 55,
            "decision": "INTERVENE",
            "intervention_type": "craving_coping",
            "recommendations": [
                "Use a coping strategy now to manage sustained elevated craving."
            ],
            "rationale": (
                "Craving has remained elevated across consecutive windows, "
                "indicating persistent momentary risk."
            ),
            "confidence": 0.9,
            "error": None,
            "reasoning_mode": "rule_based",
            "reasoning_route": "rule_based",
            "bypass_rule": "DIRECT_SUSTAINED_CRAVING_RULE",
        })
        return out

    # The mappings below are used by the rule-only experimental baseline.
    # They do not alter the default full-hybrid routing policy.
    if semantic_type == "COMBINED_RELAPSE_RISK":
        out.update({
            "risk_level": "VERY_HIGH" if severity == "VERY_HIGH" else "HIGH",
            "urgency": 90 if severity == "VERY_HIGH" else 80,
            "decision": "INTERVENE",
            "intervention_type": "craving_coping, stress_regulation",
            "recommendations": [],
            "rationale": "Deterministic rule-only baseline for combined relapse risk.",
            "confidence": 1.0,
            "error": None,
            "reasoning_mode": "rule_based",
            "reasoning_route": "rule_based",
            "bypass_rule": "BASELINE_COMBINED_RISK_RULE",
        })
        return out

    if semantic_type == "ELEVATED_STRESS":
        high = severity in {"HIGH", "VERY_HIGH"}
        out.update({
            "risk_level": "HIGH" if high else "MODERATE",
            "urgency": 65 if high else 45,
            "decision": "INTERVENE" if high else "MONITOR",
            "intervention_type": "stress_regulation" if high else "none",
            "recommendations": [],
            "rationale": "Deterministic rule-only baseline for elevated stress.",
            "confidence": 1.0,
            "error": None,
            "reasoning_mode": "rule_based",
            "reasoning_route": "rule_based",
            "bypass_rule": "BASELINE_ELEVATED_STRESS_RULE",
        })
        return out

    if semantic_type == "ELEVATED_CRAVING":
        high = severity in {"HIGH", "VERY_HIGH"}
        out.update({
            "risk_level": "HIGH" if high else "MODERATE",
            "urgency": 65 if high else 45,
            "decision": "INTERVENE" if high else "MONITOR",
            "intervention_type": "craving_coping" if high else "none",
            "recommendations": [],
            "rationale": "Deterministic rule-only baseline for elevated craving.",
            "confidence": 1.0,
            "error": None,
            "reasoning_mode": "rule_based",
            "reasoning_route": "rule_based",
            "bypass_rule": "BASELINE_ELEVATED_CRAVING_RULE",
        })
        return out

    if semantic_type in {"LOW_MOTIVATION", "LOW_SELF_EFFICACY"}:
        out.update({
            "risk_level": "HIGH" if severity in {"HIGH", "VERY_HIGH"} else "MODERATE",
            "urgency": 55 if severity in {"HIGH", "VERY_HIGH"} else 40,
            "decision": "MONITOR",
            "intervention_type": "none",
            "recommendations": [],
            "rationale": f"Deterministic rule-only baseline for {semantic_type.lower()}.",
            "confidence": 1.0,
            "error": None,
            "reasoning_mode": "rule_based",
            "reasoning_route": "rule_based",
            "bypass_rule": "BASELINE_MOTIVATIONAL_RULE",
        })
        return out

    out.update({
        "risk_level": "UNKNOWN",
        "urgency": 0,
        "decision": "MONITOR",
        "intervention_type": "none",
        "recommendations": [],
        "rationale": "No rule-based reasoning route matched this event.",
        "confidence": 0.0,
        "error": None,
        "reasoning_mode": "rule_based",
        "reasoning_route": "rule_based",
        "bypass_rule": "DEFAULT_RULE_BASED",
    })
    return out

def build_request_payload(semantic_event: Dict[str, Any], route: str) -> Dict[str, Any]:
    now_ms = int(time.time() * 1000)
    payload = dict(semantic_event)
    payload["request_id"] = f"{semantic_event.get('user_id')}:{now_ms}"
    payload["scheduler_ts_ms"] = now_ms
    payload["scheduler_version"] = SCHEDULER_VERSION
    payload["reasoning_route"] = route
    return payload


def build_dropped_event(semantic_event: Dict[str, Any], reason: str, detail: str | None = None) -> Dict[str, Any]:
    return {
        "run_id": semantic_event.get("run_id"),
        "user_id": semantic_event.get("user_id"),
        "semantic_type": semantic_event.get("semantic_type"),
        "severity": semantic_event.get("severity"),
        "rule_id": semantic_event.get("rule_id"),
        "source_last_sent_at_ms": semantic_event.get("source_last_sent_at_ms"),
        "source_last_event_ts_ms": semantic_event.get("source_last_event_ts_ms"),
        "source_ksql_emit_ts_ms": semantic_event.get("source_ksql_emit_ts_ms"),
        "semantic_engine_ts_ms": semantic_event.get("engine_ts_ms"),
        "scheduler_ts_ms": int(time.time() * 1000),
        "scheduler_version": SCHEDULER_VERSION,
        "drop_reason": reason,
        "drop_detail": detail,
    }


def should_drop_by_priority(
    semantic_event: Dict[str, Any],
    pending_coalesce: Dict[Tuple[str, str, str, str], Dict[str, Any]],
) -> Tuple[bool, str | None]:

    if DISABLE_LOAD_CONTROL:
        return False, None

    if not ENABLE_PRIORITY_FILTER:
        return False, None

    severity = str(semantic_event.get("severity") or "").upper()
    pending_size = len(pending_coalesce)

    if severity == "LOW" and DROP_LOW_ALWAYS:
        return True, "low_severity"

    if severity == "MEDIUM" and pending_size >= DROP_MEDIUM_WHEN_PENDING_GE:
        return True, "medium_under_pressure"

    return False, None


def compute_priority_score(evt: Dict[str, Any], waiting_sec: float = 0.0) -> float:
    semantic_type = str(evt.get("semantic_type") or "")
    severity = str(evt.get("severity") or "").upper()
    evidence = evt.get("evidence") or {}

    craving = float(evidence.get("craving_avg") or 0.0)
    stress = float(evidence.get("stress_avg") or 0.0)

    motivation_raw = evidence.get("motivation_avg")
    self_efficacy_raw = evidence.get("self_efficacy_avg")

    motivation = 10.0 if motivation_raw is None else float(motivation_raw)
    self_efficacy = 10.0 if self_efficacy_raw is None else float(self_efficacy_raw)

    score = 0.0

    severity_weight = {
        "VERY_HIGH": 100.0,
        "HIGH": 70.0,
        "MEDIUM": 40.0,
        "LOW": 10.0,
    }
    score += severity_weight.get(severity, 0.0)

    type_weight = {
        "COMBINED_RELAPSE_RISK": 35.0,
        "RISING_STRESS_TRAJECTORY": 18.0,
        "RISING_CRAVING_TRAJECTORY": 18.0,
        "SUSTAINED_ELEVATED_CRAVING": 15.0,
        "ELEVATED_STRESS": 12.0,
        "ELEVATED_CRAVING": 12.0,
        "LOW_MOTIVATION": 10.0,
        "LOW_SELF_EFFICACY": 10.0,
    }
    score += type_weight.get(semantic_type, 0.0)

    evidence_strength = max(
        craving,
        stress,
        max(0.0, 10.0 - motivation),
        max(0.0, 10.0 - self_efficacy),
    )

    score += min(20.0, evidence_strength / 5.0)

    # Aging bonus prevents starvation of events that remain pending.
    score += min(15.0, waiting_sec * 5.0)

    return score


def choose_route_with_score(evt: Dict[str, Any], score: float) -> str:
    semantic_type = str(evt.get("semantic_type") or "")
    severity = str(evt.get("severity") or "").upper()
    if FORCE_ROUTE:
        return FORCE_ROUTE
    evidence = evt.get("evidence") or {}

    # routing_hint is retained as metadata but does not override
    # scheduler-level routing decisions.
    routing_hint = str(evt.get("routing_hint") or "").strip().lower()

    # Combined or very-high-risk states use the high-capacity LLM.
    if semantic_type == "COMBINED_RELAPSE_RISK" or severity == "VERY_HIGH":
        return "llm"

    # Stable temporal trajectories are handled deterministically.
    if semantic_type == "RISING_STRESS_TRAJECTORY":
        return "rule_based"

    if semantic_type == "RISING_CRAVING_TRAJECTORY":
        return "rule_based"

    if semantic_type == "SUSTAINED_ELEVATED_CRAVING":
        return "rule_based"

    # Single-variable and motivational states use the fast LLM
    # unless the priority score indicates a stronger condition.
    fast_candidate_types = {
        "ELEVATED_STRESS",
        "ELEVATED_CRAVING",
        "LOW_MOTIVATION",
        "LOW_SELF_EFFICACY",
    }

    if semantic_type in fast_candidate_types:
        if severity == "HIGH" and score >= 95:
            return "llm"

        if severity in {"MEDIUM", "HIGH"}:
            return "fast_llm"

    if severity == "HIGH" and score >= 95:
        return "llm"

    if severity in {"MEDIUM", "HIGH"}:
        return "fast_llm"

    return "drop"

def emit_reasoning_event(
    producer: KafkaProducer,
    tracker: CommitTracker,
    entry: Dict[str, Any],
    out: Dict[str, Any],
    tag: str,
) -> None:
    send_future = producer.send(OUT_REASONING_TOPIC, key=entry["key"], value=out)
    send_future.get(timeout=10)

    tracker.mark_done(entry["topic"], entry["partition"], entry["offset"])
    tracker.commit_contiguous()

    print(
        f"[reasoning_scheduler] emitted reasoning_event tag={tag} "
        f"user_id={out.get('user_id')} semantic_type={out.get('semantic_type')} "
        f"decision={out.get('decision')} partition={entry['partition']} offset={entry['offset']}",
        flush=True,
    )


def emit_dropped(
    producer: KafkaProducer,
    tracker: CommitTracker,
    entry: Dict[str, Any],
    dropped_event: Dict[str, Any],
) -> None:
    if ENABLE_DROPPED_TOPIC:
        send_future = producer.send(OUT_DROPPED_TOPIC, key=entry["key"], value=dropped_event)
        send_future.get(timeout=10)

    tracker.mark_done(entry["topic"], entry["partition"], entry["offset"])
    tracker.commit_contiguous()

    print(
        f"[reasoning_scheduler] dropped semantic_event "
        f"user_id={dropped_event.get('user_id')} semantic_type={dropped_event.get('semantic_type')} "
        f"reason={dropped_event.get('drop_reason')} "
        f"partition={entry['partition']} offset={entry['offset']}",
        flush=True,
    )


def _send_to_route(
    route: str,
    producer: KafkaProducer,
    tracker: CommitTracker,
    entry: Dict[str, Any],
    last_enqueued_by_user: Dict[str, float],
    recent_llm_enqueues: deque,
    recent_fast_llm_enqueues: deque,
) -> None:
    semantic_event = entry["semantic_event"]
    user_id = str(semantic_event.get("user_id"))

    if route == "rule_based":
        out = build_rule_based_reasoning_output(semantic_event)
        emit_reasoning_event(producer, tracker, entry, out, tag="rule_based")
        return

    if route == "drop":
        dropped_event = build_dropped_event(
            semantic_event,
            reason="not_routed_to_model",
            detail=f"semantic_type={semantic_event.get('semantic_type')}, severity={semantic_event.get('severity')}",
        )
        emit_dropped(producer, tracker, entry, dropped_event)
        return

    if route == "llm":
        if not admission_allowed(recent_llm_enqueues, MAX_LLM_REQUESTS_PER_SEC):
            dropped_event = build_dropped_event(
                semantic_event,
                reason="global_rate_limit",
                detail=f"max_llm_requests_per_sec={MAX_LLM_REQUESTS_PER_SEC}",
            )
            emit_dropped(producer, tracker, entry, dropped_event)
            return

        request_payload = build_request_payload(semantic_event, route="llm")
        send_future = producer.send(OUT_REQUEST_TOPIC, key=user_id, value=request_payload)
        send_future.get(timeout=10)

        now = time.time()
        last_enqueued_by_user[user_id] = now
        recent_llm_enqueues.append(now)

        tracker.mark_done(entry["topic"], entry["partition"], entry["offset"])
        tracker.commit_contiguous()

        print(
            f"[reasoning_scheduler] enqueued reasoning_request "
            f"user_id={semantic_event.get('user_id')} semantic_type={semantic_event.get('semantic_type')} "
            f"partition={entry['partition']} offset={entry['offset']}",
            flush=True,
        )
        return

    if route == "fast_llm":
        if not admission_allowed(recent_fast_llm_enqueues, MAX_FAST_LLM_REQUESTS_PER_SEC):
            dropped_event = build_dropped_event(
                semantic_event,
                reason="fast_global_rate_limit",
                detail=f"max_fast_llm_requests_per_sec={MAX_FAST_LLM_REQUESTS_PER_SEC}",
            )
            emit_dropped(producer, tracker, entry, dropped_event)
            return

        request_payload = build_request_payload(semantic_event, route="fast_llm")
        send_future = producer.send(OUT_FAST_REQUEST_TOPIC, key=user_id, value=request_payload)
        send_future.get(timeout=10)

        now = time.time()
        last_enqueued_by_user[user_id] = now
        recent_fast_llm_enqueues.append(now)

        tracker.mark_done(entry["topic"], entry["partition"], entry["offset"])
        tracker.commit_contiguous()

        print(
            f"[reasoning_scheduler] enqueued fast_reasoning_request "
            f"user_id={semantic_event.get('user_id')} semantic_type={semantic_event.get('semantic_type')} "
            f"partition={entry['partition']} offset={entry['offset']}",
            flush=True,
        )
        return

    dropped_event = build_dropped_event(
        semantic_event,
        reason="unknown_route",
        detail=f"route={route}",
    )
    emit_dropped(producer, tracker, entry, dropped_event)


def flush_ready_coalesced(
    pending_coalesce: Dict[Tuple[str, str, str, str], Dict[str, Any]],
    producer: KafkaProducer,
    tracker: CommitTracker,
    last_enqueued_by_user: Dict[str, float],
    recent_llm_enqueues: deque,
    recent_fast_llm_enqueues: deque,
) -> None:
    now = time.time()
    ready_items: List[Tuple[Tuple[str, str, str, str], Dict[str, Any]]] = [
        (k, v) for k, v in pending_coalesce.items() if v["flush_at"] <= now
    ]

    if not ready_items:
        tracker.commit_contiguous()
        return

    scored = []
    for key, entry in ready_items:
        semantic_event = entry["semantic_event"]
        user_id = str(semantic_event.get("user_id"))

        if user_in_cooldown(user_id, last_enqueued_by_user):
            continue

        waiting_sec = max(0.0, now - entry["flush_at"] + COALESCE_SEC)
        score = compute_priority_score(semantic_event, waiting_sec=waiting_sec)
        route = choose_route_with_score(semantic_event, score)
        scored.append((score, route, key, entry))

    # high score first
    scored.sort(key=lambda x: x[0], reverse=True)

    for score, route, key, entry in scored:
        pending_coalesce.pop(key, None)
        _send_to_route(
            route=route,
            producer=producer,
            tracker=tracker,
            entry=entry,
            last_enqueued_by_user=last_enqueued_by_user,
            recent_llm_enqueues=recent_llm_enqueues,
            recent_fast_llm_enqueues=recent_fast_llm_enqueues,
        )

        print(
            f"[reasoning_scheduler] scored_flush "
            f"user_id={entry['semantic_event'].get('user_id')} "
            f"semantic_type={entry['semantic_event'].get('semantic_type')} "
            f"severity={entry['semantic_event'].get('severity')} "
            f"score={score:.1f} route={route}",
            flush=True,
        )

    tracker.commit_contiguous()


def main():
    consumer = build_consumer_with_retry(
        IN_TOPIC,
        bootstrap_servers=BOOTSTRAP,
        group_id=GROUP_ID,
        enable_auto_commit=False,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
        auto_offset_reset="latest",
        max_poll_records=200,
    )

    producer = build_producer_with_retry(
        bootstrap_servers=BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        linger_ms=20,
        retries=3,
        acks="all",
    )

    tracker = CommitTracker(consumer)
    pending_coalesce: Dict[Tuple[str, str, str, str], Dict[str, Any]] = {}
    last_enqueued_by_user: Dict[str, float] = {}
    recent_llm_enqueues: deque = deque()
    recent_fast_llm_enqueues: deque = deque()

    queue_metrics_interval_sec = float(
        os.getenv("QUEUE_METRICS_INTERVAL_SEC", "1.0")
    )
    last_queue_metrics_ts = 0.0

    print(
        f"[reasoning_scheduler] started bootstrap={BOOTSTRAP} "
        f"consuming={IN_TOPIC} request_topic={OUT_REQUEST_TOPIC} "
        f"fast_request_topic={OUT_FAST_REQUEST_TOPIC} reasoning_topic={OUT_REASONING_TOPIC} "
        f"dropped_topic={OUT_DROPPED_TOPIC} coalesce_sec={COALESCE_SEC} "
        f"max_pending={MAX_PENDING} user_cooldown_enabled={ENABLE_USER_COOLDOWN} "
        f"user_cooldown_sec={USER_COOLDOWN_SEC} "
        f"max_llm_requests_per_sec={MAX_LLM_REQUESTS_PER_SEC} "
        f"max_fast_llm_requests_per_sec={MAX_FAST_LLM_REQUESTS_PER_SEC} "
        f"scheduler_version={SCHEDULER_VERSION} "
        f"force_route={FORCE_ROUTE or 'hybrid'} "
        f"disable_load_control={DISABLE_LOAD_CONTROL}",
        flush=True,
    )

    try:
        while True:
            flush_ready_coalesced(
                pending_coalesce=pending_coalesce,
                producer=producer,
                tracker=tracker,
                last_enqueued_by_user=last_enqueued_by_user,
                recent_llm_enqueues=recent_llm_enqueues,
                recent_fast_llm_enqueues=recent_fast_llm_enqueues,
            )

            now_metrics = time.time()
            if now_metrics - last_queue_metrics_ts >= queue_metrics_interval_sec:
                pending_total = len(pending_coalesce)
                pending_users = len({
                    str(v["semantic_event"].get("user_id"))
                    for v in pending_coalesce.values()
                })
                ready_pending = sum(
                    1 for v in pending_coalesce.values()
                    if v["flush_at"] <= now_metrics
                )

                print(
                    f"[reasoning_scheduler] queue_metrics "
                    f"ts_ms={int(now_metrics * 1000)} "
                    f"pending_total={pending_total} "
                    f"pending_users={pending_users} "
                    f"ready_pending={ready_pending}",
                    flush=True,
                )
                last_queue_metrics_ts = now_metrics

            records = consumer.poll(timeout_ms=500, max_records=200)
            if not records:
                continue

            for tp_raw, msgs in records.items():
                topic = tp_raw.topic
                partition = tp_raw.partition

                for msg in msgs:
                    tracker.register_seen(topic, partition, msg.offset)

                    semantic_event = msg.value
                    key = str(semantic_event.get("user_id") or msg.key or "")
                    user_id = str(semantic_event.get("user_id"))
                    coalesce_key = make_coalesce_key(semantic_event)

                    entry = {
                        "topic": topic,
                        "partition": partition,
                        "offset": msg.offset,
                        "key": key,
                        "semantic_event": semantic_event,
                        "flush_at": time.time() + COALESCE_SEC,
                    }

                    bypass = build_bypass_output(semantic_event)
                    if bypass is not None:
                        emit_reasoning_event(producer, tracker, entry, bypass, tag="bypass_rule")
                        continue

                    if DISABLE_LOAD_CONTROL:
                        score = compute_priority_score(
                            semantic_event,
                            waiting_sec=0.0,
                        )
                        route = choose_route_with_score(
                            semantic_event,
                            score,
                        )

                        _send_to_route(
                            route=route,
                            producer=producer,
                            tracker=tracker,
                            entry=entry,
                            last_enqueued_by_user=last_enqueued_by_user,
                            recent_llm_enqueues=recent_llm_enqueues,
                            recent_fast_llm_enqueues=recent_fast_llm_enqueues,
                        )

                        print(
                            f"[reasoning_scheduler] no_load_control "
                            f"user_id={semantic_event.get('user_id')} "
                            f"semantic_type={semantic_event.get('semantic_type')} "
                            f"route={route}",
                            flush=True,
                        )
                        continue

                    should_drop, reason = should_drop_by_priority(semantic_event, pending_coalesce)
                    if should_drop:
                        dropped_event = build_dropped_event(
                            semantic_event,
                            reason=reason or "priority_drop",
                        )
                        emit_dropped(producer, tracker, entry, dropped_event)
                        continue

                    if len(pending_coalesce) >= MAX_PENDING:
                        dropped_event = build_dropped_event(
                            semantic_event,
                            reason="scheduler_overflow",
                            detail=f"pending_size={len(pending_coalesce)}",
                        )
                        emit_dropped(producer, tracker, entry, dropped_event)
                        continue

                    if pending_count_for_user(pending_coalesce, user_id) >= MAX_PENDING_PER_USER:
                        dropped_event = build_dropped_event(
                            semantic_event,
                            reason="per_user_pending_limit",
                            detail=f"user_id={user_id}",
                        )
                        emit_dropped(producer, tracker, entry, dropped_event)
                        continue

                    if coalesce_key in pending_coalesce:
                        old = pending_coalesce[coalesce_key]
                        tracker.mark_done(old["topic"], old["partition"], old["offset"])
                        pending_coalesce[coalesce_key] = entry
                        tracker.commit_contiguous()

                        print(
                            f"[reasoning_scheduler] coalesced user_id={semantic_event.get('user_id')} "
                            f"semantic_type={semantic_event.get('semantic_type')} "
                            f"replaced_offset={old['offset']} new_offset={msg.offset}",
                            flush=True,
                        )
                    else:
                        pending_coalesce[coalesce_key] = entry
                        print(
                            f"[reasoning_scheduler] buffered semantic_event "
                            f"user_id={semantic_event.get('user_id')} "
                            f"semantic_type={semantic_event.get('semantic_type')} "
                            f"partition={partition} offset={msg.offset}",
                            flush=True,
                        )

    except KeyboardInterrupt:
        print("[reasoning_scheduler] stopping (KeyboardInterrupt)", flush=True)

    finally:
        try:
            flush_ready_coalesced(
                pending_coalesce=pending_coalesce,
                producer=producer,
                tracker=tracker,
                last_enqueued_by_user=last_enqueued_by_user,
                recent_llm_enqueues=recent_llm_enqueues,
                recent_fast_llm_enqueues=recent_fast_llm_enqueues,
            )
        except Exception as e:
            print(f"[reasoning_scheduler] flush on shutdown error err={repr(e)}", flush=True)

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