#!/usr/bin/env python3
# reasoning_scheduler.py

import json
import os
import time
from collections import deque
from typing import Dict, Any, Tuple

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

COALESCE_SEC = float(os.getenv("COALESCE_SEC", "5"))
MAX_PENDING = int(os.getenv("MAX_PENDING", "256"))
MAX_PENDING_PER_USER = int(os.getenv("MAX_PENDING_PER_USER", "2"))

ENABLE_PRIORITY_FILTER = os.getenv("ENABLE_PRIORITY_FILTER", "true").lower() == "true"
DROP_MEDIUM_WHEN_PENDING_GE = int(os.getenv("DROP_MEDIUM_WHEN_PENDING_GE", "32"))
DROP_LOW_ALWAYS = os.getenv("DROP_LOW_ALWAYS", "true").lower() == "true"

ENABLE_DROPPED_TOPIC = os.getenv("ENABLE_DROPPED_TOPIC", "false").lower() == "true"

ENABLE_USER_COOLDOWN = os.getenv("ENABLE_USER_COOLDOWN", "true").lower() == "true"
USER_COOLDOWN_SEC = float(os.getenv("USER_COOLDOWN_SEC", "10"))

# Separate admission control for full LLM and fast LLM paths
MAX_LLM_REQUESTS_PER_SEC = float(os.getenv("MAX_LLM_REQUESTS_PER_SEC", "2"))
MAX_FAST_LLM_REQUESTS_PER_SEC = float(os.getenv("MAX_FAST_LLM_REQUESTS_PER_SEC", "4"))

SCHEDULER_VERSION = os.getenv("SCHEDULER_VERSION", "v2")


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


def make_coalesce_key(semantic_event: Dict[str, Any]) -> Tuple[str, str, str]:
    return (
        str(semantic_event.get("run_id")),
        str(semantic_event.get("user_id")),
        str(semantic_event.get("semantic_type")),
    )


def pending_count_for_user(
    pending_coalesce: Dict[Tuple[str, str, str], Dict[str, Any]],
    user_id: str,
) -> int:
    n = 0
    for _, entry in pending_coalesce.items():
        if str(entry["semantic_event"].get("user_id")) == str(user_id):
            n += 1
    return n


def user_in_cooldown(user_id: str, last_enqueued_by_user: Dict[str, float]) -> bool:
    if not ENABLE_USER_COOLDOWN:
        return False

    last_ts = last_enqueued_by_user.get(str(user_id))
    if last_ts is None:
        return False

    return (time.time() - last_ts) < USER_COOLDOWN_SEC


def admission_allowed(recent_enqueues: deque, max_rate_per_sec: float) -> bool:
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


def build_request_payload(semantic_event: Dict[str, Any], route: str = "llm") -> Dict[str, Any]:
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
    pending_coalesce: Dict[Tuple[str, str, str], Dict[str, Any]],
) -> Tuple[bool, str | None]:
    if not ENABLE_PRIORITY_FILTER:
        return False, None

    severity = str(semantic_event.get("severity") or "").upper()
    pending_size = len(pending_coalesce)

    if severity == "LOW" and DROP_LOW_ALWAYS:
        return True, "low_severity"

    if severity == "MEDIUM" and pending_size >= DROP_MEDIUM_WHEN_PENDING_GE:
        return True, "medium_under_pressure"

    return False, None


def route_event(evt: dict) -> str:
    semantic_type = str(evt.get("semantic_type") or "")
    severity = str(evt.get("severity") or "").upper()

    if semantic_type == "COMBINED_RELAPSE_RISK" or severity == "VERY_HIGH":
        return "llm"

    if semantic_type in {"RISING_STRESS_TRAJECTORY", "RISING_CRAVING_TRAJECTORY"}:
        return "rule_based"

    if severity == "HIGH" or semantic_type in {"ELEVATED_STRESS", "ELEVATED_CRAVING", "LOW_MOTIVATION"}:
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


def flush_ready_coalesced(
    pending_coalesce: Dict[Tuple[str, str, str], Dict[str, Any]],
    producer: KafkaProducer,
    tracker: CommitTracker,
    last_enqueued_by_user: Dict[str, float],
    recent_llm_enqueues: deque,
    recent_fast_llm_enqueues: deque,
) -> None:
    ready_keys = [k for k, v in pending_coalesce.items() if v["flush_at"] <= time.time()]

    for key in ready_keys:
        entry = pending_coalesce[key]
        semantic_event = entry["semantic_event"]
        user_id = str(semantic_event.get("user_id"))

        if user_in_cooldown(user_id, last_enqueued_by_user):
            continue

        route = route_event(semantic_event)

        if route == "rule_based":
            pending_coalesce.pop(key)
            out = build_rule_based_reasoning_output(semantic_event)
            emit_reasoning_event(producer, tracker, entry, out, tag="rule_based")
            continue

        if route == "drop":
            pending_coalesce.pop(key)
            dropped_event = build_dropped_event(
                semantic_event,
                reason="not_routed_to_model",
                detail=f"semantic_type={semantic_event.get('semantic_type')}, severity={semantic_event.get('severity')}",
            )
            emit_dropped(producer, tracker, entry, dropped_event)
            continue

        if route == "llm":
            if not admission_allowed(recent_llm_enqueues, MAX_LLM_REQUESTS_PER_SEC):
                pending_coalesce.pop(key)
                dropped_event = build_dropped_event(
                    semantic_event,
                    reason="global_rate_limit",
                    detail=f"max_llm_requests_per_sec={MAX_LLM_REQUESTS_PER_SEC}",
                )
                emit_dropped(producer, tracker, entry, dropped_event)
                continue

            pending_coalesce.pop(key)
            request_payload = build_request_payload(semantic_event, route="llm")

            send_future = producer.send(
                OUT_REQUEST_TOPIC,
                key=user_id,
                value=request_payload,
            )
            send_future.get(timeout=10)

            now = time.time()
            last_enqueued_by_user[user_id] = now
            recent_llm_enqueues.append(now)

            tracker.mark_done(entry["topic"], entry["partition"], entry["offset"])
            tracker.commit_contiguous()

            print(
                f"[reasoning_scheduler] enqueued reasoning_request "
                f"user_id={semantic_event.get('user_id')} "
                f"semantic_type={semantic_event.get('semantic_type')} "
                f"partition={entry['partition']} offset={entry['offset']}",
                flush=True,
            )
            continue

        if route == "fast_llm":
            if not admission_allowed(recent_fast_llm_enqueues, MAX_FAST_LLM_REQUESTS_PER_SEC):
                pending_coalesce.pop(key)
                dropped_event = build_dropped_event(
                    semantic_event,
                    reason="fast_global_rate_limit",
                    detail=f"max_fast_llm_requests_per_sec={MAX_FAST_LLM_REQUESTS_PER_SEC}",
                )
                emit_dropped(producer, tracker, entry, dropped_event)
                continue

            pending_coalesce.pop(key)
            request_payload = build_request_payload(semantic_event, route="fast_llm")

            send_future = producer.send(
                OUT_FAST_REQUEST_TOPIC,
                key=user_id,
                value=request_payload,
            )
            send_future.get(timeout=10)

            now = time.time()
            last_enqueued_by_user[user_id] = now
            recent_fast_llm_enqueues.append(now)

            tracker.mark_done(entry["topic"], entry["partition"], entry["offset"])
            tracker.commit_contiguous()

            print(
                f"[reasoning_scheduler] enqueued fast_reasoning_request "
                f"user_id={semantic_event.get('user_id')} "
                f"semantic_type={semantic_event.get('semantic_type')} "
                f"partition={entry['partition']} offset={entry['offset']}",
                flush=True,
            )
            continue

        pending_coalesce.pop(key)
        dropped_event = build_dropped_event(
            semantic_event,
            reason="unknown_route",
            detail=f"route={route}",
        )
        emit_dropped(producer, tracker, entry, dropped_event)

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
    pending_coalesce: Dict[Tuple[str, str, str], Dict[str, Any]] = {}
    last_enqueued_by_user: Dict[str, float] = {}
    recent_llm_enqueues: deque = deque()
    recent_fast_llm_enqueues: deque = deque()

    print(
        f"[reasoning_scheduler] started bootstrap={BOOTSTRAP} "
        f"consuming={IN_TOPIC} request_topic={OUT_REQUEST_TOPIC} "
        f"fast_request_topic={OUT_FAST_REQUEST_TOPIC} "
        f"reasoning_topic={OUT_REASONING_TOPIC} dropped_topic={OUT_DROPPED_TOPIC} "
        f"coalesce_sec={COALESCE_SEC} max_pending={MAX_PENDING} "
        f"user_cooldown_enabled={ENABLE_USER_COOLDOWN} user_cooldown_sec={USER_COOLDOWN_SEC} "
        f"max_llm_requests_per_sec={MAX_LLM_REQUESTS_PER_SEC} "
        f"max_fast_llm_requests_per_sec={MAX_FAST_LLM_REQUESTS_PER_SEC} "
        f"scheduler_version={SCHEDULER_VERSION}",
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