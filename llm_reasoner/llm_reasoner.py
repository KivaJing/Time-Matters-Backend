#!/usr/bin/env python3
# llm_reasoner.py

import json
import os
import time
import threading
from typing import Dict, Any, Tuple
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED

import requests
from kafka import KafkaConsumer, KafkaProducer
from kafka.structs import TopicPartition, OffsetAndMetadata
from kafka.errors import NoBrokersAvailable, KafkaError


BOOTSTRAP = os.getenv("BOOTSTRAP", "localhost:9092")

IN_TOPIC = os.getenv("IN_TOPIC", "reasoning_requests")
OUT_TOPIC = os.getenv("OUT_TOPIC", "reasoning_events")
GROUP_ID = os.getenv("GROUP_ID", "llm-reasoner")

OLLAMA_URL = os.getenv("OLLAMA_URL", "http://ollama:11434/api/chat")
OLLAMA_TAGS_URL = os.getenv("OLLAMA_TAGS_URL", "http://ollama:11434/api/tags")
MODEL = os.getenv("MODEL", "llama3.2:3b")

TIMEOUT_SEC = int(os.getenv("TIMEOUT_SEC", "60"))
CONNECT_TIMEOUT_SEC = float(os.getenv("CONNECT_TIMEOUT_SEC", "5"))
READ_TIMEOUT_SEC = float(os.getenv("READ_TIMEOUT_SEC", str(TIMEOUT_SEC)))

OLLAMA_WAIT_RETRIES = int(os.getenv("OLLAMA_WAIT_RETRIES", "30"))
OLLAMA_WAIT_SEC = float(os.getenv("OLLAMA_WAIT_SEC", "2"))

MAX_WORKERS = int(os.getenv("MAX_WORKERS", "1"))
MAX_INFLIGHT = int(os.getenv("MAX_INFLIGHT", "4"))

MAX_REQUEST_AGE_MS = int(os.getenv("MAX_REQUEST_AGE_MS", "15000"))

PROMPT_VERSION = os.getenv("PROMPT_VERSION", "v5-compact-decision-only")
SCHEMA_VERSION = os.getenv("SCHEMA_VERSION", "v3")

OLLAMA_TEMPERATURE = float(os.getenv("OLLAMA_TEMPERATURE", "0.0"))
OLLAMA_NUM_PREDICT = int(os.getenv("OLLAMA_NUM_PREDICT", "200"))


def build_consumer_with_retry(*args, retries: int = 60, sleep_sec: float = 2.0, **kwargs) -> KafkaConsumer:
    last = None
    for i in range(retries):
        try:
            return KafkaConsumer(*args, **kwargs)
        except NoBrokersAvailable as e:
            last = e
            print(f"[llm_reasoner] Kafka consumer: no broker ({i+1}/{retries}), retry in {sleep_sec}s...", flush=True)
            time.sleep(sleep_sec)
    raise last


def build_producer_with_retry(retries: int = 60, sleep_sec: float = 2.0, **kwargs) -> KafkaProducer:
    last = None
    for i in range(retries):
        try:
            return KafkaProducer(**kwargs)
        except NoBrokersAvailable as e:
            last = e
            print(f"[llm_reasoner] Kafka producer: no broker ({i+1}/{retries}), retry in {sleep_sec}s...", flush=True)
            time.sleep(sleep_sec)
    raise last


def wait_for_ollama() -> None:
    last_err = None
    for i in range(OLLAMA_WAIT_RETRIES):
        try:
            r = requests.get(
                OLLAMA_TAGS_URL,
                timeout=(CONNECT_TIMEOUT_SEC, min(READ_TIMEOUT_SEC, 10)),
            )
            r.raise_for_status()
            data = r.json()
            model_names = [m.get("name") for m in data.get("models", [])]
            print(
                f"[llm_reasoner] Ollama ready ({i+1}/{OLLAMA_WAIT_RETRIES}). "
                f"available_models={model_names}",
                flush=True,
            )
            return
        except Exception as e:
            last_err = e
            print(
                f"[llm_reasoner] waiting for Ollama ({i+1}/{OLLAMA_WAIT_RETRIES}) "
                f"url={OLLAMA_TAGS_URL} err={repr(e)}",
                flush=True,
            )
            time.sleep(OLLAMA_WAIT_SEC)
    raise RuntimeError(f"Ollama not ready after retries: {repr(last_err)}")


def build_prompt(request_event: Dict[str, Any]) -> Tuple[str, str]:
    compact_input = {
        "semantic_type": request_event.get("semantic_type"),
        "severity": request_event.get("severity"),
        "rule_id": request_event.get("rule_id"),
        "evidence": request_event.get("evidence", {}),
    }

    system = (
        "You are a relapse-risk reasoning assistant for smoking cessation support. "
        "Return exactly one valid JSON object only. "
        "Do not output markdown, code fences, or any extra text. "
        "Do not include explanations outside the JSON."
    )

    user = {
        "task": (
            "Estimate intervention urgency and whether the system should MONITOR, INTERVENE, "
            "or SUPPRESS based on the semantic risk event."
        ),
        "input": compact_input,
        "required_output_json_schema": {
            "urgency": "Integer 0-100",
            "decision": "One of: MONITOR, INTERVENE, SUPPRESS",
            "confidence": "Float 0.0-1.0",
        },
    }
    return system, json.dumps(user, ensure_ascii=False)


def extract_json_object(text: str) -> Dict[str, Any]:
    text = text.strip()

    try:
        return json.loads(text)
    except Exception:
        pass

    if text.startswith("```"):
        lines = text.splitlines()
        if len(lines) >= 3:
            text2 = "\n".join(lines[1:-1]).strip()
            try:
                return json.loads(text2)
            except Exception:
                pass

    start = text.find("{")
    end = text.rfind("}")
    if start != -1 and end != -1 and end > start:
        candidate = text[start:end + 1]
        return json.loads(candidate)

    raise ValueError("No valid JSON object found in LLM response")


def call_ollama(system: str, user: str) -> Tuple[Dict[str, Any], int, str | None]:
    t0 = time.time()
    try:
        payload = {
            "model": MODEL,
            "stream": False,
            "format": "json",
            "options": {
                "temperature": OLLAMA_TEMPERATURE,
                "num_predict": OLLAMA_NUM_PREDICT,
            },
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
        }

        r = requests.post(
            OLLAMA_URL,
            json=payload,
            timeout=(CONNECT_TIMEOUT_SEC, READ_TIMEOUT_SEC),
        )
        r.raise_for_status()

        data = r.json()
        text = data["message"]["content"].strip()
        parsed = extract_json_object(text)

        llm_ms = int((time.time() - t0) * 1000)
        return parsed, llm_ms, None

    except Exception as e:
        llm_ms = int((time.time() - t0) * 1000)
        return {}, llm_ms, repr(e)


def normalize_result(result: Dict[str, Any]) -> Dict[str, Any]:
    decision = str(result.get("decision", "MONITOR")).upper()
    if decision not in {"MONITOR", "INTERVENE", "SUPPRESS"}:
        decision = "MONITOR"

    try:
        urgency = int(result.get("urgency", 0))
    except Exception:
        urgency = 0
    urgency = max(0, min(100, urgency))

    try:
        confidence = float(result.get("confidence", 0.0))
    except Exception:
        confidence = 0.0
    confidence = max(0.0, min(1.0, confidence))

    return {
        "urgency": urgency,
        "decision": decision,
        "confidence": confidence,
    }


def risk_from_semantic_and_urgency(semantic_type: str, severity: str, urgency: int) -> str:
    semantic_type = str(semantic_type or "")
    severity = str(severity or "").upper()

    if semantic_type == "COMBINED_RELAPSE_RISK":
        if severity == "VERY_HIGH" or urgency >= 85:
            return "VERY_HIGH"
        return "HIGH"

    if semantic_type in {"RISING_STRESS_TRAJECTORY", "RISING_CRAVING_TRAJECTORY"}:
        if urgency >= 85:
            return "VERY_HIGH"
        if urgency >= 65:
            return "HIGH"
        if urgency >= 40:
            return "MODERATE"
        return "LOW"

    if semantic_type == "ELEVATED_STRESS":
        return "HIGH" if urgency >= 65 else "MODERATE"

    if semantic_type == "ELEVATED_CRAVING":
        return "HIGH" if urgency >= 65 else "MODERATE"

    if urgency >= 85:
        return "VERY_HIGH"
    if urgency >= 65:
        return "HIGH"
    if urgency >= 40:
        return "MODERATE"
    return "LOW"


def intervention_type_from_semantic(semantic_type: str, decision: str) -> str:
    if decision != "INTERVENE":
        return "none"

    semantic_type = str(semantic_type or "")

    if semantic_type == "RISING_STRESS_TRAJECTORY":
        return "stress_regulation"
    if semantic_type == "RISING_CRAVING_TRAJECTORY":
        return "craving_coping"
    if semantic_type == "COMBINED_RELAPSE_RISK":
        return "craving_coping, stress_regulation"
    if semantic_type == "ELEVATED_STRESS":
        return "stress_regulation"
    if semantic_type == "ELEVATED_CRAVING":
        return "craving_coping"

    return "none"


def rationale_from_fields(semantic_type: str, severity: str, urgency: int, decision: str) -> str:
    return (
        f"semantic_type={semantic_type}, severity={severity}, urgency={urgency}, "
        f"decision={decision} after rule-normalized postprocessing."
    )


def build_output_base(request_event: Dict[str, Any], llm_ms: int) -> Dict[str, Any]:
    return {
        "run_id": request_event.get("run_id"),
        "user_id": request_event.get("user_id"),
        "window_start_ms": request_event.get("source_window_start_ms"),
        "window_end_ms": request_event.get("source_window_end_ms"),
        "semantic_type": request_event.get("semantic_type"),
        "severity": request_event.get("severity"),
        "rule_id": request_event.get("rule_id"),
        "source_last_sent_at_ms": request_event.get("source_last_sent_at_ms"),
        "source_last_event_ts_ms": request_event.get("source_last_event_ts_ms"),
        "source_ksql_emit_ts_ms": request_event.get("source_ksql_emit_ts_ms"),
        "semantic_engine_ts_ms": request_event.get("engine_ts_ms"),
        "scheduler_ts_ms": request_event.get("scheduler_ts_ms"),
        "scheduler_version": request_event.get("scheduler_version"),
        "request_id": request_event.get("request_id"),
        "reasoning_ts_ms": int(time.time() * 1000),
        "model": MODEL,
        "prompt_version": PROMPT_VERSION,
        "schema_version": SCHEMA_VERSION,
        "llm_latency_ms": llm_ms,
        "evidence": request_event.get("evidence", {}),
    }


def build_output_from_request_event(
    request_event: Dict[str, Any],
    result: Dict[str, Any],
    llm_ms: int,
    err: str | None,
) -> Dict[str, Any]:
    out = build_output_base(request_event, llm_ms)

    if err:
        out.update({
            "risk_level": "UNKNOWN",
            "urgency": 0,
            "decision": "MONITOR",
            "intervention_type": "none",
            "recommendations": [],
            "rationale": "LLM invocation failed. Fallback response generated.",
            "confidence": 0.0,
            "error": err,
            "reasoning_mode": "llm_fallback",
            "bypass_rule": None,
        })
        return out

    normalized = normalize_result(result)
    semantic_type = str(request_event.get("semantic_type") or "")
    severity = str(request_event.get("severity") or "")
    urgency = normalized["urgency"]
    decision = normalized["decision"]

    risk_level = risk_from_semantic_and_urgency(semantic_type, severity, urgency)
    intervention_type = intervention_type_from_semantic(semantic_type, decision)

    out.update({
        "risk_level": risk_level,
        "urgency": urgency,
        "decision": decision,
        "intervention_type": intervention_type,
        "recommendations": [],
        "rationale": rationale_from_fields(semantic_type, severity, urgency, decision),
        "confidence": normalized["confidence"],
        "error": None,
        "reasoning_mode": "llm",
        "bypass_rule": None,
    })
    return out


def process_request_event(request_event: Dict[str, Any]) -> Tuple[Dict[str, Any], str]:
    system, user = build_prompt(request_event)
    result, llm_ms, err = call_ollama(system, user)

    if err:
        print(
            f"[llm_reasoner] LLM ERROR user_id={request_event.get('user_id')} "
            f"semantic_type={request_event.get('semantic_type')} err={err}",
            flush=True,
        )
        out = build_output_from_request_event(request_event, {}, llm_ms, err)
        return out, "fallback"

    out = build_output_from_request_event(request_event, result, llm_ms, None)
    return out, "ok"


class CommitTracker:
    def __init__(self, consumer):
        self.consumer = consumer
        self.lock = threading.Lock()
        self.next_commit_offset = {}
        self.completed_offsets = {}

    def register_seen(self, topic: str, partition: int, offset: int) -> None:
        key = (topic, partition)
        with self.lock:
            if key not in self.next_commit_offset:
                self.next_commit_offset[key] = offset
                self.completed_offsets[key] = set()

    def mark_done(self, topic: str, partition: int, offset: int) -> None:
        key = (topic, partition)
        with self.lock:
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

        with self.lock:
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

        try:
            commit_map = {}
            for (topic, partition), next_offset in commits.items():
                tp = TopicPartition(topic, partition)
                commit_map[tp] = self._make_offset_meta(next_offset)

            self.consumer.commit(offsets=commit_map)

            for (topic, partition), next_offset in commits.items():
                print(
                    f"[llm_reasoner] committed topic={topic} partition={partition} next_offset={next_offset}",
                    flush=True,
                )
        except Exception as e:
            print(f"[llm_reasoner] commit error err={repr(e)}", flush=True)


class LatestSeenTracker:
    def __init__(self):
        self.lock = threading.Lock()
        self.latest_scheduler_ts_by_user: Dict[str, int] = {}

    def update_seen(self, user_id: str, scheduler_ts_ms: int) -> None:
        with self.lock:
            prev = self.latest_scheduler_ts_by_user.get(user_id)
            if prev is None or scheduler_ts_ms >= prev:
                self.latest_scheduler_ts_by_user[user_id] = scheduler_ts_ms

    def is_latest(self, user_id: str, scheduler_ts_ms: int) -> bool:
        with self.lock:
            latest = self.latest_scheduler_ts_by_user.get(user_id)
            return latest is not None and scheduler_ts_ms >= latest


def main():
    wait_for_ollama()

    consumer = build_consumer_with_retry(
        IN_TOPIC,
        bootstrap_servers=BOOTSTRAP,
        group_id=GROUP_ID,
        enable_auto_commit=False,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
        auto_offset_reset="latest",
        max_poll_records=MAX_INFLIGHT,
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
    latest_seen = LatestSeenTracker()
    executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)
    inflight = {}

    print(
        f"[llm_reasoner] started bootstrap={BOOTSTRAP} consuming={IN_TOPIC} "
        f"producing={OUT_TOPIC} model={MODEL} workers={MAX_WORKERS} inflight={MAX_INFLIGHT} "
        f"max_request_age_ms={MAX_REQUEST_AGE_MS} num_predict={OLLAMA_NUM_PREDICT}",
        flush=True,
    )
    print(f"[llm_reasoner] ollama_url={OLLAMA_URL}", flush=True)

    try:
        while True:
            if len(inflight) < MAX_INFLIGHT:
                records = consumer.poll(
                    timeout_ms=500,
                    max_records=max(1, MAX_INFLIGHT - len(inflight)),
                )

                if records:
                    for tp_raw, msgs in records.items():
                        topic = tp_raw.topic
                        partition = tp_raw.partition

                        for msg in msgs:
                            tracker.register_seen(topic, partition, msg.offset)
                            request_event = msg.value
                            key = msg.key or ""
                            user_id = str(request_event.get("user_id") or key or "")
                            scheduler_ts_ms = request_event.get("scheduler_ts_ms")

                            try:
                                scheduler_ts_ms = int(scheduler_ts_ms)
                            except Exception:
                                tracker.mark_done(topic, partition, msg.offset)
                                tracker.commit_contiguous()
                                print(
                                    f"[llm_reasoner] skipped malformed reasoning_request "
                                    f"user_id={user_id} partition={partition} offset={msg.offset}",
                                    flush=True,
                                )
                                continue

                            now_ms = int(time.time() * 1000)
                            age_ms = now_ms - scheduler_ts_ms

                            latest_seen.update_seen(user_id, scheduler_ts_ms)

                            if age_ms > MAX_REQUEST_AGE_MS:
                                tracker.mark_done(topic, partition, msg.offset)
                                tracker.commit_contiguous()
                                print(
                                    f"[llm_reasoner] dropped expired reasoning_request "
                                    f"user_id={user_id} semantic_type={request_event.get('semantic_type')} "
                                    f"age_ms={age_ms} partition={partition} offset={msg.offset}",
                                    flush=True,
                                )
                                continue

                            if not latest_seen.is_latest(user_id, scheduler_ts_ms):
                                tracker.mark_done(topic, partition, msg.offset)
                                tracker.commit_contiguous()
                                print(
                                    f"[llm_reasoner] skipped stale reasoning_request "
                                    f"user_id={user_id} semantic_type={request_event.get('semantic_type')} "
                                    f"partition={partition} offset={msg.offset}",
                                    flush=True,
                                )
                                continue

                            print(
                                f"[llm_reasoner] received reasoning_request_latest "
                                f"user_id={user_id} semantic_type={request_event.get('semantic_type')} "
                                f"scheduler_ts_ms={scheduler_ts_ms} partition={partition} offset={msg.offset}",
                                flush=True,
                            )

                            future = executor.submit(process_request_event, request_event)
                            inflight[future] = {
                                "topic": topic,
                                "partition": partition,
                                "offset": msg.offset,
                                "key": user_id,
                                "request_event": request_event,
                                "user_id": user_id,
                                "scheduler_ts_ms": scheduler_ts_ms,
                            }

            if not inflight:
                time.sleep(0.05)
                continue

            done, _ = wait(inflight.keys(), timeout=0.2, return_when=FIRST_COMPLETED)
            if not done:
                continue

            for future in done:
                meta = inflight.pop(future)
                topic = meta["topic"]
                partition = meta["partition"]
                offset = meta["offset"]
                key = meta["key"]
                request_event = meta["request_event"]
                user_id = meta["user_id"]
                scheduler_ts_ms = meta["scheduler_ts_ms"]

                try:
                    if not latest_seen.is_latest(user_id, scheduler_ts_ms):
                        tracker.mark_done(topic, partition, offset)
                        tracker.commit_contiguous()
                        print(
                            f"[llm_reasoner] discarded superseded reasoning_result "
                            f"user_id={user_id} semantic_type={request_event.get('semantic_type')} "
                            f"partition={partition} offset={offset}",
                            flush=True,
                        )
                        continue

                    out, status = future.result()

                    send_future = producer.send(OUT_TOPIC, key=key, value=out)
                    send_future.get(timeout=10)

                    tracker.mark_done(topic, partition, offset)
                    tracker.commit_contiguous()

                    print(
                        f"[llm_reasoner] emitted reasoning_event status={status} "
                        f"user_id={out.get('user_id')} risk={out.get('risk_level')} "
                        f"urgency={out.get('urgency')} decision={out.get('decision')} "
                        f"llm_ms={out.get('llm_latency_ms')} partition={partition} offset={offset}",
                        flush=True,
                    )

                except KafkaError as e:
                    print(
                        f"[llm_reasoner] KAFKA ERROR user_id={request_event.get('user_id')} "
                        f"partition={partition} offset={offset} err={repr(e)}",
                        flush=True,
                    )
                    time.sleep(1)

                except Exception as e:
                    print(
                        f"[llm_reasoner] ERROR user_id={request_event.get('user_id')} "
                        f"partition={partition} offset={offset} err={repr(e)}",
                        flush=True,
                    )
                    time.sleep(0.2)

    except KeyboardInterrupt:
        print("[llm_reasoner] stopping (KeyboardInterrupt)", flush=True)

    finally:
        try:
            for future, meta in list(inflight.items()):
                try:
                    out, status = future.result(timeout=1)

                    if latest_seen.is_latest(meta["user_id"], meta["scheduler_ts_ms"]):
                        send_future = producer.send(OUT_TOPIC, key=meta["key"], value=out)
                        send_future.get(timeout=10)
                        print(
                            f"[llm_reasoner] drained reasoning_event status={status} "
                            f"user_id={out.get('user_id')} partition={meta['partition']} offset={meta['offset']}",
                            flush=True,
                        )

                    tracker.mark_done(meta["topic"], meta["partition"], meta["offset"])

                except Exception as e:
                    print(f"[llm_reasoner] drain error err={repr(e)}", flush=True)

            tracker.commit_contiguous()

        except Exception:
            pass

        try:
            executor.shutdown(wait=False, cancel_futures=True)
        except Exception:
            pass
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