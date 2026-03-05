import json
import time
import requests
import os
from typing import Dict, Any

from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable, KafkaError


BOOTSTRAP = os.getenv("BOOTSTRAP", "localhost:9092")
IN_TOPIC = os.getenv("IN_TOPIC", "semantic_events")
OUT_TOPIC = os.getenv("OUT_TOPIC", "reasoning_events")
GROUP_ID = os.getenv("GROUP_ID", "llm-reasoner-v1")

OLLAMA_URL = os.getenv("OLLAMA_URL", "http://127.0.0.1:11434/api/chat")
MODEL = os.getenv("MODEL", "llama3.1:8b")
TIMEOUT_SEC = int(os.getenv("TIMEOUT_SEC", "8"))


def build_consumer_with_retry(*args, retries: int = 60, sleep_sec: float = 2.0, **kwargs) -> KafkaConsumer:
    last = None
    for i in range(retries):
        try:
            return KafkaConsumer(*args, **kwargs)
        except NoBrokersAvailable as e:
            last = e
            print(f"[llm_reasoner] Kafka consumer: no broker ({i+1}/{retries}), retry in {sleep_sec}s...")
            time.sleep(sleep_sec)
    raise last


def build_producer_with_retry(retries: int = 60, sleep_sec: float = 2.0, **kwargs) -> KafkaProducer:
    last = None
    for i in range(retries):
        try:
            return KafkaProducer(**kwargs)
        except NoBrokersAvailable as e:
            last = e
            print(f"[llm_reasoner] Kafka producer: no broker ({i+1}/{retries}), retry in {sleep_sec}s...")
            time.sleep(sleep_sec)
    raise last


def build_prompt(semantic_event: Dict[str, Any]) -> tuple[str, str]:
    system = (
        "You are a risk assessment assistant for system instability monitoring. "
        "You MUST output ONLY a single valid JSON object, with no markdown, no code fences, "
        "no extra text. Do NOT fabricate data that is not present in the input. "
        "If information is missing, use null or a conservative default. "
        "The JSON must contain all required fields exactly as specified."
    )

    user = {
        "task": (
            "Based on recent instability patterns, assess the risk level and recommend "
            "intervention urgency."
        ),
        "input": semantic_event,
        "required_output_json_schema": {
            "risk_level": "One of: LOW, MEDIUM, HIGH, CRITICAL, UNKNOWN",
            "urgency": "Integer 0-100 (higher means more urgent)",
            "recommendations": "Array of short actionable sentences",
            "rationale": "1-3 short paragraphs explaining why",
            "confidence": "Float 0.0-1.0",
        },
    }

    return system, json.dumps(user, ensure_ascii=False)


def call_ollama(system: str, user: str) -> tuple[Dict[str, Any], int, str | None]:
    t0 = time.time()
    try:
        payload = {
            "model": MODEL,
            "stream": False,
            "temperature": 0.2,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
        }
        r = requests.post(OLLAMA_URL, json=payload, timeout=TIMEOUT_SEC)
        r.raise_for_status()
        data = r.json()
        text = data["message"]["content"].strip()
        llm_ms = int((time.time() - t0) * 1000)
        return json.loads(text), llm_ms, None
    except Exception as e:
        llm_ms = int((time.time() - t0) * 1000)
        return {}, llm_ms, str(e)


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
        linger_ms=20,   # allow batching
        retries=3,
        acks="all",
    )

    print(f"[llm_reasoner] started bootstrap={BOOTSTRAP} consuming={IN_TOPIC} producing={OUT_TOPIC} model={MODEL}", flush=True)
    print(f"[llm_reasoner] ollama_url={OLLAMA_URL}")

    try:
        while True:
            records = consumer.poll(timeout_ms=1000, max_records=100)
            if not records:
                continue

            for tp, msgs in records.items():
                for msg in msgs:
                    try:
                        semantic_event = msg.value
                        key = msg.key or ""

                        system, user = build_prompt(semantic_event)
                        result, llm_ms, err = call_ollama(system, user)

                        out = {
                            "run_id": semantic_event.get("run_id"),
                            "user_id": semantic_event.get("user_id"),
                            "window_start_ms": semantic_event.get("source_window_start_ms"),
                            "window_end_ms": semantic_event.get("source_window_end_ms"),
                            "model": MODEL,
                            "prompt_version": "v1.0",
                            "schema_version": "v1",
                            "llm_latency_ms": llm_ms,
                        }

                        if err:
                            out.update({
                                "risk_level": "UNKNOWN",
                                "urgency": 0,
                                "recommendations": [],
                                "rationale": "LLM invocation failed. Fallback response generated.",
                                "confidence": 0.0,
                                "error": err,
                            })
                        else:
                            out.update({
                                "risk_level": result.get("risk_level", "UNKNOWN"),
                                "urgency": result.get("urgency", 0),
                                "recommendations": result.get("recommendations", []),
                                "rationale": result.get("rationale", ""),
                                "confidence": result.get("confidence", 0.0),
                                "error": None,
                            })

                        # Correctness-first: wait for broker ack, then commit input offset
                        fut = producer.send(OUT_TOPIC, key=key, value=out)
                        fut.get(timeout=10)

                        consumer.commit()

                        # keep log short
                        print(f"[llm_reasoner] emitted reasoning_event user_id={out.get('user_id')} risk={out.get('risk_level')} urgency={out.get('urgency')} llm_ms={llm_ms}", flush=True)

                    except KafkaError as e:
                        print("[llm_reasoner] KAFKA ERROR:", repr(e))
                        time.sleep(1)
                    except Exception as e:
                        print("[llm_reasoner] ERROR:", repr(e))
                        time.sleep(0.2)

    except KeyboardInterrupt:
        print("[llm_reasoner] stopping (KeyboardInterrupt)")
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