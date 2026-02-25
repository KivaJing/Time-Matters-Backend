import json
import time
import requests
from kafka import KafkaConsumer, KafkaProducer

BOOTSTRAP = "localhost:9092"
IN_TOPIC = "semantic_events"
OUT_TOPIC = "reasoning_events"
GROUP_ID = "llm-reasoner-v1"

OLLAMA_URL = "http://127.0.0.1:11434/api/chat"
MODEL = "llama3.1:8b"
TIMEOUT_SEC = 8

def build_prompt(semantic_event: dict) -> tuple[str, str]:
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
            "confidence": "Float 0.0-1.0"
        },
        "output_example": {
            "risk_level": "MEDIUM",
            "urgency": 55,
            "recommendations": [
                "Increase monitoring frequency for the next 10 minutes.",
                "Consider throttling non-critical workload if trigger rate increases."
            ],
            "rationale": "Trigger frequency increased across consecutive windows while severity is MEDIUM.",
            "confidence": 0.72
        }
    }

    return system, json.dumps(user, ensure_ascii=False)

def call_ollama(system: str, user: str) -> tuple[dict, int, str | None]:
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
    consumer = KafkaConsumer(
        IN_TOPIC,
        bootstrap_servers=BOOTSTRAP,
        group_id=GROUP_ID,
        enable_auto_commit=False,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
        auto_offset_reset="latest",
    )

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
    )

    print(f"[llm_reasoner] consuming {IN_TOPIC} -> producing {OUT_TOPIC}, model={MODEL}")

    for msg in consumer:
        semantic_event = msg.value
        key = msg.key or ""

        print(f"[llm_reasoner] got semantic_event key={key} user_id={semantic_event.get('user_id')} window_end={semantic_event.get('source_window_end_ms')}")


        system, user = build_prompt(semantic_event)
        result, llm_ms, err = call_ollama(system, user)

        print(f"[llm_reasoner] ollama_ms={llm_ms} err={err is not None}")
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
            print("[llm_reasoner] ollama_error:", err)
        else:
            out.update({
                "risk_level": result.get("risk_level", "UNKNOWN"),
                "urgency": result.get("urgency", 0),
                "recommendations": result.get("recommendations", []),
                "rationale": result.get("rationale", ""),
                "confidence": result.get("confidence", 0.0),
                "error": None,
            })

        producer.send(OUT_TOPIC, key=key, value=out)
        producer.flush()
        consumer.commit()

if __name__ == "__main__":
    main()