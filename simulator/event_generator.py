import json
import time
import uuid
import random
import os
from confluent_kafka import Producer

BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = "raw_events"
RUN_ID = str(uuid.uuid4())

producer = Producer({
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "linger.ms": 20,
    "batch.num.messages": 10000,
    "acks": "1",
})

def delivery_report(err, msg):
    if err is not None:
        print("delivery failed:", err)
    else:
        print("sent", msg.topic(), msg.partition(), msg.offset())

def send_event(user_id: str, reaction_time: int, scenario: str):
    now_ms = int(time.time() * 1000)
    event = {
        "event_id": str(uuid.uuid4()),
        "run_id": RUN_ID,
        "timestamp": now_ms,
        "user_id": user_id,
        "event_type": "hand_swipe",
        "reaction_time_ms": reaction_time,
        "scenario": scenario,
        "sent_at_ms": now_ms
    }

    producer.produce(
        TOPIC,
        key=user_id.encode("utf-8"),
        value=json.dumps(event).encode("utf-8"),
        callback=delivery_report
    )
    producer.poll(0)

def baseline_phase(user_id, duration_sec=5, seed=42, interval_sec=1.0):
    random.seed(seed + hash(user_id) % 1000)
    end_t = time.time() + duration_sec
    while time.time() < end_t:
        rt = 300 + random.randint(-20, 20)
        send_event(user_id, rt, "baseline")
        time.sleep(interval_sec)

def trigger_phase(user_id, interval_sec=0.8):
    rts = [300, 350, 420, 500, 650, 800]
    for rt in rts:
        send_event(user_id, rt, "trigger")
        time.sleep(interval_sec)

if __name__ == "__main__":
    users = ["user_1", "user_2", "user_3"]
    MODE = "baseline+trigger"  # baseline-only / trigger-only

    print("RUN_ID:", RUN_ID, "MODE:", MODE)

    if MODE == "baseline-only":
        for u in users:
            baseline_phase(u, duration_sec=10)

    elif MODE == "trigger-only":
        for u in users:
            trigger_phase(u)

    else:
        for u in users:
            baseline_phase(u, duration_sec=5)
        for u in users:
            trigger_phase(u)

    producer.flush(10)