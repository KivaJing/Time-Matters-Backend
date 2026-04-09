#!/usr/bin/env python3
import asyncio
import json
import os
import signal
import threading
import time
import uuid
from typing import Set

from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable
from websockets.asyncio.server import serve
from websockets.exceptions import ConnectionClosed

BOOTSTRAP = os.getenv("BOOTSTRAP", "localhost:9092")

EVENTS_TOPIC = os.getenv("EVENTS_TOPIC", "intervention_events")
FEEDBACK_TOPIC = os.getenv("FEEDBACK_TOPIC", "intervention_feedback")

EVENTS_GROUP_ID = os.getenv("EVENTS_GROUP_ID", "unity-events-bridge")
WS_HOST = os.getenv("WS_HOST", "0.0.0.0")
WS_PORT = int(os.getenv("WS_PORT", "8765"))

clients: Set = set()
loop = None
producer = None


def now_ms() -> int:
    return int(time.time() * 1000)


def build_consumer_with_retry(*args, retries: int = 60, sleep_sec: float = 2.0, **kwargs):
    last = None
    for i in range(retries):
        try:
            return KafkaConsumer(*args, **kwargs)
        except NoBrokersAvailable as e:
            last = e
            print(f"[bridge] consumer no broker ({i+1}/{retries}), retry in {sleep_sec}s...", flush=True)
            time.sleep(sleep_sec)
    raise last


def build_producer_with_retry(retries: int = 60, sleep_sec: float = 2.0, **kwargs):
    last = None
    for i in range(retries):
        try:
            return KafkaProducer(**kwargs)
        except NoBrokersAvailable as e:
            last = e
            print(f"[bridge] producer no broker ({i+1}/{retries}), retry in {sleep_sec}s...", flush=True)
            time.sleep(sleep_sec)
    raise last


async def broadcast_obj(evt: dict):
    """
    Stamp the event at the bridge immediately before websocket send.
    This timestamp is the backend-side send baseline for Unity delivery.
    """
    if "event_id" not in evt or not evt["event_id"]:
        evt["event_id"] = str(uuid.uuid4())

    evt["bridge_sent_ts_ms"] = now_ms()
    payload = json.dumps(evt, ensure_ascii=False)

    dead = []
    delivered = 0

    for ws in list(clients):
        try:
            await ws.send(payload)
            delivered += 1
        except Exception as e:
            print(f"[bridge] send error: {e!r}", flush=True)
            dead.append(ws)

    for ws in dead:
        clients.discard(ws)

    print(
        f"[bridge] broadcast event_id={evt.get('event_id')} user_id={evt.get('user_id')} "
        f"action={evt.get('action')} urgency={evt.get('urgency')} "
        f"bridge_sent_ts_ms={evt.get('bridge_sent_ts_ms')} clients={delivered}",
        flush=True,
    )


def kafka_events_worker():
    global loop
    consumer = build_consumer_with_retry(
        EVENTS_TOPIC,
        bootstrap_servers=BOOTSTRAP,
        group_id=EVENTS_GROUP_ID,
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
        auto_offset_reset="latest",
    )

    print(f"[bridge] consuming Kafka topic={EVENTS_TOPIC} bootstrap={BOOTSTRAP}", flush=True)

    while True:
        records = consumer.poll(timeout_ms=1000, max_records=100)
        if not records:
            continue

        for _, msgs in records.items():
            for msg in msgs:
                try:
                    evt = dict(msg.value)

                    print(
                        f"[bridge] forward to unity user_id={evt.get('user_id')} "
                        f"action={evt.get('action')} urgency={evt.get('urgency')}",
                        flush=True,
                    )

                    if loop is not None:
                        asyncio.run_coroutine_threadsafe(broadcast_obj(evt), loop)
                except Exception as e:
                    print(f"[bridge] kafka_events_worker error: {e!r}", flush=True)


async def ws_handler(websocket):
    global producer
    clients.add(websocket)
    print(f"[bridge] client connected total={len(clients)}", flush=True)

    try:
        async for message in websocket:
            try:
                obj = json.loads(message)
                msg_type = obj.get("type")

                if msg_type == "intervention_feedback":
                    bridge_feedback_received_ts_ms = now_ms()
                    obj["bridge_feedback_received_ts_ms"] = bridge_feedback_received_ts_ms

                    bridge_sent_ts_ms = obj.get("bridge_sent_ts_ms", 0)
                    if bridge_sent_ts_ms:
                        obj["bridge_roundtrip_ms"] = max(
                            0, bridge_feedback_received_ts_ms - bridge_sent_ts_ms
                        )
                    else:
                        obj["bridge_roundtrip_ms"] = -1

                    key = obj.get("user_id", "")
                    producer.send(FEEDBACK_TOPIC, key=key, value=obj).get(timeout=10)

                    print(
                        f"[bridge] feedback -> kafka event_id={obj.get('event_id')} "
                        f"user_id={obj.get('user_id')} action={obj.get('action')} "
                        f"result={obj.get('result')} "
                        f"unity_render_ms={obj.get('unity_render_ms')} "
                        f"bridge_roundtrip_ms={obj.get('bridge_roundtrip_ms')}",
                        flush=True,
                    )
                else:
                    print(f"[bridge] unknown ws message type={msg_type}", flush=True)
            except Exception as e:
                print(f"[bridge] ws_handler message error: {e!r}", flush=True)
    except ConnectionClosed:
        pass
    finally:
        clients.discard(websocket)
        print(f"[bridge] client disconnected total={len(clients)}", flush=True)


async def main():
    global loop, producer
    loop = asyncio.get_running_loop()

    producer = build_producer_with_retry(
        bootstrap_servers=BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        linger_ms=20,
        retries=3,
        acks="all",
    )

    threading.Thread(target=kafka_events_worker, daemon=True).start()

    async with serve(ws_handler, WS_HOST, WS_PORT):
        print(f"[bridge] websocket listening on ws://{WS_HOST}:{WS_PORT}", flush=True)

        stop = asyncio.Future()

        def _stop(*_):
            if not stop.done():
                stop.set_result(None)

        signal.signal(signal.SIGINT, _stop)
        signal.signal(signal.SIGTERM, _stop)

        await stop


if __name__ == "__main__":
    asyncio.run(main())