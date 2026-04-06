#!/usr/bin/env python3
import argparse
import json
import os
import sys
import time
from typing import Iterable, Dict, Any, Optional

from confluent_kafka import Producer


def read_jsonl(path: str) -> Iterable[Dict[str, Any]]:
    with open(path, 'r', encoding='utf-8') as f:
        for line_no, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                evt = json.loads(line)
            except Exception as e:
                raise ValueError(f'Invalid JSON on line {line_no}: {e}')
            if not isinstance(evt, dict):
                raise ValueError(f'Line {line_no} is not a JSON object')
            yield evt


def build_producer(bootstrap_servers: str) -> Producer:
    return Producer({
        'bootstrap.servers': bootstrap_servers,
        'linger.ms': 20,
        'batch.num.messages': 10000,
        'acks': '1',
    })


def maybe_int(v: Any) -> Optional[int]:
    try:
        if v is None:
            return None
        return int(v)
    except Exception:
        return None


def choose_timestamp_field(evt: Dict[str, Any], override: Optional[str]) -> Optional[str]:
    if override:
        return override
    candidates = [
        'timestamp',
        'sent_at_ms',
        'source_last_sent_at_ms',
        'source_ksql_emit_ts_ms',
        'engine_ts_ms',
        'semantic_engine_ts_ms',
        'scheduler_ts_ms',
        'reasoning_ts_ms',
        'intervention_ts_ms',
    ]
    for f in candidates:
        if evt.get(f) is not None:
            return f
    return None


def delivery_report(err, msg):
    if err is not None:
        sys.stderr.write(f'delivery failed: {err}\n')


def main():
    ap = argparse.ArgumentParser(description='Replay a frozen JSONL event log into Kafka.')
    ap.add_argument('--input-jsonl', required=True, help='Source JSONL event log')
    ap.add_argument('--bootstrap-servers', default=os.getenv('BOOTSTRAP_SERVERS', 'localhost:9092'))
    ap.add_argument('--topic', required=True, help='Destination Kafka topic')
    ap.add_argument('--new-run-id', required=True, help='Run id to assign to replayed events')
    ap.add_argument('--key-field', default='user_id', help='Field to use as Kafka key')
    ap.add_argument('--source-run-id', default='', help='Optional filter to replay only one run_id from the JSONL file')
    ap.add_argument('--pace-mode', choices=['original', 'fixed', 'none'], default='original', help='Replay timing mode')
    ap.add_argument('--fixed-interval-sec', type=float, default=0.0, help='Used when --pace-mode=fixed')
    ap.add_argument('--timestamp-field', default='', help='Explicit source timestamp field for original pacing')
    ap.add_argument('--rewrite-timestamps', action='store_true', help='Rewrite event timestamps to wall-clock replay time')
    ap.add_argument('--rewrite-fields', default='timestamp,sent_at_ms', help='Comma-separated fields to rewrite when --rewrite-timestamps is set')
    ap.add_argument('--max-events', type=int, default=0, help='Optional cap on number of replayed events')
    ap.add_argument('--verbose', action='store_true')
    args = ap.parse_args()

    producer = build_producer(args.bootstrap_servers)
    rewrite_fields = [x.strip() for x in args.rewrite_fields.split(',') if x.strip()]

    events = []
    for evt in read_jsonl(args.input_jsonl):
        if args.source_run_id and evt.get('run_id') != args.source_run_id:
            continue
        events.append(evt)
        if args.max_events and len(events) >= args.max_events:
            break

    if not events:
        raise RuntimeError('No events selected for replay.')

    ts_field = choose_timestamp_field(events[0], args.timestamp_field or None)
    if args.pace_mode == 'original' and not ts_field:
        raise RuntimeError('Could not infer timestamp field for original pacing. Use --timestamp-field.')

    base_ts = maybe_int(events[0].get(ts_field)) if ts_field else None
    replay_start = time.time()

    sent = 0
    for idx, evt in enumerate(events):
        out = dict(evt)
        out['run_id'] = args.new_run_id
        out['_replay_source_run_id'] = evt.get('run_id')
        out['_replay_ordinal'] = idx

        if args.rewrite_timestamps:
            now_ms = int(time.time() * 1000)
            for f in rewrite_fields:
                if f in out:
                    out[f] = now_ms

        if args.pace_mode == 'original':
            evt_ts = maybe_int(evt.get(ts_field))
            if evt_ts is None:
                raise RuntimeError(f'Event {idx} missing timestamp field {ts_field}')
            offset_sec = (evt_ts - base_ts) / 1000.0
            target = replay_start + offset_sec
            sleep_sec = max(0.0, target - time.time())
            if sleep_sec > 0:
                time.sleep(sleep_sec)
        elif args.pace_mode == 'fixed' and idx > 0:
            time.sleep(max(0.0, args.fixed_interval_sec))

        key_val = out.get(args.key_field)
        key_bytes = None if key_val is None else str(key_val).encode('utf-8')

        producer.produce(
            args.topic,
            key=key_bytes,
            value=json.dumps(out, ensure_ascii=False).encode('utf-8'),
            callback=delivery_report,
        )
        producer.poll(0)
        sent += 1

        if args.verbose and (sent <= 5 or sent % 1000 == 0):
            sys.stderr.write(
                f'replayed={sent} topic={args.topic} key={key_val} replay_ordinal={idx}\n'
            )

    producer.flush(30)
    print(json.dumps({
        'status': 'ok',
        'topic': args.topic,
        'new_run_id': args.new_run_id,
        'events_replayed': sent,
        'pace_mode': args.pace_mode,
        'timestamp_field': ts_field,
    }, ensure_ascii=False))


if __name__ == '__main__':
    main()
