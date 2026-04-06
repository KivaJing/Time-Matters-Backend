#!/usr/bin/env python3
import argparse
import json
import os
import time
from confluent_kafka import Consumer


def safe_int(v):
    try:
        if v is None:
            return None
        return int(v)
    except Exception:
        return None


def main():
    ap = argparse.ArgumentParser(description='Dump a Kafka topic to JSONL with optional run_id filtering.')
    ap.add_argument('--bootstrap-servers', default=os.getenv('BOOTSTRAP_SERVERS', 'localhost:9092'))
    ap.add_argument('--topic', required=True)
    ap.add_argument('--output-jsonl', required=True)
    ap.add_argument('--run-id', default='')
    ap.add_argument('--from-beginning', action='store_true')
    ap.add_argument('--warmup-sec', type=int, default=0)
    ap.add_argument('--measure-sec', type=int, default=0)
    ap.add_argument('--time-field', default='', help='Optional event time field for measurement window filtering')
    args = ap.parse_args()

    consumer = Consumer({
        'bootstrap.servers': args.bootstrap_servers,
        'group.id': f'dump-topic-{int(time.time()*1000)}',
        'auto.offset.reset': 'earliest' if args.from_beginning else 'latest',
        'enable.auto.commit': True,
    })
    consumer.subscribe([args.topic])

    start = time.time()
    measure_start = start + args.warmup_sec
    measure_end = measure_start + args.measure_sec if args.measure_sec > 0 else None
    measure_start_ms = int(measure_start * 1000)
    measure_end_ms = int(measure_end * 1000) if measure_end else None

    written = 0
    try:
        with open(args.output_jsonl, 'w', encoding='utf-8') as out:
            while True:
                if measure_end and time.time() > measure_end:
                    break
                msg = consumer.poll(1.0)
                if msg is None or msg.error():
                    if args.measure_sec == 0 and written > 0:
                        continue
                    continue
                try:
                    evt = json.loads(msg.value().decode('utf-8'))
                except Exception:
                    continue

                if args.run_id and evt.get('run_id') != args.run_id:
                    continue

                if args.time_field:
                    ts = safe_int(evt.get(args.time_field))
                    if ts is None:
                        continue
                    if ts < measure_start_ms:
                        continue
                    if measure_end_ms and ts > measure_end_ms:
                        continue

                out.write(json.dumps(evt, ensure_ascii=False) + '\n')
                written += 1
    finally:
        consumer.close()

    print(json.dumps({'status': 'ok', 'written': written, 'topic': args.topic, 'output': args.output_jsonl}, ensure_ascii=False))


if __name__ == '__main__':
    main()
