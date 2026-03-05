#!/usr/bin/env python3
import json
import os
import sys
import time
from datetime import datetime
from urllib import request, error


SQL_TEMPLATE = """\
SET 'auto.offset.reset' = '{auto_offset_reset}';
SET 'cache.max.bytes.buffering' = '{cache_max_bytes_buffering}';
SET 'processing.guarantee' = '{processing_guarantee}';
SET 'ksql.streams.commit.interval.ms' = '{commit_interval_ms}';

CREATE STREAM IF NOT EXISTS raw_events (
  run_id STRING,
  user_id STRING,
  timestamp BIGINT,
  reaction_time_ms INT
) WITH (
  KAFKA_TOPIC='raw_events',
  VALUE_FORMAT='JSON',
  TIMESTAMP='timestamp',
  PARTITIONS=3
);

{drop_stmt}

CREATE TABLE {out_table}
WITH (
  KAFKA_TOPIC='{out_topic}',
  VALUE_FORMAT='JSON',
  KEY_FORMAT='JSON'
) AS
SELECT
  run_id,
  user_id,
  WINDOWSTART AS window_start_ms,
  WINDOWEND   AS window_end_ms,
  COUNT(*)    AS event_count,
  MAX(reaction_time_ms) AS max_rt,
  MIN(reaction_time_ms) AS min_rt,
  MAX(timestamp) AS last_event_ts_ms,
  UNIX_TIMESTAMP() AS ksql_emit_ts_ms,
  (UNIX_TIMESTAMP() - MAX(timestamp)) AS ksql_latency_ms
FROM raw_events
WINDOW HOPPING (SIZE {window_size_sec} SECONDS, ADVANCE BY {window_advance_sec} SECOND)
GROUP BY run_id, user_id
HAVING COUNT(*) >= {min_events}
   AND (MAX(reaction_time_ms) - MIN(reaction_time_ms)) >= {rt_span_ms}
EMIT CHANGES;
"""

def http_post_json(url: str, payload: dict, timeout_sec: int = 30) -> dict:
    data = json.dumps(payload).encode("utf-8")
    req = request.Request(
        url,
        data=data,
        method="POST",
        headers={"Content-Type": "application/vnd.ksql.v1+json; charset=utf-8"},
    )
    try:
        with request.urlopen(req, timeout=timeout_sec) as resp:
            body = resp.read().decode("utf-8")
            return json.loads(body) if body else {}
    except error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"HTTP {e.code} {e.reason}\n{body}") from None
    except Exception as e:
        raise RuntimeError(f"Request failed: {e}") from None

def wait_ksqldb_ready(ksql_url: str, wait_sec: int = 60, interval_sec: int = 2):
    deadline = time.time() + wait_sec
    while time.time() < deadline:
        try:
            # ksql endpoint expects POST; but we can still check TCP by a short POST with empty statement
            payload = {"ksql": "SHOW STREAMS;", "streamsProperties": {}}
            http_post_json(ksql_url, payload, timeout_sec=5)
            return True
        except Exception as e:
            print(f"[ksql_deployer] ksqlDB not ready yet: {e}; retrying...")
            time.sleep(interval_sec)
    return False

def load_config(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        cfg = json.load(f)

    # basic validation / defaults
    required = [
        "ksqldb_url", "raw_topic", "out_table", "out_topic",
        "window_size_sec", "window_advance_sec", "min_events", "rt_span_ms"
    ]
    missing = [k for k in required if k not in cfg]
    if missing:
        raise ValueError(f"Missing keys in config: {missing}")

    cfg.setdefault("auto_offset_reset", "earliest")
    cfg.setdefault("cache_max_bytes_buffering", 0)
    cfg.setdefault("processing_guarantee", "at_least_once")
    cfg.setdefault("commit_interval_ms", 100)
    cfg.setdefault("drop_output_topic", True)

    return cfg


def build_sql(cfg: dict) -> str:
    drop_stmt = f"DROP TABLE IF EXISTS {cfg['out_table']} " + ("DELETE TOPIC;" if cfg["drop_output_topic"] else ";")
    return SQL_TEMPLATE.format(
        auto_offset_reset=cfg["auto_offset_reset"],
        cache_max_bytes_buffering=cfg["cache_max_bytes_buffering"],
        processing_guarantee=cfg["processing_guarantee"],
        commit_interval_ms=cfg["commit_interval_ms"],
        raw_topic=cfg["raw_topic"],
        out_table=cfg["out_table"],
        out_topic=cfg["out_topic"],
        window_size_sec=int(cfg["window_size_sec"]),
        window_advance_sec=int(cfg["window_advance_sec"]),
        min_events=int(cfg["min_events"]),
        rt_span_ms=int(cfg["rt_span_ms"]),
        drop_stmt=drop_stmt,
    )


def main():
    cfg_path = sys.argv[1] if len(sys.argv) > 1 else "ksql_config.json"
    cfg = load_config(cfg_path)

    sql = build_sql(cfg)

    # archive generated sql for reproducibility
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir = os.path.join("runs", f"ksql_{ts}")
    os.makedirs(out_dir, exist_ok=True)
    sql_path = os.path.join(out_dir, "deployed.sql")
    with open(sql_path, "w", encoding="utf-8") as f:
        f.write(sql)

    # Submit to ksqlDB
    ksql_url = cfg["ksqldb_url"].rstrip("/") + "/ksql"
    payload = {"ksql": sql, "streamsProperties": {}}

    print(f"Submitting SQL to {ksql_url}")
    print(f"Saved rendered SQL to {sql_path}")

    if not wait_ksqldb_ready(ksql_url, wait_sec=90):
        raise RuntimeError("ksqlDB did not become ready in time")

    resp = http_post_json(ksql_url, payload, timeout_sec=60)
    # ksql returns a list of command statuses / errors
    print(json.dumps(resp, indent=2))


if __name__ == "__main__":
    main()