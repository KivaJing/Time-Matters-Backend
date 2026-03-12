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

CREATE STREAM IF NOT EXISTS raw_state_events (
  event_id STRING,
  run_id STRING,
  user_id STRING,
  timestamp BIGINT,
  event_type STRING,
  logical_step INT,
  logical_minutes_per_step DOUBLE,
  craving INT,
  stress INT,
  motivation INT,
  self_efficacy INT,
  strategy_use INT,
  receptivity INT,
  is_moving INT,
  in_meeting INT,
  location_type STRING,
  scenario STRING,
  sent_at_ms BIGINT
) WITH (
  KAFKA_TOPIC='{raw_topic}',
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

  COUNT(*) AS event_count,

  AVG(CAST(craving AS DOUBLE)) AS craving_avg,
  AVG(CAST(stress AS DOUBLE)) AS stress_avg,
  AVG(CAST(motivation AS DOUBLE)) AS motivation_avg,
  AVG(CAST(self_efficacy AS DOUBLE)) AS self_efficacy_avg,

  MIN(motivation) AS motivation_min,
  MIN(self_efficacy) AS self_efficacy_min,
  MAX(craving) AS craving_max,
  MAX(stress) AS stress_max,

  LATEST_BY_OFFSET(strategy_use) AS latest_strategy_use,
  LATEST_BY_OFFSET(receptivity) AS latest_receptivity,
  LATEST_BY_OFFSET(is_moving) AS latest_is_moving,
  LATEST_BY_OFFSET(in_meeting) AS latest_in_meeting,
  LATEST_BY_OFFSET(location_type) AS latest_location_type,
  LATEST_BY_OFFSET(scenario) AS latest_scenario,

  MAX(sent_at_ms) AS last_sent_at_ms,
  MAX(timestamp) AS last_event_ts_ms,
  UNIX_TIMESTAMP() AS ksql_emit_ts_ms,
  (UNIX_TIMESTAMP() - MAX(sent_at_ms)) AS ksql_latency_ms

FROM raw_state_events
WINDOW HOPPING (SIZE {window_size_sec} SECONDS, ADVANCE BY {window_advance_sec} SECONDS)
GROUP BY run_id, user_id
HAVING COUNT(*) >= {min_events}
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

    if "KSQLDB_URL" in os.environ:
        cfg["ksqldb_url"] = os.environ["KSQLDB_URL"]

    required = [
        "ksqldb_url",
        "raw_topic",
        "out_table",
        "out_topic",
        "window_size_sec",
        "window_advance_sec",
        "min_events",
    ]
    missing = [k for k in required if k not in cfg]
    if missing:
        raise ValueError(f"Missing keys in config: {missing}")

    cfg.setdefault("auto_offset_reset", "earliest")
    cfg.setdefault("cache_max_bytes_buffering", 0)
    cfg.setdefault("processing_guarantee", "at_least_once")
    cfg.setdefault("commit_interval_ms", 50)
    cfg.setdefault("drop_output_topic", True)
    return cfg


def build_sql(cfg: dict) -> str:
    drop_stmt = f"DROP TABLE IF EXISTS {cfg['out_table']} " + (
        "DELETE TOPIC;" if cfg["drop_output_topic"] else ";"
    )

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
        drop_stmt=drop_stmt,
    )


def main():
    cfg_path = sys.argv[1] if len(sys.argv) > 1 else "ksql_config.json"
    cfg = load_config(cfg_path)
    sql = build_sql(cfg)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir = os.path.join("runs", f"ksql_{ts}")
    os.makedirs(out_dir, exist_ok=True)
    sql_path = os.path.join(out_dir, "deployed.sql")
    with open(sql_path, "w", encoding="utf-8") as f:
        f.write(sql)

    ksql_url = cfg["ksqldb_url"].rstrip("/") + "/ksql"
    payload = {"ksql": sql, "streamsProperties": {}}

    print(f"Submitting SQL to {ksql_url}")
    print(f"Saved rendered SQL to {sql_path}")

    if not wait_ksqldb_ready(ksql_url, wait_sec=90):
        raise RuntimeError("ksqlDB did not become ready in time")

    resp = http_post_json(ksql_url, payload, timeout_sec=60)
    print(json.dumps(resp, indent=2))


if __name__ == "__main__":
    main()
