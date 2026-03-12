SET 'auto.offset.reset' = 'earliest';
SET 'cache.max.bytes.buffering' = '0';
SET 'processing.guarantee' = 'at_least_once';
SET 'ksql.streams.commit.interval.ms' = '50';

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
  KAFKA_TOPIC='raw_state_events',
  VALUE_FORMAT='JSON',
  TIMESTAMP='timestamp',
  PARTITIONS=3
);

DROP TABLE IF EXISTS risk_state_windows DELETE TOPIC;

CREATE TABLE risk_state_windows
WITH (
  KAFKA_TOPIC='risk_state_windows',
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
WINDOW HOPPING (SIZE 25 SECONDS, ADVANCE BY 5 SECONDS)
GROUP BY run_id, user_id
HAVING COUNT(*) >= 1
EMIT CHANGES;
