SET 'auto.offset.reset' = 'earliest';
SET 'cache.max.bytes.buffering' = '0';
SET 'processing.guarantee' = 'at_least_once';
SET 'ksql.streams.commit.interval.ms' = '50';

CREATE STREAM IF NOT EXISTS raw_events (
  event_id STRING,
  run_id STRING,
  user_id STRING,
  timestamp BIGINT,
  event_type STRING,
  reaction_time_ms INT,
  scenario STRING,
  sent_at_ms BIGINT
) WITH (
  KAFKA_TOPIC='raw_events',
  VALUE_FORMAT='JSON',
  TIMESTAMP='timestamp',
  PARTITIONS=3
);

DROP TABLE IF EXISTS complex_events_eval_w10_c50 DELETE TOPIC;

CREATE TABLE complex_events_eval_w10_c50
WITH (
  KAFKA_TOPIC='complex_events_eval_w10_c50',
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
  MAX(sent_at_ms) AS last_sent_at_ms,
  MAX(timestamp) AS last_event_ts_ms,
  LATEST_BY_OFFSET(scenario) AS latest_scenario,
  UNIX_TIMESTAMP() AS ksql_emit_ts_ms,
  (UNIX_TIMESTAMP() - MAX(sent_at_ms)) AS ksql_latency_ms
FROM raw_events
WINDOW HOPPING (SIZE 10 SECONDS, ADVANCE BY 1 SECOND)
GROUP BY run_id, user_id
HAVING COUNT(*) >= 5
   AND (MAX(reaction_time_ms) - MIN(reaction_time_ms)) >= 300
EMIT CHANGES;
