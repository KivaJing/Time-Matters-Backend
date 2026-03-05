SET 'auto.offset.reset' = 'earliest';
SET 'cache.max.bytes.buffering' = '0';
SET 'processing.guarantee' = 'at_least_once';
SET 'ksql.streams.commit.interval.ms' = '50';

CREATE STREAM IF NOT EXISTS raw_events (
  run_id STRING,
  user_id STRING,
  timestamp BIGINT,
  reaction_time_ms INT
) WITH (
  KAFKA_TOPIC='raw_events',
  VALUE_FORMAT='JSON',
  TIMESTAMP='timestamp'
);

DROP TABLE IF EXISTS complex_events_eval DELETE TOPIC;

CREATE TABLE complex_events_eval
WITH (
  KAFKA_TOPIC='complex_events_eval',
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
WINDOW HOPPING (SIZE 5 SECONDS, ADVANCE BY 1 SECOND)
GROUP BY run_id, user_id
HAVING COUNT(*) >= 5
   AND (MAX(reaction_time_ms) - MIN(reaction_time_ms)) >= 300
EMIT CHANGES;
