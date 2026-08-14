-- Run once before the first restart with config.d/system-logs.xml mounted.
-- Updating the existing definitions prevents ClickHouse from preserving the old
-- tables under numbered names when the configured TTL changes.

ALTER TABLE system.query_log MODIFY TTL event_date + INTERVAL 3 DAY DELETE;
ALTER TABLE system.trace_log MODIFY TTL event_date + INTERVAL 3 DAY DELETE;
ALTER TABLE system.part_log MODIFY TTL event_date + INTERVAL 3 DAY DELETE;
ALTER TABLE system.text_log MODIFY TTL event_date + INTERVAL 3 DAY DELETE;
ALTER TABLE system.metric_log MODIFY TTL event_date + INTERVAL 3 DAY DELETE;
ALTER TABLE system.error_log MODIFY TTL event_date + INTERVAL 3 DAY DELETE;
ALTER TABLE system.query_metric_log MODIFY TTL event_date + INTERVAL 3 DAY DELETE;
ALTER TABLE system.asynchronous_metric_log MODIFY TTL event_date + INTERVAL 3 DAY DELETE;
ALTER TABLE system.processors_profile_log MODIFY TTL event_date + INTERVAL 3 DAY DELETE;
ALTER TABLE system.asynchronous_insert_log MODIFY TTL event_date + INTERVAL 3 DAY DELETE;
ALTER TABLE system.background_schedule_pool_log MODIFY TTL event_date + INTERVAL 3 DAY DELETE;

TRUNCATE TABLE system.query_log SYNC;
TRUNCATE TABLE system.trace_log SYNC;
TRUNCATE TABLE system.part_log SYNC;
TRUNCATE TABLE system.text_log SYNC;
TRUNCATE TABLE system.metric_log SYNC;
TRUNCATE TABLE system.error_log SYNC;
TRUNCATE TABLE system.query_metric_log SYNC;
TRUNCATE TABLE system.asynchronous_metric_log SYNC;
TRUNCATE TABLE system.processors_profile_log SYNC;
TRUNCATE TABLE system.asynchronous_insert_log SYNC;
TRUNCATE TABLE system.background_schedule_pool_log SYNC;
