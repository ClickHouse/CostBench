-- Read-only provider evidence for the failed canonical R2 run.
-- Run on the control warehouse after system tables have settled.

-- 1. Exact provider totals for the complete failed target lifetime.
SELECT
    sum(committed_records) AS provider_committed_records,
    sum(committed_bytes) AS provider_committed_bytes,
    sum(COALESCE(size(errors), 0)) AS provider_error_count,
    min(commit_version) AS min_commit_version,
    max(commit_version) AS max_commit_version,
    min(commit_time) AS first_commit_time,
    max(commit_time) AS last_commit_time
FROM system.lakeflow.zerobus_ingest
WHERE table_name =
    'costbench.rt_full_serverless_baseline_r2_20260917.quotes'
  AND commit_time >= CAST('2026-09-17T18:27:26Z' AS TIMESTAMP)
  AND commit_time < CAST('2026-09-18T12:00:00Z' AS TIMESTAMP);

-- 2. Per-stream commits around the failure.
SELECT
    date_trunc('minute', commit_time) AS commit_minute,
    stream_id,
    sum(committed_records) AS committed_records,
    sum(committed_bytes) AS committed_bytes,
    sum(COALESCE(size(errors), 0)) AS error_count,
    min(commit_version) AS min_commit_version,
    max(commit_version) AS max_commit_version,
    min(commit_time) AS first_commit_time,
    max(commit_time) AS last_commit_time
FROM system.lakeflow.zerobus_ingest
WHERE table_name =
    'costbench.rt_full_serverless_baseline_r2_20260917.quotes'
  AND commit_time >= CAST('2026-09-18T11:35:00Z' AS TIMESTAMP)
  AND commit_time < CAST('2026-09-18T12:00:00Z' AS TIMESTAMP)
GROUP BY commit_minute, stream_id
ORDER BY commit_minute, stream_id;

-- 3. Provider stream lifecycle and error payloads around the failure.
SELECT
    stream_id,
    event_time,
    opened_time,
    closed_time,
    producer_id,
    protocol,
    data_format,
    errors
FROM system.lakeflow.zerobus_stream
WHERE table_name =
    'costbench.rt_full_serverless_baseline_r2_20260917.quotes'
  AND event_time >= CAST('2026-09-18T11:35:00Z' AS TIMESTAMP)
  AND event_time < CAST('2026-09-18T12:00:00Z' AS TIMESTAMP)
ORDER BY event_time, stream_id;

-- 4. Any provider-recorded stream errors across the entire run.
SELECT
    stream_id,
    event_time,
    opened_time,
    closed_time,
    errors
FROM system.lakeflow.zerobus_stream
WHERE table_name =
    'costbench.rt_full_serverless_baseline_r2_20260917.quotes'
  AND event_time >= CAST('2026-09-17T18:27:26Z' AS TIMESTAMP)
  AND event_time < CAST('2026-09-18T12:00:00Z' AS TIMESTAMP)
  AND COALESCE(size(errors), 0) > 0
ORDER BY event_time, stream_id;

-- 5. Metadata only; this does not scan the raw table.
DESCRIBE DETAIL
costbench.rt_full_serverless_baseline_r2_20260917.quotes;
