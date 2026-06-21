```sql
-- Ingest window for the QUOTES table since the benchmark started.
SELECT
    MIN(last_load_time) AS first_ingest,
    MAX(last_load_time) AS last_ingest,
    COUNT(*)            AS load_events,
    SUM(row_count)      AS rows_loaded
FROM snowflake.account_usage.copy_history
WHERE table_name        = 'QUOTES'
  AND table_schema_name = 'STOCKHOUSE'
  AND last_load_time   >= TO_TIMESTAMP_TZ('2026-06-09 08:43:06.073 -0700');
```

| FIRST_INGEST | LAST_INGEST | LOAD_EVENTS | ROWS_LOADED |
|---|---|---:|---:|
| 2026-06-09 08:43:06.073 -0700 | 2026-06-10 14:19:28.795 -0700 | 100949 | 105546024533 |


```sql
-- Start/end lag between QUOTES ingest activity and automatic clustering activity.
-- Ingest window verified via queries_ingest_window.sql (timestamps below come
-- straight from that result).
WITH
ingest AS (
    SELECT
        TO_TIMESTAMP_TZ('2026-06-09 08:43:06.073 -0700') AS first_ingest,
        TO_TIMESTAMP_TZ('2026-06-10 14:19:28.795 -0700') AS last_ingest
),
clustering AS (
    SELECT
        MIN(start_time) AS first_cluster,
        MAX(end_time)   AS last_cluster
    FROM snowflake.account_usage.automatic_clustering_history
    WHERE table_name   = 'QUOTES'
      AND schema_name  = 'STOCKHOUSE'
      AND credits_used > 0
      AND start_time  >= (SELECT first_ingest FROM ingest)
)
SELECT
    i.first_ingest,
    c.first_cluster,
    TIMEDIFF('second', i.first_ingest, c.first_cluster) / 60.0 AS start_lag_min,
    i.last_ingest,
    c.last_cluster,
    TIMEDIFF('second', i.last_ingest, c.last_cluster)  / 60.0 AS end_lag_min
FROM ingest i, clustering c;
```

| FIRST_INGEST | FIRST_CLUSTER | START_LAG_MIN | LAST_INGEST | LAST_CLUSTER | END_LAG_MIN |
|---|---|---:|---|---|---:|
| 2026-06-09 08:43:06.073 -0700 | 2026-06-09 09:00:00.000 -0700 | 16.900000 | 2026-06-10 14:19:28.795 -0700 | 2026-06-10 16:00:00.000 -0700 | 100.533333 |