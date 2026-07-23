-- =============================================================================
-- Snowflake metrics reference — every metric query the benchmark scripts run, in
-- one place, with a short description. Copy/paste into a Snowflake worksheet (or
-- `snow sql`) when you'd rather query directly than run the ops/ scripts.
--
-- Conventions: replace <SCHEMA> (e.g. STOCKHOUSE_T0 / _T1 / _T2), <RAW> (QUOTES or
-- QUOTES_IT), <ROLLUP> (QUOTES_DAILY / QUOTES_DAILY_IT / QUOTES_DAILY_IMV), <HOURS>
-- (lookback window), and warehouse names to match your run. Each block notes the
-- script that automates it.
--
-- Caveats up front:
--   • SNOWFLAKE.ACCOUNT_USAGE.* views lag up to ~3h — final cost numbers settle hours after a run.
--   • INFORMATION_SCHEMA table functions only cover recent history / the current session's scope.
--   • SYSTEM$CLUSTERING_INFORMATION is point-in-time (never historized) — sample it DURING the run.
-- =============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE BENCH2COST;
USE SCHEMA BENCH2COST.<SCHEMA>;


-- =============================================================================
-- 1. QUERY LATENCY  (runner_common.py — run_drilldown.py / run_dashboard.py)
-- Server-side execution time of a query, in ms. The runners capture each query's
-- id (cur.sfqid) then look up EXECUTION_TIME — this is the latency we report, NOT
-- client wall-clock. Disable the result cache first so the query actually executes.
-- =============================================================================
ALTER SESSION SET USE_CACHED_RESULT = FALSE;   -- the runners set this each session

-- by a specific query id (what the runner does right after running the query):
SELECT EXECUTION_TIME            -- milliseconds
FROM TABLE(BENCH2COST.INFORMATION_SCHEMA.QUERY_HISTORY())
WHERE QUERY_ID = '<query_id>';

-- recent queries on a warehouse (ad-hoc browsing):
SELECT query_id, start_time, warehouse_name, execution_time/1000 AS exec_sec,
       left(query_text, 80) AS query
FROM TABLE(BENCH2COST.INFORMATION_SCHEMA.QUERY_HISTORY())
WHERE warehouse_name = '<WAREHOUSE>'
ORDER BY start_time DESC
LIMIT 50;


-- =============================================================================
-- 2. DATA VOLUME / ROW COUNTS  (runner_common.py, lag.sh)
-- The x-axis of the latency-vs-volume charts; also a quick freshness sanity check.
-- =============================================================================
SELECT COUNT(*) AS raw_rows FROM <RAW>;          -- base/raw table volume
SELECT COUNT(*) AS rollup_rows FROM <ROLLUP>;    -- rollup logical rows

-- rollup freshness sanity (lag.sh): rows the rollup has "caught up" to vs the base table
SELECT (SELECT COUNT(*) FROM <RAW>)                       AS base_rows,
       (SELECT COALESCE(SUM(n_quotes),0) FROM <ROLLUP>)   AS rollup_reflected_rows;


-- =============================================================================
-- 3. INTERACTIVE-TABLE REFRESH LATENCY & FRESHNESS LAG  (ops/it_refresh.sh, ops/collect_it_refresh.sh)
-- Per-refresh history for interactive tables (QUOTES_IT, QUOTES_DAILY_IT). duration =
-- refresh_end - refresh_start; STALENESS_AT_DONE = how stale the data was when the refresh
-- finished (= data_timestamp -> refresh_end) — the IT analogue of an MV's behind_by.
-- NOTE (T2): an interactive *materialized view* (QUOTES_DAILY_IMV) may instead show up under
-- MATERIALIZED_VIEW_REFRESH_HISTORY — see block 4 if this returns no rows for the IMV.
-- =============================================================================
-- full raw history (ops/it_refresh.sh polls this):
SELECT *
FROM TABLE(INFORMATION_SCHEMA.INTERACTIVE_TABLE_REFRESH_HISTORY())
WHERE database_name = 'BENCH2COST' AND schema_name = '<SCHEMA>'
ORDER BY refresh_end_time;

-- chart-ready shape (ops/collect_it_refresh.sh -> CSV for the it_lag chart). The timestamp
-- format matches the renderer's parser; STALENESS_AT_DONE_SEC is the lag series.
SELECT name,
       TO_CHAR(refresh_end_time, 'YYYY-MM-DD HH24:MI:SS.FF3 TZHTZM')  AS refresh_end_time,
       DATEDIFF('second', data_timestamp,    refresh_end_time)        AS staleness_at_done_sec,
       DATEDIFF('second', refresh_start_time, refresh_end_time)       AS duration_sec,
       state
FROM TABLE(INFORMATION_SCHEMA.INTERACTIVE_TABLE_REFRESH_HISTORY())
WHERE database_name = 'BENCH2COST' AND schema_name = '<SCHEMA>'
ORDER BY refresh_end_time;


-- =============================================================================
-- 4. MATERIALIZED-VIEW FRESHNESS LAG & REFRESH HISTORY  (ops/mv_latency.sh, ops/lag.sh, ops/mv_billing.sh)
-- A Snowflake MV is maintained by a serverless background service; `behind_by` (from SHOW) is
-- how far it trails the base table. There is no warehouse to assign — SHOW is metadata-only/free.
-- =============================================================================
-- freshness lag right now (ops/mv_latency.sh polls this; `behind_by` e.g. '14m28s'):
SHOW MATERIALIZED VIEWS LIKE '<ROLLUP>' IN SCHEMA BENCH2COST.<SCHEMA>;
--   -> read columns: behind_by, rows, bytes, refreshed_on, invalid

-- per-refresh history + serverless credits (ops/mv_billing.sh); covers an interactive MV (IMV) too:
SELECT COUNT(*)                       AS refreshes,
       COALESCE(SUM(credits_used),0)  AS mv_credits,
       MAX(end_time)                  AS last_refresh_end
FROM SNOWFLAKE.ACCOUNT_USAGE.MATERIALIZED_VIEW_REFRESH_HISTORY
WHERE table_name = '<ROLLUP>'
  AND start_time >= DATEADD(hour, -<HOURS>, CURRENT_TIMESTAMP());


-- =============================================================================
-- 5. STORAGE SIZE  (ops/collect_storage.sh, results/storage-size-*.md)
-- `bytes` (= ACTIVE_BYTES) is live compressed on-disk size, comparable to ClickHouse
-- data_compressed / bytes_on_disk. TIME_TRAVEL + FAILSAFE is extra retained storage Snowflake
-- keeps that ClickHouse does not carry by default.
-- =============================================================================
-- quick view via SHOW (rows + bytes + cluster_by, metadata-only):
SHOW TABLES             LIKE '<RAW>'    IN SCHEMA BENCH2COST.<SCHEMA>;
SHOW MATERIALIZED VIEWS LIKE '<ROLLUP>' IN SCHEMA BENCH2COST.<SCHEMA>;
SHOW INTERACTIVE TABLES IN SCHEMA BENCH2COST.<SCHEMA>;   -- interactive tables: rows/target_lag/state

-- active + time-travel + fail-safe breakdown:
SELECT table_name, active_bytes, time_travel_bytes, failsafe_bytes
FROM BENCH2COST.INFORMATION_SCHEMA.TABLE_STORAGE_METRICS
WHERE table_schema = '<SCHEMA>' AND table_name IN ('<RAW>','<ROLLUP>');

-- active bytes + row count joined (what ops/collect_storage.sh writes to storage.json):
SELECT m.table_name, m.active_bytes, t.row_count
FROM BENCH2COST.INFORMATION_SCHEMA.TABLE_STORAGE_METRICS m
JOIN BENCH2COST.INFORMATION_SCHEMA.TABLES t USING (table_schema, table_name)
WHERE m.table_schema = '<SCHEMA>' AND m.table_name IN ('<RAW>','<ROLLUP>');


-- =============================================================================
-- 6. CLUSTERING DEPTH  (ops/clustering_lag.sh)
-- Point-in-time clustering quality for a clustered table. Lower average_depth = better
-- pruning. NOT historized by Snowflake — sample it DURING the run to capture the ramp.
-- Needs no warehouse; on a ~100B-row table it takes seconds (scans partition metadata).
-- =============================================================================
SELECT SYSTEM$CLUSTERING_INFORMATION('<RAW>', '(sym, t)');         -- raw table, cluster key (sym, t)
SELECT SYSTEM$CLUSTERING_INFORMATION('<ROLLUP>', '(sym, day)');    -- rollup, cluster key (sym, day)


-- =============================================================================
-- 7. CREDITS / COST  (ops/mv_billing.sh; T2 streaming: t2/README.md)
-- All from SNOWFLAKE.ACCOUNT_USAGE (lags up to ~3h). credits_used are the durable metric;
-- $ = credits * your per-credit rate (~$3/credit Enterprise on-demand).
-- =============================================================================
-- warehouse compute (ingest / refresh / reader warehouses):
SELECT warehouse_name, COALESCE(SUM(credits_used),0) AS credits
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE start_time >= DATEADD(hour, -<HOURS>, CURRENT_TIMESTAMP())
  AND warehouse_name IN ('BENCH2COST_GEN2_XSMALL','BENCH2COST_SMALL_GEN2','BENCH2COST_GEN2_MEDIUM','BENCH2COST_IT_SMALL')
GROUP BY 1 ORDER BY credits DESC;

-- automatic-clustering credits (serverless; only if CLUSTER BY is set):
SELECT table_name, COALESCE(SUM(credits_used),0) AS credits
FROM SNOWFLAKE.ACCOUNT_USAGE.AUTOMATIC_CLUSTERING_HISTORY
WHERE start_time >= DATEADD(hour, -<HOURS>, CURRENT_TIMESTAMP())
  AND table_name IN ('<RAW>','<ROLLUP>')
GROUP BY 1;

-- MV maintenance (interactive-MV refresh) credits for THIS schema. Exact query used for T2 RUN8.
-- Per-hour detail rows are in results/t2/mv_refresh.csv; this returns the total.
-- (T2 RUN8 total: 9.05 credits over 38 hourly rows, ~$27 @ $3/cr.) Block 4 has the fuller history.
SELECT SUM(credits_used) AS mv_refresh_credits
FROM SNOWFLAKE.ACCOUNT_USAGE.MATERIALIZED_VIEW_REFRESH_HISTORY
WHERE schema_name = '<SCHEMA>';        -- T2 RUN8: 'STOCKHOUSE_T2_RUN8'

-- Snowpipe Streaming ingest cost (T2 — serverless, no ingest warehouse). Attribute the streaming
-- credits to THIS schema's pipe by joining METERING_HISTORY -> PIPES on pipe id + name. Exact query
-- used for T2 RUN8. Per-hour detail rows are in results/t2/snowpipe_streaming.csv; this returns the total.
-- (T2 RUN8 total: 17.72 credits over 36 hourly rows, ~$53 @ $3/cr.)
SELECT SUM(m.credits_used) AS streaming_credits
FROM SNOWFLAKE.ACCOUNT_USAGE.METERING_HISTORY m
JOIN SNOWFLAKE.ACCOUNT_USAGE.PIPES p
  ON m.entity_id = p.pipe_id
 AND m.name      = p.pipe_name
 AND m.service_type = 'SNOWPIPE_STREAMING'
 AND p.schema_name  = '<SCHEMA>';       -- T2 RUN8: 'STOCKHOUSE_T2_RUN8'


-- =============================================================================
-- 8. OBJECT INVENTORY / CONFIG  (preflight.sh, setup_interactive.sh)
-- Confirm what exists and how warehouses/interactive tables are configured.
-- =============================================================================
SHOW WAREHOUSES;
SHOW SCHEMAS IN DATABASE BENCH2COST;
SHOW PIPES IN SCHEMA BENCH2COST.<SCHEMA>;                  -- T2 streaming pipe (QUOTES_IT_PIPE)
SELECT GET_DDL('warehouse', '<WAREHOUSE>');                -- exact warehouse DDL (size/generation/gen2)
SELECT GET_DDL('table', 'BENCH2COST.<SCHEMA>.<RAW>');      -- table/IT DDL (cluster key, target_lag, refresh wh)
