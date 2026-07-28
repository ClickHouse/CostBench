-- =============================================================================
-- T2 RUN8 — extract the server-side timings for every timed benchmark query from
-- ACCOUNT_USAGE.QUERY_HISTORY, one CSV per arm, aligned 1:1 with the JSONL results
-- in results/t2/.
--
-- WHY: the runners recorded EXECUTION_TIME only, which on a streaming-fed interactive
-- MV excluded the dominant cost. For the MV-std arm, median compilation was 10.3s vs
-- 1.0s execution — 91% of wall-clock time was compilation, and reported latency was
-- 12.2x lower than TOTAL_ELAPSED_TIME. These extracts recover the full split so the
-- results can be re-derived. (runner_common.py now captures all three going forward.)
--
-- ALIGNMENT CONTRACT: within one arm, ordering each query_hash's rows by start_time
-- gives iteration 1..N in JSONL line order. So row i of hash q == JSONL line i,
-- result[q]. Verified exactly for the MV-std arm: all 872 EXECUTION_TIME values
-- reproduce the JSONL `result` values with max diff 0.0000s.
--
-- TIMEZONE: START_TIME is TIMESTAMP_LTZ and renders in the session TIMEZONE. The
-- JSONL is UTC. Set the session to UTC so exports are directly comparable, and use
-- explicit +0000 literals in predicates so the filters hold regardless.
--
-- WINDOW BOUNDS: lower = the arm's iteration_started_at of line 1; upper = the arm's
-- iteration_finished_at of the last line PLUS ONE SECOND, because the JSONL truncates
-- timestamps to whole seconds (the MV-std arm's final query starts at 07:26:56.230,
-- 0.23s after the recorded 07:26:56 finish).
--
-- HASH INVENTORY — all 10 timed queries of RUN8. Three arms share warehouse
-- SNOWPIPES_IT_READ_SMALL and schema STOCKHOUSE_T2_RUN8 over the same window, so for
-- those, query_hash is the ONLY thing that separates one arm from another.
--
--   arm               warehouse                    q  query_hash
--   dashboard_mv_std  BENCH2COST_GEN2_SMALL_DASH   1  4138da05ca5b8b3b12d91e3c60b451c2
--                     (interactive MV, std wh)     2  032fd86ef70df033fb74f37a3a6ff43a
--                                                  3  198a3ec3a02513cbd5e088271fcf95ba
--                                                  4  2adb24066ebed97154fb420445501a46
--   dashboard_imv_iv  SNOWPIPES_IT_READ_SMALL      1..4  SAME FOUR HASHES as above
--                     (interactive MV, IWH)              — identical query text
--   dashboard_raw_iv  SNOWPIPES_IT_READ_SMALL      1  49f0fd2569c4079e89daa0b84f877f1d
--                     (raw QUOTES_IT, IWH)         2  7c0f8300c30ea00f6fdf768e653b1045
--                                                  3  120292e60f2c28d5fd83f1c198caa296
--                                                  4  da2f485cec29a5f67e49a05ad1cfa1b7
--   drilldown         SNOWPIPES_IT_READ_SMALL      1  81bfa7bfde17377a3c0dd2f00ea3883f
--                     (raw QUOTES_IT, IWH)         2  98a25b7faf10ec93061165a1efbd79dd
-- =============================================================================

ALTER SESSION SET TIMEZONE = 'UTC';


-- =============================================================================
-- 0. DISCOVERY — map warehouse + query_hash to arms. Run this first; it's how the
--    unknown hashes get filled in below, and it re-confirms the known ones.
--    Support queries (the runners' COUNT(*)s and timing lookups) show up here too,
--    on the tracking warehouse (BENCH_STREAM) — ignore those rows.
-- =============================================================================
SELECT warehouse_name,
       query_hash,
       COUNT(*)                                      AS runs,
       COUNT_IF(execution_status = 'SUCCESS')        AS ok,
       COUNT_IF(execution_status <> 'SUCCESS')       AS failed,
       COUNT_IF(error_code IN ('000630', '000604'))  AS timeouts,
       MEDIAN(compilation_time)                      AS med_compile_ms,
       MEDIAN(execution_time)                        AS med_exec_ms,
       MIN(start_time)                               AS first_seen,
       MAX(start_time)                               AS last_seen,
       ANY_VALUE(LEFT(query_text, 100))              AS sample_text
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE schema_name = 'STOCKHOUSE_T2_RUN8'
  AND query_type  = 'SELECT'
  AND start_time >= '2026-07-20 12:15:08 +0000'::TIMESTAMP_TZ
  AND start_time <  '2026-07-22 08:10:00 +0000'::TIMESTAMP_TZ
GROUP BY 1, 2
HAVING COUNT(*) > 5          -- drop ad-hoc one-offs; every real arm has 40+ runs
ORDER BY warehouse_name, runs DESC;


-- =============================================================================
-- 1. ARM: dashboard_mv_std — interactive MV via a STANDARD warehouse
--    -> results/t2/dashboard_mv_std_20260720T121508Z.jsonl   (218 iterations x 4)
--    EXPECT: 872 rows, 218 per hash, 0 timeouts.  [VALIDATED against the JSONL]
-- =============================================================================
WITH bench AS (
  SELECT *,
         DECODE(query_hash,
           '4138da05ca5b8b3b12d91e3c60b451c2', 1,   -- Q1 single-symbol all-time summary
           '032fd86ef70df033fb74f37a3a6ff43a', 2,   -- Q2 watchlist summary
           '198a3ec3a02513cbd5e088271fcf95ba', 3,   -- Q3 top movers (full rollup scan)
           '2adb24066ebed97154fb420445501a46', 4)   -- Q4 daily activity time series
           AS q
  FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
  WHERE warehouse_name = 'BENCH2COST_GEN2_SMALL_DASH'
    AND schema_name    = 'STOCKHOUSE_T2_RUN8'   -- REQUIRED: STOCKHOUSE_T2_RUN7 reused these
                                                -- same 4 hashes on this same warehouse at 12:10Z
    AND query_hash IN ('4138da05ca5b8b3b12d91e3c60b451c2',
                       '032fd86ef70df033fb74f37a3a6ff43a',
                       '198a3ec3a02513cbd5e088271fcf95ba',
                       '2adb24066ebed97154fb420445501a46')
    AND start_time >= '2026-07-20 12:15:08 +0000'::TIMESTAMP_TZ
    AND start_time <  '2026-07-22 07:26:57 +0000'::TIMESTAMP_TZ
    -- Excludes 4 further iterations that ran at 07:36/07:47/07:57/08:07Z and were never
    -- written to the JSONL. Including them would break the positional alignment, and they
    -- are all post-ingest (IMV consolidated to 8 partitions, ~0.12s elapsed).
)
SELECT q,
       ROW_NUMBER() OVER (PARTITION BY q ORDER BY start_time) AS iteration,
       query_id, query_hash, warehouse_name, start_time, execution_status, error_code,
       total_elapsed_time, compilation_time, execution_time,
       queued_provisioning_time + queued_overload_time + queued_repair_time AS queued_time,
       bytes_scanned, percentage_scanned_from_cache,
       partitions_scanned, partitions_total,
       bytes_spilled_to_local_storage, bytes_spilled_to_remote_storage,
       rows_produced
FROM bench
ORDER BY iteration, q;


-- =============================================================================
-- 2. ARM: dashboard_imv_iv — interactive MV via the INTERACTIVE warehouse
--    -> results/t2/dashboard_imv_iv_20260720T121508Z.jsonl   (239 iterations x 4)
--    EXPECT: 956 rows, 239 per hash, 763 timeouts, 193 SUCCESS.
--    Same 4 hashes as arm 1 (identical query text) — warehouse_name is the ONLY
--    thing separating the two arms.
-- =============================================================================
WITH bench AS (
  SELECT *,
         DECODE(query_hash,
           '4138da05ca5b8b3b12d91e3c60b451c2', 1,
           '032fd86ef70df033fb74f37a3a6ff43a', 2,
           '198a3ec3a02513cbd5e088271fcf95ba', 3,
           '2adb24066ebed97154fb420445501a46', 4)
           AS q
  FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
  WHERE warehouse_name = 'SNOWPIPES_IT_READ_SMALL'
    AND schema_name    = 'STOCKHOUSE_T2_RUN8'
    AND query_hash IN ('4138da05ca5b8b3b12d91e3c60b451c2',
                       '032fd86ef70df033fb74f37a3a6ff43a',
                       '198a3ec3a02513cbd5e088271fcf95ba',
                       '2adb24066ebed97154fb420445501a46')
    AND start_time >= '2026-07-20 12:15:08 +0000'::TIMESTAMP_TZ
    AND start_time <  '2026-07-22 07:25:59 +0000'::TIMESTAMP_TZ
    -- NO execution_status filter: the 763 timed-out queries are FAIL rows (error_code
    -- 000630) and they are the point — their COMPILATION_TIME is what explains the
    -- timeouts. Expect NULL EXECUTION_TIME on the ones killed during compilation.
)
SELECT q,
       ROW_NUMBER() OVER (PARTITION BY q ORDER BY start_time) AS iteration,
       query_id, query_hash, warehouse_name, start_time, execution_status, error_code,
       total_elapsed_time, compilation_time, execution_time,
       queued_provisioning_time + queued_overload_time + queued_repair_time AS queued_time,
       bytes_scanned, percentage_scanned_from_cache,
       partitions_scanned, partitions_total,
       bytes_spilled_to_local_storage, bytes_spilled_to_remote_storage,
       rows_produced
FROM bench
ORDER BY iteration, q;


-- =============================================================================
-- 3. ARM: dashboard_raw_iv — RAW interactive table via the INTERACTIVE warehouse
--    -> results/t2/dashboard_raw_iv_20260720T121508Z.jsonl   (242 iterations x 4)
--    EXPECT: 968 rows, 242 per hash, 523 timeouts, 445 SUCCESS.
--
--    Hash order assumed to follow t2/queries_dashboard_raw.sql — CONFIRM via sample_text
--    in block 0, all four hitting QUOTES_IT (not QUOTES_DAILY_IMV):
--      Q1 "COUNT(DISTINCT TO_DATE(...)) AS days_traded ... FROM QUOTES_IT WHERE sym = 'AAPL'"
--      Q2 "sym, COUNT(DISTINCT TO_DATE(...)) ... WHERE sym IN (...) GROUP BY sym"
--      Q3 "sym, (MAX(bp) - MIN(bp)) / MIN(bp) * 100 AS pct_range ... GROUP BY sym"
--      Q4 "TO_DATE(...) AS day, COUNT(*) AS total_quotes ... GROUP BY day ORDER BY day"
--    Do NOT confuse these with the 2 drilldown hashes in block 4 — the raw dashboard and
--    the drilldown run on the SAME warehouse and schema in the SAME window, so query_hash
--    is the only discriminator between them.
-- =============================================================================
WITH bench AS (
  SELECT *,
         DECODE(query_hash,
           '49f0fd2569c4079e89daa0b84f877f1d', 1,   -- Q1 single-symbol summary (raw scan)
           '7c0f8300c30ea00f6fdf768e653b1045', 2,   -- Q2 watchlist summary (raw scan)
           '120292e60f2c28d5fd83f1c198caa296', 3,   -- Q3 top movers (full raw scan)
           'da2f485cec29a5f67e49a05ad1cfa1b7', 4)   -- Q4 daily activity (full raw scan)
           AS q
  FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
  WHERE warehouse_name = 'SNOWPIPES_IT_READ_SMALL'
    AND schema_name    = 'STOCKHOUSE_T2_RUN8'
    AND query_hash IN ('49f0fd2569c4079e89daa0b84f877f1d',
                       '7c0f8300c30ea00f6fdf768e653b1045',
                       '120292e60f2c28d5fd83f1c198caa296',
                       'da2f485cec29a5f67e49a05ad1cfa1b7')
    AND start_time >= '2026-07-20 12:15:08 +0000'::TIMESTAMP_TZ
    AND start_time <  '2026-07-22 07:24:01 +0000'::TIMESTAMP_TZ
)
SELECT q,
       ROW_NUMBER() OVER (PARTITION BY q ORDER BY start_time) AS iteration,
       query_id, query_hash, warehouse_name, start_time, execution_status, error_code,
       total_elapsed_time, compilation_time, execution_time,
       queued_provisioning_time + queued_overload_time + queued_repair_time AS queued_time,
       bytes_scanned, percentage_scanned_from_cache,
       partitions_scanned, partitions_total,
       bytes_spilled_to_local_storage, bytes_spilled_to_remote_storage,
       rows_produced
FROM bench
ORDER BY iteration, q;


-- =============================================================================
-- 4. ARM: drilldown — RAW interactive table, 2 queries, INTERACTIVE warehouse
--    -> results/t2/drilldown_20260720T121508Z.jsonl          (43 iterations x 2)
--    EXPECT: 86 rows, 43 per hash, 0 timeouts.
--    Hash order assumed to follow t2/queries_raw_it.sql — CONFIRM via sample_text in
--    block 0: Q1 contains MIN_BY(bp, t) / DATE_TRUNC('hour', ...), Q2 contains
--    STDDEV_POP((bp + ap) / 2). Swap the DECODE mapping if they are reversed.
-- =============================================================================
WITH bench AS (
  SELECT *,
         DECODE(query_hash,
           '81bfa7bfde17377a3c0dd2f00ea3883f', 1,   -- Q1 hourly OHLC (MIN_BY/MAX_BY)
           '98a25b7faf10ec93061165a1efbd79dd', 2)   -- Q2 tick stats (STDDEV_POP)
           AS q
  FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
  WHERE warehouse_name = 'SNOWPIPES_IT_READ_SMALL'
    AND schema_name    = 'STOCKHOUSE_T2_RUN8'
    AND query_hash IN ('81bfa7bfde17377a3c0dd2f00ea3883f',
                       '98a25b7faf10ec93061165a1efbd79dd')
    AND start_time >= '2026-07-20 12:15:08 +0000'::TIMESTAMP_TZ
    AND start_time <  '2026-07-22 06:36:55 +0000'::TIMESTAMP_TZ
)
SELECT q,
       ROW_NUMBER() OVER (PARTITION BY q ORDER BY start_time) AS iteration,
       query_id, query_hash, warehouse_name, start_time, execution_status, error_code,
       total_elapsed_time, compilation_time, execution_time,
       queued_provisioning_time + queued_overload_time + queued_repair_time AS queued_time,
       bytes_scanned, percentage_scanned_from_cache,
       partitions_scanned, partitions_total,
       bytes_spilled_to_local_storage, bytes_spilled_to_remote_storage,
       rows_produced
FROM bench
ORDER BY iteration, q;


-- =============================================================================
-- 5. VALIDATION — all four arms in one query. Run this BEFORE exporting anything.
--    Every column must match the table below. If any number differs, a window bound
--    or a hash list is wrong and the extracts must NOT be used for a backfill — the
--    positional alignment to the JSONL would shift silently rather than fail loudly.
--
--      arm               rows  shapes  per_hash  timeouts   ok
--      dashboard_mv_std   872       4       218         0  872
--      dashboard_imv_iv   956       4       239       763  193
--      dashboard_raw_iv   968       4       242       523  445
--      drilldown           86       2        43         0   86
--
--    min_per_hash must equal max_per_hash (a balanced arm); if they differ, one query
--    of that arm ran a different number of times than the others and the per-hash
--    row_number() no longer lines up with the JSONL iteration index.
--
--    For the two 0-timeout arms, EXECUTION_TIME must additionally reproduce the JSONL
--    `result` values exactly — verified for dashboard_mv_std (872/872, max diff
--    0.0000s). For the two IWH arms, validate on row/timeout counts plus exact
--    agreement on the SUCCESS rows only: a timed-out query has no execution time.
--
--    Also sanity-check med_compile_ms on the IWH arms. The JSONL recorded 763/956 and
--    523/968 as timeouts against a 5s cap; if those FAIL rows show compile times near
--    or above 5000ms with NULL execution_time, they died in COMPILATION. If instead
--    they show small compile times and ~5000ms execution, they died in EXECUTION —
--    a different conclusion for the interactive arms, worth resolving before backfill.
-- =============================================================================
WITH tagged AS (
  SELECT *,
         CASE
           WHEN warehouse_name = 'BENCH2COST_GEN2_SMALL_DASH'
                AND query_hash IN ('4138da05ca5b8b3b12d91e3c60b451c2','032fd86ef70df033fb74f37a3a6ff43a',
                                   '198a3ec3a02513cbd5e088271fcf95ba','2adb24066ebed97154fb420445501a46')
                AND start_time < '2026-07-22 07:26:57 +0000'::TIMESTAMP_TZ THEN 'dashboard_mv_std'
           WHEN warehouse_name = 'SNOWPIPES_IT_READ_SMALL'
                AND query_hash IN ('4138da05ca5b8b3b12d91e3c60b451c2','032fd86ef70df033fb74f37a3a6ff43a',
                                   '198a3ec3a02513cbd5e088271fcf95ba','2adb24066ebed97154fb420445501a46')
                AND start_time < '2026-07-22 07:25:59 +0000'::TIMESTAMP_TZ THEN 'dashboard_imv_iv'
           WHEN warehouse_name = 'SNOWPIPES_IT_READ_SMALL'
                AND query_hash IN ('49f0fd2569c4079e89daa0b84f877f1d','7c0f8300c30ea00f6fdf768e653b1045',
                                   '120292e60f2c28d5fd83f1c198caa296','da2f485cec29a5f67e49a05ad1cfa1b7')
                AND start_time < '2026-07-22 07:24:01 +0000'::TIMESTAMP_TZ THEN 'dashboard_raw_iv'
           WHEN warehouse_name = 'SNOWPIPES_IT_READ_SMALL'
                AND query_hash IN ('81bfa7bfde17377a3c0dd2f00ea3883f','98a25b7faf10ec93061165a1efbd79dd')
                AND start_time < '2026-07-22 06:36:55 +0000'::TIMESTAMP_TZ THEN 'drilldown'
         END AS arm
  FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
  WHERE schema_name = 'STOCKHOUSE_T2_RUN8'
    AND start_time >= '2026-07-20 12:15:08 +0000'::TIMESTAMP_TZ
    AND start_time <  '2026-07-22 07:26:57 +0000'::TIMESTAMP_TZ   -- widest arm bound
)
SELECT arm,
       COUNT(*)                                     AS rows_total,
       COUNT(DISTINCT query_hash)                   AS shapes,
       MIN(runs_per_hash)                           AS min_per_hash,
       MAX(runs_per_hash)                           AS max_per_hash,
       COUNT_IF(error_code IN ('000630', '000604')) AS timeouts,
       COUNT_IF(execution_status = 'SUCCESS')       AS ok,
       MEDIAN(compilation_time)                     AS med_compile_ms,
       MEDIAN(execution_time)                       AS med_exec_ms,
       MEDIAN(total_elapsed_time)                   AS med_elapsed_ms,
       MIN(start_time)                              AS first_query,
       MAX(start_time)                              AS last_query
FROM (SELECT *, COUNT(*) OVER (PARTITION BY arm, query_hash) AS runs_per_hash FROM tagged)
WHERE arm IS NOT NULL
GROUP BY arm
ORDER BY arm;
