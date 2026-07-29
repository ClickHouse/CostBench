-- =============================================================================
-- Extract server-side timings for every timed benchmark query, all tiers, from
-- ACCOUNT_USAGE.QUERY_HISTORY. Feeds analysis/backfill_timings.py, which rewrites the result
-- JSONLs so `result` is TOTAL_ELAPSED_TIME with compilation_time/execution_time alongside.
--
-- WHY: runner_common.py used to record EXECUTION_TIME only, omitting compilation. On T2's
-- streaming-fed interactive MV that was 88% of the real latency — the published figure was
-- 12.4x too low. The runner now records all three, so only these historical runs need this.
--
-- WORKFLOW: run the DISCOVERY block for the period -> confirm each arm's schema, warehouse and
-- hash->query mapping from sample_text -> run that arm's block -> export one CSV per arm ->
-- check block V against the EXPECT table -> backfill_timings.py <csv...> [--write].
--
-- WINDOWS come from the result files: lower = line 1's iteration_started_at, upper = the last
-- line's iteration_finished_at PLUS ONE SECOND (the JSONL truncates to whole seconds, so the
-- final query can start a fraction after the recorded finish — that cost a row on T2).
--
--   arm                  results/…                              iters x q = rows   timeouts
--   T0 dashboard         t0/dashboard_20260609T154304Z.jsonl      163 x 4 =  652          0
--   T0 drilldown         t0/drilldown_20260617T155914Z.jsonl       40 x 2 =   80          0
--   T1 dashboard         t1/dashboard_20260617T154602Z.jsonl      244 x 4 =  976          0
--   T1 drilldown         t1/drilldown_20260617T154602Z.jsonl       41 x 2 =   82          2
--   T2 dashboard_mv_std  t2/dashboard_mv_std_2026…Z.jsonl         218 x 4 =  872          0
--   T2 dashboard_imv_iv  t2/dashboard_imv_iv_2026…Z.jsonl         239 x 4 =  956        763
--   T2 dashboard_raw_iv  t2/dashboard_raw_iv_2026…Z.jsonl         242 x 4 =  968        523
--   T2 drilldown         t2/drilldown_2026…Z.jsonl                 43 x 2 =   86          0
--
-- TWO ACCOUNTS — these arms are NOT all in one place:
--   T0 dashboard  PARIS  · STOCKHOUSE        · BENCH2COST_SMALL_GEN2
--   T0 drilldown  LONDON · STOCKHOUSE_T0     · BENCH2COST_GEN2_SMALL_T0
--   T1 both       LONDON · STOCKHOUSE_T1     · BENCH2COST_IT_SMALL
--   T2 all four   LONDON · STOCKHOUSE_T2_RUN8· BENCH2COST_GEN2_SMALL_DASH / SNOWPIPES_IT_READ_SMALL
-- Schema and warehouse vary PER ARM, not per tier. Block V only validates arms in the account
-- you are connected to.
--
-- HASH COLLISION — T1 drilldown and T2 drilldown share BOTH hashes
--   (81bfa7bfde17377a3c0dd2f00ea3883f, 98a25b7faf10ec93061165a1efbd79dd): identical SQL against
--   an unqualified FROM QUOTES_IT. Only schema + warehouse + window separate them. Never filter
--   a drilldown arm on hash alone.
--
-- Do NOT infer the q1..q4 order from med_exec_ms: it rises q1<q2<q3<q4 on T2 (772/866/2128/3458
-- ms) but FAILS on T1, where the rollup is small enough that q4 is cheapest (90/104/134/45 ms).
-- Take the mapping from query_sample text, which is unambiguous.
--
-- ACCOUNT_USAGE keeps 365 days. GET_QUERY_OPERATOR_STATS (remote read %, metrics_reference 6c)
-- only keeps 14 — so if operator-level detail is wanted, capture it while the run is recent.
-- =============================================================================

ALTER SESSION SET TIMEZONE = 'UTC';   -- START_TIME is TIMESTAMP_LTZ; match the JSONL's UTC


-- =============================================================================
-- D. DISCOVERY — run this for the period you are reconstructing, then read off schema_name,
--    warehouse_name and query_hash per arm. Deliberately filters NEITHER schema nor warehouse:
--    those are what you are here to learn. Every real arm ran 40+ times.
--      T0 dashboard : 2026-06-09 15:43:04 .. 2026-06-10 21:18:12   (Paris)
--      T0/T1 arms   : 2026-06-17 15:46:02 .. 2026-06-19 08:33:58   (London, 3 arms concurrent)
--      T2 arms      : 2026-07-20 12:15:08 .. 2026-07-22 07:26:57   (London, 4 arms concurrent)
-- =============================================================================
SELECT schema_name, warehouse_name, warehouse_size, warehouse_type, query_hash,
       COUNT(*)                                      AS runs,
       COUNT_IF(execution_status = 'SUCCESS')        AS ok,
       COUNT_IF(error_code IN ('000630', '000604'))  AS timeouts,
       MEDIAN(compilation_time)                      AS med_compile_ms,
       MEDIAN(execution_time)                        AS med_exec_ms,
       MEDIAN(total_elapsed_time)                    AS med_elapsed_ms,
       MIN(start_time)                               AS first_seen,
       MAX(start_time)                               AS last_seen,
       ANY_VALUE(LEFT(query_text, 130))              AS query_sample
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE query_type = 'SELECT'
  AND start_time >= '<WINDOW_START> +0000'::TIMESTAMP_TZ
  AND start_time <  '<WINDOW_END> +0000'::TIMESTAMP_TZ
GROUP BY 1, 2, 3, 4, 5
HAVING COUNT(*) >= 20
ORDER BY warehouse_name, runs DESC;


-- =============================================================================
-- 1. ARM: T0 dashboard  -> results/t0/dashboard_20260609T154304Z.jsonl
--    EXPECT: 652 rows, 163 per hash, 0 timeouts.
-- =============================================================================
WITH bench AS (
  SELECT *,
         DECODE(query_hash,
           '5a1b576018ce7df649dc5e0ad6bd0d76', 1,   -- single-symbol all-time summary
           'c7ae3cc7962c11bc97381e6df56b0e3a', 2,   -- watchlist summary
           'ba1f0eb3db5d08288aa50d5b8b311afa', 3,   -- top movers (full rollup scan)
           '4023099af69dc938d721a9191a465449', 4)   -- daily activity time series
           AS q
  FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
  WHERE warehouse_name = 'BENCH2COST_SMALL_GEN2'   -- standard Gen2 Small; PARIS account
    AND schema_name    = 'STOCKHOUSE'              -- NOT STOCKHOUSE_T0, which is the drilldown arm
    AND query_hash IN ('5a1b576018ce7df649dc5e0ad6bd0d76', 'c7ae3cc7962c11bc97381e6df56b0e3a',
                       'ba1f0eb3db5d08288aa50d5b8b311afa', '4023099af69dc938d721a9191a465449')
    AND start_time >= '2026-06-09 15:43:04 +0000'::TIMESTAMP_TZ
    AND start_time <  '2026-06-10 21:18:12 +0000'::TIMESTAMP_TZ
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
-- 2. ARM: T0 drilldown  (LONDON account)  -> results/t0/drilldown_20260617T155914Z.jsonl
--    EXPECT: 80 rows, 40 per hash, 0 timeouts.
--    Overlaps T1 in time — the warehouse + hash filters are what separate them.
-- =============================================================================
WITH bench AS (
  SELECT *,
         DECODE(query_hash,
           'bb4cf593cb383ec183b5e8cae4fba8f2', 1,
           '664528618f5fcd871b5fa67ed24bbc17', 2) AS q
  FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
  WHERE warehouse_name = 'BENCH2COST_GEN2_SMALL_T0'   -- LONDON account
    AND schema_name    = 'STOCKHOUSE_T0'
    AND query_hash IN ('bb4cf593cb383ec183b5e8cae4fba8f2', '664528618f5fcd871b5fa67ed24bbc17')
    AND start_time >= '2026-06-17 15:59:14 +0000'::TIMESTAMP_TZ
    AND start_time <  '2026-06-19 08:04:25 +0000'::TIMESTAMP_TZ
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
-- 3. ARM: T1 dashboard  -> results/t1/dashboard_20260617T154602Z.jsonl
--    EXPECT: 976 rows, 244 per hash, 0 timeouts.
--    Zero timeouts on an INTERACTIVE warehouse is worth a second look: T2's interactive
--    dashboard arm timed out on 763 of 956. If T1 really had none, its rollup was small
--    enough to compile inside the cap — which is itself a finding about partition count.
-- =============================================================================
WITH bench AS (
  SELECT *,
         DECODE(query_hash,
           'efc99dfb4e75c8090e7c5f15a5ab4890', 1,
           'e8e2f176d29c0adbdc6a98665e8c7baa', 2,
           'a55ea2c89d70307c76537477a5c9b144', 3,
           'bf634500f741e0a2f2ecdf96cbba3f3b', 4) AS q
  FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
  WHERE warehouse_name = 'BENCH2COST_IT_SMALL'
    AND schema_name    = 'STOCKHOUSE_T1'
    AND query_hash IN ('efc99dfb4e75c8090e7c5f15a5ab4890', 'e8e2f176d29c0adbdc6a98665e8c7baa',
                       'a55ea2c89d70307c76537477a5c9b144', 'bf634500f741e0a2f2ecdf96cbba3f3b')
    AND start_time >= '2026-06-17 15:46:02 +0000'::TIMESTAMP_TZ
    AND start_time <  '2026-06-19 08:33:58 +0000'::TIMESTAMP_TZ
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
-- 4. ARM: T1 drilldown  -> results/t1/drilldown_20260617T154602Z.jsonl
--    EXPECT: 82 rows, 41 per hash, 2 timeouts.
--    NO execution_status filter — those 2 FAIL/000630 rows are wanted: their
--    COMPILATION_TIME is what says whether they died compiling or executing.
-- =============================================================================
WITH bench AS (
  SELECT *,
         DECODE(query_hash,
           '81bfa7bfde17377a3c0dd2f00ea3883f', 1,
           '98a25b7faf10ec93061165a1efbd79dd', 2) AS q
  FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
  WHERE warehouse_name = 'BENCH2COST_IT_SMALL'
    AND schema_name    = 'STOCKHOUSE_T1'
    AND query_hash IN ('81bfa7bfde17377a3c0dd2f00ea3883f', '98a25b7faf10ec93061165a1efbd79dd')
    AND start_time >= '2026-06-17 15:46:02 +0000'::TIMESTAMP_TZ
    AND start_time <  '2026-06-19 07:49:40 +0000'::TIMESTAMP_TZ
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
-- 5. ARM: dashboard_mv_std — interactive MV via a STANDARD warehouse
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
-- 6. ARM: dashboard_imv_iv — interactive MV via the INTERACTIVE warehouse
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
-- 7. ARM: dashboard_raw_iv — RAW interactive table via the INTERACTIVE warehouse
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
-- 8. ARM: drilldown — RAW interactive table, 2 queries, INTERACTIVE warehouse
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
-- V1. VALIDATION — T0/T1 arms — all four arms in one query. Run BEFORE exporting anything.
--
--      arm            rows  shapes  per_hash  timeouts   ok
--      T0 dashboard    652       4       163         0  652
--      T0 drilldown     80       2        40         0   80
--      T1 dashboard    976       4       244         0  976
--      T1 drilldown     82       2        41         2   80
--
--    min_per_hash MUST equal max_per_hash. If they differ, one query of that arm ran a
--    different number of times than its siblings and the per-hash ROW_NUMBER() no longer
--    lines up with the JSONL iteration index — the backfill would attach timings to the
--    wrong iteration silently rather than failing.
--
--    Then, for the three 0-timeout arms, EXECUTION_TIME must reproduce the JSONL `result`
--    values exactly; backfill_timings.py enforces that. (On T2 it matched 872/872 at
--    max diff 0.0000s, which is what proved the alignment.)
-- =============================================================================
WITH tagged AS (
  SELECT *,
         CASE
           WHEN warehouse_name = 'BENCH2COST_SMALL_GEN2'
                AND query_hash IN ('5a1b576018ce7df649dc5e0ad6bd0d76', 'c7ae3cc7962c11bc97381e6df56b0e3a',
                                   'ba1f0eb3db5d08288aa50d5b8b311afa', '4023099af69dc938d721a9191a465449')
                AND start_time <  '2026-06-10 21:18:12 +0000'::TIMESTAMP_TZ THEN 'T0 dashboard'
           WHEN warehouse_name = 'BENCH2COST_GEN2_SMALL_T0'
                AND query_hash IN ('bb4cf593cb383ec183b5e8cae4fba8f2', '664528618f5fcd871b5fa67ed24bbc17')
                AND start_time >= '2026-06-17 15:59:14 +0000'::TIMESTAMP_TZ
                AND start_time <  '2026-06-19 08:04:25 +0000'::TIMESTAMP_TZ THEN 'T0 drilldown'
           WHEN warehouse_name = 'BENCH2COST_IT_SMALL'
                AND query_hash IN ('efc99dfb4e75c8090e7c5f15a5ab4890', 'e8e2f176d29c0adbdc6a98665e8c7baa',
                                   'a55ea2c89d70307c76537477a5c9b144', 'bf634500f741e0a2f2ecdf96cbba3f3b')
                AND start_time <  '2026-06-19 08:33:58 +0000'::TIMESTAMP_TZ THEN 'T1 dashboard'
           WHEN warehouse_name = 'BENCH2COST_IT_SMALL'
                AND query_hash IN ('81bfa7bfde17377a3c0dd2f00ea3883f', '98a25b7faf10ec93061165a1efbd79dd')
                AND start_time <  '2026-06-19 07:49:40 +0000'::TIMESTAMP_TZ THEN 'T1 drilldown'
         END AS arm
  FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
  WHERE start_time >= '2026-06-09 15:43:04 +0000'::TIMESTAMP_TZ
    AND start_time <  '2026-06-19 08:33:58 +0000'::TIMESTAMP_TZ   -- widest arm bound
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


-- =============================================================================
-- V2. VALIDATION — T2 arms — all four arms in one query. Run this BEFORE exporting anything.
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
