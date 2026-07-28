-- =============================================================================
-- T0 / T1 — extract server-side timings for every timed benchmark query from
-- ACCOUNT_USAGE.QUERY_HISTORY, so the result JSONLs can be backfilled the way T2 was.
--
-- WHY: runner_common.py recorded EXECUTION_TIME only, which omits compilation. On T2's
-- streaming-fed interactive MV that turned out to be 86% of the real latency (median
-- 10.5s compile vs 1.06s execute, reported latency 12.4x too low). T0 and T1 were
-- collected by the same runner, so they carry the same omission and need checking.
--
-- WORKFLOW (same as t2/extract_query_history.sql):
--   1. run the DISCOVERY blocks -> read off schema_name, warehouse_name and query_hash
--      per arm, and confirm the hash->query mapping from sample_text
--   2. fill the <<PLACEHOLDERS>> in blocks 1-4, run them, export one CSV per arm
--   3. run block 5 and check every number against the EXPECT table before backfilling
--
-- WINDOWS: derived from the result files themselves — lower bound = line 1's
-- iteration_started_at, upper bound = the last line's iteration_finished_at PLUS ONE
-- SECOND (the JSONL truncates to whole seconds, so the final query can start a fraction
-- after the recorded finish; that cost us a row on T2 until it was corrected).
--
--   arm            results/…                            iters x q = rows   timeouts
--   T0 dashboard   t0/dashboard_20260609T154304Z.jsonl    163 x 4 =  652          0
--   T0 drilldown   t0/drilldown_20260617T155914Z.jsonl     40 x 2 =   80          0
--   T1 dashboard   t1/dashboard_20260617T154602Z.jsonl    244 x 4 =  976          0
--   T1 drilldown   t1/drilldown_20260617T154602Z.jsonl     41 x 2 =   82          2
--
-- CAUTION 1 — T1 AND T2 DRILLDOWN SHARE THE SAME TWO QUERY HASHES
--   (81bfa7bfde17377a3c0dd2f00ea3883f, 98a25b7faf10ec93061165a1efbd79dd). Both arms run
--   byte-identical SQL against an unqualified `FROM QUOTES_IT`, so query_hash cannot tell
--   them apart. Only schema_name + warehouse_name + the time window can:
--     T1: STOCKHOUSE_T1 / BENCH2COST_IT_SMALL      / 2026-06-17..19
--     T2: STOCKHOUSE_T2_RUN8 / SNOWPIPES_IT_READ_SMALL / 2026-07-20..22
--   Never filter a drilldown arm on hash alone.
--
-- CAUTION 2 — THE THREE 06-17 ARMS RAN CONCURRENTLY. T0 drilldown, T1 dashboard and T1
-- drilldown all overlap 2026-06-17..06-19, and the two drilldown arms have the same query
-- COUNT (2). Time alone cannot separate them. What does:
--   • warehouse_name — T0 ran on a standard Gen2 warehouse ('Gen2 Small', cluster_size
--     2.7), T1 on an interactive one ('Interactive Small', 1.2)
--   • query_hash — T0 queries QUOTES / QUOTES_DAILY, T1 queries QUOTES_IT /
--     QUOTES_DAILY_IT, so the SQL text (and therefore the hash) differs
-- T0's and T1's hash sets are disjoint from each other, but NOT from T2's — see CAUTION 1.
--
-- ACCOUNT_USAGE keeps 365 days; the oldest of these ran 2026-06-09, so all four are in range.
-- =============================================================================

ALTER SESSION SET TIMEZONE = 'UTC';   -- START_TIME is TIMESTAMP_LTZ; match the JSONL's UTC


-- =============================================================================
-- 0a. DISCOVERY — the 2026-06-17..06-19 period: T0 drilldown + BOTH T1 arms at once.
--     Deliberately does NOT filter schema or warehouse: those are what you are here to
--     learn. Every real arm ran 40+ times, so HAVING drops ad-hoc noise.
--     Read off: which (schema, warehouse) pair is T0 vs T1, and which hash is which query.
-- =============================================================================
SELECT schema_name,
       warehouse_name,
       warehouse_size,
       warehouse_type,
       query_hash,
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
  AND start_time >= '2026-06-17 15:46:02 +0000'::TIMESTAMP_TZ
  AND start_time <  '2026-06-19 08:33:58 +0000'::TIMESTAMP_TZ
GROUP BY 1, 2, 3, 4, 5
HAVING COUNT(*) >= 20
ORDER BY warehouse_name, runs DESC;


-- =============================================================================
-- 0b. DISCOVERY — the 2026-06-09..06-10 period: the T0 dashboard arm, which ran a week
--     earlier and alone. Expect 4 hashes at ~163 runs each on the T0 warehouse.
-- =============================================================================
SELECT schema_name,
       warehouse_name,
       warehouse_size,
       query_hash,
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
  AND start_time >= '2026-06-09 15:43:04 +0000'::TIMESTAMP_TZ
  AND start_time <  '2026-06-10 21:18:12 +0000'::TIMESTAMP_TZ
GROUP BY 1, 2, 3, 4
HAVING COUNT(*) >= 20
ORDER BY warehouse_name, runs DESC;


-- =============================================================================
-- HASH -> QUERY MAPPING. `q` must follow the queries file's order, because the JSONL's
-- result array is in that order and the backfill aligns positionally. Identify each hash
-- from its query_sample:
--
--   T0 dashboard — queries_mv.sql, FROM QUOTES_DAILY
--     q1  COUNT(*) AS days_traded … WHERE sym = 'AAPL'
--     q2  sym, COUNT(*) AS days_traded … WHERE sym IN ('AAPL','MSFT',…) GROUP BY sym
--     q3  sym, (MAX(bp_max) - MIN(bp_min)) / MIN(bp_min) * 100 AS pct_range … LIMIT 20
--     q4  day, SUM(n_quotes) AS total_quotes … GROUP BY day ORDER BY day
--   T1 dashboard — queries_mv_it.sql, same four shapes but FROM QUOTES_DAILY_IT
--   T0 drilldown — queries_raw.sql, FROM QUOTES
--     q1  DATE_TRUNC('hour', …) AS hour, MIN_BY(bp, t) AS open … (hourly OHLCV)
--     q2  COUNT(*) AS ticks, AVG((bp + ap) / 2) … STDDEV_POP(…) (risk & liquidity)
--   T1 drilldown — queries_raw_it.sql, same two shapes but FROM QUOTES_IT
--
-- The mapping below was taken from query_sample text, which is unambiguous. Do NOT try to
-- confirm it from med_exec_ms ordering: that heuristic works on T2 (772/866/2128/3458 ms,
-- rising q1..q4) but FAILS on T1, whose rollup is small enough that q4 is the cheapest
-- query (q1 90ms, q2 104ms, q3 134ms, q4 45ms). Cost ordering is a property of data volume,
-- not of query position.
-- =============================================================================


-- =============================================================================
-- 1. ARM: T0 dashboard  -> results/t0/dashboard_20260609T154304Z.jsonl
--    EXPECT: 652 rows, 163 per hash, 0 timeouts.
-- =============================================================================
WITH bench AS (
  SELECT *,
         DECODE(query_hash,
           '<<T0_DASH_Q1_HASH>>', 1,
           '<<T0_DASH_Q2_HASH>>', 2,
           '<<T0_DASH_Q3_HASH>>', 3,
           '<<T0_DASH_Q4_HASH>>', 4) AS q
  FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
  WHERE warehouse_name = 'BENCH2COST_GEN2_SMALL_T0'
    AND schema_name    = 'STOCKHOUSE_T0'
    AND query_hash IN ('<<T0_DASH_Q1_HASH>>', '<<T0_DASH_Q2_HASH>>',
                       '<<T0_DASH_Q3_HASH>>', '<<T0_DASH_Q4_HASH>>')
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
-- 2. ARM: T0 drilldown  -> results/t0/drilldown_20260617T155914Z.jsonl
--    EXPECT: 80 rows, 40 per hash, 0 timeouts.
--    Overlaps T1 in time — the warehouse + hash filters are what separate them.
-- =============================================================================
WITH bench AS (
  SELECT *,
         DECODE(query_hash,
           'bb4cf593cb383ec183b5e8cae4fba8f2', 1,
           '664528618f5fcd871b5fa67ed24bbc17', 2) AS q
  FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
  WHERE warehouse_name = 'BENCH2COST_GEN2_SMALL_T0'
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
-- 5. VALIDATION — all four arms in one query. Run BEFORE exporting anything.
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
           WHEN warehouse_name = 'BENCH2COST_GEN2_SMALL_T0'
                AND query_hash IN ('<<T0_DASH_Q1_HASH>>', '<<T0_DASH_Q2_HASH>>',
                                   '<<T0_DASH_Q3_HASH>>', '<<T0_DASH_Q4_HASH>>')
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
