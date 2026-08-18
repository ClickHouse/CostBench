-- =============================================================================
-- OBSERVABILITY / ACTIVITY-TRACKING QUERIES for the Redshift T2 run.
--
-- Every query behind the telemetry and the post-run evidence files, in one place. Each section says
-- WHICH ENDPOINT to run it on and WHICH ARTIFACT it produces, so a result can be traced back to the
-- statement that made it.
--
--   WRITER = cb-quotes-rt-wg          (128 RPU) streaming ingest + both child MV refreshes
--   READER = cb-quotes-rt-reader-wg   ( 32 RPU) dashboard + drilldown reads, via datashare.
--                                     NOT publicly accessible -> query it from inside the VPC.
--
-- Substitute the run window everywhere it appears:
--   :T0 = '2026-08-12 16:53:56'   run start
--   :T1 = '2026-08-14 00:20:03'   producer DONE  <-- use this for anything freshness/cost related
--   :T2 = '2026-08-14 08:05:00'   controllers stopped (includes the idle post-ingest tail)
--
-- WHY :T1 MATTERS. After the producer stops, no new Kafka timestamps arrive, so freshness metrics
-- measure elapsed wall-clock instead of staleness (they climb to 27,783 s). Cut every freshness and
-- cost figure at :T1. Use :T2 only when you deliberately want the tail (e.g. to price the idle time,
-- or to bill read queries that ran after ingest finished).
--
-- FIVE TRAPS, ALL HIT DURING THIS RUN — see the notes at each query:
--   1. `query_text ILIKE %s` with a '%...%' parameter fails to bind (wildcards vs paramstyle).
--   2. Several MEDIAN() over DIFFERENT columns in one SELECT is rejected outright.
--   3. MV backing tables are named mv_tbl__<mv>__0 in SVV_TABLE_INFO, not <mv>.
--   4. The READER intermittently returns an EMPTY result set for an aggregate that must return a row
--      -> always retry on empty rather than recording "no rows".
--   5. is_rrscan = 't' only means a range-restricted scan was ATTEMPTED. It does NOT prove blocks
--      were skipped — compare scanned rows against rows returned.
-- =============================================================================


-- #############################################################################
-- SECTION 1 — DURING THE RUN (monitor_lag.py, WRITER)
-- These MUST be sampled live: SYS_STREAM_SCAN_STATES is effectively point-in-time.
-- #############################################################################

-- 1.1  Per-partition streaming freshness -> lag_<ts>.jsonl
--      max_latency_s = how far quotes_streamed trails the Kafka tip (the headline "behind_by").
--      lag_from_latest = backlog rows; latest_position = consumed offset (tip = position + lag).
--      raw_rows for the volume axis is derived from SUM(latest_position) — free, because the topic is
--      recreated per run so offsets start at 0. A COUNT(*) here would scan 113B rows on the very
--      workgroup being measured.
SELECT partition_id, lag_from_latest, max_latency_s, latest_position
FROM (
    SELECT partition_id, lag_from_latest, max_latency_s, latest_position,
           ROW_NUMBER() OVER (PARTITION BY partition_id ORDER BY record_time DESC) AS rn
    FROM SYS_STREAM_SCAN_STATES
    WHERE TRIM(mv_name) = 'quotes_streamed'
) WHERE rn = 1;

-- 1.2  Rollup freshness -> refresh_<ts>.jsonl (child_freshness_s)
--      ALREADY END-TO-END: now() minus the KAFKA ARRIVAL timestamp of the newest row in the child,
--      so it spans MSK -> streaming MV -> child. Do NOT add streamed_latency_s to it (an earlier
--      version did and double-counted the streaming hop).
--      To get the Snowflake `behind_by` analogue (child vs its SOURCE) SUBTRACT the streaming hop:
--          behind_source = child_freshness_s - max_latency_s
--      Computed server-side (GETDATE()) to avoid client clock skew. Only run this on quotes_daily:
--      the watermark is an aggregate over ~1.9M rollup rows, so MAX() is cheap. The same probe on
--      quotes_typed would be a full scan of 113B rows.
SELECT DATEDIFF(second, MAX(watermark_kafka_ts), GETDATE()) AS child_freshness_s
FROM quotes_daily;

-- 1.3  Server's own verdict on the last refresh -> refresh_<ts>.jsonl
--      THE ACCEPTANCE SIGNAL. Expected status text is
--        "Refresh successfully updated MV incrementally, but MV depends on an MV that is not up to date."
--      The trailing clause is normal (the streaming MV gained rows mid-refresh).
--      "MV was already updated..." = a no-op, NOT a failure. Anything mentioning a FULL recompute
--      invalidates the run at this scale — stop and investigate.
--      NB duration reads ~0 here (populated asynchronously); trust the client-measured duration.
SELECT refresh_type, status, duration / 1e6 AS server_seconds
FROM SYS_MV_REFRESH_HISTORY
WHERE TRIM(mv_name) = 'quotes_typed'      -- or 'quotes_daily'
ORDER BY start_time DESC LIMIT 1;

-- 1.4  The maintenance operations themselves (tagged so they're identifiable in query history)
--      RESTRICT is the default but is passed explicitly: CASCADE would also refresh the streaming MV
--      underneath, conflating ingest with child maintenance.
/* costbench:refresh:typed */ REFRESH MATERIALIZED VIEW quotes_typed RESTRICT;
/* costbench:refresh:daily */ REFRESH MATERIALIZED VIEW quotes_daily RESTRICT;

-- 1.5  Ingest errors — must stay empty for the run to be valid
SELECT COUNT(*) FROM SYS_STREAM_SCAN_ERRORS;

-- 1.6  Refresh topology check (run once after setup, before starting the clock)
--      Only quotes_streamed may be autorefresh = t; Redshift disallows AUTO REFRESH on an MV defined
--      on another MV, which is why the two children need an explicit controller.
SELECT TRIM(name) AS mv, autorefresh, is_stale, state
FROM SVV_MV_INFO WHERE schema_name = 'public' ORDER BY 1;


-- #############################################################################
-- SECTION 2 — DURING THE RUN (runner_redshift.py, READER)
-- #############################################################################

-- 2.1  Session setup for every measured connection
SET enable_result_cache_for_session TO off;   -- else a repeat returns ~0 ms from cache
SET search_path TO shared_public;             -- shared objects live in the datashare's external schema

-- 2.2  Volume axis, kept OFF the measured path -> raw_rows / mv_rows in the read JSONL
--      n_quotes is a COUNT(*) per (sym, day), so SUM(n_quotes) is exactly the rows the rollup has
--      absorbed. Scans ~1.9M rows instead of 113B. `--counts-mode exact` swaps in the true COUNT(*),
--      which then lands on the workgroup under measurement.
SELECT COALESCE(SUM(n_quotes), 0) AS raw_rows FROM quotes_daily;
SELECT COUNT(*) AS mv_rows FROM quotes_daily;

-- 2.3  Server-side timing for the query just executed -> result / compilation_time / execution_time
--      pg_last_query_id() is read immediately after the timed query on the same session. Retry a few
--      times: a just-finished query can take ~1 s to land in the view.
--      FUTURE IMPROVEMENT: persist query_id into the JSONL. It isn't currently stored, so attaching
--      billed cost later (section 4.6) has to join on (time window + exact elapsed_s).
SELECT elapsed_time, compile_time, execution_time
FROM SYS_QUERY_HISTORY WHERE query_id = pg_last_query_id();


-- #############################################################################
-- SECTION 3 — POST-RUN COST (get_metrics.py). Run ONCE PER WORKGROUP.
-- Serverless bills RPU-seconds per workgroup, so writer and reader are separate lines:
--   Published T2 model = shared writer capacity-time + MSK broker-hours/storage + allocated reader queries
--   Client cross-AZ and RMS are retained as scope notes but excluded from the main comparison.
-- #############################################################################

-- 3.1  Metered compute -> optional per-workgroup usage audit
--      (feeds writer_cost_breakdown.json; the per-statement detail is 4.6)
--      compute_seconds = RPU-seconds actually used; charged_seconds = what is billed.
SELECT COALESCE(SUM(compute_seconds), 0) AS compute_seconds,
       COALESCE(SUM(charged_seconds), 0) AS charged_seconds,
       COALESCE(MAX(compute_capacity), 0) AS max_compute_capacity_rpu
FROM SYS_SERVERLESS_USAGE
WHERE start_time >= :T0 AND end_time <= :T1;

-- 3.2  Was the writer billed continuously? -> writer_cost_breakdown.json
--      This run: metered = 100.3% of wall-clock x base RPU, i.e. the writer billed its full 128 RPU
--      for the entire 31.4 h because ingest never let it idle. Writer cost is therefore
--      base_rpu x duration and is NOT decomposable per operation: removing a refresh would not lower
--      the bill unless it let the workgroup idle or allowed a lower base capacity.
SELECT SUM(compute_seconds)                              AS metered_rpu_seconds,
       DATEDIFF(second, :T0, :T1)                        AS wall_clock_seconds,
       MAX(compute_capacity)                             AS base_rpu,
       100.0 * SUM(compute_seconds)
             / (DATEDIFF(second, :T0, :T1) * MAX(compute_capacity)) AS capacity_time_pct
FROM SYS_SERVERLESS_USAGE
WHERE start_time >= :T0 AND start_time <= :T1;

-- 3.3  Was extra compute for autonomics ever used? -> operational evidence
--      This run returned 0. Combined with extraComputeForAutomaticOptimization = null (the serverless
--      DEFAULT is disabled), that is why background VacuumSort was starved: AWS suspends autonomics
--      under sustained load when extra compute is unavailable. Enable with
--        aws redshift-serverless update-workgroup --workgroup-name <wg> \
--            --extra-compute-for-automatic-optimization --region eu-west-2
SELECT SUM(charged_extra_compute_for_automatic_optimization_seconds) AS extra_optimization_rpu_seconds
FROM SYS_SERVERLESS_USAGE
WHERE start_time >= :T0 AND start_time <= :T1;

-- 3.4  Storage footprint -> table_state_writer.csv (see also 4.2)
--      TRAP 3: MV backing tables are mv_tbl__<mv>__0, so filtering on "table" IN ('quotes_typed',...)
--      returns ZERO rows. Match on LIKE '%quotes%'.
SELECT "table" AS table_name, size AS size_mb, tbl_rows
FROM SVV_TABLE_INFO
WHERE schema = 'public' AND LOWER("table") LIKE '%quotes%'
ORDER BY size DESC;

-- 3.5  Per-MV refresh summary (incremental vs full, failures)
--      Reports incremental-vs-full and failures. Deliberately NOT split into per-MV dollars: billing
--      is per workgroup per minute while ingest and both refreshes overlap.
--      TRAP: don't test for full recompute with `status ILIKE '%full%'` — "successfully" contains
--      "full". Match on the word "incrementally" instead.
SELECT TRIM(mv_name) AS mv, COALESCE(TRIM(refresh_type), 'unknown') AS refresh_type,
       COUNT(*) AS n,
       SUM(CASE WHEN POSITION('incrementally' IN LOWER(status)) > 0 THEN 1 ELSE 0 END) AS incremental,
       SUM(CASE WHEN POSITION('already updated' IN LOWER(status)) > 0 THEN 1 ELSE 0 END) AS no_op,
       AVG(duration) / 1e6 AS avg_seconds, MAX(duration) / 1e6 AS max_seconds
FROM SYS_MV_REFRESH_HISTORY
WHERE start_time >= :T0 AND start_time <= :T1
GROUP BY 1, 2 ORDER BY 1, 2;

-- 3.6  Read-query timing summary
--      TRAP 1: `query_text ILIKE %s` with '%...%' fails to bind -> use POSITION() with a
--              wildcard-free parameter.
--      TRAP 2: MEDIAN() is an ordered-set aggregate; several of them over DIFFERENT columns in one
--              SELECT is rejected with "within group ORDER BY clauses ... must be the same".
--              Run one median per statement.
SELECT COUNT(*) AS n, MAX(elapsed_time) / 1e6 AS max_elapsed_s
FROM SYS_QUERY_HISTORY
WHERE query_type = 'SELECT' AND start_time >= :T0 AND start_time <= :T1
  AND POSITION(LOWER('quotes_daily') IN LOWER(query_text)) > 0;

SELECT MEDIAN(elapsed_time) / 1e6 AS median_elapsed_s     -- repeat separately for
FROM SYS_QUERY_HISTORY                                     -- compile_time and execution_time
WHERE query_type = 'SELECT' AND start_time >= :T0 AND start_time <= :T1
  AND POSITION(LOWER('quotes_daily') IN LOWER(query_text)) > 0;


-- #############################################################################
-- SECTION 4 — POST-RUN EVIDENCE (the "why was it slow" files)
-- #############################################################################

-- 4.1  Scan-level pruning evidence -> scan_evidence_reader.csv   [READER]
--      THE key artifact. TRAP 5: is_rrscan = 't' was true for ~99% of queries here, yet the
--      drilldowns still scanned 9-19 BILLION rows to return ~55M (97-356x amplification). The flag
--      only means a range-restricted scan was ATTEMPTED; judge pruning by scanned-vs-returned rows.
--      table_name arrives fully qualified through the datashare, e.g.
--      quotes_shared.public.mv_tbl__quotes_typed__0 — match with LIKE, not equality.
WITH measured_queries AS (
    SELECT query_id, start_time, elapsed_time, query_text
    FROM sys_query_history
    WHERE start_time >= :T0 AND start_time <= :T2
      AND query_type = 'SELECT'
      AND (   LOWER(query_text) LIKE '%from quotes_typed%'
           OR LOWER(query_text) LIKE '%from quotes_streamed%'
           OR LOWER(query_text) LIKE '%from quotes_daily%' )
),
scan_metrics AS (
    SELECT query_id, TRIM(table_name) AS table_name,
        MAX(CASE WHEN is_rrscan = 't' THEN 1 ELSE 0 END) AS used_range_restricted_scan,
        SUM(input_rows)  AS scanned_rows,
        SUM(output_rows) AS rows_after_filter,
        SUM(input_bytes) AS scanned_bytes,
        SUM(blocks_read) AS blocks_read,
        SUM(local_read_io)  AS local_blocks,
        SUM(remote_read_io) AS remote_blocks,
        SUM(duration)::DECIMAL(38,0) / 1000000.0 AS scan_seconds,
        MAX(data_skewness) AS max_data_skewness_pct,
        MAX(time_skewness) AS max_time_skewness_pct,
        SUM(spilled_block_local_disk)  AS local_spill_blocks,
        SUM(spilled_block_remote_disk) AS remote_spill_blocks
    FROM sys_query_detail
    WHERE LOWER(TRIM(metrics_level)) = 'step' AND LOWER(TRIM(step_name)) = 'scan'
    GROUP BY query_id, TRIM(table_name)
)
SELECT q.query_id, q.start_time, q.elapsed_time::FLOAT8 / 1000000 AS elapsed_seconds,
       s.*, LEFT(q.query_text, 250) AS query_text
FROM measured_queries q JOIN scan_metrics s USING (query_id)
ORDER BY q.start_time, q.query_id, s.table_name;

-- 4.2  Final physical state -> table_state_writer.csv   [WRITER]
--      This run: both large MVs finished 99.97% unsorted despite SORTKEY (sym, t) — every streaming
--      batch spans the whole symbol range, so each overlaps the entire leading-key range and lands in
--      the global unsorted region. Also shows the typed projection is LARGER than the SUPER original
--      (5.33 TB vs 3.70 TB for identical rows).
--      Caveat: vacuum_sort_benefit read 0.00 on tables that are 99.97% unsorted — treat with caution.
SELECT GETDATE() AS observed_at, table_id, "table" AS table_name, diststyle, sortkey1, sortkey_num,
       size AS size_mb, tbl_rows, unsorted, stats_off, skew_rows, vacuum_sort_benefit
FROM svv_table_info
WHERE schema = 'public' AND LOWER("table") LIKE '%quotes%'
ORDER BY table_name;

-- 4.3  Automatic sort maintenance -> auto_optimization_writer.csv   [WRITER]
--      This run: 58 VacuumSort attempts, 44 terminated with Redshift-reported INTERNAL ERRORS,
--      retrying every ~3-5 min for 20 h. Report those as service-reported failures with root cause
--      unknown absent AWS Support — not as misconfiguration.
SELECT TRIM(task_type) AS task_type, TRIM(object_type) AS object_type, TRIM(object_ids) AS object_ids,
       TRIM(status) AS status, TRIM(event) AS event, event_time, TRIM(task_details) AS task_details
FROM sys_automatic_optimization
WHERE event_time >= :T0 AND event_time <= :T2 AND task_type LIKE '%VacuumSort%'
ORDER BY event_time;

-- 4.4  What the successful vacuums actually sorted -> vacuum_history_writer.csv   [WRITER]
--      Distinguishes "a successful partial sort" from "the whole MV was sorted". This run: the large
--      MVs were vacuumed 3x each, ran 25-37 min, and moved ZERO net rows into the sorted region
--      (sortedrows_after = sortedrows_before). quotes_daily got 1,259 background vacuums, all
--      Delete Only — never a sort.
SELECT table_name, table_id, vacuum_type, is_automatic, status, start_time, end_time,
       duration::FLOAT8 / 1000000 AS duration_seconds,
       rows_before_vacuum, size_before_vacuum, sortedrows_before_vacuum, sortedrows_after_vacuum
FROM sys_vacuum_history
WHERE start_time >= :T0 AND start_time <= :T2
ORDER BY start_time, table_id;

-- 4.5  Which workgroup ran the refreshes?  (they can only run on the writer: in Serverless a
--      namespace owns its objects and has a 1:1 workgroup association, so a datashare consumer can
--      READ the MVs but cannot refresh them). Run on both endpoints and compare.
SELECT COUNT(*) AS tagged_refresh_statements
FROM sys_query_history
WHERE start_time BETWEEN :T0 AND :T1
  AND POSITION('costbench:refresh' IN query_text) > 0;

-- 4.6  Per-statement allocated compute RPU-seconds -> billed_per_query_{reader,writer}_full.csv
--      Same method as query-side-only/redshift-serverless/.../get_metrics.sh:
--          share        = statement elapsed_time / SUM(elapsed_time) in the bucket
--          allocated_compute_rpu_s = bucket compute_seconds * share
--      The share denominator covers EVERY statement in the bucket, so shares sum to 1 and nothing is
--      over-attributed. Hourly buckets here (the ClickBench run used daily, because there queries ran
--      serially with nothing else on the workgroup).
--      MEANINGFUL AS A NORMALIZED ALLOCATION ON THE READER: reads are the only user workload and it
--      idles between them. This remains an allocation, not a literal invoice reconstruction.
--      NOT A MARGINAL COST ON THE WRITER: it was saturated (see 3.2) and ~2.59 statements were in
--      flight on average, so this splits a FIXED pot. Read it as share of activity, and note that
--      shares summing to 1 forces any invisible work (vacuum/autonomics) onto visible statements.
--      TRAP 4: on the READER, retry when this returns an empty result set.
WITH usage AS (
    SELECT DATE_TRUNC('hour', start_time) AS bucket,
           SUM(compute_seconds)  AS compute_seconds,
           SUM(charged_seconds)  AS charged_seconds,
           MAX(compute_capacity) AS rpu
    FROM sys_serverless_usage
    WHERE start_time >= :T0 AND start_time <= :T2
    GROUP BY 1
),
q AS (
    SELECT query_id, start_time, query_type, elapsed_time, execution_time, compile_time, queue_time,
           LEFT(query_text, 200) AS query_text,
           DATE_TRUNC('hour', start_time) AS bucket,
           SUM(elapsed_time) OVER (PARTITION BY DATE_TRUNC('hour', start_time)) AS bucket_elapsed
    FROM sys_query_history
    WHERE start_time >= :T0 AND start_time <= :T2
)
SELECT q.query_id, q.start_time, q.query_type,
       q.elapsed_time::FLOAT8 / 1e6   AS elapsed_s,
       q.execution_time::FLOAT8 / 1e6 AS execution_s,
       q.compile_time::FLOAT8 / 1e6   AS compile_s,
       q.queue_time::FLOAT8 / 1e6     AS queue_s,
       u.rpu,
       q.elapsed_time::FLOAT8 / NULLIF(q.bucket_elapsed, 0)                      AS share_of_bucket,
       u.compute_seconds * (q.elapsed_time::FLOAT8 / NULLIF(q.bucket_elapsed, 0)) AS allocated_compute_rpu_seconds,
       q.query_text
FROM q JOIN usage u ON u.bucket = q.bucket
ORDER BY q.start_time;


-- #############################################################################
-- SECTION 5 — DATASHARE VERIFICATION (run before starting the clock)
-- Recreating the MVs invalidates the share, so this must be re-checked every run.
-- #############################################################################

-- 5.1  Producer side [WRITER] — the share must list the three objects AND a consumer
SELECT share_name, object_name, object_type FROM SVV_DATASHARE_OBJECTS WHERE share_name = 'quotes_share';
SELECT share_name, consumer_namespace FROM SVV_DATASHARE_CONSUMERS;

-- 5.2  Consumer side [READER] — must return rows, not "Publicly accessible consumer cannot access
--      object in the database". If it does, the reader workgroup is publicly accessible: a public
--      consumer cannot read datashare objects at all (set publicly_accessible = false).
SELECT COUNT(*) FROM shared_public.quotes_streamed;
SELECT COUNT(*) FROM shared_public.quotes_typed;
SELECT COUNT(*) FROM shared_public.quotes_daily;


-- #############################################################################
-- SECTION 6 — COST INPUTS THAT ARE **NOT** SQL
-- Kept here so there is one place to look. Redshift compute/storage come from the SYS_* queries in
-- sections 3 and 4.6; the lines below have no system view behind them.
-- #############################################################################

-- 6.1  MSK THROUGHPUT — CloudWatch, not SQL. Client cross-AZ is excluded from CostBench.
--      DEFAULT-level (free) metrics, summed across brokers:
--        MessagesInPerSec  -> EPS into MSK                       (dimensions: Cluster Name, Broker ID)
--        BytesInPerSec     -> COMPRESSED bytes/s from clients     (+ Topic dimension)
--      BytesInPerSec only starts emitting once the topic has received data, and both read ~0 while
--      the producer is stopped. Query definition: ops/cloudwatch_msk_throughput.json
--
--        aws cloudwatch get-metric-data --region eu-west-2 \
--          --start-time "$(date -u -v-15M +%Y-%m-%dT%H:%M:%SZ)" \
--          --end-time   "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
--          --metric-data-queries file://ops/cloudwatch_msk_throughput.json --output json
--
--      This run: ~999K EPS and ~28.5 MB/s COMPRESSED (=> 28.5 B/row; lz4 ~4.8:1 vs ~138 B raw).
--      That measurement is why the AWS MSK Sizing sheet's 9-broker recommendation was wrong for us:
--      the sheet's 100 MB/s input is UNCOMPRESSED, ours is 28.5 MB/s on the wire -> 3 brokers.

-- 6.2  MSK COST -> results/t2/msk_cost.json
--      Computed from cluster spec x uptime; there is no system view. Reproduce with get_metrics.py:
--
--        RS_HOST=<writer> python get_metrics.py --since :T0 --until :T1 \
--            --msk-hours 31.451944 --msk-brokers 3 --msk-broker-price 0.47175 \
--            --msk-storage-gb 1500 --msk-storage-price 0.116
--
--      Lines, and how much to trust each:
--        broker-hours   3 x $0.47175/hr x 31.451944h     = $44.51   deterministic
--        EBS            1500 GB x $0.116/GB-mo prorated  = $7.50   deterministic
--        replication    inter-broker cross-AZ          = $0.00   FREE on MSK provisioned (MSK FAQ:
--                       "not charged for data transfer within the cluster in a Region, including
--                       data transfer between brokers"). The Sizing sheet's big cross-AZ line counts
--                       this free traffic and overstates cost by orders of magnitude.
--        client x-AZ    excluded by benchmark-owner policy       = $0.00
--        TOTAL                                                 = $52.01
--
--      Cross-AZ telemetry may be retained for diagnostics but is not a cost input here.

-- 6.3  Invoice audit (not used by the normalized CostBench model) — Cost Explorer, needs AWS creds.
--      The reproducible summaries use explicit eu-west-2 prices captured in
--      costs/pricing_eu-west-2.json. Cost Explorer can still audit the account-level invoice.
--
--        aws ce get-cost-and-usage --region us-east-1 \
--          --time-period Start=2026-08-12,End=2026-08-15 --granularity DAILY \
--          --metrics UnblendedCost \
--          --group-by Type=DIMENSION,Key=SERVICE \
--          --filter '{"Dimensions":{"Key":"SERVICE","Values":["Amazon Redshift","Amazon Managed Streaming for Apache Kafka"]}}'
--
--      NOTE Cost Explorer bills the MSK cluster's FULL uptime (live since ~2026-08-06), whereas
--      msk_cost.json attributes only this run's 31.4h window — the right basis for a benchmark
--      comparison, but not what appears on the invoice.

-- 6.4  Autonomics setting (the config gap behind the vacuum failures) — control-plane, not SQL.
--      Returned null for this run = DISABLED = the serverless default, which matches the 0 RPU-seconds
--      measured by query 3.3.
--        aws redshift-serverless get-workgroup --workgroup-name cb-quotes-rt-wg --region eu-west-2 \
--          --query 'workgroup.extraComputeForAutomaticOptimization'
