-- =============================================================================
-- Redshift Serverless streaming ingestion for the quotes benchmark, from Amazon MSK.
--
-- THREE OBJECTS, A FORK (not a cascade):
--
--     kafka."quotes"  (MSK topic, external schema)
--            |
--            v
--     quotes_streamed        streaming MV, SUPER payload, AUTO REFRESH YES   <- only auto MV
--          /        \
--         v          v
--   quotes_typed   quotes_daily
--   typed rows     (sym, day) rollup
--   manual refresh  manual refresh          <- both defined DIRECTLY on quotes_streamed
--
-- WHY a fork and not quotes_streamed -> quotes_typed -> quotes_daily:
-- chaining would add the typed refresh lag on top of the daily refresh lag, and would make
-- dashboard freshness depend on the typed branch. Keeping both children directly on the streaming
-- MV lets them be scheduled, measured and costed independently. The small duplication (each child
-- casts the fields it needs out of `payload`) is intentional.
--
-- WHY the landing layer stays SUPER instead of shredding 12 typed columns at ingest:
-- AWS's streaming-ingestion guidance is explicit — using JSON_EXTRACT_PATH_TEXT per column re-parses
-- the record once PER COLUMN ("if you extract 10 columns ... each JSON record is parsed 10 times"),
-- which raises ingestion latency. The recommended pattern is JSON_PARSE into SUPER at ingest and
-- extract downstream with PartiQL — which is exactly this fork.
-- https://docs.aws.amazon.com/redshift/latest/dg/materialized-view-streaming-ingestion.html
--
-- The benchmark then measures BOTH read paths against the same data:
--   * drilldown on live SUPER      (queries_drilldown_super.sql -> quotes_streamed)
--   * drilldown on typed columns   (queries_drilldown_typed.sql -> quotes_typed)
--   * dashboard on the rollup      (queries_dashboard.sql       -> quotes_daily)
--
-- REFRESH: only quotes_streamed is AUTO REFRESH (Redshift disallows AUTO REFRESH on an MV defined
-- on another MV). The two children are refreshed explicitly with RESTRICT (the default) by the
-- controller in monitor_lag.py. Do NOT use CASCADE and do NOT manually refresh quotes_streamed:
-- each child must consume whatever state of quotes_streamed is committed when its own refresh
-- transaction starts. https://docs.aws.amazon.com/redshift/latest/dg/materialized-view-refresh-sql-command.html
--
-- CREATE ALL THREE BEFORE STARTING THE TIMED STREAM, so no initial full backfill is mixed into the
-- steady-state refresh measurements.
-- =============================================================================

-- 1. External schema over MSK. URI = MSK TLS bootstrap brokers (comma-separated host:9094), from
--    `terraform output msk_bootstrap_tls`. AUTHENTICATION none works because MSK presents
--    Amazon-trusted certs; the keyword is URI (not KAFKA_BROKERS), order: FROM KAFKA URI ... AUTHENTICATION.
CREATE EXTERNAL SCHEMA IF NOT EXISTS kafka
FROM KAFKA
URI 'b-1.cbquotesrtmsk.3tt8fa.c2.kafka.eu-west-2.amazonaws.com:9094,b-2.cbquotesrtmsk.3tt8fa.c2.kafka.eu-west-2.amazonaws.com:9094,b-3.cbquotesrtmsk.3tt8fa.c2.kafka.eu-west-2.amazonaws.com:9094'
AUTHENTICATION none;

-- ---------------------------------------------------------------------------
-- 2. quotes_streamed — the live semi-structured landing layer (the ONLY streaming MV).
--    Keeps the full record as SUPER `payload`, PLUS `sym` and `t` promoted to typed columns so the
--    MV can carry SORTKEY (sym, t) — the same physical ordering as Snowflake's CLUSTER BY (sym, t)
--    and as quotes_typed. Without promoting them there is nothing to sort on: SORTKEY needs real
--    columns and cannot reference a path inside a SUPER value.
--
--    COST OF THIS CHOICE (measure it, don't assume): each promoted field is extracted INLINE, so a
--    record is parsed 3x (2x JSON_EXTRACT_PATH_TEXT + 1x JSON_PARSE) instead of once. AWS warns that
--    per-column extraction re-parses the record per column and raises ingestion latency, so 1M-EPS
--    keep-up MUST be re-verified after this change (SYS_STREAM_SCAN_STATES lag must stay bounded).
--    A subquery that parses once and reuses it is NOT possible here: a streaming MV rejects it with
--    "Materialized view over stream could not be created, reason: Column aliases are not supported."
--
--    WATCH OUT: giving this MV a sort key gives background vacuum something to sort, and per the
--    REFRESH docs a vacuum on a base object can mark dependent MVs for FULL RECOMPUTE even when they
--    are incremental. Check SYS_MV_REFRESH_HISTORY for refresh_type flipping away from incremental —
--    a full recompute of a 113B-row child would invalidate the run.
--
--    Malformed JSON is not expected (we control the producer); a parse failure is an experiment
--    failure to investigate, not something to silently skip.
--    NOTE clause order: table attributes (DISTSTYLE/SORTKEY) must precede AUTO REFRESH, else Redshift
--    raises a bare "syntax error at or near SORTKEY".
-- ---------------------------------------------------------------------------
CREATE MATERIALIZED VIEW quotes_streamed
DISTSTYLE EVEN
SORTKEY (sym, t)
AUTO REFRESH YES
AS
SELECT
    kafka_partition,
    kafka_offset,
    kafka_timestamp,
    JSON_EXTRACT_PATH_TEXT(FROM_VARBYTE(kafka_value, 'utf-8'), 'sym')::varchar(16) AS sym,
    JSON_EXTRACT_PATH_TEXT(FROM_VARBYTE(kafka_value, 'utf-8'), 't')::bigint        AS t,
    JSON_PARSE(FROM_VARBYTE(kafka_value, 'utf-8'))                                 AS payload  -- SUPER
FROM kafka."quotes";

-- ---------------------------------------------------------------------------
-- 3. quotes_typed — typed row projection ON quotes_streamed (physical typed columns).
--    This is the optimized raw-row representation and the Redshift analogue of Snowflake's typed
--    QUOTES_IT / ClickHouse's typed quotes table. The typed drilldown suite queries THIS.
--
--    SORTKEY (sym, t) mirrors Snowflake's CLUSTER BY (sym, t) — the drilldowns filter on sym, so the
--    sort key drives zone-map pruning. DISTSTYLE EVEN deliberately, NOT DISTKEY(sym): distributing
--    by symbol would put every AAPL row on a single slice, so the single-symbol drilldown would run
--    effectively single-slice. EVEN keeps scan parallelism while the sort key supplies the pruning.
--
--    Kafka metadata is retained so freshness/reconciliation can be measured per partition and so a
--    watermark (MAX(kafka_timestamp) / MAX(kafka_offset)) is available.
--    `i` stays SUPER — the source field is an array, not a scalar.
--    `as` is a reserved word -> quoted, both in the source navigation and the output column.
-- ---------------------------------------------------------------------------
CREATE MATERIALIZED VIEW quotes_typed
DISTSTYLE EVEN
SORTKEY (sym, t)
AS
SELECT
    kafka_partition,
    kafka_offset,
    kafka_timestamp,
    sym,                                    -- already typed on the parent (no re-cast, no re-parse)
    payload.bx::smallint          AS bx,
    payload.bp::float8            AS bp,
    payload.bs::bigint            AS bs,
    payload.ax::smallint          AS ax,
    payload.ap::float8            AS ap,
    payload."as"::bigint          AS "as",
    payload.c::smallint           AS c,
    payload.i                     AS i,     -- array -> stays SUPER
    t,                                      -- already typed on the parent (epoch milliseconds)
    payload.q::bigint             AS q,
    payload.z::smallint           AS z
FROM quotes_streamed;

-- ---------------------------------------------------------------------------
-- 4. quotes_daily — (sym, day) rollup, ALSO directly on quotes_streamed (NOT on quotes_typed).
--    It re-extracts the handful of fields it needs from `payload`; that duplication is what keeps
--    dashboard freshness independent of the typed branch.
--
--    INCREMENTAL-REFRESH ELIGIBILITY (verify with SVV_MV_INFO after creating — see below):
--    COUNT/SUM/MIN/MAX are supported; MEDIAN/PERCENTILE/STDDEV/APPROXIMATE, DISTINCT aggregates,
--    window functions and subqueries are NOT. Date-time functions are "mutable" and generally block
--    incremental refresh, EXCEPT DATE(timestamp), DATE_PART and DATE_TRUNC(timestamp, interval) —
--    so the day bucket uses DATE_TRUNC('day', <timestamp>) rather than an epoch/interval cast.
--    https://docs.aws.amazon.com/redshift/latest/dg/materialized-view-refresh-sql-command.html
-- ---------------------------------------------------------------------------
CREATE MATERIALIZED VIEW quotes_daily
DISTSTYLE EVEN
SORTKEY (sym, day)
AS
SELECT
    sym,                                                                 -- typed on the parent
    DATE_TRUNC('day', TIMESTAMP 'epoch' + t / 1000 * INTERVAL '1 second') AS day,
    COUNT(*)                                                             AS n_quotes,
    MIN(payload.bp::float8)                                              AS bp_min,
    MAX(payload.bp::float8)                                              AS bp_max,
    MIN(payload.ap::float8)                                              AS ap_min,
    MAX(payload.ap::float8)                                              AS ap_max,
    SUM(payload.bs::bigint)                                              AS bs_sum,
    SUM(payload."as"::bigint)                                            AS as_sum,
    SUM(payload.ap::float8 - payload.bp::float8)                         AS spread_sum,
    MAX(kafka_timestamp)                                                 AS watermark_kafka_ts
FROM quotes_streamed
GROUP BY 1, 2;

-- ---- checks ----------------------------------------------------------------
--   -- ACCEPTANCE GATE: both children must report incremental, not full recompute.
--   SELECT name, is_stale, autorefresh, state FROM SVV_MV_INFO WHERE schema_name = 'public';
--   SELECT TRIM(mv_name) mv, refresh_type, status, start_time, duration/1e6 AS secs
--     FROM SYS_MV_REFRESH_HISTORY ORDER BY start_time DESC LIMIT 20;
--
--   SELECT COUNT(*) FROM quotes_streamed;                  -- run repeatedly -> rows/sec
--   SELECT * FROM SYS_STREAM_SCAN_STATES  ORDER BY record_time DESC LIMIT 20;  -- per-partition lag
--   SELECT * FROM SYS_STREAM_SCAN_ERRORS  ORDER BY record_time DESC LIMIT 20;  -- skipped/oversize records
--
--   -- children are refreshed by monitor_lag.py; manually they are:
--   REFRESH MATERIALIZED VIEW quotes_typed RESTRICT;
--   REFRESH MATERIALIZED VIEW quotes_daily RESTRICT;
