-- =============================================================================
-- stockhouse quotes ingest benchmark — Snowflake translation
-- Target: BENCH2COST.STOCKHOUSE (AWS eu-west-3, Enterprise, Gen2 warehouses)
--
-- ClickHouse -> Snowflake rollup mechanism:
--   create.sql uses a ClickHouse INCREMENTAL MATERIALIZED VIEW (quotes_daily_mv)
--   feeding an AggregatingMergeTree (quotes_daily), maintained synchronously on
--   every INSERT. The faithful Snowflake peer is a **MATERIALIZED VIEW** — also
--   incremental + always-fresh (auto-maintained by a serverless background
--   service). Verified empirically that a Snowflake MV accepts this exact rollup,
--   including the derived GROUP BY key TO_DATE(TO_TIMESTAMP_NTZ(t,3)). MVs allow
--   GROUP BY + COUNT/MIN/MAX/SUM + expressions in the SELECT; the only rule is
--   that every GROUP BY key appears in the SELECT list (ours does). MVs forbid
--   joins, HAVING, ORDER BY, UNION, window functions, subqueries, multi-table —
--   none of which we use.
--
--   Refresh-model mapping:  SF Materialized View <-> CH incremental MV
--                           SF Dynamic Table     <-> CH refreshable MV
--   We use the MV as primary (matches the CH benchmark). A Dynamic Table variant
--   is included (commented) for a controllable-lag / refreshable-MV comparison.
--
-- Warehouses: ingest COPY -> BENCH2COST_GEN2_XSMALL; reader (dashboard/drilldown
--   queries) -> BENCH2COST_SMALL_GEN2; MV maintenance -> serverless (no warehouse).
-- =============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE BENCH2COST;
USE SCHEMA STOCKHOUSE;


-- =============================================================================
-- Base table: raw stock quotes  (CH `quotes` MergeTree, ORDER BY (sym, t))
-- Already exists and is fed by continuous COPY at ~1M rows/sec. Reference DDL.
-- Type mapping: String->VARCHAR, UInt8->NUMBER(3,0), Float64->FLOAT,
--   UInt64->NUMBER(20,0), Array(UInt8)->ARRAY. `as` reserved -> "AS". `t` = millis.
-- Clustering: CLUSTER BY (sym, t) mirrors CH's ORDER BY (sym, t) and accelerates
-- the sym-filtered drilldown. NOTE: ingest is time-ordered (not sym-ordered), so
-- Automatic Clustering reclusters continuously during the 1M-EPS run — a separate
-- serverless cost we measure (SNOWFLAKE.ACCOUNT_USAGE.AUTOMATIC_CLUSTERING_HISTORY).
-- For the UNCLUSTERED baseline, omit the CLUSTER BY line (or run drop_clustering.sh).
-- (CREATE TABLE IF NOT EXISTS won't re-cluster an existing table — use add_clustering.sh
--  / ALTER TABLE QUOTES CLUSTER BY (sym, t) for the live table.)
-- =============================================================================
CREATE TABLE IF NOT EXISTS QUOTES (
    sym   VARCHAR,
    bx    NUMBER(3,0),
    bp    FLOAT,
    bs    NUMBER(20,0),
    ax    NUMBER(3,0),
    ap    FLOAT,
    "AS"  NUMBER(20,0),
    c     NUMBER(3,0),
    i     ARRAY,
    t     NUMBER(20,0),
    q     NUMBER(20,0),
    z     NUMBER(3,0)
)
CLUSTER BY (sym, t);


-- =============================================================================
-- Rollup: per-(sym, day) summary as a MATERIALIZED VIEW (PRIMARY)
--   CH quotes_daily (AggregatingMergeTree) + quotes_daily_mv  ->  Snowflake MV.
--   Always-fresh, serverless background maintenance (no warehouse to assign).
--
-- Column mapping (CH SimpleAggregateFunction -> plain Snowflake aggregate):
--   n_quotes = COUNT(*) | bp_min/max = MIN/MAX(bp) | ap_min/max = MIN/MAX(ap)
--   bs_sum = SUM(bs) | as_sum = SUM("AS") | spread_sum = SUM(ap - bp)
-- day = TO_DATE(TO_TIMESTAMP_NTZ(t, 3))  -- scale 3 = milliseconds.
--
-- Dashboard queries read this by name (queries_mv.sql -> QUOTES_DAILY).
-- NOTE at 1M EPS: MV maintenance is serverless + continuous and can lag minutes
-- under heavy ingest — that lag + its maintenance credits are part of what we
-- measure (it's the cost of an always-fresh rollup).
-- CLUSTER BY (sym, day): sym is the leading key so the sym-filtered dashboard
-- queries get prefix pruning. QUOTES_DAILY is tiny, so reclustering cost is
-- negligible. (Omit CLUSTER BY for the unclustered baseline.)
-- =============================================================================
CREATE OR REPLACE MATERIALIZED VIEW QUOTES_DAILY
CLUSTER BY (sym, day)
AS
SELECT
    sym,
    TO_DATE(TO_TIMESTAMP_NTZ(t, 3))  AS day,
    COUNT(*)        AS n_quotes,
    MIN(bp)         AS bp_min,
    MAX(bp)         AS bp_max,
    MIN(ap)         AS ap_min,
    MAX(ap)         AS ap_max,
    SUM(bs)         AS bs_sum,
    SUM("AS")       AS as_sum,
    SUM(ap - bp)    AS spread_sum
FROM QUOTES
GROUP BY sym, TO_DATE(TO_TIMESTAMP_NTZ(t, 3));


-- =============================================================================
-- ALTERNATIVE (commented): Dynamic Table — peer to a CH *refreshable* MV.
-- Use this instead of the MV for a controllable-lag comparison. Assign a refresh
-- warehouse SEPARATE from the ingest warehouse. Re-add CLUSTER BY (sym, day) if
-- the dashboard-query phase needs prefix pruning (cheap; QUOTES_DAILY is tiny).
-- =============================================================================
-- CREATE OR REPLACE DYNAMIC TABLE QUOTES_DAILY_DT
--     TARGET_LAG   = '1 minute'
--     WAREHOUSE    = BENCH2COST_GEN2_XSMALL
--     REFRESH_MODE = AUTO
--     INITIALIZE   = ON_CREATE
-- AS
-- SELECT sym, TO_DATE(TO_TIMESTAMP_NTZ(t, 3)) AS day,
--        COUNT(*) AS n_quotes, MIN(bp) AS bp_min, MAX(bp) AS bp_max,
--        MIN(ap) AS ap_min, MAX(ap) AS ap_max, SUM(bs) AS bs_sum,
--        SUM("AS") AS as_sum, SUM(ap - bp) AS spread_sum
-- FROM QUOTES GROUP BY sym, TO_DATE(TO_TIMESTAMP_NTZ(t, 3));
