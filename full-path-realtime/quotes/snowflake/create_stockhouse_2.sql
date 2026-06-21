-- =============================================================================
-- Fresh schema for the clustering re-run: BENCH2COST.STOCKHOUSE_2
--
-- Why a new schema: the original STOCKHOUSE.QUOTES / QUOTES_DAILY carry ~1.85 TiB of
-- time-travel + fail-safe and a CREATE-OR-REPLACE-churned MV, which muddies storage and
-- clustering measurements. A clean schema gives an uncontaminated clustered run whose
-- clustering lag we track live (SYSTEM$CLUSTERING_INFORMATION via ops/clustering_lag.sh —
-- depth is point-in-time and never historized, so it MUST be sampled during the run).
--
-- Everything is created clustered up front (this run IS the clustered experiment), so no
-- separate add_clustering.sh step is needed. Run this once, then drive the run with
-- SF_SCHEMA=STOCKHOUSE_2 (every ops/ script + ingest + runners read that env var).
-- =============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE BENCH2COST;
CREATE SCHEMA IF NOT EXISTS STOCKHOUSE_2;
USE SCHEMA STOCKHOUSE_2;

-- Internal stage the ingester PUTs re-encoded row groups to (schema-scoped; the original
-- lives in STOCKHOUSE, so it must be recreated here). ingest.py COPYs with an inline
-- FILE_FORMAT=(TYPE=PARQUET), so a plain internal stage is all that's needed.
CREATE STAGE IF NOT EXISTS QUOTES_INT_STAGE;

-- Raw quotes, clustered by (sym, t) — mirrors ClickHouse ORDER BY (sym, t).
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

-- Per-(sym, day) rollup as a materialized view, clustered by (sym, day).
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

-- Automatic Clustering is ON by default once CLUSTER BY is set. Verify:
SHOW TABLES LIKE 'QUOTES' IN SCHEMA BENCH2COST.STOCKHOUSE_2;
SHOW MATERIALIZED VIEWS LIKE 'QUOTES_DAILY' IN SCHEMA BENCH2COST.STOCKHOUSE_2;
