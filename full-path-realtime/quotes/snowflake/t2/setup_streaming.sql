-- =============================================================================
-- T2 — Snowpipe Streaming (high-performance) -> interactive table; interactive MV for the rollup.
-- Fresh schema BENCH2COST.STOCKHOUSE_T2, separate from STOCKHOUSE / _T0 / _T1.
--
-- Architecture:
--   client (t2/stream_quotes.py) --Snowpipe Streaming SDK--> QUOTES_IT_PIPE --> QUOTES_IT
--       (interactive table, streaming target; fed by the pipe, not a refresh)
--   QUOTES_DAILY_IMV  (interactive materialized view ON QUOTES_IT) = the (sym, day) rollup
--       (an IT can't source another IT, so the rollup must be an interactive MV)
--
-- Warehouses: streaming ingest is SERVERLESS (no warehouse); the interactive MV is SERVERLESS
-- (verified — no maintenance wh needed). Dashboard/drilldown READS run on an existing interactive
-- warehouse passed via SF_WAREHOUSE (e.g. SNOWPIPES_IT_READ_SMALL or BENCH2COST_IT_SMALL); tracking
-- on SF_TRACK_WAREHOUSE (e.g. BENCH). So this script creates NO warehouses.
--
-- Cost = streaming credits (METERING_HISTORY SERVICE_TYPE='SNOWPIPE_STREAMING') + IMV maintenance + read wh.
-- Re-runnable: CREATE OR REPLACE on the IT/pipe/IMV (cheap while empty; re-streams from scratch).
-- =============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE BENCH2COST;
CREATE SCHEMA IF NOT EXISTS STOCKHOUSE_T2;
USE SCHEMA STOCKHOUSE_T2;

-- ---- raw streaming-target interactive table -----------------------------------
-- Explicit columns (a streaming target is a base IT, not AS SELECT). CLUSTER BY (sym, t).
CREATE OR REPLACE INTERACTIVE TABLE QUOTES_IT (
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

-- ---- Snowpipe Streaming pipe targeting the interactive table ------------------
-- High-performance Snowpipe Streaming pipes use COPY INTO ... FROM (SELECT $1:<field>::<type> ...
-- FROM TABLE(DATA_SOURCE(TYPE => 'STREAMING'))). $1 is the streamed row; project each field
-- (source parquet/SDK keys are lowercase, incl. "as"). NOT the bare FROM TABLE(...) + MATCH_BY_
-- COLUMN_NAME form (that errors). Add CLUSTER_AT_INGEST_TIME=TRUE in the COPY to precluster if needed.
CREATE OR REPLACE PIPE QUOTES_IT_PIPE AS
COPY INTO QUOTES_IT (sym, bx, bp, bs, ax, ap, "AS", c, i, t, q, z)
FROM (
    SELECT $1:sym::VARCHAR,
           $1:bx::NUMBER(3,0),
           $1:bp::FLOAT,
           $1:bs::NUMBER(20,0),
           $1:ax::NUMBER(3,0),
           $1:ap::FLOAT,
           $1:"as"::NUMBER(20,0),
           $1:c::NUMBER(3,0),
           $1:i::ARRAY,
           $1:t::NUMBER(20,0),
           $1:q::NUMBER(20,0),
           $1:z::NUMBER(3,0)
    FROM TABLE(DATA_SOURCE(TYPE => 'STREAMING'))
);

-- ---- aggregate rollup as an INTERACTIVE MATERIALIZED VIEW on QUOTES_IT ---------
-- Serverless-maintained. IMPORTANT: NO `CLUSTER BY` — an interactive MV doesn't accept one, and
-- with it Snowflake silently creates a plain (non-materialized) VIEW instead. Source must be an
-- interactive table; no joins (ours is a single-table GROUP BY). Then attach it + its source IT to
-- the interactive read warehouse (see ALTER below). Per docs: docs.snowflake.com/en/user-guide/interactive
CREATE OR REPLACE INTERACTIVE MATERIALIZED VIEW QUOTES_DAILY_IMV
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
FROM QUOTES_IT
GROUP BY sym, TO_DATE(TO_TIMESTAMP_NTZ(t, 3));

-- Attach the IMV + its source IT to the interactive READ warehouse (PARENTHESES required;
-- without them the ALTER errors). Adjust to your account's interactive read wh.
ALTER WAREHOUSE SNOWPIPES_IT_READ_SMALL ADD TABLES (QUOTES_IT, QUOTES_DAILY_IMV);

SHOW INTERACTIVE TABLES IN SCHEMA BENCH2COST.STOCKHOUSE_T2;
SHOW MATERIALIZED VIEWS IN SCHEMA BENCH2COST.STOCKHOUSE_T2;
SHOW PIPES IN SCHEMA BENCH2COST.STOCKHOUSE_T2;
