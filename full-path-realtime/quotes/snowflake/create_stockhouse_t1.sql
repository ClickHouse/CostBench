-- =============================================================================
-- T1 (interactive-table) fresh schema — placeholder name STOCKHOUSE_2 (setup_schema.sh
-- sed-substitutes it to $SF_SCHEMA, e.g. STOCKHOUSE_T1).
--
-- DIFFERENCE vs create_stockhouse_2.sql: NO standard QUOTES_DAILY materialized view.
-- In T1 the rollup is the INTERACTIVE table QUOTES_DAILY_IT (created by ops/setup_interactive.sh),
-- which REPLACES the MV. Creating the standard MV here would be redundant and would add its own
-- maintenance/clustering cost during ingest, polluting T1's per-component cost attribution.
--
-- So this DDL creates only: schema + internal stage + clustered base table QUOTES. The two
-- interactive tables (QUOTES_IT, QUOTES_DAILY_IT) are created afterwards by setup_interactive.sh.
-- =============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE BENCH2COST;
CREATE SCHEMA IF NOT EXISTS STOCKHOUSE_2;
USE SCHEMA STOCKHOUSE_2;

-- Internal stage the ingester PUTs re-encoded row groups to (schema-scoped).
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

-- NO standard QUOTES_DAILY MV here (the interactive QUOTES_DAILY_IT replaces it; see above).
SHOW TABLES LIKE 'QUOTES' IN SCHEMA BENCH2COST.STOCKHOUSE_2;
