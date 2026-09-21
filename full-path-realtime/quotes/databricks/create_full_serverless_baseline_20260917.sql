-- Canonical full-source Serverless SQL baseline R2 target.
--
-- Replace both __...__ principal placeholders before execution. Run once on
-- the control warehouse as the benchmark administrator. This file
-- intentionally contains no DROP or IF NOT EXISTS statements. Stop if any
-- statement fails.
-- R2 uses a fresh schema because the first target was consumed by an aborted
-- run whose query wrapper incorrectly used the qualification schema.

CREATE SCHEMA costbench.rt_full_serverless_baseline_r2_20260917;

GRANT USE CATALOG ON CATALOG costbench
TO `__SERVICE_PRINCIPAL_APPLICATION_ID__`;

GRANT USE SCHEMA
ON SCHEMA costbench.rt_full_serverless_baseline_r2_20260917
TO `__SERVICE_PRINCIPAL_APPLICATION_ID__`;

CREATE TABLE costbench.rt_full_serverless_baseline_r2_20260917.quotes (
    sym STRING,
    bx SMALLINT,
    bp DOUBLE,
    bs BIGINT,
    ax SMALLINT,
    ap DOUBLE,
    `as` BIGINT,
    c SMALLINT,
    i ARRAY<SMALLINT>,
    t BIGINT,
    q BIGINT,
    z SMALLINT
)
USING DELTA
CLUSTER BY (sym, t)
TBLPROPERTIES (
    'delta.enableDeletionVectors' = 'true',
    'delta.enableRowTracking' = 'true',
    'delta.enableChangeDataFeed' = 'true'
);

GRANT SELECT, MODIFY
ON TABLE costbench.rt_full_serverless_baseline_r2_20260917.quotes
TO `__SERVICE_PRINCIPAL_APPLICATION_ID__`;

-- This statement creates nothing. Require:
--   The Materialized View can be incrementally refreshed.
--   No issues detected.
EXPLAIN CREATE MATERIALIZED VIEW
costbench.rt_full_serverless_baseline_r2_20260917.quotes_daily_explain_only
USING DELTA
CLUSTER BY (sym, day)
REFRESH POLICY INCREMENTAL STRICT
TRIGGER ON UPDATE AT MOST EVERY INTERVAL 1 MINUTE
AS
SELECT
    sym,
    date_add(
        DATE '1970-01-01',
        CAST(
            floor(
                CAST(t AS DECIMAL(20, 0))
                / CAST(86400000 AS DECIMAL(20, 0))
            ) AS INT
        )
    ) AS day,
    count(*) AS n_quotes,
    min(bp) AS bp_min,
    max(bp) AS bp_max,
    min(ap) AS ap_min,
    max(ap) AS ap_max,
    sum(bs) AS bs_sum,
    sum(`as`) AS as_sum,
    sum(ap - bp) AS spread_sum
FROM costbench.rt_full_serverless_baseline_r2_20260917.quotes
GROUP BY sym, day;

CREATE MATERIALIZED VIEW
costbench.rt_full_serverless_baseline_r2_20260917.quotes_daily
USING DELTA
CLUSTER BY (sym, day)
REFRESH POLICY INCREMENTAL STRICT
TRIGGER ON UPDATE AT MOST EVERY INTERVAL 1 MINUTE
AS
SELECT
    sym,
    date_add(
        DATE '1970-01-01',
        CAST(
            floor(
                CAST(t AS DECIMAL(20, 0))
                / CAST(86400000 AS DECIMAL(20, 0))
            ) AS INT
        )
    ) AS day,
    count(*) AS n_quotes,
    min(bp) AS bp_min,
    max(bp) AS bp_max,
    min(ap) AS ap_min,
    max(ap) AS ap_max,
    sum(bs) AS bs_sum,
    sum(`as`) AS as_sum,
    sum(ap - bp) AS spread_sum
FROM costbench.rt_full_serverless_baseline_r2_20260917.quotes
GROUP BY sym, day;

-- Retain the creator's access before transferring ownership.
GRANT SELECT, MANAGE
ON MATERIALIZED VIEW
costbench.rt_full_serverless_baseline_r2_20260917.quotes_daily
TO `__BENCHMARK_ADMIN_PRINCIPAL__`;

ALTER MATERIALIZED VIEW
costbench.rt_full_serverless_baseline_r2_20260917.quotes_daily
SET OWNER TO `__SERVICE_PRINCIPAL_APPLICATION_ID__`;

-- Metadata-only verification. Do not query either table's contents.
DESCRIBE SCHEMA EXTENDED
costbench.rt_full_serverless_baseline_r2_20260917;

DESCRIBE DETAIL
costbench.rt_full_serverless_baseline_r2_20260917.quotes;

DESCRIBE HISTORY
costbench.rt_full_serverless_baseline_r2_20260917.quotes
LIMIT 2;

SHOW TBLPROPERTIES
costbench.rt_full_serverless_baseline_r2_20260917.quotes;

DESCRIBE TABLE EXTENDED
costbench.rt_full_serverless_baseline_r2_20260917.quotes
AS JSON;

DESCRIBE TABLE EXTENDED
costbench.rt_full_serverless_baseline_r2_20260917.quotes_daily
AS JSON;

DESCRIBE TABLE EXTENDED
costbench.rt_full_serverless_baseline_r2_20260917.quotes_daily;

SHOW GRANTS ON TABLE
costbench.rt_full_serverless_baseline_r2_20260917.quotes;

SHOW GRANTS ON MATERIALIZED VIEW
costbench.rt_full_serverless_baseline_r2_20260917.quotes_daily;
