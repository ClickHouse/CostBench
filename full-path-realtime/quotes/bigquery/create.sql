-- Rendered by setup.py. Do not replace the placeholders by hand.
--
-- Deliberately unpartitioned: the canonical raw queries filter by symbol but
-- not by time, matching the ClickHouse MergeTree ORDER BY (sym, t) workload.
-- BigQuery clustering is the closest physical-layout counterpart.
CREATE TABLE IF NOT EXISTS `__PROJECT_ID__.__DATASET_ID__.quotes`
(
    sym STRING,
    bx  INT64,
    bp  FLOAT64,
    bs  INT64,
    ax  INT64,
    ap  FLOAT64,
    `as` INT64,
    c   INT64,
    i   ARRAY<INT64>,
    t   INT64,
    q   INT64,
    z   INT64
)
CLUSTER BY sym, t;

-- BigQuery's native incremental materialized view is the counterpart to the
-- ClickHouse insert-time MV target. Refresh is asynchronous/best effort, but a
-- direct query of this MV remains current by combining cached data with base-
-- table deltas. No max_staleness option is set.
CREATE MATERIALIZED VIEW IF NOT EXISTS `__PROJECT_ID__.__DATASET_ID__.quotes_daily`
CLUSTER BY sym, day
OPTIONS (
    enable_refresh = TRUE,
    refresh_interval_minutes = 1
)
AS
SELECT
    sym,
    DATE(TIMESTAMP_MILLIS(t)) AS day,
    COUNT(*)                  AS n_quotes,
    MIN(bp)                   AS bp_min,
    MAX(bp)                   AS bp_max,
    MIN(ap)                   AS ap_min,
    MAX(ap)                   AS ap_max,
    SUM(bs)                   AS bs_sum,
    SUM(`as`)                 AS as_sum,
    SUM(ap - bp)              AS spread_sum
FROM `__PROJECT_ID__.__DATASET_ID__.quotes`
GROUP BY sym, day;
