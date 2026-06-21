DROP TABLE IF EXISTS workspace.benchmarking.quotes;
DROP MATERIALIZED VIEW IF EXISTS workspace.benchmarking.quotes_daily;

CREATE TABLE workspace.benchmarking.quotes (
    sym         STRING,
    bx          SMALLINT,
    bp          DOUBLE,
    bs          BIGINT,
    ax          SMALLINT,
    ap          DOUBLE,
    `as`        BIGINT,
    c           SMALLINT,
    i           ARRAY<SMALLINT>,
    t           BIGINT,
    q           BIGINT,
    z           SMALLINT
)
USING DELTA
CLUSTER BY (sym, t)
TBLPROPERTIES (
    delta.enableRowTracking    = true,
    delta.enableChangeDataFeed = true
);

CREATE OR REPLACE MATERIALIZED VIEW workspace.benchmarking.quotes_daily
CLUSTER BY (sym, day)
TRIGGER ON UPDATE
AS SELECT
    sym,
    to_date(from_unixtime(t / 1000)) AS day,
    count(*)        AS n_quotes,
    min(bp)         AS bp_min,
    max(bp)         AS bp_max,
    min(ap)         AS ap_min,
    max(ap)         AS ap_max,
    sum(bs)         AS bs_sum,
    sum(`as`)       AS as_sum,
    sum(ap - bp)    AS spread_sum
FROM workspace.benchmarking.quotes
GROUP BY sym, day;