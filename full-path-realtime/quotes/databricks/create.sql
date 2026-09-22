-- The catalog and schema, including their explicit non-default managed storage
-- location, are administrator prerequisites. Render the placeholders before use.
DROP MATERIALIZED VIEW IF EXISTS __CATALOG__.__SCHEMA__.quotes_daily;
DROP TABLE IF EXISTS __CATALOG__.__SCHEMA__.quotes;

CREATE TABLE __CATALOG__.__SCHEMA__.quotes (
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
    'delta.enableDeletionVectors' = 'true',
    'delta.enableRowTracking' = 'true',
    'delta.enableChangeDataFeed' = 'true'
);

CREATE MATERIALIZED VIEW __CATALOG__.__SCHEMA__.quotes_daily
USING DELTA
CLUSTER BY (sym, day)
REFRESH POLICY INCREMENTAL STRICT
TRIGGER ON UPDATE AT MOST EVERY INTERVAL 1 MINUTE
AS SELECT
    sym,
    date_add(
        DATE '1970-01-01',
        CAST(
            floor(
                CAST(t AS DECIMAL(20, 0))
                / CAST(86400000 AS DECIMAL(20, 0))
            ) AS INT
        )
    )               AS day,
    count(*)        AS n_quotes,
    min(bp)         AS bp_min,
    max(bp)         AS bp_max,
    min(ap)         AS ap_min,
    max(ap)         AS ap_max,
    sum(bs)         AS bs_sum,
    sum(`as`)       AS as_sum,
    sum(ap - bp)    AS spread_sum
FROM __CATALOG__.__SCHEMA__.quotes
GROUP BY sym, day;