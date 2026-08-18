-- Raw-table drill-down queries: same order and logical work as ClickHouse T2.
-- The runner replaces __PROJECT_ID__ and __DATASET_ID__ and disables cache.

-- 1. Hourly OHLCV bars.
SELECT
    TIMESTAMP_TRUNC(TIMESTAMP_MILLIS(t), HOUR) AS hour,
    MIN_BY(bp, t)                                           AS open,
    MAX(bp)                                                AS high,
    MIN(bp)                                                AS low,
    MAX_BY(bp, t)                                           AS close,
    SUM(bs)                                                AS volume,
    SAFE_DIVIDE(SUM(bp * bs), SUM(bs))                     AS vwap,
    STDDEV_POP(bp)                                         AS volatility,
    AVG(ap - bp)                                           AS avg_spread,
    COUNT(*)                                               AS ticks
FROM `__PROJECT_ID__.__DATASET_ID__.quotes`
WHERE sym = 'AAPL'
GROUP BY hour
ORDER BY hour;

-- 2. Risk and liquidity profile (B7).
-- BigQuery has no native population skew/kurtosis aggregate, so the CTE uses
-- population central moments. kurtPop is raw population kurtosis (not excess),
-- matching ClickHouse kurtPop. APPROX_QUANTILES is not TDigest; see README.
WITH aggregates AS
(
    SELECT
        COUNT(*) AS n,
        AVG((bp + ap) / 2) AS avg_mid,
        STDDEV_POP((bp + ap) / 2) AS mid_volatility,
        AVG(ap - bp) AS avg_spread,
        STDDEV_POP(ap - bp) AS spread_volatility,
        MAX(ap - bp) AS max_spread,
        CORR(ap - bp, bs + `as`) AS corr_spread_depth,
        AVG(SAFE_DIVIDE(CAST(bs AS FLOAT64) - CAST(`as` AS FLOAT64), bs + `as`)) AS avg_book_imbalance,
        APPROX_QUANTILES(ap - bp, 100) AS spread_quantiles,
        SUM(ap - bp) AS s1,
        SUM(POW(ap - bp, 2)) AS s2,
        SUM(POW(ap - bp, 3)) AS s3,
        SUM(POW(ap - bp, 4)) AS s4
    FROM `__PROJECT_ID__.__DATASET_ID__.quotes`
    WHERE sym = 'AAPL'
),
moments AS
(
    SELECT
        *,
        SAFE_DIVIDE(s1, n) AS mean_x,
        SAFE_DIVIDE(s2, n) - POW(SAFE_DIVIDE(s1, n), 2) AS mu2,
        SAFE_DIVIDE(s3, n)
          - 3 * SAFE_DIVIDE(s1, n) * SAFE_DIVIDE(s2, n)
          + 2 * POW(SAFE_DIVIDE(s1, n), 3) AS mu3,
        SAFE_DIVIDE(s4, n)
          - 4 * SAFE_DIVIDE(s1, n) * SAFE_DIVIDE(s3, n)
          + 6 * POW(SAFE_DIVIDE(s1, n), 2) * SAFE_DIVIDE(s2, n)
          - 3 * POW(SAFE_DIVIDE(s1, n), 4) AS mu4
    FROM aggregates
)
SELECT
    n AS ticks,
    avg_mid,
    mid_volatility,
    avg_spread,
    spread_volatility,
    IF(mu2 > 0, SAFE_DIVIDE(mu3, POW(mu2, 1.5)), NULL) AS spread_skew,
    IF(mu2 > 0, SAFE_DIVIDE(mu4, POW(mu2, 2)), NULL) AS spread_kurtosis,
    max_spread,
    corr_spread_depth,
    avg_book_imbalance,
    -- BigQuery result arrays cannot contain NULL elements. A sparse input can
    -- make APPROX_QUANTILES return NULL, so use a named struct to preserve both
    -- percentile positions while retaining correct NULL semantics.
    STRUCT(
        spread_quantiles[SAFE_OFFSET(95)] AS p95,
        spread_quantiles[SAFE_OFFSET(99)] AS p99
    ) AS spread_p95_p99
FROM moments;
