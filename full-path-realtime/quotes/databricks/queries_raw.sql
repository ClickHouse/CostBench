-- Raw-table drill-down queries: same order and logical work as ClickHouse T2.
-- Lakehouse//RT runs in ANSI mode, so arithmetic uses explicit widening and
-- guarded division. Both queries filter only by symbol, never by time.

-- 1. Hourly OHLCV bars.
SELECT
    date_trunc('HOUR', timestamp_millis(t))                 AS hour,
    min_by(bp, t)                                           AS open,
    max(bp)                                                 AS high,
    min(bp)                                                 AS low,
    max_by(bp, t)                                           AS close,
    sum(CAST(bs AS DECIMAL(38, 0)))                         AS volume,
    try_divide(
        sum(CAST(bp AS DOUBLE) * CAST(bs AS DOUBLE)),
        sum(CAST(bs AS DOUBLE))
    )                                                       AS vwap,
    stddev_pop(bp)                                          AS volatility,
    avg(CAST(ap AS DOUBLE) - CAST(bp AS DOUBLE))            AS avg_spread,
    count(*)                                                AS ticks
FROM __CATALOG__.__SCHEMA__.__RAW_TABLE__
WHERE sym = 'AAPL'
GROUP BY hour
ORDER BY hour;

-- 2. Risk and liquidity profile (B7).
-- Databricks skewness/kurtosis do not express ClickHouse's population/raw
-- contract directly, so derive population central moments from raw moments.
WITH raw_metrics AS
(
    SELECT
        (
            CAST(bp AS DOUBLE) + CAST(ap AS DOUBLE)
        ) / CAST(2 AS DOUBLE)                               AS mid,
        CAST(ap AS DOUBLE) - CAST(bp AS DOUBLE)             AS spread,
        CAST(bs AS DOUBLE) + CAST(`as` AS DOUBLE)           AS depth,
        try_divide(
            CAST(bs AS DOUBLE) - CAST(`as` AS DOUBLE),
            CAST(bs AS DOUBLE) + CAST(`as` AS DOUBLE)
        )                                                   AS book_imbalance
    FROM __CATALOG__.__SCHEMA__.__RAW_TABLE__
    WHERE sym = 'AAPL'
),
aggregates AS
(
    SELECT
        count(*)                                            AS n,
        avg(mid)                                            AS avg_mid,
        stddev_pop(mid)                                     AS mid_volatility,
        avg(spread)                                         AS avg_spread,
        stddev_pop(spread)                                  AS spread_volatility,
        max(spread)                                         AS max_spread,
        corr(spread, depth)                                 AS corr_spread_depth,
        avg(book_imbalance)                                 AS avg_book_imbalance,
        percentile_approx(
            spread,
            array(CAST(0.95 AS DOUBLE), CAST(0.99 AS DOUBLE))
        )                                                   AS spread_p95_p99,
        avg(spread)                                         AS raw_m1,
        avg(power(spread, 2))                               AS raw_m2,
        avg(power(spread, 3))                               AS raw_m3,
        avg(power(spread, 4))                               AS raw_m4
    FROM raw_metrics
),
moments AS
(
    SELECT
        *,
        raw_m2 - power(raw_m1, 2)                           AS mu2,
        raw_m3
            - CAST(3 AS DOUBLE) * raw_m1 * raw_m2
            + CAST(2 AS DOUBLE) * power(raw_m1, 3)          AS mu3,
        raw_m4
            - CAST(4 AS DOUBLE) * raw_m1 * raw_m3
            + CAST(6 AS DOUBLE) * power(raw_m1, 2) * raw_m2
            - CAST(3 AS DOUBLE) * power(raw_m1, 4)          AS mu4
    FROM aggregates
)
SELECT
    n                                                       AS ticks,
    avg_mid,
    mid_volatility,
    avg_spread,
    spread_volatility,
    CASE
        WHEN mu2 > CAST(0 AS DOUBLE)
        THEN try_divide(mu3, power(mu2, CAST(1.5 AS DOUBLE)))
        ELSE NULL
    END                                                     AS spread_skew,
    CASE
        WHEN mu2 > CAST(0 AS DOUBLE)
        THEN try_divide(mu4, power(mu2, CAST(2 AS DOUBLE)))
        ELSE NULL
    END                                                     AS spread_kurtosis,
    max_spread,
    corr_spread_depth,
    avg_book_imbalance,
    spread_p95_p99
FROM moments;
