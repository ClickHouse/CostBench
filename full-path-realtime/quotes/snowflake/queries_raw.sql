-- =============================================================================
-- DRILLDOWN (T0 standard) — TWO queries, run ~hourly against the standard QUOTES table.
-- Both are single-symbol (sym only, no time filter) drilldowns; cost scales with
-- the per-sym data volume.
--
--   Q1 = HOURLY OHLCV BARS  — per-hour OHLC + VWAP + volume + volatility + spread.
--   Q2 = RISK & LIQUIDITY PROFILE (a.k.a. "B7") — single-row microstructure panel:
--        realized volatility, spread distribution + tail risk (skew/kurtosis/p95/p99),
--        order-book imbalance, spread-vs-depth correlation.
--
-- Same two queries as queries_raw_it.sql, but FROM the standard QUOTES table (T0
-- baseline) rather than the interactive QUOTES_IT.
-- CH idiom mapping notes are in queries_raw_it.sql.
--
-- The runner times each query separately -> result: [[q1_secs],[q2_secs]].
-- v1 single-query drilldown is preserved in queries_raw_v1.sql.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Q1. HOURLY OHLCV BARS
-- -----------------------------------------------------------------------------
SELECT
    DATE_TRUNC('hour', TO_TIMESTAMP_NTZ(t, 3)) AS hour,
    MIN_BY(bp, t)                           AS open,
    MAX(bp)                                 AS high,
    MIN(bp)                                 AS low,
    MAX_BY(bp, t)                           AS close,
    SUM(bs)                                 AS volume,
    SUM(bp * bs) / SUM(bs)                  AS vwap,
    STDDEV_POP(bp)                          AS volatility,
    AVG(ap - bp)                            AS avg_spread,
    COUNT(*)                                AS ticks
FROM QUOTES
WHERE sym = 'AAPL'
GROUP BY hour
ORDER BY hour;

-- -----------------------------------------------------------------------------
-- Q2. RISK & LIQUIDITY PROFILE  ("B7")
-- -----------------------------------------------------------------------------
SELECT
    COUNT(*)                                 AS ticks,
    AVG((bp + ap) / 2)                       AS avg_mid,
    STDDEV_POP((bp + ap) / 2)                AS mid_volatility,
    AVG(ap - bp)                             AS avg_spread,
    STDDEV_POP(ap - bp)                      AS spread_volatility,
    SKEW(ap - bp)                            AS spread_skew,
    KURTOSIS(ap - bp)                        AS spread_kurtosis,
    MAX(ap - bp)                             AS max_spread,
    CORR(ap - bp, bs + "AS")                 AS corr_spread_depth,
    AVG((bs - "AS") / NULLIF(bs + "AS", 0))  AS avg_book_imbalance,
    APPROX_PERCENTILE(ap - bp, 0.95)         AS spread_p95,
    APPROX_PERCENTILE(ap - bp, 0.99)         AS spread_p99
FROM QUOTES
WHERE sym = 'AAPL';
