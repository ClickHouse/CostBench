-- =============================================================================
-- DRILLDOWN — TWO queries, run ~hourly against the raw quotes table.
-- Both are single-symbol (sym only, no time filter) drilldowns; cost scales with
-- that symbol's data volume. Exercises the (sym, t) sort key.
--
--   Q1 = HOURLY OHLCV BARS  — the canonical "show me this symbol's chart" query:
--        per-hour OHLC + VWAP + volume + volatility + spread. High-cardinality
--        GROUP BY (where ClickHouse's hash aggregation wins).
--   Q2 = RISK & LIQUIDITY PROFILE (a.k.a. "B7") — a single-row microstructure
--        stats panel: realized volatility, spread distribution + tail risk
--        (skew/kurtosis/p95/p99), order-book imbalance, spread-vs-depth corr.
--        Single-pass aggregation (where ClickHouse stays I/O-bound while the
--        per-aggregate overhead accumulates on the other engine).
--
-- The runner times each query separately -> result: [[q1_secs],[q2_secs]].
-- v1 single-query drilldown is preserved in queries_raw_v1.sql.
-- (A daily-bars variant of Q1 lives in the project history if a coarser grain
--  is wanted.)
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Q1. HOURLY OHLCV BARS
-- -----------------------------------------------------------------------------
SELECT
    toStartOfHour(fromUnixTimestamp64Milli(t)) AS hour,
    argMin(bp, t)                           AS open,
    max(bp)                                 AS high,
    min(bp)                                 AS low,
    argMax(bp, t)                           AS close,
    sum(bs)                                 AS volume,
    sum(bp * bs) / sum(bs)                  AS vwap,
    stddevPop(bp)                           AS volatility,
    avg(ap - bp)                            AS avg_spread,
    count()                                 AS ticks
FROM quotes
WHERE sym = 'AAPL'
GROUP BY hour
ORDER BY hour;

-- -----------------------------------------------------------------------------
-- Q2. RISK & LIQUIDITY PROFILE  ("B7")
-- -----------------------------------------------------------------------------
SELECT
    count()                                                       AS ticks,
    avg((bp + ap) / 2)                                            AS avg_mid,
    stddevPop((bp + ap) / 2)                                      AS mid_volatility,
    avg(ap - bp)                                                  AS avg_spread,
    stddevPop(ap - bp)                                            AS spread_volatility,
    skewPop(ap - bp)                                              AS spread_skew,
    kurtPop(ap - bp)                                              AS spread_kurtosis,
    max(ap - bp)                                                  AS max_spread,
    corr(ap - bp, bs + `as`)                                      AS corr_spread_depth,
    avg((toFloat64(bs) - toFloat64(`as`)) / nullIf(bs + `as`, 0)) AS avg_book_imbalance,
    quantilesTDigest(0.95, 0.99)(ap - bp)                         AS spread_p95_p99
FROM quotes
WHERE sym = 'AAPL';