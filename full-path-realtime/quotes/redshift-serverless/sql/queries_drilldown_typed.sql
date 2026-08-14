-- =============================================================================
-- DRILLDOWN — TYPED suite. Runs against `quotes_typed` (physical typed columns,
-- SORTKEY (sym, t)). This is the Redshift analogue of Snowflake's typed QUOTES_IT and
-- ClickHouse's typed quotes table.
--
-- PAIRED FILE: queries_drilldown_super.sql runs the SAME logic against the live SUPER
-- payload in quotes_streamed. The two files must stay logically equivalent — the whole point
-- is to measure the cost of semi-structured vs typed access on identical data and logic.
-- Any change here must be mirrored there.
--
-- Ported from ../snowflake/t2/queries_raw_it.sql. Redshift substitutions (see also
-- the notes in queries_dashboard.sql):
--   MIN_BY(bp,t) / MAX_BY(bp,t)  -> no argMin/argMax in Redshift: ROW_NUMBER() windows pick the
--                                   first/last bp per hour (open/close). Kept because dropping them
--                                   would make the timed query materially less work than Snowflake's.
--   SKEW / KURTOSIS              -> central moments from single-pass raw sums (no SKEW/KURTOSIS).
--   CORR / COVAR_POP             -> Redshift has NEITHER; Pearson corr from AVG + STDDEV_POP.
--   APPROX_PERCENTILE(x,p)       -> APPROXIMATE PERCENTILE_DISC(p) WITHIN GROUP (ORDER BY x).
--   TO_TIMESTAMP_NTZ(t,3)        -> TIMESTAMP 'epoch' + t/1000 * INTERVAL '1 second'.
--   integer ratios               -> cast to float8 (bigint/bigint truncates toward zero).
-- The sym filter is on the typed `sym` column, aligned with SORTKEY (sym, t) for zone-map pruning.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Q1. HOURLY OHLCV BARS
-- -----------------------------------------------------------------------------
WITH src AS (
    SELECT DATE_TRUNC('hour', TIMESTAMP 'epoch' + t / 1000 * INTERVAL '1 second') AS hour,
           bp, bs, ap, t
    FROM quotes_typed
    WHERE sym = 'AAPL'
),
ranked AS (
    SELECT hour, bp, bs, ap,
           ROW_NUMBER() OVER (PARTITION BY hour ORDER BY t ASC)  AS rn_first,
           ROW_NUMBER() OVER (PARTITION BY hour ORDER BY t DESC) AS rn_last
    FROM src
)
SELECT
    hour,
    MAX(CASE WHEN rn_first = 1 THEN bp END)      AS "open",   -- quoted: OPEN/CLOSE are reserved words in Redshift
    MAX(bp)                                      AS high,
    MIN(bp)                                      AS low,
    MAX(CASE WHEN rn_last = 1 THEN bp END)       AS "close",
    SUM(bs)                                      AS volume,
    SUM(bp * bs) / NULLIF(SUM(bs), 0)            AS vwap,
    STDDEV_POP(bp)                               AS volatility,
    AVG(ap - bp)                                 AS avg_spread,
    COUNT(*)                                     AS ticks
FROM ranked
GROUP BY hour
ORDER BY hour;

-- -----------------------------------------------------------------------------
-- Q2. RISK & LIQUIDITY PROFILE  ("B7")
-- Single-pass raw moments (n, Σx, Σx², Σx³, Σx⁴) -> population skew + excess kurtosis in the
-- outer SELECT. spread = ap-bp, mid = (bp+ap)/2, depth = bs+as.
-- -----------------------------------------------------------------------------
WITH s AS (
    SELECT (ap - bp)          AS spread,
           (bp + ap) / 2.0    AS mid,
           bs                 AS bs,
           "as"               AS az
    FROM quotes_typed
    WHERE sym = 'AAPL'
),
agg AS (
    SELECT COUNT(*)::float8                                        AS n,
           SUM(spread)                                            AS s1,
           SUM(POWER(spread, 2))                                  AS s2,
           SUM(POWER(spread, 3))                                  AS s3,
           SUM(POWER(spread, 4))                                  AS s4,
           AVG(mid)                                               AS avg_mid,
           STDDEV_POP(mid)                                        AS mid_volatility,
           STDDEV_POP(spread)                                     AS spread_volatility,
           MAX(spread)                                            AS max_spread,
           -- Pearson corr without CORR()/COVAR_POP():
           (AVG(spread * (bs + az)::float8) - AVG(spread) * AVG((bs + az)::float8))
               / NULLIF(STDDEV_POP(spread) * STDDEV_POP((bs + az)::float8), 0) AS corr_spread_depth,
           AVG((bs - az)::float8 / NULLIF(bs + az, 0))            AS avg_book_imbalance,
           APPROXIMATE PERCENTILE_DISC(0.95) WITHIN GROUP (ORDER BY spread) AS spread_p95,
           APPROXIMATE PERCENTILE_DISC(0.99) WITHIN GROUP (ORDER BY spread) AS spread_p99
    FROM s
)
SELECT
    n                                                             AS ticks,
    avg_mid,
    mid_volatility,
    s1 / n                                                        AS avg_spread,
    spread_volatility,
    ((s3 / n) - 3 * (s1 / n) * (s2 / n) + 2 * POWER(s1 / n, 3))
        / NULLIF(POWER((s2 / n) - POWER(s1 / n, 2), 1.5), 0)      AS spread_skew,
    ((s4 / n) - 4 * (s1 / n) * (s3 / n) + 6 * POWER(s1 / n, 2) * (s2 / n) - 3 * POWER(s1 / n, 4))
        / NULLIF(POWER((s2 / n) - POWER(s1 / n, 2), 2), 0) - 3    AS spread_kurtosis,
    max_spread,
    corr_spread_depth,
    avg_book_imbalance,
    spread_p95,
    spread_p99
FROM agg;
