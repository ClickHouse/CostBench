-- =============================================================================
-- DRILLDOWN — SUPER suite. Runs against the LIVE streaming MV `quotes_streamed`, navigating and
-- casting the semi-structured `payload` (SUPER) on every access.
--
-- PAIRED FILE: queries_drilldown_typed.sql runs the SAME logic against the typed columns in
-- quotes_typed. The two files must stay logically equivalent — the whole point is to measure the
-- cost of semi-structured vs typed access on identical data and logic. Mirror any change.
--
-- The ONLY differences from the typed suite are how the MEASURE columns are read:
--     payload.bp::float8   vs   bp
--     payload."as"::bigint vs   "as"      ("as" is a reserved word -> quoted in both)
--
-- The FILTER is identical in both suites (`WHERE sym = 'AAPL'`) and deliberately uses the typed
-- `sym` column, which quotes_streamed promotes out of the payload and carries as the leading
-- SORTKEY column — the same physical ordering as quotes_typed. That holds layout CONSTANT across
-- the two suites, so the measured difference is the cost of SUPER navigation + casting per field,
-- not a pruning artefact. (Filtering on `payload.sym::varchar` instead would skip the zone maps
-- and conflate the two effects.)
--
-- Redshift substitutions are identical to the typed suite (no MIN_BY/MAX_BY, no SKEW/KURTOSIS,
-- no CORR/COVAR_POP, APPROXIMATE PERCENTILE_DISC, float8 casts on integer ratios) — see the
-- header of queries_drilldown_typed.sql for the full mapping.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Q1. HOURLY OHLCV BARS
-- -----------------------------------------------------------------------------
WITH src AS (
    SELECT DATE_TRUNC('hour', TIMESTAMP 'epoch' + payload.t::bigint / 1000 * INTERVAL '1 second') AS hour,
           payload.bp::float8 AS bp,
           payload.bs::bigint AS bs,
           payload.ap::float8 AS ap,
           payload.t::bigint  AS t
    FROM quotes_streamed
    WHERE sym = 'AAPL'   -- typed, leading SORTKEY column: same pruning as the typed suite
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
    SELECT (payload.ap::float8 - payload.bp::float8)      AS spread,
           (payload.bp::float8 + payload.ap::float8) / 2.0 AS mid,
           payload.bs::bigint                             AS bs,
           payload."as"::bigint                           AS az
    FROM quotes_streamed
    WHERE sym = 'AAPL'   -- typed, leading SORTKEY column: same pruning as the typed suite
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
