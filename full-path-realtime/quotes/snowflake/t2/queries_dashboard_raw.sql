-- =============================================================================
-- Dashboard queries computed DIRECTLY FROM THE RAW interactive table QUOTES_IT
-- (no rollup). Same 4 dashboard questions as t2/queries_mv_imv.sql, but each one
-- aggregates raw ticks on the fly instead of reading the pre-aggregated
-- QUOTES_DAILY_IMV. Used by the T2 "dashboard vs raw" workloads (#3 interactive
-- warehouse, #4 standard warehouse) to compare against the MV-backed dashboard.
--
-- Column derivation (QUOTES_IT raw -> the MV's rollup columns):
--   day        = TO_DATE(TO_TIMESTAMP_NTZ(t, 3))     (t = epoch millis)
--   n_quotes   = COUNT(*)          spread_sum = SUM(ap - bp)
--   bp_min/max = MIN/MAX(bp)       ap_min/max = MIN/MAX(ap)
--   bs_sum     = SUM(bs)           as_sum     = SUM("AS")
-- Filters/grouping match the MV version exactly so results are comparable.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1. SINGLE-SYMBOL ALL-TIME SUMMARY  (WHERE sym = 'AAPL')
-- -----------------------------------------------------------------------------
SELECT
    COUNT(DISTINCT TO_DATE(TO_TIMESTAMP_NTZ(t, 3)))  AS days_traded,
    COUNT(*)                                         AS total_quotes,
    MIN(bp)                                          AS lowest_bid,
    MAX(bp)                                          AS highest_bid,
    MIN(ap)                                          AS lowest_ask,
    MAX(ap)                                          AS highest_ask,
    SUM(ap - bp) / COUNT(*)                          AS avg_spread,
    SUM(bs)                                          AS total_bid_volume,
    SUM("AS")                                        AS total_ask_volume
FROM QUOTES_IT
WHERE sym = 'AAPL';

-- -----------------------------------------------------------------------------
-- 2. WATCHLIST ALL-TIME SUMMARY  (WHERE sym IN (...), GROUP BY sym)
-- -----------------------------------------------------------------------------
SELECT
    sym,
    COUNT(DISTINCT TO_DATE(TO_TIMESTAMP_NTZ(t, 3)))  AS days_traded,
    COUNT(*)                                         AS total_quotes,
    MIN(bp)                                          AS lowest_bid,
    MAX(bp)                                          AS highest_bid,
    SUM(ap - bp) / COUNT(*)                          AS avg_spread
FROM QUOTES_IT
WHERE sym IN ('AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'META', 'NVDA', 'NFLX')
GROUP BY sym
ORDER BY total_quotes DESC;

-- -----------------------------------------------------------------------------
-- 3. TOP MOVERS HISTORICALLY — no filter, full raw scan
-- -----------------------------------------------------------------------------
SELECT
    sym,
    (MAX(bp) - MIN(bp)) / MIN(bp) * 100  AS pct_range
FROM QUOTES_IT
GROUP BY sym
ORDER BY ABS(pct_range) DESC
LIMIT 20;

-- -----------------------------------------------------------------------------
-- 4. DAILY MARKET ACTIVITY TIME SERIES — no filter, full raw scan
-- -----------------------------------------------------------------------------
SELECT
    TO_DATE(TO_TIMESTAMP_NTZ(t, 3))  AS day,
    COUNT(*)                         AS total_quotes,
    SUM(bs) + SUM("AS")              AS total_volume,
    SUM(ap - bp) / COUNT(*)          AS avg_spread
FROM QUOTES_IT
GROUP BY TO_DATE(TO_TIMESTAMP_NTZ(t, 3))
ORDER BY day;
