-- Dashboard queries: same order and logical work as ClickHouse T2.
-- The runner replaces __PROJECT_ID__ and __DATASET_ID__ and disables cache.

-- 1. Single-symbol all-time summary.
SELECT
    COUNT(*)                           AS days_traded,
    SUM(n_quotes)                      AS total_quotes,
    MIN(bp_min)                        AS lowest_bid,
    MAX(bp_max)                        AS highest_bid,
    MIN(ap_min)                        AS lowest_ask,
    MAX(ap_max)                        AS highest_ask,
    SAFE_DIVIDE(SUM(spread_sum), SUM(n_quotes)) AS avg_spread,
    SUM(bs_sum)                        AS total_bid_volume,
    SUM(as_sum)                        AS total_ask_volume
FROM `__PROJECT_ID__.__DATASET_ID__.quotes_daily`
WHERE sym = 'AAPL';

-- 2. Watchlist all-time summary.
SELECT
    sym,
    COUNT(*)                           AS days_traded,
    SUM(n_quotes)                      AS total_quotes,
    MIN(bp_min)                        AS lowest_bid,
    MAX(bp_max)                        AS highest_bid,
    SAFE_DIVIDE(SUM(spread_sum), SUM(n_quotes)) AS avg_spread
FROM `__PROJECT_ID__.__DATASET_ID__.quotes_daily`
WHERE sym IN ('AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'META', 'NVDA', 'NFLX')
GROUP BY sym
ORDER BY total_quotes DESC;

-- 3. Top historical movers: full MV scan.
SELECT
    sym,
    SAFE_DIVIDE(MAX(bp_max) - MIN(bp_min), MIN(bp_min)) * 100 AS pct_range
FROM `__PROJECT_ID__.__DATASET_ID__.quotes_daily`
GROUP BY sym
ORDER BY ABS(pct_range) DESC
LIMIT 20;

-- 4. Daily market activity: full MV scan.
SELECT
    day,
    SUM(n_quotes)                      AS total_quotes,
    SUM(bs_sum) + SUM(as_sum)          AS total_volume,
    SAFE_DIVIDE(SUM(spread_sum), SUM(n_quotes)) AS avg_spread
FROM `__PROJECT_ID__.__DATASET_ID__.quotes_daily`
GROUP BY day
ORDER BY day;
