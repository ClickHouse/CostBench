-- =============================================================================
-- Redshift dashboard queries — run every ~10 min against the rollup MV `quotes_daily`
-- (the (sym, day) aggregate; Snowflake QUOTES_DAILY_IMV / ClickHouse quotes_daily equivalent).
-- Plain columns (n_quotes, bp_min/bp_max, ap_min/ap_max, bs_sum, as_sum, spread_sum), summed/
-- min/maxed directly. Standard SQL — ports 1:1 from ../snowflake/t2/queries_mv_imv.sql.
-- =============================================================================

-- 1. SINGLE-SYMBOL ALL-TIME SUMMARY
SELECT
    COUNT(*)                         AS days_traded,
    SUM(n_quotes)                    AS total_quotes,
    MIN(bp_min)                      AS lowest_bid,
    MAX(bp_max)                      AS highest_bid,
    MIN(ap_min)                      AS lowest_ask,
    MAX(ap_max)                      AS highest_ask,
    SUM(spread_sum) / SUM(n_quotes)  AS avg_spread,
    SUM(bs_sum)                      AS total_bid_volume,
    SUM(as_sum)                      AS total_ask_volume
FROM quotes_daily
WHERE sym = 'AAPL';

-- 2. WATCHLIST ALL-TIME SUMMARY
SELECT
    sym,
    COUNT(*)                         AS days_traded,
    SUM(n_quotes)                    AS total_quotes,
    MIN(bp_min)                      AS lowest_bid,
    MAX(bp_max)                      AS highest_bid,
    SUM(spread_sum) / SUM(n_quotes)  AS avg_spread
FROM quotes_daily
WHERE sym IN ('AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'META', 'NVDA', 'NFLX')
GROUP BY sym
ORDER BY total_quotes DESC;

-- 3. TOP MOVERS HISTORICALLY — full rollup scan
-- (wrap in a subquery: Redshift rejects a SELECT alias inside a function — ABS(pct_range) — in ORDER BY)
SELECT sym, pct_range FROM (
    SELECT
        sym,
        (MAX(bp_max) - MIN(bp_min)) / MIN(bp_min) * 100 AS pct_range
    FROM quotes_daily
    GROUP BY sym
) t
ORDER BY ABS(pct_range) DESC
LIMIT 20;

-- 4. DAILY MARKET ACTIVITY TIME SERIES — full rollup scan
SELECT
    day,
    SUM(n_quotes)                    AS total_quotes,
    SUM(bs_sum) + SUM(as_sum)        AS total_volume,
    SUM(spread_sum) / SUM(n_quotes)  AS avg_spread
FROM quotes_daily
GROUP BY day
ORDER BY day;
