-- =============================================================================
-- Dashboard queries — run every ~10 minutes against the MV.
-- All queries filter by sym only or not at all; never by day. Cost scales
-- with the total data volume, so the benchmark answer changes as the dataset
-- grows. (sym, day) sort key lets sym-filtered queries do a prefix scan.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1. SINGLE-SYMBOL ALL-TIME SUMMARY
--    Filter by one sym. Uses (sym, day) sort as a single-sym prefix scan.
--    Cost grows with how much historical data we have for that symbol.
-- -----------------------------------------------------------------------------
SELECT
    count()                          AS days_traded,
    sum(n_quotes)                    AS total_quotes,
    min(bp_min)                      AS lowest_bid,
    max(bp_max)                      AS highest_bid,
    min(ap_min)                      AS lowest_ask,
    max(ap_max)                      AS highest_ask,
    sum(spread_sum) / sum(n_quotes)  AS avg_spread,
    sum(bs_sum)                      AS total_bid_volume,
    sum(as_sum)                      AS total_ask_volume
FROM quotes_daily
WHERE sym = 'AAPL';

-- -----------------------------------------------------------------------------
-- 2. WATCHLIST ALL-TIME SUMMARY
--    Small set of symbols, no day filter. Each sym is a prefix scan in
--    (sym, day) order. Cost scales with total data across those symbols.
-- -----------------------------------------------------------------------------
SELECT
    sym,
    count()                          AS days_traded,
    sum(n_quotes)                    AS total_quotes,
    min(bp_min)                      AS lowest_bid,
    max(bp_max)                      AS highest_bid,
    sum(spread_sum) / sum(n_quotes)  AS avg_spread
FROM quotes_daily
WHERE sym IN ('AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'META', 'NVDA', 'NFLX')
GROUP BY sym
ORDER BY total_quotes DESC;

-- -----------------------------------------------------------------------------
-- 3. TOP MOVERS HISTORICALLY — no filter, full MV scan
--    Per-symbol historical price range across the entire data window.
--    Cost scales with the full MV size.
-- -----------------------------------------------------------------------------
SELECT
    sym,
    (max(bp_max) - min(bp_min)) / min(bp_min) * 100 AS pct_range
FROM quotes_daily
GROUP BY sym
ORDER BY abs(pct_range) DESC
LIMIT 20;

-- -----------------------------------------------------------------------------
-- 4. DAILY MARKET ACTIVITY TIME SERIES — no filter, full MV scan
--    Market-wide totals per day. Cost scales with the full MV size.
-- -----------------------------------------------------------------------------
SELECT
    day,
    sum(n_quotes)                    AS total_quotes,
    sum(bs_sum) + sum(as_sum)        AS total_volume,
    sum(spread_sum) / sum(n_quotes)  AS avg_spread
FROM quotes_daily
GROUP BY day
ORDER BY day;
