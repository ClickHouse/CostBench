-- =============================================================================
-- Dashboard queries — run every ~10 minutes against the QUOTES_DAILY_IMV (interactive materialized view, streaming arch)
-- (Snowflake Dynamic Table; CH quotes_daily AggregatingMergeTree equivalent).
--
-- All queries filter by sym only or not at all; never by day. Cost scales with
-- total data volume, so the benchmark answer changes as the dataset grows.
-- CLUSTER BY (sym, day) lets sym-filtered queries prune to a sym prefix.
--
-- CH -> Snowflake idiom mapping: count() -> COUNT(*), abs() -> ABS().
-- The rollup columns are plain values (CH SimpleAggregateFunction merged), so
-- they are summed/min/maxed directly just like in ClickHouse.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1. SINGLE-SYMBOL ALL-TIME SUMMARY
--    Filter by one sym. Uses (sym, day) clustering as a single-sym prefix scan.
--    Cost grows with how much historical data we have for that symbol.
-- -----------------------------------------------------------------------------
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
FROM QUOTES_DAILY_IMV
WHERE sym = 'AAPL';

-- -----------------------------------------------------------------------------
-- 2. WATCHLIST ALL-TIME SUMMARY
--    Small set of symbols, no day filter. Each sym is a prefix scan in
--    (sym, day) order. Cost scales with total data across those symbols.
-- -----------------------------------------------------------------------------
SELECT
    sym,
    COUNT(*)                         AS days_traded,
    SUM(n_quotes)                    AS total_quotes,
    MIN(bp_min)                      AS lowest_bid,
    MAX(bp_max)                      AS highest_bid,
    SUM(spread_sum) / SUM(n_quotes)  AS avg_spread
FROM QUOTES_DAILY_IMV
WHERE sym IN ('AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'META', 'NVDA', 'NFLX')
GROUP BY sym
ORDER BY total_quotes DESC;

-- -----------------------------------------------------------------------------
-- 3. TOP MOVERS HISTORICALLY — no filter, full rollup scan
--    Per-symbol historical price range across the entire data window.
--    Cost scales with the full rollup size.
-- -----------------------------------------------------------------------------
SELECT
    sym,
    (MAX(bp_max) - MIN(bp_min)) / MIN(bp_min) * 100 AS pct_range
FROM QUOTES_DAILY_IMV
GROUP BY sym
ORDER BY ABS(pct_range) DESC
LIMIT 20;

-- -----------------------------------------------------------------------------
-- 4. DAILY MARKET ACTIVITY TIME SERIES — no filter, full rollup scan
--    Market-wide totals per day. Cost scales with the full rollup size.
-- -----------------------------------------------------------------------------
SELECT
    day,
    SUM(n_quotes)                    AS total_quotes,
    SUM(bs_sum) + SUM(as_sum)        AS total_volume,
    SUM(spread_sum) / SUM(n_quotes)  AS avg_spread
FROM QUOTES_DAILY_IMV
GROUP BY day
ORDER BY day;
