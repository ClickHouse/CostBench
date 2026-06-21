-- =============================================================================
-- DRILLDOWN — one query, run ~hourly against the raw QUOTES_IT interactive table.
-- Filter by sym only (no time filter) so query cost scales with the per-sym
-- data volume, exercising the CLUSTER BY (sym) on the base table.
--
-- CH -> Snowflake idiom mapping:
--   count()                     -> COUNT(*)
--   fromUnixTimestamp64Milli(x) -> TO_TIMESTAMP_NTZ(x, 3)   (scale 3 = millis)
--   uniq(x)                     -> APPROX_COUNT_DISTINCT(x)
--   avg(ap - bp)                -> AVG(ap - bp)
--   `as`                        -> "AS"  (reserved word, double-quoted)
-- =============================================================================

SELECT
    COUNT(*)                          AS total_ticks,
    TO_TIMESTAMP_NTZ(MIN(t), 3)       AS first_tick,
    TO_TIMESTAMP_NTZ(MAX(t), 3)       AS last_tick,

    MIN(bp)                           AS lowest_bid,
    MAX(bp)                           AS highest_bid,
    MIN(ap)                           AS lowest_ask,
    MAX(ap)                           AS highest_ask,

    AVG(ap - bp)                      AS avg_spread,
    SUM(bs)                           AS total_bid_volume,
    SUM("AS")                         AS total_ask_volume,

    APPROX_COUNT_DISTINCT(bx)         AS unique_bid_exchanges,
    APPROX_COUNT_DISTINCT(ax)         AS unique_ask_exchanges
FROM QUOTES_IT
WHERE sym = 'AAPL';
