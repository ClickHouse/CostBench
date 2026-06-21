-- =============================================================================
-- DRILLDOWN — one query, run ~hourly against the raw quotes table.
-- Filter by sym only (no time filter) so query cost scales with the per-sym
-- data volume, exercising the (sym, t) sort key on the base table.
-- =============================================================================

SELECT
    count()                           AS total_ticks,
    fromUnixTimestamp64Milli(min(t))  AS first_tick,
    fromUnixTimestamp64Milli(max(t))  AS last_tick,

    min(bp)                           AS lowest_bid,
    max(bp)                           AS highest_bid,
    min(ap)                           AS lowest_ask,
    max(ap)                           AS highest_ask,

    avg(ap - bp)                      AS avg_spread,
    sum(bs)                           AS total_bid_volume,
    sum(`as`)                         AS total_ask_volume,

    uniq(bx)                          AS unique_bid_exchanges,
    uniq(ax)                          AS unique_ask_exchanges
FROM quotes
WHERE sym = 'AAPL';
