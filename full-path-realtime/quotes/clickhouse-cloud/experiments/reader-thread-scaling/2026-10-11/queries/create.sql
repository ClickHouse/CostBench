-- =============================================================================
-- Base table: raw stock quotes
-- Sort key (sym, t) — the per-symbol drilldown query (run hourly) reads a
-- single sym's contiguous block; performance scales with that symbol's
-- data size, which is the point.
-- =============================================================================
CREATE TABLE IF NOT EXISTS quotes (
    sym         String,
    bx          UInt8,
    bp          Float64,
    bs          UInt64,
    ax          UInt8,
    ap          Float64,
    `as`        UInt64,
    c           UInt8,
    i           Array(UInt8),
    t           UInt64,
    q           UInt64,
    z           UInt8
)
ENGINE = MergeTree
ORDER BY (sym, t);

-- =============================================================================
-- MV target: per-symbol-per-day summary (AggregatingMergeTree)
--
-- Sort key (sym, day) — most dashboard queries are sym-centric (watchlist,
-- single-stock view, drilldown summary). The single-sym query has speed that
-- scales with the data size for that sym, exercising the clustering.
-- =============================================================================
CREATE TABLE IF NOT EXISTS quotes_daily (
    sym         String,
    day         Date,
    n_quotes    SimpleAggregateFunction(sum, UInt64),
    bp_min      SimpleAggregateFunction(min, Float64),
    bp_max      SimpleAggregateFunction(max, Float64),
    ap_min      SimpleAggregateFunction(min, Float64),
    ap_max      SimpleAggregateFunction(max, Float64),
    bs_sum      SimpleAggregateFunction(sum, UInt64),
    as_sum      SimpleAggregateFunction(sum, UInt64),
    spread_sum  SimpleAggregateFunction(sum, Float64)
)
ENGINE = AggregatingMergeTree
ORDER BY (sym, day);

-- =============================================================================
-- MV: aggregates every INSERT into `quotes` into per-symbol-per-day summaries
--
-- NOTE: CREATE MATERIALIZED VIEW IF NOT EXISTS does NOT alter an existing
-- view's SELECT. If you change the schema, drop both `quotes_daily_mv` and
-- `quotes_daily` first, then re-run this file.
-- =============================================================================
CREATE MATERIALIZED VIEW IF NOT EXISTS quotes_daily_mv TO quotes_daily AS
SELECT
    sym,
    toDate(toDateTime(intDiv(t, 1000))) AS day,

    toUInt64(count())  AS n_quotes,
    min(bp)            AS bp_min,
    max(bp)            AS bp_max,
    min(ap)            AS ap_min,
    max(ap)            AS ap_max,
    sum(bs)            AS bs_sum,
    sum(`as`)          AS as_sum,
    sum(ap - bp)       AS spread_sum
FROM quotes
GROUP BY sym, day;
