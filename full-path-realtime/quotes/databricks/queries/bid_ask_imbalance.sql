SELECT
    sym,
    avg(bs - `as`) AS avg_size_imbalance,
    sum(bs)        AS total_bid_size,
    sum(`as`)      AS total_ask_size,
    sum(bs) - sum(`as`) AS net_imbalance
FROM workspace.benchmarking.quotes
GROUP BY sym
ORDER BY net_imbalance DESC
LIMIT 50;
