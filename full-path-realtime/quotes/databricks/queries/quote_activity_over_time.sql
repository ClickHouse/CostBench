SELECT
    date_trunc('hour', timestamp_micros(t)) AS hour,
    count(*)                                AS quote_count
FROM workspace.benchmarking.quotes
GROUP BY hour
ORDER BY hour;
