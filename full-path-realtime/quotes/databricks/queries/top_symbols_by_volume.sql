SELECT
    sym,
    count(*) AS quote_count
FROM workspace.benchmarking.quotes
GROUP BY sym
ORDER BY quote_count DESC
LIMIT 50;
