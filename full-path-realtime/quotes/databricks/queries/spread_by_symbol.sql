SELECT
    sym,
    avg(ap - bp) AS avg_spread,
    min(ap - bp) AS min_spread,
    max(ap - bp) AS max_spread,
    count(*)     AS quote_count
FROM workspace.benchmarking.quotes
GROUP BY sym
ORDER BY avg_spread DESC
LIMIT 50;
