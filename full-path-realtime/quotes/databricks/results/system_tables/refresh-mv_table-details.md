```sql
SELECT
  usage_date,
  sku_name,
  SUM(usage_quantity)  AS dbus,
  usage_metadata.dlt_pipeline_id AS pipeline_id
FROM system.billing.usage
WHERE billing_origin_product = 'SQL'
 AND usage_metadata.dlt_pipeline_id = (SELECT origin.pipeline_id
FROM event_log(TABLE(workspace.benchmarking.quotes_daily))
LIMIT 1)
 AND usage_date >= current_date() - INTERVAL 7 DAYS
GROUP BY usage_date, sku_name, pipeline_id
ORDER BY usage_date DESC;
```

| usage_date | sku_name | dbus | pipeline_id |
|---|---|---:|---|
| 2026-06-11 | PREMIUM_JOBS_SERVERLESS_COMPUTE_EUROPE_IRELAND | 26.859913633333300000 | b346c423-5d99-48e2-87dc-e46d041101c1 |
| 2026-06-10 | PREMIUM_JOBS_SERVERLESS_COMPUTE_EUROPE_IRELAND | 72.489734660000000000 | b346c423-5d99-48e2-87dc-e46d041101c1 |
| 2026-06-09 | PREMIUM_JOBS_SERVERLESS_COMPUTE_EUROPE_IRELAND | 11.555454570000000000 | b346c423-5d99-48e2-87dc-e46d041101c1 |