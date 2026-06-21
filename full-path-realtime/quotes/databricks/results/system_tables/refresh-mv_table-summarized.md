```sql
select sum(dbus)
FROM (

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
ORDER BY usage_date DESC
);
```

| sum(dbus) |
|---:|
| 110.905102863333333513 |
