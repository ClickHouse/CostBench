```sql
SELECT
    SUM(usage_quantity)
FROM system.storage.predictive_optimization_operations_history
WHERE catalog_name = 'workspace'
  AND schema_name = 'benchmarking'
  AND table_name = 'quotes'
```

| sum(usage_quantity) |
|---:|
| 143.066240827580113380 |