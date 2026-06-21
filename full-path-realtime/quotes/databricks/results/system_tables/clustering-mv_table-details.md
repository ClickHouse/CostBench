```sql
SELECT
    operation_type,
    start_time,
    end_time,
    operation_status,
    usage_unit,
    usage_quantity,
    operation_metrics
FROM system.storage.predictive_optimization_operations_history
WHERE catalog_name = 'workspace'
  AND schema_name = 'benchmarking'
  AND table_name = 'quotes_daily'
ORDER BY start_time DESC;
```

| operation_type | start_time | end_time | operation_status | usage_unit | usage_quantity | operation_metrics |
|---|---|---|---|---|---:|---|
| CLUSTERING | 2026-06-11T04:06:13.863+00:00 | 2026-06-11T04:06:33.327+00:00 | SUCCESSFUL | ESTIMATED_DBU | 0.163667408333333320 | `{"number_of_removed_files":"3","number_of_clustered_files":"1","amount_of_data_removed_bytes":"28403483",...}` |
| CLUSTERING | 2026-06-10T23:07:07.267+00:00 | 2026-06-10T23:07:29.837+00:00 | SUCCESSFUL | ESTIMATED_DBU | 0.177290715000000000 | `{"number_of_removed_files":"3","number_of_clustered_files":"1","amount_of_data_removed_bytes":"18715903",...}` |
| CLUSTERING | 2026-06-10T14:07:14.848+00:00 | 2026-06-10T14:07:37.700+00:00 | SUCCESSFUL | ESTIMATED_DBU | 0.182399945000000060 | `{"number_of_removed_files":"4","number_of_clustered_files":"1","amount_of_data_removed_bytes":"37826391",...}` |
| CLUSTERING | 2026-06-10T07:50:45.550+00:00 | 2026-06-10T07:50:54.311+00:00 | SUCCESSFUL | ESTIMATED_DBU | 0.040742372796265476 | `{"number_of_removed_files":"4","number_of_clustered_files":"1","amount_of_data_removed_bytes":"25348067",...}` |
| CLUSTERING | 2026-06-10T00:36:56.090+00:00 | 2026-06-10T00:37:24.137+00:00 | SUCCESSFUL | ESTIMATED_DBU | 0.174002388333333340 | `{"number_of_removed_files":"2","number_of_clustered_files":"1","amount_of_data_removed_bytes":"9980080",...}` |
| ANALYZE | 2026-06-09T21:04:13.947+00:00 | 2026-06-09T21:04:14.751+00:00 | SUCCESSFUL | ESTIMATED_DBU | 0.000852783969819985 | `{"amount_of_scanned_bytes":"2194959","number_of_scanned_files":"1","staleness_percentage_reduced":"100"}` |
| ANALYZE | 2026-06-09T16:26:47.433+00:00 | 2026-06-09T16:26:48.308+00:00 | SUCCESSFUL | ESTIMATED_DBU | 0.003291993784239359 | `{"amount_of_scanned_bytes":"3701191","number_of_scanned_files":"1","staleness_percentage_reduced":"100"}` |




