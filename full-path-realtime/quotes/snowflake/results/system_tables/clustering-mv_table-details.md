```sql
SELECT *
FROM snowflake.account_usage.automatic_clustering_history
WHERE database_name = 'BENCH2COST'
  AND schema_name   = 'STOCKHOUSE'
  AND table_name    = 'QUOTES_DAILY'
ORDER BY start_time DESC;
```
| START_TIME | END_TIME | CREDITS_USED | NUM_BYTES_RECLUSTERED | NUM_ROWS_RECLUSTERED | TABLE_ID | TABLE_NAME | SCHEMA_ID | SCHEMA_NAME | DATABASE_ID | DATABASE_NAME |
|---|---|---:|---:|---:|---:|---|---:|---|---:|---|
| 2026-06-09 08:00:00.000 -0700 | 2026-06-09 09:00:00.000 -0700 | 0.000032222 | 0 | 0 | 119886 | QUOTES_DAILY | 1859 | STOCKHOUSE | 70 | BENCH2COST |