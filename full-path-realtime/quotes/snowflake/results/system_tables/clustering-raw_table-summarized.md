```sql
SELECT SUM(CREDITS_USED)
FROM snowflake.account_usage.automatic_clustering_history
WHERE database_name = 'BENCH2COST'
  AND schema_name   = 'STOCKHOUSE'
  AND table_name    = 'QUOTES';

```
| SUM(CREDITS_USED) |
|---:|
| 54.124673266 |