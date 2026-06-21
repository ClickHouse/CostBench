```sql
SELECT
    formatReadableQuantity(sum(rows)) AS rows,
    formatReadableSize(sum(data_uncompressed_bytes)) AS data_size_uncompressed,
    formatReadableSize(sum(data_compressed_bytes)) AS data_size_compressed,
    formatReadableSize(sum(bytes_on_disk)) AS total_size_on_disk
FROM system.parts
WHERE active AND (database = 'test1') AND (`table` = 'quotes_daily')
```

```
   ┌─rows─────────┬─data_size_uncompressed─┬─data_size_compressed─┬─total_size_on_disk─┐
1. │ 1.88 million │ 168.53 MiB             │ 58.06 MiB            │ 58.07 MiB          │
   └──────────────┴────────────────────────┴──────────────────────┴────────────────────┘
```



```sql
SELECT
    sum(rows) AS rows,
    sum(data_uncompressed_bytes) AS data_size_uncompressed,
    sum(data_compressed_bytes) AS data_size_compressed,
    sum(bytes_on_disk) AS total_size_on_disk
FROM system.parts
WHERE active AND (database = 'test1') AND (`table` = 'quotes_daily')
```

```
   ┌────rows─┬─data_size_uncompressed─┬─data_size_compressed─┬─total_size_on_disk─┐
1. │ 1884551 │              176716867 │             60879741 │           60894528 │
   └─────────┴────────────────────────┴──────────────────────┴────────────────────┘
```
