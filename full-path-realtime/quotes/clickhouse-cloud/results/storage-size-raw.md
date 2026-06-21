```sql
SELECT
    formatReadableQuantity(sum(rows)) AS rows,
    formatReadableSize(sum(data_uncompressed_bytes)) AS data_size_uncompressed,
    formatReadableSize(sum(data_compressed_bytes)) AS data_size_compressed,
    formatReadableSize(sum(bytes_on_disk)) AS total_size_on_disk
FROM system.parts
WHERE active AND (database = 'test1') AND (`table` = 'quotes')
```

```
   ┌─rows───────────┬─data_size_uncompressed─┬─data_size_compressed─┬─total_size_on_disk─┐
1. │ 113.22 billion │ 8.08 TiB               │ 361.43 GiB           │ 361.83 GiB         │
   └────────────────┴────────────────────────┴──────────────────────┴────────────────────┘
```



```sql
SELECT
    sum(rows) AS rows,
    sum(data_uncompressed_bytes) AS data_size_uncompressed,
    sum(data_compressed_bytes) AS data_size_compressed,
    sum(bytes_on_disk) AS total_size_on_disk
FROM system.parts
WHERE active AND (database = 'test1') AND (`table` = 'quotes')
```

```
   ┌─────────rows─┬─data_size_uncompressed─┬─data_size_compressed─┬─total_size_on_disk─┐
1. │ 113219565734 │          8886105675750 │         388085802880 │       388514215808 │ 
   └──────────────┴────────────────────────┴──────────────────────┴────────────────────┘
```