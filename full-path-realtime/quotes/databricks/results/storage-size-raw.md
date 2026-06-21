```sql
DESCRIBE DETAIL workspace.benchmarking.quotes;
```

```

   ┌─numFiles─┬─sizeInBytes──┬─size_compressed─┐
   │   14,201 │ 704753961457 │ 704.75 GB       │
   └──────────┴──────────────┴─────────────────┘

```

`sizeInBytes` is the compressed (zstd) size of the data files in the current
Delta snapshot — 704.75 GB / 656.35 GiB across 14,201 files. Excludes time-travel
versions, the `_delta_log`, and separately-stored Change Data Feed data.

Captured: 2026-06-11 · cluster key (sym, t)



```sql
SELECT COUNT(*) from workspace.benchmarking.quotes;
```

```
102948290040
```