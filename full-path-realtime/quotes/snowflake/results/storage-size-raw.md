Raw table storage (QUOTES). Snowflake stores data compressed and does not expose an
uncompressed size. `bytes` (= `ACTIVE_BYTES`) is the live compressed on-disk size, comparable
to ClickHouse `data_size_compressed` / `total_size_on_disk`.

```sql
SHOW TABLES LIKE 'QUOTES' IN SCHEMA BENCH2COST.STOCKHOUSE;
```

| name | rows | bytes | cluster_by |
|---|---|---|---|
| QUOTES | 105,546,024,533 | 626,411,195,904 | LINEAR(sym, t) |

Readable: **105.55 billion rows**, **583.39 GiB** on disk (compressed).

Storage breakdown — active vs time-travel vs fail-safe (additional retained storage Snowflake
keeps that ClickHouse does not carry by default):

```sql
SELECT TABLE_NAME, ACTIVE_BYTES, TIME_TRAVEL_BYTES, FAILSAFE_BYTES
FROM BENCH2COST.INFORMATION_SCHEMA.TABLE_STORAGE_METRICS
WHERE TABLE_SCHEMA = 'STOCKHOUSE' AND TABLE_NAME = 'QUOTES';
```

| metric | bytes | readable |
|---|---|---|
| ACTIVE (on-disk, compressed) | 626,411,195,904 | 583.39 GiB |
| TIME_TRAVEL | 1,165,973,184,000 | 1.06 TiB |
| FAILSAFE | 845,100,370,944 | 787.06 GiB |

For reference, ClickHouse on the same dataset: 113.22 billion rows, 361.43 GiB compressed on
disk (`quotes/clickhouse-cloud/results/storage-size-raw.md`) — i.e. Snowflake's active table is
~1.6× larger on disk (~5.9 vs ~3.4 bytes/row), before time-travel and fail-safe.
