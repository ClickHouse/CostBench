Materialized-view storage (QUOTES_DAILY). Snowflake stores data compressed and does not expose
an uncompressed size. `bytes` (= `ACTIVE_BYTES`) is the live compressed on-disk size. Measured
after ingest stopped and the MV settled (`behind_by` 0s, compacted); it was ~910 MiB mid-ingest
per the lag tracker.

```sql
SHOW MATERIALIZED VIEWS LIKE 'QUOTES_DAILY' IN SCHEMA BENCH2COST.STOCKHOUSE;
```

| name | rows | bytes | behind_by |
|---|---|---|---|
| QUOTES_DAILY | 14,878,682 | 478,583,808 | 0s |

**Two different row numbers — read carefully:**

| measure | value | what it is |
|---|---|---|
| `SHOW ... rows` (metadata) | 14,878,682 | physical materialized rows on disk |
| `SELECT count(*)` | 1,719,669 | logical `(sym, day)` groups (merged at query time) |

A Snowflake MV is maintained incrementally — each maintenance increment *appends* new
partial-aggregate fragments for the `(sym, day)` groups touched by newly-ingested
micro-partitions instead of updating one row per group in place, and those fragments are only
consolidated by a background compaction. So the physical count (and the ~456 MiB on disk) carry
~8.6× fragmentation over the 1.72M logical groups. The **1.72M** figure is the one comparable to
ClickHouse. (The base table shows no such gap: `SHOW rows` == `count(*)` == 105,546,024,533,
since it has no aggregation.)

Readable: **1.72 million logical rows** (14.88M physical), **456.41 MiB** on disk (compressed).

Storage breakdown — active vs time-travel vs fail-safe. The query returns one row per physical
table version; the MV was `CREATE OR REPLACE`'d several times during the experiment, so only the
live version has active bytes — the other 8 rows are dropped versions still holding residual
fail-safe storage (it ages out over the fail-safe window).

```sql
SELECT TABLE_NAME, ACTIVE_BYTES, TIME_TRAVEL_BYTES, FAILSAFE_BYTES
FROM BENCH2COST.INFORMATION_SCHEMA.TABLE_STORAGE_METRICS
WHERE TABLE_SCHEMA = 'STOCKHOUSE' AND TABLE_NAME = 'QUOTES_DAILY';
```

| metric (live version) | bytes | readable |
|---|---|---|
| ACTIVE (on-disk, compressed) | 478,583,808 | 456.41 MiB |
| TIME_TRAVEL | 1,614,868,480 | 1.50 GiB |
| FAILSAFE | 1,011,780,608 | 964.91 MiB |

Dropped versions hold an additional ~686 MB of residual fail-safe (653,629,440 + 31,736,832 +
304,640 bytes) with 0 active bytes.

For reference, ClickHouse on the same dataset: 1.88 million rows, 58.06 MiB compressed on disk
(`quotes/clickhouse-cloud/results/storage-size-mv.md`). Comparing logical group counts (CH 1.88M
vs SF 1.72M) they are close — CH has slightly more because it ingested 113B rows vs Snowflake's
105.5B. On disk Snowflake is ~8× larger (456 MiB vs 58 MiB), driven by the un-compacted physical
fragments described above.
