```sql
-- DESCRIBE DETAIL rejects an MV ("expects a table but ... is a view") and the
-- MV's Unity Catalog managed path can't be read directly (LOCATION_OVERLAP).
-- Read "Total Size (bytes)" from the # Detailed Table Information section.
DESCRIBE EXTENDED workspace.benchmarking.quotes_daily;
```

```

   ┌─Total Size (bytes)─┬─size_compressed─┐
   │           74044112 │ 74.04 MB        │
   └────────────────────┴─────────────────┘

```

The `quotes_daily` materialized view (one row per sym/day) is 74.04 MB /
70.61 MiB — 0.011% of the base table's footprint. Refreshes incrementally
(`enzymeMode=Advanced`, Predictive Optimization enabled). `numFiles` is not
reported by `DESCRIBE EXTENDED` for an MV.

Captured: 2026-06-11 · cluster key (sym, day) · pipeline b346c423-5d99-48e2-87dc-e46d041101c1




```sql
SELECT COUNT(*) from workspace.benchmarking.quotes_daily;
```

```
1679553
```
