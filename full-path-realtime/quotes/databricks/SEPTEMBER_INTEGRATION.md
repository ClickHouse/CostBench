# September full-run CostBench integration

**PR #42 is fully integrated for the accepted real-time benchmark window.**

Only `results/serverless_baseline_full_20260918T170453Z` is in scope. This is
Serverless SQL X-Small, one cluster, AWS Ireland (`eu-west-1`), Premium public
pricing, with Zerobus Arrow ingestion. June, qualification, smaller runs, and
future Lakehouse//RT comparisons are excluded.

## Evidence and matching

All 34 artifacts in the PR #42 package pass their recorded SHA-256 and size
checks. The raw package is unchanged. It contains 113,219,565,734 completed
rows, with exact row reconciliation and no duplicate surplus or provider errors.

| Workload | Accepted observations | Query jobs per system | Comparison horizon |
|---|---:|---:|---:|
| Dashboard | 189 | 756 | 112,848,979,521 rows |
| Drill-down | 32 | 64 | 111,648,774,193 rows |

These remain the supplied validator's accepted prefixes. The first full-row
observations and subsequent warm observations remain outside these prefixes.
The 100B chart display cap does not change matching or costs.

ClickHouse files are under
`../clickhouse-cloud/results_t2/matched/databricks_serverless_20260918/`.
Each workload retains its matched JSONL, `.match.json` report, and empty count
marker. No matching, query-summary calculation, latency rendering, or freshness
rendering was repeated for PR #42. Maximum progress gaps remain 0.25735% for
dashboard and 1.45086% for drill-down. Databricks progress is client durability
progress; matching does not establish identical query-visible rows or MV freshness.

## Evidence roles

Paths in this table are relative to this provider directory. `RUN` means
`results/serverless_baseline_full_20260918T170453Z`.

| Role | Source |
|---|---|
| Table definitions and workload semantics | `create_full_serverless_baseline_r3_20260918.sql`, `queries_mv.sql`, `queries_raw.sql` |
| Run resources, configuration, timing, dataset | `RUN/run_context.json`, `RUN/ingest/ingest_summary.json` |
| Accepted selections and validation | `RUN/validation/validation_report_settled_20260921T092624Z.json` |
| Raw query observations | `RUN/mv/dashboard_20260918T170453Z.jsonl`, `RUN/raw/drilldown_20260918T170453Z.jsonl` |
| Freshness measurements | `RUN/freshness/mv_freshness_20260918T170453Z.jsonl` |
| Zerobus allocated DBUs | `RUN/ingest/zerobus_ingest_allocation.csv` |
| MV-refresh allocated DBUs and effective list-price cross-check | `RUN/freshness/mv_refresh_allocation.csv` |
| Predictive Optimization operation estimates | `RUN/ingest/predictive_optimization_allocation.csv` |
| Allocation method | `export_allocation_details.sql` |
| Package integrity | `RUN/validation/manifest.json` |
| Owner-defined real-time cost boundary | `costs/scopes/serverless_20260918.json` |
| Checked-in prices | `costs/pricings/zerobus_ingest.json`, `serverless_maintenance.json`, `sql_serverless_compute.json` |
| Accepted matching and derived provenance | `RUN/integration/manifest.json`, `derived_manifest.json`, `claims.csv` |
| Cost chart inputs | `costs/out/serverless_20260918/{fresh_data_path,full_path}.json` |

## Cost reconciliation

The supplied usage window is **2026-09-18 17:04:53.000 UTC through
2026-09-20 00:33:42.627 UTC**, with an exclusive end at producer completion.

| Component | Quantity / basis | USD |
|---|---|---:|
| Zerobus | 190 records; 1,098.926116716623 allocated DBUs × $0.39 | 428.58118552 |
| MV refresh | 2,958 records; 532.814506420162 allocated DBUs × $0.39 | 207.79765750 |
| Predictive Optimization | 14 operations; 151.861301053869 estimated DBUs × $0.39 | 59.22590741 |
| **Fresh-data-path cost** | All three preparation components | **695.60475043** |
| Dashboard query cost | Accepted runtime × 6 DBU/hour × $0.91/DBU ÷ 3600 | 0.92996540 |
| Drill-down query cost | Same normalization | 1.71838637 |
| **Full-path cost** | Preparation plus matched queries | **698.25310220** |

Zerobus DBUs replace the earlier $461.24545239 committed-byte proxy. The
$32.66426687 reduction is a change of measured input, not a price change.
Decimal/binary GB sensitivity no longer affects the primary September result.
The original byte evidence and checked-in volume-price fields remain available.

The offline parser checks package hashes, run resources, interval overlap,
SKU, signed corrections, duplicate record IDs, and checked-in prices. It sums
`allocated_dbu` once, without applying the overlap fraction again. All 1,044 MV
rows without table metadata are retained through verified pipeline attribution.
Their omission would undercount MV usage. Exported MV prices and costs reconcile
with the checked-in $0.39 rate. Predictive Optimization remains an estimate;
its CSV is counted once and reconciles with the compact JSON summary.

## Accepted real-time comparison scope

The benchmark owner defines this comparison around active ingestion and the
accepted concurrent queries. Preparation costs include ingestion, MV refresh,
and layout maintenance within the supplied allocation window, ending at producer
completion. Query costs retain the 189 dashboard and 32 drill-down observations.
Post-ingestion work and warm-window queries are outside the comparison.

The scope is recorded in `costs/scopes/serverless_20260918.json`, validated against
the run context and all three allocation inputs, and hash-tracked in summaries.
All required components are present: `total_cost_usd` is numeric,
`missing_components` is empty, and the complete cost-performance score is
**752.491424912947×** the matched ClickHouse baseline. This editorial score is
`(preparation cost + query cost) × query runtime`, normalized to the pair's
ClickHouse result. No additional run or usage export is required.

## Reproduce this allocation integration

```bash
cd "$(git rev-parse --show-toplevel)/full-path-realtime"
# Install uv and make it available on PATH before running these commands.
ALLOCATIONS_ONLY=1 bash quotes/databricks/costs/_commands_september.txt
COSTS_ONLY=1 bash quotes/databricks/visualizations/_commands.txt
COSTS_ONLY=1 bash quotes/global/visualizations/_commands.txt
uv run --python 3.12 python -m unittest discover \
  -s quotes/databricks/tests -p 'test_*.py'
```

`ALLOCATIONS_ONLY=1` verifies the new package and unchanged accepted inputs,
then calls these existing wrappers in order, using their full paths in the
notebook:

```bash
bash costs/summarize_zerobus.sh \
  "$RUN/ingest/zerobus_ingest_allocation.csv" \
  costs/pricings/zerobus_ingest.json "$OUT/zerobus.json"
bash costs/summarize_clustering.sh \
  "$RUN/ingest/predictive_optimization_allocation.csv" \
  costs/pricings/serverless_maintenance.json "$OUT/clustering.json"
bash costs/summarize_mv_refresh.sh \
  "$RUN/freshness/mv_refresh_allocation.csv" \
  costs/pricings/serverless_maintenance.json "$OUT/mv_refresh.json"
python3 costs/summarize_components.py assemble "$OUT"
uv run --python 3.12 python integrate_september.py finalize
```

The wrapper-level example assumes the provider directory as working directory,
`RUN=$PWD/results/serverless_baseline_full_20260918T170453Z`, and
`OUT=$PWD/costs/out/serverless_20260918`. Use the first notebook command for a
self-contained invocation. Without the costs-only switches, notebooks reproduce
the full integration, including the accepted 189/32 matching and query summaries.

Outputs are under:

- `costs/out/serverless_20260918/` for component and combined summaries;
- `RUN/integration/` for accepted provenance, claims, and readiness;
- `RUN/charts/` for provider-pair charts;
- `../global/results/charts/` for global charts.

Cost charts are regenerated in standard and wide layouts from the same numbers.
Provider-pair and global cost charts include the complete accepted Databricks
result. Query-latency charts and accepted matching remain unchanged. Global lag
still does not substitute refresh age for a comparable raw-to-MV time series.

Cost scope excludes producer VM, object-storage capacity/requests, network,
control SQL, query-warehouse idle/minimum charges, and warm-window queries.
Prices are public-list inputs; normalized query cost and estimated optimization
must not be described as an invoice total.
