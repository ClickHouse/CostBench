# BigQuery benchmark visualizations

## ClickHouse versus BigQuery MV lag

`plot_mv_lag.py` reads BigQuery's per-minute freshness JSONL and the matching
ingest summary. It plots BigQuery's measured `watermark_lag_sec` against the
acknowledged base-table row count and adds ClickHouse's incremental MV as a
synthetic zero-second baseline.

The active-ingestion series includes the first freshness observation at the
complete ingest row count and excludes all later observations at that unchanged
row count. The publication chart plots one actual maximum-lag observation per
BigQuery refresh-watermark cycle. This removes the dense minute-level sawtooth
without smoothing or inventing intermediate values. No row-count interpolation
is applied.

Run `_commands.txt` to produce both the standard and true slide-wide variants.
Set `CHART_BACKGROUND_COLOR` once at the top of that file (the default is the
slide background `#161614`). The wide renderers expand their plots and type,
rather than centering the narrow chart on a larger canvas.

Run individual renderers with `uv` so the declared Matplotlib dependency is
isolated:

```bash
uv run visualizations/plot_mv_lag.py \
  --freshness results/bq-full-t2-20260810_152224/freshness/mv_freshness_20260810T152442Z.jsonl \
  --ingest-summary results/bq-full-t2-20260810_152224/ingest/ingest_summary.json \
  --output-dir results/bq-full-t2-20260810_152224/charts
```

Use `--aggregation rolling-mean --smooth-window 31` for a longer-wave trend, or
`--aggregation raw` for every one-minute observation.

The command writes PNG and SVG charts, the exact plotted observations as CSV,
and a JSON provenance summary into the selected output directory.

## ClickHouse versus BigQuery aggregate-query latency

`plot_aggregate_query_latency.py` renders the four dashboard queries in their
SQL-file order. Each system is plotted at its own observed base-table row
counts on a shared linear axis; iteration numbers are not joined. The default
publication window is `0 < raw_rows <= 100 billion`, and the exact unsmoothed
runner timings are shown.

```bash
uv run visualizations/plot_aggregate_query_latency.py \
  --clickhouse ../clickhouse-cloud/results_t2/mv/dashboard_20260808T065559Z.jsonl \
  --bigquery results/bq-full-t2-20260810_152224/mv/dashboard_20260810T152439Z.jsonl \
  --output-dir results/bq-full-t2-20260810_152224/charts
```

The pairwise chart can also include a compact evidence strip below the plots.
It reports the progress-matched accumulated runtime and ClickHouse query cost,
plus BigQuery's capacity and on-demand costs as alternative models. Those two
BigQuery prices are disclosed side by side and are never added together.

The renderer verifies all four BigQuery jobs in every selected iteration have
no error, have `cache_hit=false`, and agree with the aligned `result` array. It
writes PNG and SVG charts, the exact plotted observations as CSV, and a JSON
provenance/statistics summary.

## ClickHouse versus BigQuery drill-down-query latency

`plot_drilldown_query_latency.py` renders the two raw-table drill-down queries
in their SQL-file order: Hourly OHLCV bars and Risk & liquidity (B7). It uses
the same methodology and visual grammar as the aggregate-query chart: exact
unsmoothed runner timings, each system at its own observed base-table row
counts, a linear axis, and a default publication window through 100 billion
rows.

```bash
uv run visualizations/plot_drilldown_query_latency.py \
  --clickhouse ../clickhouse-cloud/results_t2/raw/drilldown_20260808T065602Z.jsonl \
  --bigquery results/bq-full-t2-20260810_152224/raw/drilldown_20260810T152440Z.jsonl \
  --output-dir results/bq-full-t2-20260810_152224/charts
```

The renderer verifies both BigQuery jobs in every selected iteration have no
error, have `cache_hit=false`, and agree with the aligned `result` array. It
writes PNG and SVG charts, the exact plotted observations as CSV, and a JSON
provenance/statistics summary.

## Complete-ingest and fresh-path cost

`plot_ingest_fresh_path_cost.py` compares the cost of complete ingestion and
background query-ready maintenance. ClickHouse Enterprise is shown as one
bundled write-service cost. BigQuery is stacked from Storage Write API ingest
and automatic MV refresh using Enterprise capacity pricing; automatic
reclustering is disclosed as a zero separate charge. Storage, read queries, and
BigQuery query-time delta merges are outside this chart.

```bash
uv run visualizations/plot_ingest_fresh_path_cost.py \
  --clickhouse-cost ../clickhouse-cloud/costs/out_t2/ingest.json \
  --bigquery-ingest-cost costs/out/bq-full-t2-20260810_152224/ingest.json \
  --bigquery-mv-refresh-cost costs/out/bq-full-t2-20260810_152224/mv_refresh.json \
  --bigquery-serverless-pricing costs/pricings/serverless.json \
  --bigquery-write-api-pricing costs/pricings/storage_write_api.json \
  --tier Enterprise \
  --region us \
  --output-dir results/bq-full-t2-20260810_152224/charts
```

The renderer recomputes and validates both BigQuery costs against the supplied
pricing files and writes PNG, SVG, and JSON provenance outputs.

The former standalone accumulated-runtime/query-cost chart is deprecated. The
same evidence now appears directly below the aggregate and drill-down latency
plots, where viewers can connect the shape of the latency series to the matched
total runtime and cost without changing slides.

## Full-path cost-performance

`plot_full_path_cost_performance.py` combines complete fresh-data-path cost,
matched active-ingestion query cost, and accumulated query runtime into the
score `(fresh path cost + query cost) × accumulated query runtime`; lower is
better. ClickHouse Enterprise is the baseline. BigQuery capacity and on-demand
are shown as two alternative pricing models and are never added together.

```bash
uv run visualizations/plot_full_path_cost_performance.py \
  --clickhouse-ingest-cost ../clickhouse-cloud/costs/out_t2/ingest.json \
  --clickhouse-dashboard-cost ../clickhouse-cloud/costs/out_t2/matched/bigquery/dashboard.json \
  --clickhouse-drilldown-cost ../clickhouse-cloud/costs/out_t2/matched/bigquery/drilldown.json \
  --bigquery-ingest-cost costs/out/bq-full-t2-20260810_152224/ingest.json \
  --bigquery-mv-refresh-cost costs/out/bq-full-t2-20260810_152224/mv_refresh.json \
  --bigquery-dashboard-cost costs/out/bq-full-t2-20260810_152224/dashboard.json \
  --bigquery-drilldown-cost costs/out/bq-full-t2-20260810_152224/drilldown.json \
  --bigquery-serverless-pricing costs/pricings/serverless.json \
  --bigquery-write-api-pricing costs/pricings/storage_write_api.json \
  --tier Enterprise \
  --region us \
  --output-dir results/bq-full-t2-20260810_152224/charts
```

The renderer validates the input costs against the pricing files and writes
PNG, SVG, and JSON provenance outputs. The chart itself deliberately has no
overall title, formula subtitle, or footer so it can sit beneath article-level
copy.
