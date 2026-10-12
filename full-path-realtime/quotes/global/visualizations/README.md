# Global real-time benchmark visualizations

This directory combines the accepted ClickHouse Cloud, Snowflake, BigQuery, Redshift Serverless, and Databricks Serverless SQL results. Start with the [quotes methodology and evidence map](../../README.md) or the [Part 6 measurement index](../../part6-measurements/README.md).

## Results and evidence

| Measurement | Downloadable values | Calculations and source paths |
| --- | --- | --- |
| Aggregate-query latency | [CSV](../results/charts/aggregate_query_latency_all_systems_data.csv) | [Summary](../results/charts/aggregate_query_latency_all_systems_summary.json) |
| Drill-down query latency | [CSV](../results/charts/drilldown_query_latency_all_systems_data.csv) | [Summary](../results/charts/drilldown_query_latency_all_systems_summary.json) |
| Pre-aggregation lag | [Observations](../results/charts/mv_lag_all_systems_data.csv), [display curve](../results/charts/mv_lag_all_systems_curve.csv) | [Summary](../results/charts/mv_lag_all_systems_summary.json) |
| Complete preparation cost | [CSV](../results/charts/ingest_fresh_path_cost_all_systems_data.csv) | [Summary](../results/charts/ingest_fresh_path_cost_all_systems_summary.json) |
| End-to-end performance per dollar | [CSV](../results/charts/full_path_cost_performance_all_systems_data.csv) | [Summary](../results/charts/full_path_cost_performance_all_systems_summary.json) |
| Total cost versus accumulated query runtime | [CSV](../results/charts/full_path_cost_vs_query_runtime_all_systems_data.csv) | [Summary](../results/charts/full_path_cost_vs_query_runtime_all_systems_summary.json) |

[manifest.json](manifest.json) identifies the accepted provider inputs and required labels for each chart. It includes Databricks' September full-run integration, its accepted query observations, and its preparation and full-path summaries. Provider guides retain the matching and pricing details.

## Measurement definitions

**Query latency.** Charts read the original runner observations and plot each provider at its own observed row progress. The presentation cap is inclusive at 100 billion rows. Aggregate lines use a provider-local centered seven-observation median; drill-down lines use five observations. Snowflake's aggregate series retains its accepted pairwise Tukey upper-fence policy. No new outlier policy is applied to the other providers or workloads.

**Pre-aggregation lag.** The common chart measures the lag between incoming raw data and maintained summaries. ClickHouse's baseline is zero because its incremental materialized view processes the same inserted blocks as its raw table. BigQuery's default direct-query reconciliation produces current answers separately from its summary-refresh lag. Databricks records refresh age and a row-watermark gap rather than the same time-lag measure; the manifest keeps it out of this common lag chart and points to the run-specific [freshness evidence](../../databricks/SEPTEMBER_INTEGRATION.md).

**Preparation cost.** Complete-ingest values come from accepted Snowflake, BigQuery, Redshift, and Databricks summaries. ClickHouse source identities are checked before combining them. BigQuery Capacity and On-demand are alternative prices. Each Redshift SUPER or typed read-layout comparison includes the shared writer-plus-MSK path once.

**End-to-end performance per dollar.** Each provider or pricing/layout alternative is normalized to ClickHouse within its own accepted pairwise query window. The score is `(preparation cost + matched query cost) × accumulated query runtime`; lower is better. The score chart uses `log10(relative score)`, with ClickHouse's 1× reference at the origin. The global chart combines accepted pairwise ratios.

**Cost versus runtime.** The map reads the accepted total cost and accumulated runtime from those same summaries. The axes are logarithmic and inverted: faster is right and lower cost is up. Each provider retains its accepted query window; the source summary records the inputs. Query cost uses the accepted normalized allocation.

## Reproduce the charts

From the repository root:

```sh
bash full-path-realtime/quotes/global/visualizations/_commands.txt
```

The command notebook uses `uv` by default, supports overriding `UV`, and writes PNG, SVG, source CSVs, and JSON summaries to [global/results/charts/](../results/charts/). Rebuild provider cost summaries first when their inputs change; follow the [documented reproduction order](../../README.md#reproduce-reconciliation-cost-and-charts).

Every renderer checks the exact required provider/alternative label set declared in the manifest before drawing. Generated summaries use repository-relative source paths and hashes. The standard and true 5156×2900 slide-wide variants use the same accepted inputs; wide filenames end in `_wide`.

The command file defines `CHART_BACKGROUND_COLOR` once, defaulting to `#161614`. `COSTS_ONLY=1` preserves latency and freshness outputs; `WIDE_ONLY=1` generates only wide variants. The global latency charts omit the pairwise runtime/cost strip because the providers retain their own accepted comparison windows.
