# Read-Side (Query-Side) Benchmark

How much **query performance you get per dollar** over already-loaded data, across the major cloud data warehouses.

This is the read-side benchmark of [CostBench](../). For the write-side benchmark — the cost of keeping continuously ingested data query-ready — see [`full-path-realtime/`](../full-path-realtime/).

📊 **[Explore the results in the interactive benchmark explorer →](https://clickhouse.com/blog/cloud-data-warehouses-cost-performance-comparison#interactive-benchmark-explorer)**

## Systems covered

- ClickHouse Cloud — [`clickhouse-cloud/`](clickhouse-cloud/)
- Snowflake — [`snowflake/`](snowflake/)
- Databricks (SQL Serverless) — [`databricks/`](databricks/)
- Google BigQuery — [`bigquery/`](bigquery/)
- Amazon Redshift Serverless — [`redshift-serverless/`](redshift-serverless/)

## Methodology

- **Workload.** Based on [ClickBench](https://github.com/ClickHouse/ClickBench): 43 production-derived analytical queries (clickstream, logs, dashboard-style aggregations) over a real, anonymized dataset.
- **Scales.** Run at **1B**, **10B**, and **100B rows** to see how cost and performance evolve as data grows.
- **No tuning.** Standard ClickBench rules apply — no engine-specific optimizations, no materialized views, no hand-tuning. Out-of-the-box behavior on each system.
- **Hot runtimes, caches disabled.** Best of three runs; query result caches disabled everywhere they exist.
- **Native storage formats.** Each engine runs against its native format (MergeTree, Delta Lake, Snowflake micro-partitions, BigQuery Capacitor, etc.).
- **Real billing models.** Each vendor's actual compute pricing model is applied per query, normalized to per-second metering for a clean comparison. Pricing assumptions, units, and conversion logic are all in the repo.
- **Single comparable metric.** `cost-performance score = runtime × cost` (smaller is better). The best system is the 1× baseline; everything else is reported as N× worse.

Full methodology, configuration choices, and per-vendor billing logic are documented in the accompanying [blog posts](../README.md#read-more) and in each vendor's subfolder.

## Headline result

Across 1B, 10B, and 100B rows, **ClickHouse Cloud is the only system that stays in the "Fast & Low-Cost" quadrant as data scales.** At 100B rows, the nearest competitor is 23× worse in cost-performance, and most other systems fall into the hundreds-of-times-worse range.

You can slice the full result set — vendors, tiers, cluster sizes, scales, runtime vs. cost vs. cost-performance ranking — in the **[interactive benchmark explorer](https://clickhouse.com/blog/cloud-data-warehouses-cost-performance-comparison#interactive-benchmark-explorer)**.

## Layout

Each vendor folder follows the same structure:

- `clickbench/` — run scripts and raw per-config benchmark results
- `pricings/` — pricing descriptors used for cost calculation
- `results/`, `results_1B/`, `results_10B/`, `results_100B/` — enriched results (runtime + compute + storage cost)
- `enrich.sh` — applies the pricing model to raw runtimes

Shared tooling lives alongside the vendor folders:

- `_viz/`, `_viz2/`, `_analyze/` — chart and analysis tooling
- `_INGEST_TEST/` — ingestion test scripts
