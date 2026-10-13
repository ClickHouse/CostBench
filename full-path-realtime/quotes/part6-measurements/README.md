# Measurements behind CostBench Part 6

This guide links the quantitative visuals in [Why ClickHouse Cloud wins at real-time analytics](https://clickhouse.com/blog/why-clickhouse-cloud-wins-at-real-time-analytics) to their measurements, queries, calculations, and reproduction instructions. Visual numbers follow the final 01–25 article sequence.

## Find a visual

| Visuals | Measurements | Start here |
| --- | --- | --- |
| 02, 03, 25 | End-to-end score, preparation and query cost, accumulated query runtime | [End-to-end performance per dollar](#end-to-end-performance-per-dollar) |
| 04 | Pre-aggregation freshness | [Pre-aggregation freshness](#pre-aggregation-freshness) |
| 05 | Cost of preparing incoming data | [Preparation cost](#preparation-cost) |
| 09–12 | Writer throughput, CPU work, compression, row reduction, and background merges | [Writer preparation measurements](#writer-preparation-measurements) |
| 13 | Accumulated query cost and runtime | [Query cost and runtime](#query-cost-and-runtime) |
| 14, 22 | Aggregate-query latency and current summaries | [Query latency and freshness](#query-latency-and-freshness) |
| 16, 17 | Reader throughput, latency, CPU use, and query memory | [Reader execution measurements](#reader-execution-measurements) |
| 18 | Query throughput across CPU-core settings | [CPU-core scaling](#cpu-core-scaling) |
| 19 | Query-side and end-to-end performance-per-dollar scores | [Query-side and end-to-end scores](#query-side-and-end-to-end-scores) |
| 21 | One query distributed across 1–100 nodes | [Horizontal query scaling](#horizontal-query-scaling) |

## End-to-end performance per dollar

Start with the [quotes benchmark methodology and accepted evidence map](../README.md) and the [global chart guide](../global/visualizations/README.md). The score combines preparation cost and matched query cost, then multiplies by accumulated query runtime. Each provider comparison retains its accepted inputs and matched query window.

- [Score values and provider source paths](../global/results/charts/full_path_cost_performance_all_systems_summary.json)
- [Downloadable score values](../global/results/charts/full_path_cost_performance_all_systems_data.csv)
- [Cost-versus-runtime values and sources](../global/results/charts/full_path_cost_vs_query_runtime_all_systems_summary.json)
- [Downloadable cost-versus-runtime values](../global/results/charts/full_path_cost_vs_query_runtime_all_systems_data.csv)
- [Chart commands](../global/visualizations/_commands.txt)

The individual comparison guides retain the preparation, query-cost, and runtime calculations: [Snowflake](../snowflake/README.md), [Databricks](../databricks/SEPTEMBER_INTEGRATION.md), [BigQuery](../bigquery/README.md), and [Redshift Serverless](../redshift-serverless/README.md). Part 6 uses BigQuery Capacity and the Redshift SUPER read path for its main comparison.

## Pre-aggregation freshness

The [global chart guide](../global/visualizations/README.md) describes the common pre-aggregation-lag chart. Its [measurements](../global/results/charts/mv_lag_all_systems_data.csv), [displayed curve](../global/results/charts/mv_lag_all_systems_curve.csv), and [source summary](../global/results/charts/mv_lag_all_systems_summary.json) are published together.

ClickHouse's raw data and incremental summaries advance from the same inserted blocks; its baseline comes from the [table and materialized-view definitions](../clickhouse-cloud/create.sql). Databricks records refresh age and a row-watermark gap separately; see its [accepted integration](../databricks/SEPTEMBER_INTEGRATION.md) and [run evidence](../databricks/results/serverless_baseline_full_20260918T170453Z/integration/). The [global manifest](../global/visualizations/manifest.json) records that difference explicitly.

## Preparation cost

- [Downloadable preparation-cost values](../global/results/charts/ingest_fresh_path_cost_all_systems_data.csv)
- [Accepted provider inputs and calculations](../global/results/charts/ingest_fresh_path_cost_all_systems_summary.json)
- [Provider configurations, pricing, and reproduction order](../README.md)

These totals cover complete ingestion and its preparation work. BigQuery Capacity and On-demand are alternative prices; each Redshift read-layout comparison includes the same writer-plus-MSK path once.

## Writer preparation measurements

Use the [writer preparation README](../clickhouse-cloud/results_t2/utilization/writer/preparation-metrics/README.md) for visuals 09–12. It maps each chart to extraction SQL, source CSVs, formulas, and exact chart payloads. It covers processing throughput, CPU work by ingestion stage, raw-data compression, summary-row reduction, background consolidation, and the service's resource band.

The measurements come from the two-node, four-vCPU ingest service during the original continuous-ingestion run. The package contains commands to regenerate and check every published value from the committed exports. Filenames retain the earlier 08a–08d identifiers.

## Query cost and runtime

The accumulated query curves retain the accepted observations and pricing for each comparison. Start with the provider-specific chart guides: [Snowflake](../snowflake/visualizations/README.md), [Databricks](../databricks/visualizations/README.md), [BigQuery](../bigquery/visualizations/README.md), and [Redshift Serverless](../redshift-serverless/visualizations/README.md).

The [global manifest](../global/visualizations/manifest.json) and [end-to-end summaries](#end-to-end-performance-per-dollar) connect those inputs to the comparison totals. Query cost uses the accepted normalized allocation; accumulated runtime is the sum of the selected query durations.

## Query latency and freshness

Visuals 14 and 22 use the A4 full-history aggregate-query latency through 100 billion quotes alongside the systems' summary freshness. The exact query is in [ClickHouse's aggregate SQL](../clickhouse-cloud/queries_mv.sql); corresponding queries and observations are listed in the [global manifest](../global/visualizations/manifest.json).

- [Aggregate-query measurements](../global/results/charts/aggregate_query_latency_all_systems_data.csv)
- [Latency definitions and source paths](../global/results/charts/aggregate_query_latency_all_systems_summary.json)
- [Original ClickHouse runner observations](../clickhouse-cloud/results_t2/mv/dashboard_20260808T065559Z.jsonl)
- [Pre-aggregation freshness](#pre-aggregation-freshness)

## Reader execution measurements

Use the [reader resource and display README](../clickhouse-cloud/results_t2/utilization/reader/query-metrics/query-resources/README.md) for visuals 16 and 17. It explains the current throughput and latency displays, consumed CPU work, per-query memory peaks, and effective aggregation throughput, with source exports and commands for exact offline reproduction.

The [original query-metrics README](../clickhouse-cloud/results_t2/utilization/reader/query-metrics/README.md) links the extraction SQL, full query-log exports, individual-query measurements, and original runner files. These measurements describe the 16-vCPU reader while fresh quotes were being ingested. Filenames retain the earlier 11a/11b identifiers.

## CPU-core scaling

Visual 18 uses the [cache-controlled CPU scaling experiment](../clickhouse-cloud/experiments/reader-thread-scaling/2026-10-11-cache-modes/README.md): the original D1/D2 queries, fixed data, and six measured core settings on one 59-vCPU / 236-GiB node.

The README links the [findings](../clickhouse-cloud/experiments/reader-thread-scaling/2026-10-11-cache-modes/REPORT.md), [combined measurements](../clickhouse-cloud/experiments/reader-thread-scaling/2026-10-11-cache-modes/derived/combined-workload.csv), exact display payload, cache checks, and offline/live reproduction instructions. It is a separate experiment from the continuous-ingestion reader measurements. Filenames retain the earlier 11c identifier.

## Query-side and end-to-end scores

Visual 19 brings together two CostBench scopes:

- [Query-side benchmark](../../../query-side-only/README.md): query performance per dollar over already-prepared data, with each provider's recorded runtimes and pricing.
- [End-to-end quotes benchmark](../README.md): continuous ingestion, preparation, freshness, and query execution together, using the accepted [score summaries](#end-to-end-performance-per-dollar).

Each score retains the measurements and calculations of its own study.

## Horizontal query scaling

Visual 21 uses a separate UK property-price aggregation experiment in [ClickHouse/examples](https://github.com/ClickHouse/examples/tree/3b6da4b0d0607d4debe5783021ffd2019058637e/blog-examples/ParallelReplicasBench). The fixed one-trillion-row query groups by county, town, and district and calculates counts, sums, and averages. Seven configurations use 1, 3, 10, 20, 40, 80, and 100 nodes, each with 89 CPU cores.

- [Recorded results and per-run statistics](https://github.com/ClickHouse/examples/tree/3b6da4b0d0607d4debe5783021ffd2019058637e/blog-examples/ParallelReplicasBench/results/2025-08-30_horiz_uk_t1_n1-3-10-20-40-80-100_c89_r10)
- [Configuration and selected-run summary](https://github.com/ClickHouse/examples/blob/3b6da4b0d0607d4debe5783021ffd2019058637e/blog-examples/ParallelReplicasBench/results/2025-08-30_horiz_uk_t1_n1-3-10-20-40-80-100_c89_r10/summary_matrix-nodes-1_3_10_20_40_80_100_cores-89_2025-08-30_165307.json)
- [Exact aggregation SQL](https://github.com/ClickHouse/examples/blob/3b6da4b0d0607d4debe5783021ffd2019058637e/blog-examples/ParallelReplicasBench/datasets/uk_property_prices/aggregation.sql)
- [Dataset loading instructions](https://github.com/ClickHouse/examples/blob/3b6da4b0d0607d4debe5783021ffd2019058637e/blog-examples/ParallelReplicasBench/datasets/uk_property_prices/README.md)
- [Benchmark runner and reproduction](https://github.com/ClickHouse/examples/blob/3b6da4b0d0607d4debe5783021ffd2019058637e/blog-examples/ParallelReplicasBench/README.md)

At each setting, the visual uses the fastest recorded run and the initiator's global `query_bytes_per_sec / 1e9` for decimal GB/s. The simpler 100-billion-row, 1.45-TB/s demonstration in the folded note is documented separately in the [parallel-replicas article](https://clickhouse.com/blog/clickhouse-parallel-replicas).

## Reproducing results

The [quotes benchmark README](../README.md#reproduce-reconciliation-cost-and-charts) gives the order for reconciliation, cost calculation, and chart generation. Each measurement package above includes its own offline derivation and validation commands. Published exports allow the displayed calculations to be regenerated without access to the original live services. Repeating the stock-quotes workload requires a [licensed dataset](../README.md#data-source-and-licensing).
