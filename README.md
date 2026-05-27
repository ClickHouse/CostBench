# CostBench

**An open benchmark for cloud data warehouse cost-performance — performance-per-dollar, not just speed.**

CostBench measures how much performance each dollar actually buys you on the major cloud data warehouses, so teams can choose the system that delivers the most value for real-time analytical workloads.

📊 **[Explore the results in the interactive benchmark explorer →](https://clickhouse.com/blog/cloud-data-warehouses-cost-performance-comparison#interactive-benchmark-explorer)**

---

## Why cost-performance, not just performance

Most benchmarks tell you how fast a query runs. That is useful, but incomplete. In cloud data platforms, speed and cost are inseparable.

If warehouse A is faster than warehouse B, A looks better on a performance chart. But if A costs three times more to run, you could spend the same budget on a larger configuration of B, get more compute, and finish the workload faster than A for less money overall.

That comparison is hard because every platform exposes cost differently — credits, DBUs, slot-seconds, compute units, RPUs. The unit names differ, but the underlying question is the same:

> **How much compute did the system need to finish the workload, and what did that compute cost?**

CostBench answers that question directly, on equal footing across vendors.

## What CostBench measures

CostBench frames cost-performance along two dimensions:

- **Read-side cost-performance** — how much query performance you get per dollar.
- **Write-side cost-performance** — how efficiently each dollar turns fresh ingest into query-ready data.

Together, they answer the question that matters when picking a platform: *which system gives you the most performance per dollar for real-time analytical workloads?*

The current release focuses on the **read side** (analytical queries over already-loaded data). Initial **write-side** results are also available, starting with Snowflake as a contrast point for ClickHouse; broader write-side coverage is coming.

## Systems covered

CostBench currently runs the same workload across the five major cloud data warehouses:

- ClickHouse Cloud
- Snowflake
- Databricks (SQL Serverless)
- Google BigQuery
- Amazon Redshift Serverless

Each system's actual compute billing model is applied to the raw runtimes, so cost numbers reflect what you would really be charged.

## Methodology in brief

- **Workload.** Based on [ClickBench](https://github.com/ClickHouse/ClickBench): 43 production-derived analytical queries (clickstream, logs, dashboard-style aggregations) over a real, anonymized dataset.
- **Scales.** Run at **1B**, **10B**, and **100B rows** to see how cost and performance evolve as data grows.
- **No tuning.** Standard ClickBench rules apply — no engine-specific optimizations, no materialized views, no hand-tuning. Out-of-the-box behavior on each system.
- **Hot runtimes, caches disabled.** Best of three runs; query result caches disabled everywhere they exist.
- **Native storage formats.** Each engine runs against its native format (MergeTree, Delta Lake, Snowflake micro-partitions, BigQuery Capacitor, etc.).
- **Real billing models.** Each vendor's actual compute pricing model is applied per query, normalized to per-second metering for a clean comparison. Pricing assumptions, units, and conversion logic are all in the repo.
- **Single comparable metric.** `cost-performance score = runtime × cost` (smaller is better). The best system is the 1× baseline; everything else is reported as N× worse.

Full methodology, configuration choices, and per-vendor billing logic are documented in the accompanying blog posts (linked below) and in the repo.

## Headline result (read side)

Across 1B, 10B, and 100B rows, **ClickHouse Cloud is the only system that stays in the "Fast & Low-Cost" quadrant as data scales.** At 100B rows, the nearest competitor is 23× worse in cost-performance, and most other systems fall into the hundreds-of-times-worse range.

You can slice the full result set — vendors, tiers, cluster sizes, scales, runtime vs. cost vs. cost-performance ranking — in the **[interactive benchmark explorer](https://clickhouse.com/blog/cloud-data-warehouses-cost-performance-comparison#interactive-benchmark-explorer)**.

## Open and reproducible

Cost-performance claims should be inspectable. The repository publishes:

- the workload and query set,
- scripts used to run each system,
- per-vendor configurations and cluster sizes,
- pricing models and assumptions used for cost calculation,
- raw JSON result files with per-query runtimes, compute cost, and storage cost,
- the methodology behind the unified cost-performance score.

If a result looks surprising, you can inspect the setup that produced it. If a configuration can be improved, it can be reviewed and corrected in the open — issues and pull requests are welcome.

## Read more

Four companion blog posts walk through the motivation, billing models, results, and write-side analysis in detail:

- **[Introducing CostBench: an open benchmark for data warehouse cost-performance](https://clickhouse.com/blog/costbench-data-warehouse-cost-performance)** — what CostBench is and why cost-performance matters in the agentic era.
- **[How the 5 major cloud data warehouses really bill you: a unified, engineer-friendly guide](https://clickhouse.com/blog/how-cloud-data-warehouses-bill-you)** — credits, DBUs, compute units, slot-seconds, RPUs, explained on equal footing.
- **[How the 5 major cloud data warehouses compare on cost-performance](https://clickhouse.com/blog/cloud-data-warehouses-cost-performance-comparison)** — full read-side results at 1B / 10B / 100B rows, including the [interactive explorer](https://clickhouse.com/blog/cloud-data-warehouses-cost-performance-comparison#interactive-benchmark-explorer).
- **[Agentic analytics starts with query-ready data: the write-side cost of Snowflake vs. ClickHouse](https://clickhouse.com/blog/write-side-cost-performance-snowflake-clickhouse)** — measuring what it costs to keep continuously ingested data query-ready.

## Roadmap

- Broader write-side coverage beyond the initial Snowflake vs. ClickHouse comparison.
- Additional cluster sizes, tiers, and pricing options surfaced through the interactive explorer.
- Follow-ups exploring Snowflake Gen 2 warehouses, QAS, 5XL/6XL tiers, and Interactive Warehouses.
- A separate benchmark comparing engines over open table formats (Delta Lake, Apache Iceberg, Apache Hudi).

## Contributing

CostBench is open precisely so configurations and pricing assumptions can be reviewed in the open. If you spot a setup that can be improved, a pricing detail that should be updated, or a vendor configuration worth adding, please open an issue or pull request.

## License

See [LICENSE](LICENSE) in this repository.
