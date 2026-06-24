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
- **Full-path cost-performance** — how efficiently each dollar turns fresh ingest into query-ready data.

Together, they answer the question that matters when picking a platform: *which system gives you the most performance per dollar for real-time analytical workloads?*

The current release focuses on the **read side** (analytical queries over already-loaded data). Initial **write-side** results are also available, starting with Snowflake as a contrast point for ClickHouse; broader full-path coverage is coming.

Each dimension is its own benchmark, with its own workload, scales, and billing logic:

- **[Read-side benchmark →](query-side-only/)** — query cost-performance over already-loaded data.
- **[Full-path benchmark →](full-path-realtime/)** — the cost of keeping continuously ingested data query-ready.

## Systems covered

CostBench currently runs the same workload across the five major cloud data warehouses:

- ClickHouse Cloud
- Snowflake
- Databricks (SQL Serverless)
- Google BigQuery
- Amazon Redshift Serverless

Each system's actual compute billing model is applied to the raw runtimes, so cost numbers reflect what you would really be charged.

## Methodology in brief

The same principles apply to every benchmark in this repository:

- **Real data, real workloads.** Production-derived workloads run over real, anonymized datasets at meaningful scale, rather than synthetic micro-benchmarks.
- **Real billing models.** Each vendor's actual compute and storage pricing is applied to the measured runtimes and normalized to a common basis, so costs are comparable across engines.
- **One comparable metric.** Runtime and cost are combined into a single cost-performance score, so systems can be ranked on equal footing.
- **Transparent, open, and reproducible.** Every workload, configuration, pricing model, and raw result is published, so any number can be inspected and re-run.

The detailed methodology for each benchmark — the exact workload, scales, and per-vendor billing logic — lives with that benchmark, in [`query-side-only/`](query-side-only/) and [`full-path-realtime/`](full-path-realtime/).

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

## Contributing

CostBench is open precisely so configurations and pricing assumptions can be reviewed in the open. If you spot a setup that can be improved, a pricing detail that should be updated, or a vendor configuration worth adding, please open an issue or pull request.

## License

See [LICENSE](LICENSE) in this repository.
