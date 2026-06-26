
# Full-path real-time cost-performance benchmarks

This directory contains benchmark workloads for measuring end-to-end real-time analytics cost and performance.

Unlike query-only benchmarks, these workloads cover the full path from data ingestion to query-ready analytical results, including:

- ingesting source data
- maintaining derived or serving structures
- refreshing or updating query-ready data
- running dashboard and drilldown queries
- collecting cost and performance results across systems

## Workloads

- `quotes/` — stock quotes benchmark using bid/ask market data.
- `hits/` — web analytics data.
