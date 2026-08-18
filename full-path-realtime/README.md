# Full-path real-time benchmarks

This directory contains CostBench workloads that measure the complete operating path of a real-time
analytics system: continuous ingest, query-ready raw data, maintained aggregates, freshness, query
latency under active ingestion, and the cost of keeping the path live.

## Workloads

| Workload | Status | Scope |
|---|---|---|
| [Quotes](quotes/) | Current accepted multi-provider study | ClickHouse Cloud, Snowflake, BigQuery, and Redshift Serverless at roughly 1M events/s and 100B+ rows |
| [Hits](hits/) | Workload implementation | Web analytics data; not part of the current accepted global quotes synthesis |

## Common benchmark contract

- Ingest is rate-controlled and progress is recorded continuously.
- Raw and aggregate query suites run on fixed schedules while ingest remains active.
- Query observations are reconciled by base-table row progress, not by assuming iteration numbers
  align across providers.
- Fresh-data-path cost covers the provider-specific components required to ingest and maintain the
  query-ready state for the complete run.
- Query cost is reported for the accepted active-ingestion comparison window.
- Persisted materialized-view lag is kept distinct from query-time freshness correction.
- Source data, cost inputs, filters, smoothing, exclusions, and chart geometry are disclosed in
  machine-readable summaries.

The shared matcher is documented in [`utils/README.md`](utils/README.md). It writes matched JSONL,
iteration lists, and a report containing source hashes and selection details. Provider-specific
READMEs document any additional accepted rule.

## Evidence flow

```text
provider runner JSONL
        ↓
row-progress reconciliation
        ↓
provider-native cost summaries
        ↓
pairwise charts and provenance
        ↓
fail-closed global manifest and charts
```

For the complete commands and accepted evidence roots, continue with the
[quotes benchmark README](quotes/README.md).
