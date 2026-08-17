# Full-path real-time quotes benchmark

This is the current accepted CostBench study for continuous real-time analytics. It compares
ClickHouse Cloud, Snowflake, Google BigQuery, and Amazon Redshift Serverless over the same NBBO-style
quotes workload while ingest, derived-data maintenance, dashboard queries, and drill-down queries
are all active.

The accepted runs ingest roughly 113 billion rows at a target rate of 1 million events per second.
Charts use observations at or below the inclusive 100 billion-row presentation cap unless their
provenance summary states a different accepted pairwise window.

## Benchmark contract

| Dimension | Contract |
|---|---|
| Source rate | Fixed-rate ingest targeting 1M events/s |
| Raw path | Provider-native query-ready representation for drill-down queries |
| Derived path | Continuously or asynchronously maintained aggregate for dashboard queries |
| Query serving | Isolated read compute where the provider supports it |
| Dashboard workload | Four aggregate queries on a fixed schedule |
| Drill-down workload | Two raw-data queries on a fixed schedule |
| Progress axis | Observed base-table row count, never assumed iteration equivalence |
| Freshness | Persisted derived-data lag; query-time delta correction is disclosed separately |
| Full-path score | `(fresh-data-path cost + matched query cost) × accumulated query runtime` |

Lower score is better. Snowflake, BigQuery, and Redshift each use an accepted ClickHouse-matched
active-ingestion window. Global relative scores reuse those pairwise results; they are not a new
cross-provider match.

BigQuery deliberately uses automatic active-window selection: every eligible active-ingestion
ClickHouse reference observation is selected and the report records `requested_count: null`.
Snowflake and Redshift freeze their accepted pairwise counts in their command notebooks.

## Accepted evidence map

| System | Implementation and run notes | Accepted results | Cost and charts |
|---|---|---|---|
| ClickHouse Cloud | [`clickhouse-cloud/`](clickhouse-cloud/) | [`results_t2/`](clickhouse-cloud/results_t2/) | [`costs/out_t2/`](clickhouse-cloud/costs/out_t2/) |
| Snowflake | [`snowflake/README.md`](snowflake/README.md) | [`results/t2/`](snowflake/results/t2/) | [`results/t2/charts/run14/`](snowflake/results/t2/charts/run14/) |
| BigQuery | [`bigquery/README.md`](bigquery/README.md) | [`results/bq-full-t2-20260810_152224/`](bigquery/results/bq-full-t2-20260810_152224/) | [`costs/out/bq-full-t2-20260810_152224/`](bigquery/costs/out/bq-full-t2-20260810_152224/) |
| Redshift Serverless | [`redshift-serverless/README.md`](redshift-serverless/README.md) | [`results/t2/`](redshift-serverless/results/t2/) | [`costs/out/t2/`](redshift-serverless/costs/out/t2/) |
| Global synthesis | [`global/visualizations/`](global/visualizations/) | Provider sources above | [`global/results/charts/`](global/results/charts/) |

The Databricks directory contains earlier ingest work and remains useful implementation evidence,
but Databricks is not included in the current accepted global full-path chart manifest.

## Reproduce reconciliation, cost, and charts

Run commands from any directory; maintained command notebooks relocate to their own repository root.
Provider credentials must remain in ignored local files or environment variables.

1. Rebuild pairwise row-progress matches:

   ```bash
   bash full-path-realtime/utils/_commands.txt
   ```

2. Rebuild ClickHouse matched query-cost summaries:

   ```bash
   bash full-path-realtime/quotes/clickhouse-cloud/costs/_commands.txt
   ```

3. Rebuild provider-native costs and pairwise charts:

   ```bash
   bash full-path-realtime/quotes/snowflake/costs/_commands.txt
   bash full-path-realtime/quotes/snowflake/visualizations/_commands.txt

   bash full-path-realtime/quotes/bigquery/costs/_commands.txt
   bash full-path-realtime/quotes/bigquery/visualizations/_commands.txt

   bash full-path-realtime/quotes/redshift-serverless/costs/_commands.txt
   bash full-path-realtime/quotes/redshift-serverless/visualizations/_commands.txt
   ```

4. Rebuild the global synthesis last:

   ```bash
   bash full-path-realtime/quotes/global/visualizations/_commands.txt
   ```

Each maintained renderer writes PNG, SVG, source CSV, and JSON provenance. It also writes a true
5156×2900 slide-wide variant. The global manifest validates the exact required provider/alternative
label set before drawing, so incomplete inputs fail closed.

## Interpretation rules

- Query latency lines use each provider's own observed row progress. No global chart joins iteration
  numbers across systems.
- Display smoothing is provider-local and disclosed. The accepted Snowflake aggregate chart retains
  its Tukey upper-fence policy; no new outlier policy is applied to other providers or workloads.
- Full-path score bars use `log10(relative score)`. The ClickHouse 1× winner is a point at the origin,
  not an artificial minimum-width bar.
- BigQuery Capacity and On-demand are pricing alternatives over the same accepted run.
- Redshift SUPER and typed are read-layout alternatives. Both reuse one shared writer-plus-MSK fresh
  path; that path is never split or charged twice.
- The absolute cost-versus-runtime chart intentionally combines distinct accepted pairwise windows
  and labels that limitation in its provenance.

## Dataset

The workload uses NBBO-style stock-market bid/ask snapshots stored as daily ZSTD-compressed Parquet
files. The source capture contains 232 daily files, approximately 651 GB compressed and 113 billion
rows; market-closed days may be empty. Files are replayed when a provider needs to continue ingest
beyond the captured sequence.

| Column | Type | Description |
|---|---|---|
| `sym` | string | Ticker symbol |
| `bx`, `ax` | uint8 | Bid and ask exchange codes |
| `bp`, `ap` | float64 | Bid and ask prices |
| `bs`, `as` | uint64 | Bid and ask sizes |
| `c`, `z` | uint8 | Condition and tape/exchange group |
| `i` | array&lt;uint8&gt; | Indicator flags |
| `t` | uint64 | Unix epoch timestamp in milliseconds |
| `q` | uint64 | Sequence number |

Provider DDL maps these logical types to native equivalents. The `as` column is quoted where it is a
reserved word.

## Data source and licensing

The quotes data comes from [Massive](https://massive.com/), a US market-data API provider. The
benchmark capture was recorded from its real-time quotes WebSocket into daily Parquet files. The
partnership terms do not permit redistribution, so this repository publishes scripts and aggregated
results, not the source dataset.

To reproduce the workload, bring a licensed quotes dataset with the same logical schema. A comparable
capture can be produced from Massive's [`WS /stocks/Q`](https://massive.com/docs/websocket/stocks/quotes)
feed; a fixed historical window can instead come from its
[`Quotes`](https://massive.com/docs/rest/stocks/trades-quotes/quotes) REST endpoint or
[Flat Files](https://massive.com/docs/flat-files/stocks/quotes). Confirm current plan, exchange, and
redistribution terms directly with Massive before collecting or sharing market data.

## Repository safety and validation

- Never add any `*_credentials.txt` file. The root ignore rule covers that filename recursively.
- Run `python3 scripts/check_repository_hygiene.py` before committing.
- Treat raw JSONL and accepted cost summaries as evidence: do not hand-edit measurements.
- Regenerate charts after any source, pricing, reconciliation, or renderer change.
