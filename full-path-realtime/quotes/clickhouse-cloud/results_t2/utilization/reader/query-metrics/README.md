# Reader query metrics

Visual numbers follow the current Part 6 article. Existing filenames retain their earlier identifiers so reproduction commands and links stay compatible.

Original numeric extraction behind Blog 6 reader visuals **16 and 17**. The measurements use the August 8–9, 2026 ClickHouse run while the raw dataset grows to **113,217,743,918 quotes**. The reader service had one node with 16 CPUs and 64 GiB; the benchmark database was `quotes_async_t4` (the historical results directory remains `results_t2`).

## Current blog values and resource band

Use [query-resources/](query-resources/README.md) for the current two-panel layouts and their **Resource utilization during query execution** bands. It includes extraction SQL/results, all 958 matched per-query memory peaks, CPU during execution with headroom, shared Gaussian display smoothing, exact 16/17 payloads and a dependency-free reproducer. These calculations exclude the idle gaps in the query schedule and preserve the original 16-vCPU / 64-GiB setup.

The tables and adapters below retain the original four-panel extraction as frozen evidence. Their byte-rate and CPU-rate CSVs are inputs to the current display adapter; the older median-smoothed latency payload is preserved for traceability, rather than used as the current blog curve.

## Frozen original extraction

| Position | Panel | Download | Calculation for one scheduled iteration |
| --- | --- | --- | --- |
| Top left | Data processing throughput | [CSV](data-processing-throughput.csv) | `sum(read_bytes) / sum(query_duration_ms / 1000)` |
| Top right | Data throughput per CPU core | [CSV](data-throughput-per-cpu-core.csv) | `sum(read_bytes) / sum((user_cpu_us + system_cpu_us) / 1000000)` |
| Bottom left | Query latency | [Display CSV](query-latency.csv), [per-query CSV](query-latency-per-query.csv) | Equal mean of query latencies within each family, using the established display smoothing below |
| Bottom right | Effective aggregation throughput | [CSV](effective-aggregation-throughput.csv) | `2 * raw_rows / ((A3.duration_ms + A4.duration_ms) / 1000)` |

Drill-down curves summarize D1 and D2, shown with dashed lines. Interactive aggregation curves summarize A1–A4, shown with solid lines. Effective aggregation throughput uses **A3 and A4 only**, since both aggregate the entire pre-aggregated table without a symbol filter.

## SQL and source results

- [00-query-log-fields.sql](queries/00-query-log-fields.sql) inspects the available log columns.
- [01-discover-benchmark-queries.sql](queries/01-discover-benchmark-queries.sql) identifies the six query shapes and their CPU counters.
- [02-export-query-log.sql](queries/02-export-query-log.sql) is the extraction query behind [query-log.csv](query-log.csv): 1,241 completed initial SELECT executions, with UTC timestamps, hashes, CPU counters, rows and bytes read.
- [03-export-profile-stats.sql](queries/03-export-profile-stats.sql) is the additional profile-event export behind [query-log-profile-stats.csv](query-log-profile-stats.csv). It verifies selected rows/bytes, CPU counters and the `max_threads` setting.
- [benchmark-aggregations.sql](queries/benchmark-aggregations.sql) contains A1–A4, copied from [queries_mv.sql](../../../../queries_mv.sql) in statement order.
- [benchmark-drill-downs.sql](queries/benchmark-drill-downs.sql) contains D1–D2, copied from [queries_raw.sql](../../../../queries_raw.sql) in statement order.

Both exports remove query IDs, hostnames and repeated query text before committing. Numeric counters, query hashes and timestamps retain their original values. The six SQL statements and their normalized query hashes are recorded in [provenance.json](provenance.json).

[metrics-per-query.csv](metrics-per-query.csv) contains the 958 active executions joined to runner iterations. [chart-data.csv](chart-data.csv) contains supporting family summaries. [chart-data.json](chart-data.json) contains the exact four-panel payload, units, scales and row coordinates used by the visual.

## Interpretation

**Data throughput** measures uncompressed bytes selected by the query engine, not physical disk or network throughput. The chart uses decimal GB/s. It averages neither the idle intervals between scheduled queries nor the per-query rates: it divides the sum of bytes by the sum of execution seconds within each two-query or four-query iteration.

**Throughput per CPU core** divides those bytes by CPU-seconds consumed across query threads (`UserTimeMicroseconds + SystemTimeMicroseconds`). A CPU-second is one core's worth of execution for one second. Thus MB/CPU-second equals MB/s per average busy core during execution. The denominator is measured CPU work, not the 16 provisioned cores, a count of threads, or elapsed time between queries. Query duration is logged in integer milliseconds; derived rates therefore have limited precision for short queries.

**Query latency** retains the established Blog 2 display convention. Each query's runner latency is smoothed with a centered rolling median (five observations for D1/D2, seven for A1–A4); the family curve is the equal mean of those independently smoothed values. Through 100B quotes, smoothing uses the observations within that horizon, with every third aggregation observation and the final observation displayed. Beyond 100B, every observation is displayed and the same median windows use the full active run. Windows shrink at their endpoints. The raw per-query CSV is included. Runner elapsed time and server `query_duration_ms` are distinct recorded durations: the former preserves the published latency chart, while the latter is the denominator for the query-log throughput panels.

**Effective aggregation throughput** expresses the raw quotes represented by a full-table answer, divided by execution time. Pre-aggregates represent the raw dataset without scanning all its quotes on each query. For example, at the endpoint A3 and A4 take 18 and 16 ms respectively; `2 × 113,217,743,918 / 0.034` is **6.66 trillion raw quotes/s**. This is logical answer coverage through pre-aggregation, not an actual raw scan rate or a measurement of sustained query capacity. The two symbol-filtered queries A1/A2 are excluded from this calculation.

No smoothing or outlier removal is applied to either byte throughput panel or the effective aggregation throughput panel. All their matched active measurements are included. The dashboard-wide idle-time averages and the earlier raster-derived telemetry charts are not inputs to these panels.

## Matching and reproduction

The original runner files are [D1/D2](../../../raw/drilldown_20260808T065602Z.jsonl) and [A1–A4](../../../mv/dashboard_20260808T065559Z.jsonl). Every expected query is matched to exactly one query-log execution starting within 20 seconds of its scheduled runner iteration. All 1,240 runner executions match uniquely; an additional A1 execution before the runner started remains in the source CSV. Positive-row iterations through each family's first complete-dataset observation are plotted: 37 D1/D2 iterations and 221 A1–A4 iterations, totaling 958 executions. Later repeated queries are retained in the source export and omitted from the growing-dataset curves. The initial zero-row observations are also retained in the source export.

The profile export agrees with the first export for all matched CPU counters, durations, rows and bytes. `SelectedRows` equals `read_rows`, `SelectedBytes` equals `read_bytes`, and all 958 active executions explicitly have `max_threads=16`. Query-cache hits are zero. Source hashes, matching rules, counts and formula definitions are recorded in [provenance.json](provenance.json).

From the repository root, with Python 3.9 or newer and no additional dependencies:

```sh
python3 full-path-realtime/quotes/clickhouse-cloud/results_t2/utilization/reader/query-metrics/derive.py
python3 full-path-realtime/quotes/clickhouse-cloud/results_t2/utilization/reader/query-metrics/derive.py --check
```

The first command regenerates all derived CSVs, the four-panel JSON and provenance from committed source exports and runner JSONL files. The second verifies byte-for-byte reproducibility without changing files. The SQL exports are bounded by dates, execution time, rows read and result count.
