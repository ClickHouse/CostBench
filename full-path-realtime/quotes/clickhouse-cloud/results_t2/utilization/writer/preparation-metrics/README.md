# Writer preparation metrics

Visual numbers follow the current Part 6 article. Existing filenames retain their earlier identifiers so reproduction commands and links stay compatible.

SQL, source results and reproduction code behind the four writer-service visuals in Blog 6. The active run spans **2026-08-08 06:55:56 UTC through 2026-08-09 19:40:58 UTC, exclusive**, while **113,217,743,918 raw quotes** arrive. The ClickHouse Cloud writer service has **two nodes with 2 vCPUs and 8 GiB each**: 4 vCPUs and 16 GiB total. The driver target is 1 million quotes/s. The benchmark database is `quotes_async_t4`; the historical results directory remains `results_t2`.

| Visual | Panels and downloadable values | Exact chart payload |
| --- | --- | --- |
| **09 — Data processing throughput and CPU efficiency** | [Processing throughput](08a-throughput.csv), [throughput per CPU core](08a-efficiency.csv) | [JSON](chart-data-08a.json) |
| **10 — Where ingestion CPU work goes** | [CPU work](08b-cpu-work.csv), [CPU breakdown](08b-cpu-share.csv) | [JSON](chart-data-08b.json) |
| **11 — Reducing data with four CPU cores** | [Raw-data compression](08c-compression-volume.csv), [pre-aggregation row reduction](08c-maintained-volume.csv) | [JSON](chart-data-08c.json) |
| **12 — Background consolidation with four CPU cores** | [Background merge throughput](08d-merge-throughput.csv), [maximum active data parts](08d-parts.csv) | [JSON](chart-data-08d.json) |

All four visuals use the same [CPU and memory utilization values](resource-utilization.csv). Each panel CSV includes every plotted series, its row coordinate and any reduction factor. The JSON files preserve the approved values, units, scales, colors and resource-band data. [chart-config.json](chart-config.json) holds presentation metadata; every numeric point is regenerated from source CSVs.

## Queries and original results

[queries/README.md](queries/README.md) links every retained query to its result CSV and distinguishes diagnostics without retained results. [source-manifest.json](source-manifest.json) records column names, result shapes, row counts, hashes, omitted diagnostic fields and extraction status. There are **97 retained SQL exports**, published as **102 source CSVs**; multi-section discovery results have one CSV per section.

The main inputs are:

| Measurement | Extraction SQL | Source CSV |
| --- | --- | --- |
| Whole-service CPU, merge bytes and merged rows | [06](queries/06-active-metric-series.sql) | [active-metric-series.csv](results/active-metric-series.csv) |
| Maximum active parts per partition | [07](queries/07-active-parts-series.sql) | [active-parts-series.csv](results/active-parts-series.csv) |
| Raw and summary NewPart counters | [08](queries/08-active-newpart-series.sql), [22](queries/22-preparation-newpart-timing-series.sql) | [NewPart](results/active-newpart-series.csv), [timing and sizes](results/newpart-timing-series.csv) |
| Materialized-view input, output and CPU | [21](queries/21-preparation-view-series.sql) | [view-series.csv](results/view-series.csv) |
| Insert CPU-stage samples | [24c chunks](queries/README.md#cpu-profiler-chunks) | Ten `cpu-stage-audit-*.csv` files; [combined samples](cpu-stage-samples.csv) |
| Matched insert-flush CPU counters | [27b chunks](queries/README.md#insert-cpu-chunks) | Thirty-seven `flush-cpu-hour-*.csv` files; [combined executions](ingestion-cpu-query-series.csv) |
| Merge input/output rows and part counts | [28](queries/28-preparation-merge-consolidation-series.sql) | [merge-consolidation-series.csv](results/merge-consolidation-series.csv) |
| Memory usage and provisioned capacity | [42](queries/42-memory-node-series.sql), [43](queries/43-memory-capacity-check.sql) | [node measurements](results/memory-node-series.csv), [capacity](results/memory-capacity-check.csv) |
| Dashboard memory cross-check | [41](queries/41-memory-dashboard-series.sql) | [memory-dashboard-series.csv](results/memory-dashboard-series.csv) |

Hostnames are replaced by stable `writer-node-1` / `writer-node-2` aliases so host-level joins remain reproducible. Sample execution IDs and repeated query text are omitted. All retained numeric counters and UTC timestamps are unchanged. Table UUIDs remain as benchmark table identities. Array-valued CSV cells contain JSON; the manifest specifies each cell's encoding.

The benchmark's [raw table, summary table and materialized-view definitions](../../../../create.sql) are also versioned in this repository.

## Calculations and interpretation

**Processing throughput (09)** adds raw-table NewPart uncompressed bytes to the whole-service `MergedUncompressedBytes` counter, then divides by elapsed wall time. It measures processing work, including repeated merge processing. The per-core panel divides the same bytes by consumed service CPU-seconds from `OSCPUVirtualTimeMicroseconds`, rather than by the four provisioned vCPUs. Decimal MB/s and MB/CPU-second are used. [chart-metrics.csv](chart-metrics.csv) contains the unsmoothed five-minute observations; [chart-display-metrics.csv](chart-display-metrics.csv) contains displayed rates.

**Ingestion CPU work (10)** uses completed insert flushes matched to successful materialized-view executions. Within each host and 30-minute bucket, symbolized CPU samples allocate the recorded `UserTimeMicroseconds + SystemTimeMicroseconds` among Parsing, Sorting, Pre-aggregation, Compression and Other ingestion. This stage allocation is an estimate, not an exact stage timer. The ordered classification is in the 24c SQL. Its codec audit excludes decompression-only frames and produces the same stage totals as the original 24 extraction. CPU work is expressed as CPU-ms per million raw quotes; percentages divide by ingestion CPU work. Destination sorting and compression are separate from pre-aggregation. [cpu-metrics.csv](cpu-metrics.csv), [cpu-work-display.csv](cpu-work-display.csv) and [cpu-share-display.csv](cpu-share-display.csv) retain the intermediate values.

**Compression and row reduction (11)** use cumulative active-run values. Compression divides unique raw NewPart `bytes_uncompressed` by `size_in_bytes`; initial stored part size includes metadata. Maintained summary rows are reconstructed as cumulative summary NewPart rows plus completed merge output rows minus completed merge input rows. No active-window mutations or part-event errors occur, raw merges preserve row counts, and part-producing counts reconcile with unique part names. Initial MV output and maintained summary rows are separate quantities. The displayed row-reduction factor divides actual cumulative raw quotes by displayed maintained summary rows. [reduction-metrics.csv](reduction-metrics.csv) retains the reconstruction, and [reduction-display.csv](reduction-display.csv) includes the factors. The raw endpoint reconstruction is 1,887,532 summary rows; the smoothed endpoint shown in the chart is approximately 1.874 million, giving approximately 60,420× reduction.

The supporting [current-part snapshot](results/current-part-snapshot.csv) and [summary-coverage snapshot](results/summary-coverage-snapshot.csv) were collected on October 10 and are kept separate from the active-run curves. The `last_modified` field in the part snapshot is the part modification time. Full part-name lineage diagnostics exceeded the unchanged scan guard and are not used; the reconstruction uses retained completed-merge counters and the checks above.

**Background consolidation (12)** divides `MergedRows` by elapsed wall time. It counts input rows processed during merges, including repeated processing; it does not count rows eliminated. Maximum active parts uses the service-wide `MaxPartCountForPartition` metric across both nodes, without a table filter. [merge-throughput-metrics.csv](merge-throughput-metrics.csv) retains raw rates, and [merge-throughput-display.csv](merge-throughput-display.csv) retains displayed rates.

**CPU and memory usage** share the same row progress. CPU utilization divides average consumed cores by four provisioned vCPUs; it does not imply particular cores were fully occupied. Memory uses `CurrentMetric_MemoryTracking`: average snapshots within each host/second, average available host/seconds per five-minute bucket, then sum both node means. `CGroupMemoryTotal` confirms 8 GiB per node throughout the run. The memory counter measures server allocations rather than OS resident memory. Each unused percentage is the arithmetic complement of the corresponding used percentage. The dashboard's independent memory aggregation differs by less than 0.05% for the whole-run mean. [memory-metrics.csv](memory-metrics.csv) retains the unsmoothed observations.

## Display smoothing

Displayed rates and CPU shares use centered positive Gaussian weights with **sigma 3,600 seconds and radius 10,800 seconds**. Numerator and denominator counters are weighted separately, then divided. Actual partial-bucket durations and midpoints are used; windows shrink at the measured boundaries. The historical centered windows include later observations. No observations or outliers are excluded.

Maintained summary-row state, maximum parts and memory usage use duration-weighted means. Raw cumulative volumes and the compression factor use actual cumulative totals. Factors, chart markers and resource values use the same selected observation. [definitions.json](definitions.json) documents all formulas and boundaries; regenerated [provenance.json](provenance.json) adds input hashes and validation totals.

## Reproduce the published values

From the repository root, with **Python 3.9+ and no additional dependencies**:

```sh
python3 full-path-realtime/quotes/clickhouse-cloud/results_t2/utilization/writer/preparation-metrics/derive.py
python3 full-path-realtime/quotes/clickhouse-cloud/results_t2/utilization/writer/preparation-metrics/derive.py --check
```

The first command regenerates the derived CSVs, four chart payloads, summaries and provenance from committed source CSVs. The second compares fresh outputs **byte for byte without changing files**. It also checks every source CSV and SQL hash, source row count, raw quote totals, CPU-stage reconciliation, both-node memory coverage and paired reduction-factor calculations.

## Re-extract source results

The SQL files preserve the original UTC filters, query limits, scan guards and chunk boundaries. The result format is a single `payload` containing a JSON array. Run the retained queries on the writer service and save each unescaped `payload` as `<source-key>.json`, using the source keys listed in the manifest. For example, the result of query 06 is `active-metric-series.json`. The original extracts use bounded chunks and at most two threads; some historical queries set `skip_unavailable_shards=1`, so full node and bucket coverage must be checked before using a new export. The memory queries require all shards.

With all 97 saved payloads in a separate directory, convert them to public CSVs:

```sh
python3 full-path-realtime/quotes/clickhouse-cloud/results_t2/utilization/writer/preparation-metrics/export_results.py \
  --input-dir /tmp/writer-payloads --output-dir /tmp/writer-source-csvs
```

The converter applies the same node aliases and diagnostic-field omissions, preserving numeric values. It writes to a separate directory for comparison. Queries require retained historical logs; metadata and current snapshots can change after extraction. Committed source exports remain sufficient for exact offline reproduction. Credentials and service connection details are not included.
