# Reader query resources and current 11a/11b display data

This package reproduces the current two reader visuals from the original Run 2 evidence. **11a** shows data processing throughput and throughput per consumed CPU core; **11b** shows query latency and effective aggregation throughput. Both use the same **Resource utilization during query execution** band.

The active window is **2026-08-08 06:55:56–2026-08-09 19:40:58 UTC**, during continuous ingestion of **113,217,743,918 quotes**. The read service was one **16-vCPU / 64-GiB** node. All **958** active executions are retained: **74 drill-downs** and **884 interactive aggregates**. No new exclusions are applied.

## Query-execution CPU and headroom

CPU utilization uses summed `UserTimeMicroseconds + SystemTimeMicroseconds`, divided by summed execution time and **16 vCPUs**. Execution time supplies the query weights. The hourly drill-down and ten-minute aggregate scheduling gaps do not enter this denominator. CPU headroom is the complement of this share; it does not quantify additional concurrent queries.

The resource curve has 442 five-minute row-progress positions joined to cumulative raw-table NewPart rows. At each timestamp it uses the same centered Gaussian display kernel as the charts. [Resource CSV](resource-display.csv) retains busy-core counts and percentages. Historical whole-service memory columns remain evidence and are not presented as query-memory utilization.

## Per-query peak memory

The extraction is [10-query-peak-memory.sql](evidence/queries/10-query-peak-memory.sql), with its [result CSV](evidence/10-query-peak-memory.csv). The `QueryFinish` memory field is the query peak. Each selected row joins uniquely to the existing query evidence using exact start time and normalized query hash; duration, rows and bytes must also match. [All 958 matched peaks](matched-query-memory.csv) preserve query name, family, iteration, raw row count and bytes.

The band shows separate arithmetic means for **Drill-downs · peak memory** and **Interactive aggs · peak memory**, with equal query weights within each iteration and Gaussian weights between iterations. The 64-GiB label is provisioned RAM. These query peaks are separate from the service's in-memory cache footprint, and no memory-utilization or memory-headroom percentage is inferred from them.

## Shared display kernel

Every current chart line uses a centered Gaussian in actual UTC time, with **sigma one hour** and **radius three hours**. End windows renormalize the available observations. Rates smooth their numerator and denominator before dividing; latency starts from original runner durations with no prior median smoothing or decimation. [Display CSV](display-metrics.csv), [memory display CSV](query-memory-display.csv), and exact [11a](processing/chart-data.json)/[11b](latency/chart-data.json) payloads are included. [Provenance](source-provenance.json) and [source hashes](source-manifest.json) document the join and formulas.

The unsmoothed byte/CPU rates and older latency display extraction remain unchanged in the parent directory. Use this package for the current blog display values, including the smoothed **3.25 GB/s** drill-down endpoint. Effective aggregation throughput is logical quotes represented by the A3/A4 answer, not physical raw scanning.

## Reproduce offline

Python 3.9+, standard library only, from the repository root:

```sh
python3 full-path-realtime/quotes/clickhouse-cloud/results_t2/utilization/reader/query-metrics/query-resources/derive.py
python3 full-path-realtime/quotes/clickhouse-cloud/results_t2/utilization/reader/query-metrics/query-resources/derive.py --check
python3 full-path-realtime/quotes/clickhouse-cloud/results_t2/utilization/reader/query-metrics/query-resources/checksums.py --check
```

The first regenerates the two JSON payloads, display/resource CSVs, memory matches and provenance. The second verifies byte-identical reproduction without modifying them; source hashes are checked first. Evidence is frozen beside the adapter so historical log retention does not prevent offline reproduction. Live queries are unnecessary. Credentials are excluded.

The newer fixed-data Hot/Cold experiment for **11c** lives separately under [reader-thread-scaling in PR #46](https://github.com/ClickHouse/CostBench/pull/46). It uses another service configuration and version and must not be mixed into Run 2's ingestion-progress curves.
