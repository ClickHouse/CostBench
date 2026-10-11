# Reader CPU scaling with hot and cold data caches

This experiment measures the original D1 and D2 drill-down queries against the unchanged **113,217,743,918-quote** dataset. It preserves one hot case and two cold cases at **2, 4, 8, 16, 30 and 59 CPU cores**. All points use one fixed **59-vCPU / 236-GiB** ClickHouse Cloud reader; query parallelism changes through `max_threads`. The experiment does not compare six physically resized services.

Start with [the measured findings](REPORT.md), [the 18-point combined CSV](derived/combined-workload.csv), or [every observation](derived/all-trials.csv). [The original queries](queries/queries_raw.sql) and [schema](queries/create.sql) come from the quotes benchmark. The service was running ClickHouse **26.6.1.2292**.

## Cache cases

| Case | Filesystem cache | Userspace page cache with distributed cache | Read through distributed cache | Query-condition cache |
| --- | --- | --- | --- | --- |
| Hot caches | on | on | on | on |
| Cold · condition cache on | off | off | off | on |
| Cold · condition cache off | off | off | off | off |

The exact settings are `enable_filesystem_cache`, `use_page_cache_with_distributed_cache`, `force_read_through_distributed_cache`, and `use_query_condition_cache`. The pilot demonstrated that disabling `enable_filesystem_cache` alone still produced userspace page-cache hits on this service. The cold treatments therefore bypass all three data-cache layers per query. No global cache purge is performed; metadata and mark caches keep their normal configuration. These treatments mean cold **data reads**, not a newly started service with every metadata cache empty.

Hot keeps normal data and query-condition caching enabled. The server's normal query-result and uncompressed-cache defaults are both off, and remain off in all cases. The benchmark measures executed queries rather than retrieval of a cached final answer. [Saved service settings](results/cache-settings.csv), per-query settings, and all profile counters are included.

## Schedule and acceptance

The pilot ran 12 queries, followed by 12 unmeasured hot warmups. The main block pairs hot and cold with query-condition caching enabled, randomizing the treatment order within each query/core cell. It contains 288 measured queries: 12 repeats × two queries × six points × two cases. A second block contains 144 cold queries with query-condition caching disabled. That block runs later, so a difference between the two cold cases includes possible between-block timing variation; it is not a fully interleaved factorial experiment.

One query runs at a time. Fixed seeds and complete plans are saved with run IDs in `raw/*manifest.json`. Every query has a unique ID, exact SQL, SQL hash, HTTP server summary, client wall time, and matching initial `QueryFinish` row. The runner stops on a failure, retains the failed observation, and never silently retries. All successful trials are retained; no outliers are removed.

`collect_logs.py` captures full `ProfileEvents` and `Settings`, not only selected counters. `analyze_cache.py` checks IDs, SQL hashes, row/byte counts, durations, CPU work, settings, result-cache status, and treatment counters. Measured hot queries must have page-cache hits, zero page-cache misses and zero S3 bytes. Measured cold queries must have positive S3 bytes and zero page-cache or distributed-cache read bytes. Condition-cache-off queries must have zero condition-cache hits. Before/after evidence checks active parts, host, server version and capacity.

## Statistics

Both drill-down queries receive equal counts. The combined rate is **total uncompressed bytes / total server execution time**, not an arithmetic average of per-query GB/s. Decimal units are used: GB = 10⁹ bytes and MB = 10⁶ bytes. Client transport time is retained separately and does not enter the rate.

For **Data throughput per CPU core**, use **total bytes / total CPU seconds**, where CPU seconds are summed `UserTimeMicroseconds + SystemTimeMicroseconds`. This is the same normalization used in blog visual 11a. `MB_s_per_selected_core` is also retained in the CSV to make the different denominator explicit. The selected-core setting controls available query parallelism; actual average busy cores are measured separately.

The CSV includes all individual queries, pooled rates, per-query peak memory, average busy cores, latency quartiles, and per-round combined-throughput quartiles. All six points are measured configurations. No numerical smoothing is applied to this experiment.

## Reproduce offline

Python 3, using only the standard library:

```sh
python3 collect_logs.py --help
python3 analyze_cache.py
python3 report.py
python3 derive_visual.py
python3 checksums.py --check
```

The last three commands need no service connection. Do not rerun collection into this completed snapshot unless deliberately replacing an extraction; the frozen CSV is enough for offline analysis. `checksums.py --check` verifies the frozen package. Recomputed statistics should match the committed CSVs.

## Repeat against a live service

Use a **new directory** for a new experiment. Copy the scripts, `queries/`, and context SQL into it, and create empty `raw/`, `results/`, `derived/`, and `sql/` directories. Supply the host, username and password privately through `CLICKHOUSE_HOST`, `CLICKHOUSE_USER`, and `CLICKHOUSE_PASSWORD`. No credentials belong in this repository. `connection.py` also supports the author's ignored local credential file; environment variables make reproduction independent of that file.

First save the results of `sql/00-identity-before.sql`, `03-tables.sql`, `04-columns.sql`, `05-parts-before.sql`, `06-capacity.sql`, `07-background-before.sql`, `08-indexes.sql`, and `cache-settings.sql` to the corresponding filenames under `results/`. Check capacity, schema, row count, background activity and supported settings before running. The supplied live runner checks the expected 59-vCPU identity. Load the original benchmark tables and data before running; this package contains queries and evidence, not the 113.2B-row dataset.

```sh
python3 run_cache_test.py --phase pilot --rounds 1
python3 run_cache_test.py --phase warmup --rounds 1
python3 run_cache_test.py --phase measured --rounds 12
python3 run_condition_cache_off.py
python3 collect_logs.py
python3 capture_after.py
python3 analyze_cache.py
python3 report.py
python3 checksums.py
```

Allow the final query logs to flush naturally before collecting; if IDs are missing, wait briefly and collect again. Do not overlap benchmark queries with collection or other load. The setting choices, counter checks and dataset identity are part of the experiment, not optional cleanup.

## Original Run 2 comparison

The **3.25 GB/s** shown in 11a is a Gaussian-smoothed endpoint from **continuous fresh-data ingestion** in August 2026, on a physically smaller 16-vCPU / 64-GiB reader running ClickHouse 26.4.1.2029. This experiment uses static, settled data and repeated queries on a different service configuration and version. Original Run 2's complete data-cache counters are unavailable. Similar numerical throughput would not establish that its cache state or workload has been reproduced. See the report for the actual 16-core values without forcing them toward that reference.

## Blog visual 11c and its resource band

The published selection is **Hot caches** plus **Cold · condition cache off**. The condition-cache-on cold case remains available for inspection and is not combined with either selected case. [Exact chart payload](derived/11c-chart-data.json), [resource values](derived/11c-resource-utilization.csv), and [source provenance](derived/11c-source-provenance.json) regenerate with `python3 derive_visual.py`. This uses the measured values from `analyze_cache.py`; it never replaces observations with ideal scaling or combines different physical nodes.

The two panels use pooled uncompressed **GB/s** and **MB/s per consumed CPU core**, matching the denominator in 11a. The CPU resource band divides pooled CPU seconds by pooled execution seconds and the selected N-core budget. It excludes scheduling idle time; it is relative to that budget, rather than measured utilization of six resized physical services. **Headroom** is its complement, without implying a quantified increase in concurrent-query capacity.

Memory values are the arithmetic mean of measured per-query peaks for each case/core point. They describe query work, rather than the whole-service cache footprint. The service capacity remains the actual **236 GiB**. The resource CSV saves unrounded percentages and bytes; the visual rounds percentages to integers and memory to one decimal MiB. All six displayed settings are measured: 2, 4, 8, 16, 30, 59. Both horizontal axes place these settings at equal intervals, matching the discrete slider. Curves connect these points, and controls/readouts snap to them.

For the original continuous-ingestion reader metrics and their resource calculations, see [the separate Run 2 package in PR #44](https://github.com/ClickHouse/CostBench/pull/44). Keep that study and this static-data cache experiment distinct.
