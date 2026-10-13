# Reader CPU scaling on a 59-vCPU service

Controlled warm-cache measurements for the six original quotes-benchmark queries against a fixed **113.2B-quote dataset**. The curve uses **CPU cores** at 2, 4, 8, 16, 30 and 59. Parallelism is controlled with `max_threads` while the physical read service stays at **59 vCPUs / 236 GiB**. This isolates query parallelism; it does not measure six physically resized service tiers.

See [REPORT.md](REPORT.md) for measured findings, environment checks and limits. The [16-vCPU baseline](../2026-10-11/README.md) is preserved separately. All six points in this folder come from the larger service; lower-core points are not spliced from the baseline.

## Evidence map

| Path | Contents |
|---|---|
| `run-design.json` | Fixed settings, repetition count, dataset and workload weights |
| `queries/`, `source-manifest.json` | Original query/schema definitions and source hashes |
| `sql/` | Exact treatment, extraction and pipeline SQL |
| `raw/*.jsonl` | Append-only observations, including pilot and warmup |
| `raw/*-manifest.json` | Predetermined shuffled execution order |
| `results/query-logs-all.csv` | Initial query records, full ProfileEvents and changed settings |
| `results/` | Before/after identity, parts, capacity and background activity |
| `derived/all-trials.csv` | Complete joined ledger |
| `derived/scaling-summary.csv` | Individual-query pooled throughput and timing distributions |
| `derived/combined-workload.csv` | Fixed primary workload mix and separate sensitivity mix |
| `derived/combined-rounds.csv` | Combined rates for each measured round |
| `derived/adjacent-core-comparisons.csv` | Individual-query speedups for neighboring core settings |
| `derived/cross-node-same-cap.csv` | Same-cap comparison with the 16-vCPU baseline |
| `derived/validation.json` | Completeness, cache and environment checks |
| `derived/*.png`, `derived/*.svg` | Evidence-native plots without smoothing |
| `SHA256SUMS` | Hashes of saved evidence, reports and code |

## Measurement contract

The six queries are D1/D2 (selective drill-downs on AAPL in the raw table), A1/A2 (small symbol selections from the daily summary), and A3/A4 (full scans of the maintained summary table). The raw table contains exactly **113,217,743,918 quotes**, and the summary table has **1,860,254 rows**. Each drill-down scans about 89.4M rows / 3.49GB uncompressed; neither scans the entire raw dataset.

One pilot round and one additional warmup round precede 12 measured rounds: 36 + 36 + **432 executions**. Query order rotates by round; core settings are shuffled using seed 6112026. Queries run sequentially, with no overlapping test queries, automatic retries or discarded outliers. The result cache is explicitly disabled; underlying data, mark and query-condition caches may warm. Parallel replicas are explicitly disabled.

**Throughput:** uncompressed read bytes / server elapsed seconds, in decimal GB/s. Each individual-query summary pools bytes and elapsed time. The primary combined mix mirrors the original frequency: D1 and D2 once each, A1–A4 six times each. Its rate is weighted bytes / weighted execution time, excluding scheduling idle gaps. The alternative one-of-each mix and the explicitly scoped drill-down-only subset are saved separately. All weights remain fixed across core settings. The drill-down-only plots also show actual read rows per second; index-skipped rows are excluded. For mixed raw/summary queries, `million_read_rows_s` counts storage rows and must not be called raw quotes per second.

**Timing:** HTTP-summary `elapsed_ns` measures server execution; client wall time, including transport, is also preserved. `FORMAT Null` avoids result-transfer overhead. These timings should not be described as application end-to-end response times. Query-log rounded milliseconds are cross-checked against the HTTP timing.

**CPU:** `UserTimeMicroseconds + SystemTimeMicroseconds`, divided by server elapsed time, gives average concurrent CPU use in core equivalents. Tiny queries can lack nonzero CPU counters; those CPU-derived fields remain unavailable rather than treating real work as zero. Their measured throughput and timing observations remain included. Peak thread counts include helper threads and do not equal busy CPU cores.

**Per-core panel:** the combined GB/s divided by the selected CPU-core count, converted to MB/s/core. This is throughput per unit of configured parallelism. The existing ingestion-progress visual normalizes by measured CPU work; these definitions must remain explicit when comparing charts.

All test queries have a 30-second server timeout, scan guards and a non-binding result limit. A3 retains its original `LIMIT 20`. Exact measured SQL is saved and verified against every ledger row's SHA-256.

## Reproduce offline

From this directory, Python 3 is sufficient for analysis; no credentials or network access are required:

```sh
python3 analyze.py
python3 combine.py
python3 extend_analysis.py
python3 report.py
```

`extend_analysis.py` reads the neighboring baseline folder for the same-cap bridge. To regenerate plots:

```sh
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements-plot.txt
.venv/bin/python plot.py
```

The analysis verifies all trial IDs, settings, byte/row counters, cache usage and saved before/after context. Plot shading is the interquartile range across 12 repeats. Bootstrap intervals describe repeat variation in this session; they do not establish portability or a production tail-latency SLA.

Verify the saved package before regeneration with `shasum -a 256 -c SHA256SUMS`. Regenerated SVG metadata can differ between rendering times even when data and plots agree.

## Repeat live

Keep this completed ledger intact. Create a new directory, copy the Python scripts, `queries/`, `source-manifest.json`, `run-design.json` and the metadata SQL files `00-identity-before.sql` through `10-log-fields.sql` into `sql/`, then create empty `raw/`, `results/` and `derived/` directories. Provide `CLICKHOUSE_HOST` (bare hostname), `CLICKHOUSE_USER` (default `default`) and `CLICKHOUSE_PASSWORD` through a private environment. Collection used Python 3.9.6; plotting used Python 3.12.14 and matplotlib 3.11.2. Python and curl are sufficient for collection. The git-ignored local reader credentials file is an optional fallback; secrets pass to curl through stdin and are never written into evidence or command arguments.

```sh
python3 capture_before.py
python3 run_test.py --phase pilot --rounds 1
python3 run_test.py --phase warmup --rounds 1
python3 run_test.py --phase measured --rounds 12
```

The preflight expects a 59-vCPU / 236-GiB reader and the fixed dataset. After the service's normal log flush:

```sh
python3 collect_logs.py --phase all
python3 analyze.py
python3 collect_context.py
python3 analyze.py
python3 combine.py
python3 extend_analysis.py
python3 report.py
.venv/bin/python plot.py
```

If query IDs have not flushed yet, wait and repeat log collection. Do not retry failed benchmark executions silently. The analysis expects exactly 432 measured observations and one host/version/capacity for the run. A restart, resize or data change invalidates this controlled comparison. The selected query mix and weighting remain fixed at every core setting.

After writing the final report, `python3 freeze_evidence.py` recreates exact measured SQL, validates its hashes and freezes the package. Live repeat scripts replace only the traceability run ID. Normal Cloud monitoring and system-table housekeeping are measured separately; the service is not assumed to be otherwise idle.

## References

The workload follows the ClickHouse `agent-query-safety` rule (readonly access and explicit row, scan and time guards) and `schema-pk-filter-on-orderby` rule (verified `sym` prefix use in the saved plans). A3/A4 intentionally scan the small complete summary table within the benchmark's bounds.

Official references: [max_threads](https://clickhouse.com/docs/reference/settings/session-settings/max-threads#max_threads), [Query complexity restrictions](https://clickhouse.com/docs/operations/settings/query-complexity), [Choosing a primary key](https://clickhouse.com/docs/best-practices/choosing-a-primary-key).
