# Read-service CPU scaling experiments

Visual numbers follow the current Part 6 article. Existing filenames retain their earlier identifiers so reproduction commands and links stay compatible.

Controlled experiments using the original quotes-benchmark queries against the fixed 113.2B-quote dataset. Visual 18 presents the combined D1/D2 drill-down workload with Hot and Cold cache cases and uses **CPU cores** for its horizontal axis and slider. Query parallelism is controlled with `max_threads`; physical service capacity, memory, server version and node identity are recorded separately.

The live test requires the benchmark raw and maintained summary tables to be loaded first. See the [quotes benchmark contract and implementation](../../../README.md) and the frozen schema in each experiment. Offline analysis needs only the files in this package.

## Current blog selection: 18

Use the [cache-controlled experiment](2026-10-11-cache-modes/README.md) for 18: Hot caches and Cold data caches with query-condition caching off, measured at every core point on the same 59-vCPU / 236-GiB node. Its [findings](2026-10-11-cache-modes/REPORT.md), [all cache-case results](2026-10-11-cache-modes/derived/combined-workload.csv), [exact 18 data](2026-10-11-cache-modes/derived/11c-chart-data.json), and [CPU/headroom plus per-query memory peaks](2026-10-11-cache-modes/derived/11c-resource-utilization.csv) are reproducible offline.

Earlier exploratory runs below are preserved as separate evidence. They are not spliced into the cache-controlled curves. The throughput-per-core panel in 18 uses consumed CPU seconds, as 16 does; a separate selected-core denominator remains in the CSV.

## Earlier completed evidence

- [2026-10-11: 16-vCPU / 64-GiB baseline](2026-10-11/README.md)
- [2026-10-11: 59-vCPU / 236-GiB extended experiment](2026-10-11-59-vcpu/README.md)
- [Extended measured findings](2026-10-11-59-vcpu/REPORT.md)
- [Three workload options](2026-10-11-59-vcpu/derived/workload-options.png)
- [Drill-down GB/s and quotes/s options](2026-10-11-59-vcpu/derived/drilldown-metric-options.png)
- [Complete scaling CSV](2026-10-11-59-vcpu/derived/combined-workload.csv)

The extended experiment measures **2, 4, 8, 16, 30 and 59 cores on one fixed 59-vCPU / 236-GiB reader**. All six points come from that node. The separate baseline provides a same-cap comparison across the resize; its lower-core observations are not spliced into the extended curve. Both runs use ClickHouse 26.6.1.2292 and the same unchanged benchmark data parts.

The combined drill-down subset rises from **0.80 to 15.55 GB/s** across 2→59 cores. From 8→16 it doubles from **2.73 to 5.46 GB/s**. Gains taper at larger settings. The original-frequency mix reaches **11.17 GB/s at 59 cores** because it also includes small aggregations. The evidence supports strong scaling for the substantial scans; it does not establish perfect linear scaling for every query or performance across six physically resized service tiers.

## Fixed combination

Primary weights mirror the original benchmark schedule: D1 and D2 once each, A1–A4 six times each per reference hour. For every setting, calculate weighted total uncompressed bytes divided by weighted total query execution time. Scheduling idle gaps are excluded. Save individual-query observations even when the visual shows a single combined rate.

The evidence includes source SQL, exact treatment SQL, raw HTTP summaries, complete server-query-log CSVs, cache/profile counters, before/after context, derived statistics, offline analysis and plotting code, dependency versions, and SHA-256 hashes. No credentials are included. Keep each completed ledger intact.

The extended analysis also saves an equal-count mix and an explicitly scoped drill-down-only subset. The latter can be expressed in GB/s or in actually processed quotes/s. Index-skipped rows are excluded. The earlier exploratory plots include throughput per selected core. Current 18 uses the consumed-CPU denominator, matching 16; both are saved explicitly in the cache-controlled CSV.

Start with the extended README to reproduce offline. Each folder includes its own exact SQL, raw ledgers, server log CSVs, metadata, scripts, plots and SHA-256 manifest. Live repeat instructions require a new evidence directory so the completed ledgers remain intact. Service credentials are supplied privately through environment variables and are excluded from the package.
