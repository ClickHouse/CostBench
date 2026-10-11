# Measured reader scaling with three cache cases

The experiment completed **432 measured queries** with no failed queries, missing logs, result-cache reads or removed outliers. Measurement window: **2026-10-11 10:40:32.547625–2026-10-11 10:56:15.048798 UTC**. The service remained on the same 59-vCPU / 236-GiB node and ClickHouse 26.6.1.2292, with identical active data parts before and after.

Two original drill-down queries, D1 and D2, each run 12 times per core point and cache case against a fixed 113.2B-quote dataset. Their primary-key filters read about 89.4M quotes and 3.49GB per query; these are actually processed bytes, not the entire dataset divided by latency.

## Combined data processing throughput

| CPU cores | Hot caches, GB/s | Cold, condition cache on, GB/s | Cold, condition cache off, GB/s |
| ---: | ---: | ---: | ---: |
| 2 | 0.803 | 0.725 | 0.731 |
| 4 | 1.521 | 1.365 | 1.379 |
| 8 | 2.726 | 2.461 | 2.519 |
| 16 | 5.459 | 4.107 | 4.463 |
| 30 | 9.627 | 5.973 | 6.558 |
| 59 | 15.560 | 9.448 | 9.848 |

## Throughput per CPU core

This uses measured CPU work, matching 11a: total uncompressed MB / total CPU seconds. It is not GB/s divided by the selected-core setting. Both denominators and actual busy-core counts are saved in the CSV.

| CPU cores | Hot, MB/s/core | Cold, condition on, MB/s/core | Cold, condition off, MB/s/core |
| ---: | ---: | ---: | ---: |
| 2 | 418.5 | 407.0 | 408.7 |
| 4 | 418.6 | 407.1 | 408.5 |
| 8 | 417.8 | 404.2 | 405.9 |
| 16 | 412.3 | 395.0 | 396.9 |
| 30 | 405.3 | 380.5 | 384.0 |
| 59 | 390.3 | 355.4 | 356.7 |

## Scaling and the original 3.25GB/s reference

- **Hot caches:** 5.459GB/s at 16 cores; 15.560GB/s at 59. From 8→16 cores, throughput changes by **2.00×**; from 16→59 by **2.85×** for a 3.69× increase in the setting.
- **Cold · condition cache on:** 4.107GB/s at 16 cores; 9.448GB/s at 59. From 8→16 cores, throughput changes by **1.67×**; from 16→59 by **2.30×** for a 3.69× increase in the setting.
- **Cold · condition cache off:** 4.463GB/s at 16 cores; 9.848GB/s at 59. From 8→16 cores, throughput changes by **1.77×**; from 16→59 by **2.21×** for a 3.69× increase in the setting.

The evidence supports scaling, with the actual cache-dependent gains shown above. It does not justify claiming perfect linear scaling across every point. Cold object reads add waiting time; the measured CPU-normalized rate and selected-core utilization help distinguish processing work from elapsed-time throughput.

The original **3.25GB/s** in 11a is a centered-Gaussian-smoothed display endpoint during continuous fresh ingestion on a 16-vCPU / 64-GiB service running 26.4.1.2029. This repeat uses a static dataset, different physical capacity and a newer version. Its cold modes do not recreate ongoing ingestion or establish the old cache state. Numerical closeness to 3.25 is not a reproduction criterion.

## Query-condition cache comparison

- At **16 cores**, cold reads yield **4.107GB/s with condition caching on** and **4.463GB/s with it off** (+8.7% off versus on).
- At **59 cores**, cold reads yield **9.448GB/s with condition caching on** and **9.848GB/s with it off** (+4.2% off versus on).

The condition-off block ran after the paired hot/cold condition-on block. That timing difference limits causal attribution of small differences. Per-round interquartile ranges and every individual trial are retained; neither case is merged into another.

## Cache and service evidence

| Case | Measured trials | S3 bytes | Page-cache hits | Page-cache misses | Query-condition cache hits |
| --- | ---: | ---: | ---: | ---: | ---: |
| Hot caches | 144 | 0 | 51300 | 0 | 2304 |
| Cold · condition cache on | 144 | 27431908823 | 0 | 0 | 2304 |
| Cold · condition cache off | 144 | 27438375399 | 0 | 0 | 0 |

All cold observations also have zero page-cache read bytes and zero distributed-cache read bytes. Full profile counters and exact settings are in the query-log CSV; the pilot includes the filesystem-off-only case demonstrating why that one setting was insufficient on this Cloud service.

The current cache settings, before/after node identity and active parts, background merges, and query activity are saved under `results/`. The raw trial ledger stores server nanosecond durations and client transport time separately.

## Individual-query detail at 16 and 59 cores

Latency is the median server execution time; memory is the arithmetic mean of per-query peak memory. All other points and quartiles are in the individual-query CSV.

| Cores | Case | Query | Median latency, ms | Average busy cores | Mean peak memory, MiB |
| ---: | --- | --- | ---: | ---: | ---: |
| 16 | Hot caches | D1 | 560.0 | 13.29 | 5.8 |
| 16 | Hot caches | D2 | 717.7 | 13.20 | 4.5 |
| 16 | Cold · condition cache on | D1 | 779.4 | 10.05 | 145.6 |
| 16 | Cold · condition cache on | D2 | 914.6 | 10.70 | 138.4 |
| 16 | Cold · condition cache off | D1 | 712.9 | 10.91 | 149.0 |
| 16 | Cold · condition cache off | D2 | 847.0 | 11.52 | 138.3 |
| 59 | Hot caches | D1 | 201.7 | 39.31 | 6.6 |
| 59 | Hot caches | D2 | 246.1 | 40.33 | 6.4 |
| 59 | Cold · condition cache on | D1 | 346.3 | 25.54 | 259.2 |
| 59 | Cold · condition cache on | D2 | 383.7 | 27.53 | 233.8 |
| 59 | Cold · condition cache off | D1 | 331.0 | 25.98 | 250.7 |
| 59 | Cold · condition cache off | D2 | 353.3 | 29.09 | 238.2 |

The activity extraction also records 1773 other initial completed queries totaling 42.4CPU seconds over the observation window. These include activity outside the test; the service was not globally isolated. The benchmark runner itself issued one workload query at a time.

## Reproduce

Run `python3 analyze_cache.py` and `python3 report.py` offline. See [README.md](README.md) for live repeat instructions, cache definitions and the complete evidence contract.
