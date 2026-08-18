# BigQuery ingestion tuning record

This file records the 2026-08-10 preparation used to choose the Storage Write
API settings for the full-path quotes benchmark. These short trials are capacity
tests, not publishable endurance-run results.

## Scope and evidence

- BigQuery location: `US` multi-region.
- Ingestion path: application-created `COMMITTED` Storage Write API streams,
  serialized Arrow record batches, and explicit per-stream offsets.
- Every trial used a fresh dataset and created both the clustered raw table and
  the native materialized view from `create.sql`.
- Automatic MV maintenance was therefore active. Dashboard, drill-down, and
  freshness-monitor processes were not running during these short trials.
- Throughput is client-observed acknowledged rows divided by elapsed ingest
  time. Provider `WRITE_API_TIMELINE_BY_PROJECT` reconciliation remains a gate
  for the full-path prep and final runs.
- Raw summaries remain on the benchmark host under
  `/home/ubuntu/bigquery/results_tuning/<run-id>/ingest/ingest_summary.json`.
- The benchmark host reported itself as `m6i.8xlarge` in AWS `us-east-2`.

The earlier verified 5M-row smoke run recorded 457,832,696 provider input
bytes, or 91.57 bytes per row. At 1M rows/s that implies approximately
91.6 MB/s of provider-observed input, well below the 3 GB/s `US` multi-region
project quota.

## Source and batch selection

The first tuning source file was:

```text
/home/ubuntu/data/stockhouse/quotes_2025-10-16.parquet
rows:              807,905,882
row groups:        6,176
minimum rows/RG:   104,732
median rows/RG:    130,818
maximum rows/RG:   130,818
```

`--batch-size 131000` keeps every source row group in one Arrow batch. Typical
serialized batches were approximately 12 MB. `--max-request-bytes 16000000`
keeps headroom below the Storage Write API gRPC `AppendRows` limit of 20 MB.
The ingester's `--max-row-groups` option was added so short trials could finish
normally and reconcile exact expected and acknowledged row counts without
processing the entire 807.9M-row file.

## Completed trials

All completed trials used:

```text
batch size:          131,000 rows
max request bytes:   16,000,000
row groups/worker:   10
retries:             0
errors:              0
```

| Run ID | Writers | Rows | Requests | Duration (s) | Acknowledged rows/s |
|---|---:|---:|---:|---:|---:|
| `quotes_tune_p16_20260810_115118` | 16 | 20,930,880 | 160 | 43.244 | 484,023 |
| `quotes_tune_p32_20260810_115208` | 32 | 41,861,760 | 320 | 49.190 | 851,028 |
| `quotes_tune_p48_20260810_115401` | 48 | 62,792,640 | 480 | 48.719 | 1,288,871 |
| `quotes_tune_p40_20260810_115539` | 40 | 52,327,200 | 400 | 49.508 | 1,056,944 |

A two-writer, 383-row-group diagnostic was intentionally stopped early after
showing roughly 70k acknowledged rows/s during its first minute. Because it did
not complete its fixed input slice, it is excluded from the completed-trial
table and must not be combined with those results.

## Steady-state correction

Each completed capacity trial above contained only ten row groups per writer.
Startup, source decoding, stream creation, and the slowest finishing writers
therefore occupied a large fraction of the approximately 49-second window.
Those results compare configurations consistently, but they do not directly
predict a day-long run's steady-state rate.

Longer full-input diagnostics produced the following live boxed metrics before
being intentionally stopped:

| Writers | Observed steady-state behavior | Interpretation |
|---:|---:|---|
| 40 | approximately 1.45M acknowledged rows/s | materially too fast for the 1M target |
| 30 | approximately 1.12M acknowledged rows/s | still above the target |
| 28 | approximately 1M acknowledged rows/s | selected sweet spot |

These were preparation diagnostics, not completed fixed-input trials, so they
do not belong in the completed-trial table and no more precise p28 result is
claimed here. Their purpose was to select a long-run operating point, not to
produce a benchmark result.

## Initially selected settings

The first full-run attempt used:

```text
--parallel 28
--batch-size 131000
--max-request-bytes 16000000
--metrics-interval 10
--quiet-worker-logs
```

This keeps the established one-request-per-row-group geometry and adjusts only
the number of committed streams. The 30- and 40-writer observations are
higher-throughput diagnostics, not automatic fallbacks: changing writers after
the final measurement begins would create a different run configuration.

## Long-run client-memory correction

The first attempted full T1 run is superseded and is not benchmark evidence. It
stopped after acknowledging 5,231,194,003 rows because three 120-second append
waits timed out during severe host memory reclaim. BigQuery had committed each
batch: every replay returned `ALREADY_EXISTS` with
`expected_offset = requested_offset + batch_rows`. The older client retried
those already-written batches until its retry budget expired.

Host telemetry identified the initiating failure mode, not a BigQuery quota:

- anonymous memory reached approximately 126.7 GiB on the 128 GiB host;
- `MemAvailable` fell to approximately 1.4 GiB;
- the host recorded heavy page scanning, major faults, NVMe reads, and blocked
  processes;
- network-interface errors and kernel OOM events were absent.

The current ingester therefore:

1. accepts an exact-offset `ALREADY_EXISTS` replay as one acknowledged batch,
   records the recovery, and reopens the append connection;
2. retains only the current Parquet reader per worker instead of every prior
   file footer;
3. records process RSS, peak RSS, Linux `MemAvailable`, and Arrow current/peak
   allocation in every ingest metric;
4. runs Python garbage collection plus glibc `malloc_trim(0)` every 60 seconds;
5. stops cleanly if system available memory falls below the explicit 16 GiB
   guard used by the full-run command.

The trim is part of the declared client configuration. It does not change row
semantics or BigQuery capacity, and its duration is retained in the metrics so
any performance disturbance remains auditable.

## Explicit source-rate correction

Writer count by itself did not reproduce a stable 1M-EPS source rate. After the
memory correction, two longer fresh-dataset validations on the same host and
day diverged materially:

| Validation | Writers | Rows | Approximate ingest behavior | Retries/errors |
|---|---:|---:|---:|---:|
| memory endurance | 28 | 600,062,166 | about 0.79M EPS while active | 0/0 |
| capacity headroom | 36 | 300,096,492 | about 1.64M steady EPS | 0/0 |

The 28-writer validation reconciled exactly to 4,587 successful provider
requests and 600,062,166 provider rows, so the lower rate was not data loss.
The variation makes concurrency an unreliable rate-control mechanism.

The canonical run now uses:

```text
--parallel 40
--target-eps 1000000
--batch-size 131000
--max-request-bytes 16000000
--metrics-interval 10
--memory-trim-interval 60
--min-system-available-gib 16
--quiet-worker-logs
```

Forty writers provide headroom; the global limiter serializes append starts at
the declared source rate and does not issue catch-up bursts after a stall. A
130,818,000-row validation converged to approximately 1M acknowledged EPS after
startup, completed with no retries or errors, and retained bounded memory.
The ingester records both `target_eps` and rate-limiter state in its manifest
and metrics. Final `elapsed_sec` ends at the last row acknowledgement; separate
`wall_elapsed_sec` retains stream-finalization and cleanup time.

## Remaining validation

Before treating the settings as final-run evidence:

1. Run the complete full-path workload in a fresh dataset with the selected
   40-writer, 1M-EPS-capped configuration.
2. Confirm the acknowledged rate remains approximately 1M rows/s while all
   query and freshness workloads are active. If it does not, finish or abort
   that run explicitly and retune in a separate fresh dataset; do not change
   writer count midway through a measured run.
3. Export `WRITE_API_TIMELINE_BY_PROJECT` with an explicit start timestamp and
   reconcile expected rows, client acknowledgments, provider rows, requests,
   input bytes, and non-`OK` buckets.
4. Preserve the raw tuning summaries with the final evidence bundle if tuning
   numbers are cited publicly. Otherwise describe them only as preparation.
