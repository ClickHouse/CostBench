# Read-service query thread scaling — 11 October 2026

This is a controlled warm-cache experiment on one 16-vCPU / 64-GiB ClickHouse Cloud read service. It changes the `max_threads` setting, not the physical service size. The six query definitions are copied from the original quotes benchmark.

See [REPORT.md](REPORT.md) for findings and limitations. This experiment does not replace the original continuous-ingestion benchmark or its production charts.

## Evidence

- `queries/`: original query definitions and schema; hashes in `source-manifest.json`.
- `sql/`: metadata extraction queries, plans, pipeline descriptions, and treatment SQL.
- `raw/`: append-only HTTP-summary observations and the predetermined trial schedules.
- `results/query-logs-all.csv`: complete server records, including all profile events and changed settings.
- `results/`: service metadata, plans, capacity and background-activity checks.
- `derived/all-trials.csv`: joined, readable trial ledger, including warmups.
- `derived/scaling-summary.csv`: 12 measured observations per query and thread limit.
- `derived/8-vs-16.csv`: throughput, latency and CPU comparisons, including paired repeat variation.
- `derived/validation.json`: completeness and environment checks.
- `derived/*.svg` and `derived/*.png`: static plots with no smoothing or discarded observations.
- `SHA256SUMS`: hashes of the saved evidence and analysis code.

`D1` is hourly OHLCV bars; `D2` is the risk/liquidity profile. Both scan AAPL using the `(sym, t)` primary-key prefix. `A1` and `A2` query small symbol selections from the daily summary table. `A3` and `A4` scan the full maintained summary table.

## Method

Thread limits: 1, 2, 4, 8, 16. One pilot round and one additional warmup round precede 12 measured rounds. Each round rotates query order and shuffles thread limits with a fixed seed. Queries execute sequentially, with no overlap or automatic retry. Pilot/warmup records stay in the ledger but are excluded from measured summaries.

The query-result cache is disabled. Underlying data, mark and query-condition caches are allowed to warm. Parallel replicas are explicitly disabled, so the variable is query parallelism on one read service. The dataset stays fixed. `FORMAT Null` avoids result-transfer cost; the HTTP summary's nanosecond server elapsed time avoids TLS/network overhead, which matters for the 5–10 ms queries. Client wall time is preserved separately.

Queries have a 30-second server timeout, scan guards, and a non-binding result limit. Existing `LIMIT 20` is preserved for A3. Result row counts and settings are verified against query logs. No observation is removed as an outlier.

Throughput is **uncompressed bytes reported as read / server elapsed seconds**, in decimal GB/s. The primary pooled throughput is `sum(read_bytes) / sum(server_elapsed_s)` for each query and cap. Median per-trial throughput and its IQR are also saved. CPU time is query-log `UserTimeMicroseconds + SystemTimeMicroseconds`; dividing it by elapsed time gives average concurrent CPU use in core equivalents. `peak_threads_usage` includes helper threads and must not be presented as the number of busy CPU cores.

## Reproduce analysis from saved evidence

From this directory, using Python 3 and curl:

```sh
python3 analyze.py
python3 combine.py
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements-plot.txt
.venv/bin/python plot.py
```

`analyze.py` uses only Python's standard library and performs no network access. It rechecks the statistical and saved environment evidence. Run `collect_context.py` only for a live rerun immediately after measurement, not to retroactively inspect a different service state.

## Repeat the live experiment

Use a **new evidence directory** so the completed 2026-10-11 ledger remains intact. Copy the Python scripts, `queries/`, and the metadata SQL from this directory, and create empty `raw/`, `results/` and `derived/` directories.

Supply `CLICKHOUSE_HOST` (bare hostname), `CLICKHOUSE_USER` (default: `default`) and `CLICKHOUSE_PASSWORD` through your private environment. The local repository's git-ignored reader credentials file is also supported as a fallback. Secrets are passed to curl through stdin and are never written into evidence or command arguments.

```sh
python3 capture_before.py
python3 run_test.py --phase pilot --rounds 1
python3 run_test.py --phase warmup --rounds 1
python3 run_test.py --phase measured --rounds 12
```

Wait for the service's normal query-log flush, then:

```sh
python3 collect_logs.py --phase all
python3 analyze.py
python3 collect_context.py
python3 combine.py
.venv/bin/python plot.py
```

If the collector reports missing query IDs, wait for the normal log flush and repeat collection. Do not silently retry failed benchmark queries. The analysis expects exactly 360 measured trials, all six query definitions, and all five caps. The context check expects a 16-vCPU / 64-GiB service; a future physical-resizing experiment needs its own explicit design and evidence directory.

The SQL snapshots first written during the pilot contain that pilot's log comment. `sql/measured-trial-*.sql` contains the exact measured treatment text, and each measured ledger row records its SQL SHA-256. The reproduction runner substitutes a fresh run ID solely for traceability.

After final reporting, run `python3 freeze_evidence.py` to regenerate exact measured SQL and the evidence hashes. Keep the original-frequency combination fixed when extending the slider to additional core settings. The combined plot's right panel divides GB/s by the selected core count; the saved individual-query CPU-normalized figures use measured CPU seconds instead.
