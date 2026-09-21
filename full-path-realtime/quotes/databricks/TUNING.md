# Databricks qualification and tuning

`qualify.py` turns `tune-endurance` into a frozen, executable matrix and
evaluates only evidence written by that matrix. Planning is offline: it does
not read credentials, call Databricks, or claim that a case ran.

## Current status

The online qualification stages are **not run**. The producer and explicit
Unity Catalog managed storage are prepared, but Lakehouse//RT is not yet
available in the workspace. No capacity, query-size, endurance, or
qualification result is asserted here, and no warehouse size or throughput
value has been invented. See [INFRASTRUCTURE_SETUP.md](INFRASTRUCTURE_SETUP.md)
for the completed preparation.

The 113,219,565,734-row run is prohibited until `qualification_report.json`
contains exact `"qualified": true`.

The September 16 six-hour characterization is documented in
[SIX_HOUR_INGEST_MV.md](SIX_HOUR_INGEST_MV.md). It is not accepted
qualification evidence: client-side journal work reduced average throughput to
534k EPS over time, and provider metadata reported 75,000 more rows than the
producer journal. The MV remained strictly incremental and Predictive
Optimization ran successfully, but both ingest failures must be resolved before
another endurance attempt.

The September 17 follow-up in [RECOVERY_40M.md](RECOVERY_40M.md) validated both
ingest fixes across three proactive stream-rotation boundaries. It sustained
999,582 average EPS, kept every five-second interval within ±10%, and produced
exact equality between 2,400,607,668 client and provider rows. This permits a
fresh six-hour retry with the same compact journal, automatic recovery
disabled, and ten-minute clean stream rotation; it does not qualify endurance
by itself.

### Preliminary 1M-EPS characterization

Two online characterization runs completed on September 15, 2026. They prove
that the prepared producer and Zerobus path can sustain the target, but they
are not substitutes for the frozen qualification matrix.

- `capacity_1m_20260915T140716Z` committed all 75,000,000 selected rows with no
  errors or ambiguous acknowledgements. It averaged 964,224 committed rows/s
  over 77.78 seconds. Five-second committed-EPS observations were bursty
  because each stream recorded durability only at five-second checkpoints.
- `ingest_mv_10m_20260915T143250Z` changed durability accounting to wait after
  every Arrow batch. It committed all 600,000,000 selected rows with no errors,
  no pending rows, and 16 cleanly closed streams. It averaged 997,778 committed
  rows/s over 601.34 seconds. Of 119 derived five-second intervals, 118 were
  within the 900,000–1,100,000 acceptance band. Peak host CPU was 2.42% and
  peak transmit throughput was 77.23 MB/s.

The second run's live freshness monitor could read Zerobus and raw-table
metadata, but its MV event-log requests were denied because the newly created
MV had not been transferred to the benchmark service principal before launch.
After ownership was corrected, a historical metadata-only recovery completed
without errors. It confirmed 600,000,000 provider records, 43,841,095,016
committed bytes, zero Zerobus errors, raw Delta version 118, and five completed
MV updates. Four updates used incremental `GROUP_AGGREGATE`; the final update
was `NO_OP`.

Incremental refresh completions occurred at 14:36:18.042, 14:39:02.541,
14:42:03.182, and 14:46:31.702 UTC. The final raw commit was at
14:43:24.999 UTC, so the next completed incremental refresh provides a
186.703-second final-drain upper bound. A subsequent `NO_OP` completed at
14:49:49.769 and confirms no source changes remained; using that later proof
point gives a fully conservative observed bound of 384.770 seconds. Each
planning event reports its source snapshot's exact `num_rows`, providing a
metadata row watermark, but reports `ingestion_source_table_version=null`.
Time lag is therefore reconstructed by aligning that watermark with monotonic
producer acknowledgements; it is not represented as exact per-row event-time
lag.

Refresh phase timing shows that compute startup, not the aggregate itself,
dominated freshness. The four incremental updates spent approximately 115,
112, 112, and 199 seconds in `WAITING_FOR_RESOURCES`; their `RUNNING` phases
lasted only about 22, 27, 26, and 23 seconds.

The trial did not preserve an explicit standalone MV performance-mode setting.
Databricks documents standard mode as the default for definition-scheduled
serverless refreshes; it trades slower startup for lower DBU consumption.
Standalone serverless MV refresh does not expose a warehouse or cluster size.

The workspace preview `System-Managed Job for Materialized Views & Streaming
Tables` was enabled on September 15, 2026 after this run. It did not expose a
Performance optimized toggle for the existing `TRIGGER ON UPDATE` MV: the
Catalog Explorer **Edit** link continued to open documentation. Databricks'
detailed toggle instructions specifically refer to a `SCHEDULE` clause.

Do not claim Performance optimized for this path. On the next fresh target,
check once while the creating user still owns the MV, before transferring
ownership to the benchmark principal. If the control remains unavailable, the
trigger-on-update result represents standard mode. A performance-optimized
system-managed schedule/SQL job or continuous Lakeflow pipeline must be
qualified and reported as a separate refresh architecture.

## Create the frozen plan

Supply non-secret environment-specific values and list query sizes in provider
order, smallest first:

```sh
"$DATABRICKS_RUNNER_PYTHON" qualify.py plan \
  --output-dir /absolute/path/to/qualification \
  --repo-dir /Users/lio/Clickhouse/CostBench \
  --data-dir /absolute/path/to/parquet \
  --catalog costbench \
  --schema rt_qualification \
  --rt-warehouse-id RT_WAREHOUSE_ID \
  --control-warehouse-id CONTROL_WAREHOUSE_ID \
  --producer-host-description 'dedicated producer host and network placement' \
  --producer-network-capacity-gbps USABLE_LINK_GBPS \
  --host https://WORKSPACE_HOST \
  --endpoint https://ZEROBUS_ENDPOINT \
  --stream-count 16 \
  --batch-size 50000 \
  --queue-capacity 64 \
  --compression NONE \
  --query-size-candidates 2X-Small X-Small Small Medium \
  --python "$DATABRICKS_RUNNER_PYTHON" \
  --producer-python "$DATABRICKS_ZEROBUS_PYTHON"
```

The command writes `qualification_plan.json` and executable
`qualification_commands.sh` atomically. Credential values are never included.
The generated plan uses the runner environment for preflight, metadata, and
query processes and the separate Zerobus environment for ingestion.
Online scripts read either `DATABRICKS_TOKEN` or
`DATABRICKS_CLIENT_ID`/`DATABRICKS_CLIENT_SECRET` from the environment.

Create `costbench.rt_qualification` once. It may inherit the explicit,
non-default managed location configured on the `costbench` catalog. All
qualification cases reuse that schema sequentially. Before every case, review
and apply `create.sql` so the raw table and materialized view are dropped and
recreated empty. Never run two qualification cases concurrently.

Run one case section at a time, for example
`QUALIFY_CASE=capacity-100k ./qualification_commands.sh`, after setting that
case's printed clean-target confirmation variable. Query-size and endurance
sections also require the explicit provisioning confirmation printed by the
script. `QUALIFY_CASE=evaluate` runs the final evaluator after
`QUALIFICATION_PLAN` and `QUALIFICATION_REPORT` are set.

Do not change the matrix JSON after planning. Its digest is checked during
evaluation. If an input changes, generate a new output directory and plan.

## Clean-target workflow

Run stages in this order and stop after any failure:

1. **10M correctness.** Provision the declared configuration, reset the
   dedicated qualification schema with `create.sql`, run online
   preflight, then run the producer, freshness monitor, four-query dashboard,
   two-query drilldown, evidence collection, cost summary, and validation.
   The producer command uses exact `--max-rows 10000000` with
   `--expected-rows 10000000 --allow-partial`. `--max-rows` is required because
   a row-group-only cap can overshoot 10M; the producer now truncates only the
   final selected row group and records an exact 10M manifest.
2. **Capacity.** Repeat on three new targets at 100k, 500k, and 1M committed
   EPS. Each case selects an exact row count for 75 seconds at its target. With
   5-second metrics this permits one warm-up plus at least ten measured
   intervals. Never resume a capacity case onto another target.
3. **Query-size sweep.** Reset the same qualification schema and use a distinct
   run ID and artifact directory for every size. Stop measured work before
   provisioning or resizing. Explicitly provision the next size, record the
   size-to-warehouse mapping, run online preflight, then run one uncached
   four-query dashboard iteration and one uncached two-query drilldown
   iteration during fixed 1M EPS ingestion. Missing size, queue,
   waiting-at-capacity, cache, canonical timing, or error evidence rejects that
   size.
4. **Endurance.** Provision the smallest passing query size and repeat online
   preflight on another fresh target. Keep the accepted 1M producer stream
   count, batch size, compression, host/endpoint, query size, and autoscaling
   settings unchanged. Run 1M EPS for 1,800–3,600 seconds with dashboard fixed
   at 600-second cadence, drilldown fixed at 3,600-second cadence, and the
   freshness monitor active. The default is 1,800 seconds. It requires one
   accepted active-ingest drilldown; a second sample scheduled exactly at a
   3,600-second producer endpoint would race completed ingestion and is not an
   acceptance requirement.

After all sweep cases, run the evaluator once to produce an interim,
non-qualified report. A successful sweep records `selected_query_size` even
though endurance remains `not_run`; use that exact value as
`SELECTED_QUERY_SIZE` for the endurance section. The evaluator's nonzero exit
at this interim point is expected and must not be treated as final
qualification.

`create.sql` contains destructive `DROP` statements. Apply it only to the
dedicated `rt_qualification` schema, after the previous case has stopped and
its artifacts have been preserved. Never point it at `rt_full_<run_id>`, a
historical result target, or a schema used by another workload. Never resize a
warehouse during a live measurement.

## Record query-size provisioning

For each `query_size_sweep` case, record the declared candidate and actual
warehouse ID in the infrastructure change record. Preserve the corresponding
`preflight_report.json`. The evaluator requires both preflight metadata and
every runner record to report the exact candidate size. Candidate order in the
plan is the selection order; the first passing candidate is selected.

If separate warehouse IDs are used, update the environment and generate a new
plan before running. Do not edit the frozen plan or silently repoint a case.

## Evaluate qualification

After all commands have completed and settled evidence has been collected:

```sh
"$DATABRICKS_RUNNER_PYTHON" qualify.py evaluate \
  --plan /absolute/path/to/qualification/qualification_plan.json \
  --output /absolute/path/to/qualification/qualification_report.json
```

The evaluator performs no network calls. It distinguishes:

- `not_run`: one or more required artifacts are absent;
- `failed`: evidence exists but is invalid or fails an acceptance gate;
- `passed`: the case has complete passing evidence;
- `planned`: the state of every case in the unevaluated plan.

Qualification requires exact 10M validation, clean and in-tolerance capacity
evidence, monotonic capacity, a passing query size with no queue/cache errors,
and accepted endurance evidence. Endurance must have a 30–60 minute measured
window (the default plan uses 1,800 seconds), at least one accepted active-
ingest drilldown, the planned number of pre-endpoint dashboard iterations,
accepted freshness, no errors, and no producer or RT warehouse configuration
drift from the accepted 1M case. Capacity and endurance also require Linux host
CPU and network telemetry. By default, p95 host CPU must remain at or below 90%
and p95 receive/transmit utilization must remain at or below 80% of
`--producer-network-capacity-gbps`. Supply the evidenced usable link capacity,
not an unverified marketing maximum.

Only a fully passing report includes frozen settings and returns exit status
zero. Missing evidence never passes.
