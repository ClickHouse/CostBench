# BigQuery full-run sequence

This is the canonical launch sequence for the complete BigQuery T2 run.
It uses 40 committed Storage Write API streams for capacity headroom and a
global fixed-rate source cap of 1M rows per second. Writer count alone was not
stable enough to reproduce the target across cold and warm validation runs.

This sequence is specifically the **on-demand** query-compute run. Do not add a
reservation after it starts. For a separate fixed-capacity rerun with a declared
N-slot resource envelope, complete `SLOTS_AND_RESERVATIONS.md` first and use a
fresh run identity and dataset.

The four workloads are:

1. Storage Write API ingest;
2. materialized-view dashboard queries every ten minutes;
3. raw-table drill-down queries every hour;
4. materialized-view refresh-watermark sampling every minute.

Use a **fresh dataset and run ID** for the real run. Do not add
`--allow-nonempty-table`, `--max-files`, or `--max-row-groups`.

## 0. Setup shell — create the fresh run

Run this in a separate setup shell:

```bash
cd /home/ubuntu/bigquery
source .venv/bin/activate

pgrep -af '[i]ngest_parquet_rows.py'
```

The last command must return no ingester. If it shows an old test, return to
that terminal, stop it with `Ctrl-C`, and repeat the check. Then run:

```bash
export GOOGLE_CLOUD_PROJECT=pmm-project-377716
export BQ_LOCATION=US
export BQ_STAMP="$(date -u +%Y%m%d_%H%M%S)"
export BQ_DATASET="quotes_full_t2_${BQ_STAMP}"
export BQ_RUN_ID="bq-full-t2-${BQ_STAMP}"
export BQ_RESULTS="$PWD/results_t2/$BQ_RUN_ID"
export SOURCE_HOST_REGION=us-east-2
export BQ_PRICING_MODEL=on_demand

mkdir -p "$BQ_RESULTS"
export BQ_RUN_ENV="$BQ_RESULTS/run.env"

printf 'export GOOGLE_CLOUD_PROJECT=%q\nexport BQ_LOCATION=%q\nexport BQ_DATASET=%q\nexport BQ_RUN_ID=%q\nexport BQ_RESULTS=%q\nexport SOURCE_HOST_REGION=%q\nexport BQ_PRICING_MODEL=%q\n' \
  "$GOOGLE_CLOUD_PROJECT" \
  "$BQ_LOCATION" \
  "$BQ_DATASET" \
  "$BQ_RUN_ID" \
  "$BQ_RESULTS" \
  "$SOURCE_HOST_REGION" \
  "$BQ_PRICING_MODEL" \
  > "$BQ_RUN_ENV"

ln -sfn "$BQ_RUN_ENV" \
  /home/ubuntu/bigquery/results_t2/current.env
```

Create the dataset and run the preflight:

```bash
python3 setup.py \
  --project "$GOOGLE_CLOUD_PROJECT" \
  --dataset "$BQ_DATASET" \
  --location "$BQ_LOCATION" \
  --run-id "$BQ_RUN_ID"

python3 preflight.py \
  --project "$GOOGLE_CLOUD_PROJECT" \
  --dataset "$BQ_DATASET" \
  --location "$BQ_LOCATION" \
  --online \
  --output "$BQ_RESULTS/preflight.json"

jq -e \
  '(.offline.errors // []) == [] and (.online.errors // []) == []' \
  "$BQ_RESULTS/preflight.json"

printf 'BQ_DATASET=%s\nBQ_RUN_ID=%s\nBQ_RESULTS=%s\n' \
  "$BQ_DATASET" "$BQ_RUN_ID" "$BQ_RESULTS"
```

Continue only if the `jq` command exits successfully. All four terminals below
source the same `current.env`, which prevents dataset, run-ID, and output-path
drift between workloads.

## 1. Terminal 1: ingest

```bash
tmux new -s bq_t2_ingest
cd /home/ubuntu/bigquery
source .venv/bin/activate
source /home/ubuntu/bigquery/results_t2/current.env

python3 ingest_parquet_rows.py \
  --dir ~/data/stockhouse \
  --pattern '*.parquet' \
  --project "$GOOGLE_CLOUD_PROJECT" \
  --dataset "$BQ_DATASET" \
  --location "$BQ_LOCATION" \
  --table quotes \
  --create-sql create.sql \
  --parallel 40 \
  --target-eps 1000000 \
  --batch-size 131000 \
  --max-request-bytes 16000000 \
  --metrics-interval 10 \
  --memory-trim-interval 60 \
  --min-system-available-gib 16 \
  --quiet-worker-logs \
  --output-dir "$BQ_RESULTS/ingest" \
  --progress-file "$BQ_RESULTS/ingest/ingest_progress.json" \
  --run-id "$BQ_RUN_ID"
```

Wait for the first boxed `INGEST STATUS` display. It should show `40/40`
workers, normally zero retries, no errors, and throughput converging around 1M
acknowledged EPS. The early average includes startup and is not the steady-state
rate. The `MEMORY` line must remain a bounded sawtooth: scheduled trims should
return RSS toward its prior baseline, and `AVAILABLE` must remain comfortably
above the 16 GiB safety floor. Crossing that floor makes the ingester stop after
its in-flight appends instead of allowing host-wide reclaim stalls to turn
successful writes into ambiguous client timeouts.

Detach with `Ctrl-B`, then `D`.

## 2. Terminal 2: dashboard queries

Start this immediately after ingest:

```bash
tmux new -s bq_t2_dashboard
cd /home/ubuntu/bigquery
source .venv/bin/activate
source /home/ubuntu/bigquery/results_t2/current.env

./run_dashboard.sh \
  --project "$GOOGLE_CLOUD_PROJECT" \
  --dataset "$BQ_DATASET" \
  --location "$BQ_LOCATION" \
  --output-dir "$BQ_RESULTS/mv" \
  --progress-file "$BQ_RESULTS/ingest/ingest_progress.json" \
  --run-id "$BQ_RUN_ID" \
  "BigQuery (GCP)" "serverless" "serverless" "full T2 final" 0
```

Wait for the first four query results, then detach with `Ctrl-B`, then `D`.

## 3. Terminal 3: drill-down queries

```bash
tmux new -s bq_t2_drilldown
cd /home/ubuntu/bigquery
source .venv/bin/activate
source /home/ubuntu/bigquery/results_t2/current.env

./run_drilldown.sh \
  --project "$GOOGLE_CLOUD_PROJECT" \
  --dataset "$BQ_DATASET" \
  --location "$BQ_LOCATION" \
  --output-dir "$BQ_RESULTS/raw" \
  --progress-file "$BQ_RESULTS/ingest/ingest_progress.json" \
  --run-id "$BQ_RUN_ID" \
  "BigQuery (GCP)" "serverless" "serverless" "full T2 final" 0
```

Wait for the first two query results, then detach with `Ctrl-B`, then `D`.

## 4. Terminal 4: MV freshness monitor

```bash
tmux new -s bq_t2_freshness
cd /home/ubuntu/bigquery
source .venv/bin/activate
source /home/ubuntu/bigquery/results_t2/current.env

python3 monitor_mv.py \
  --project "$GOOGLE_CLOUD_PROJECT" \
  --dataset "$BQ_DATASET" \
  --location "$BQ_LOCATION" \
  --progress-file "$BQ_RESULTS/ingest/ingest_progress.json" \
  --output-dir "$BQ_RESULTS/freshness" \
  --run-id "$BQ_RUN_ID"
```

Wait for the first freshness sample, then detach with `Ctrl-B`, then `D`.

## 5. Check the run after about five minutes

From any shell:

```bash
cd /home/ubuntu/bigquery
source .venv/bin/activate
source /home/ubuntu/bigquery/results_t2/current.env

tmux ls

jq '{
  acknowledged_rows,
  expected_input_rows,
  elapsed_sec,
  average_ack_rows_per_sec,
  active_workers,
  append_requests,
  retries,
  recovered_already_exists_appends,
  recovered_already_exists_rows,
  recovered_server_row_count_appends,
  recovered_server_row_count_rows,
  rate_limiter,
  memory: (.memory | {
    process_rss_bytes,
    system_available_bytes,
    arrow_allocated_bytes,
    trim_attempts,
    trim_reclaimed_rss_bytes,
    last_trim_duration_sec
  }),
  errors
}' "$BQ_RESULTS/ingest/ingest_progress.json"
```

The checkpoint passes when all four tmux sessions are alive, the ingester has
40 active workers, the sustained acknowledged rate is approximately 1M rows/s,
and `errors` remains empty. Zero retries is ideal. A nonzero retry or recovered
`ALREADY_EXISTS` count is not itself a correctness failure: it represents an
idempotent replay after an ambiguous response and remains visible in the
evidence. A server-row-count recovery is the same committed-row reconciliation
performed before a replay was needed. Stop the run if any worker error appears.
A brief fluctuation around
the target is normal; do not tune from a single ten-second interval.

Also stop and investigate if RSS rises across multiple completed trim cycles,
if system available memory trends toward 16 GiB, or if a trim takes long enough
to disturb the 120-second append timeout. Do not disable memory trimming for
the current Python/Arrow client on this host.

Attach to a session when needed:

```bash
tmux attach -t bq_t2_ingest
tmux attach -t bq_t2_dashboard
tmux attach -t bq_t2_drilldown
tmux attach -t bq_t2_freshness
```

## 6. Finish the measurement window correctly

When ingest completes, do not immediately stop the query runners. Allow both
the dashboard and drill-down runners to record one scheduled iteration at the
final raw-row count. Those observations are the `active_endpoint` measurements
defined by the benchmark contract. Then stop the dashboard, drill-down, and
freshness processes with `Ctrl-C` in their tmux sessions.

Immediately after the measured workloads stop, run the following in a shell.
The environment file is required here: `collect_evidence.py` does not discover
the active run itself, and its `$BQ_*` arguments come from `current.env`.

```bash
cd /home/ubuntu/bigquery
source .venv/bin/activate
source /home/ubuntu/bigquery/results_t2/current.env

printf 'run=%s\ndataset=%s\nresults=%s\n' \
  "$BQ_RUN_ID" "$BQ_DATASET" "$BQ_RESULTS"

export BQ_EVIDENCE_SINCE="$(
  date -u \
    -d "$(jq -r '.started_at' "$BQ_RESULTS/ingest/ingest_progress.json") - 1 minute" \
    +%Y-%m-%dT%H:%M:00Z
)"
export BQ_EVIDENCE_UNTIL="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

python3 collect_evidence.py \
  --project "$GOOGLE_CLOUD_PROJECT" \
  --dataset "$BQ_DATASET" \
  --location "$BQ_LOCATION" \
  --since "$BQ_EVIDENCE_SINCE" \
  --until "$BQ_EVIDENCE_UNTIL" \
  --run-id "$BQ_RUN_ID" \
  --output-dir "$BQ_RESULTS/evidence"

jq '.errors' "$BQ_RESULTS/evidence/evidence_summary.json"
```

Preserve the entire `$BQ_RESULTS` directory. The additional reconciliation and
correctness-capture commands are in `_commands.txt`. Require:

- no errors in `evidence_summary.json`;
- non-empty query-job, Write API timeline, and table-storage exports;
- provider `total_rows` equal to client `acknowledged_rows` and
  `expected_input_rows`;
- no unexpected Write API error codes; `ALREADY_EXISTS` is allowed only when it
  reconciles to the ingester's recovered exact-offset appends;
- recovered `ALREADY_EXISTS` appends, if any, explicitly recorded and included
  exactly once in `acknowledged_rows`;
- successful dashboard and drill-down NDJSON records with `result`,
  `billed_slot_sec`, and `billed_bytes` populated.
