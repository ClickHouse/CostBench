# Canonical Databricks 113,219,565,734-row run

This is the launch, stop, collection, and validation sequence for one fresh
full run. Do not use it for tuning or partial runs. Do not begin namespace
creation or DDL until the selected qualification report contains exact JSON
boolean `"qualified": true`.

Read [INFRASTRUCTURE_SETUP.md](INFRASTRUCTURE_SETUP.md),
[DATABRICKS_BENCHMARK_CONTRACT.md](DATABRICKS_BENCHMARK_CONTRACT.md), and
[TUNING.md](TUNING.md) first. The sequence below never uses `--max-files`,
`--max-row-groups`, `--max-rows`, `--allow-partial`, or
`--allow-nonempty-table`.

## 1. Freeze the non-secret environment

Run on the same-region, dedicated `m6i.8xlarge`-equivalent producer. Use Python
3.12 and install the checked-in requirements into two environments. The SQL
kernel requires PyArrow 23.x while Zerobus 1.8 requires PyArrow earlier than
22, so combining the requirement files into one environment is invalid.

```sh
cd /absolute/path/to/CostBench/full-path-realtime/quotes/databricks
python3.12 -m venv .venv-runner
python3.12 -m venv .venv-zerobus
.venv-runner/bin/python -m pip install -r requirements-runner.txt
.venv-zerobus/bin/python -m pip install -r requirements-zerobus.txt
```

Create a non-secret environment file. Use a new run ID, a new result root
outside the checked-in historical `results/` directory, the dedicated
`costbench` catalog, and a new full-run schema:

```sh
umask 077
cat > /absolute/path/to/databricks-full-run.env <<'EOF'
export PATH="$HOME/.local/bin:$PATH"
export DBX_BENCH_DIR="/absolute/path/to/CostBench/full-path-realtime/quotes/databricks"
export DATABRICKS_RUNNER_PYTHON="$DBX_BENCH_DIR/.venv-runner/bin/python"
export DATABRICKS_ZEROBUS_PYTHON="$DBX_BENCH_DIR/.venv-zerobus/bin/python"
export SOURCE_DIR="/absolute/path/to/canonical-stockhouse-parquet"
export QUALIFICATION_REPORT="/absolute/path/to/qualification/qualification_report.json"

export DATABRICKS_HOST="https://<WORKSPACE_HOST>"
export DATABRICKS_CLOUD="<aws|azure|gcp>"
export DATABRICKS_REGION="<WORKSPACE_REGION>"
export DATABRICKS_WORKSPACE_ID="<NUMERIC_WORKSPACE_ID>"
export DATABRICKS_CATALOG="costbench"
export DATABRICKS_SCHEMA="rt_full_<UNIQUE_FULL_RUN_ID>"
export DATABRICKS_RAW_TABLE="quotes"
export DATABRICKS_MV_TABLE="quotes_daily"
export DATABRICKS_RT_WAREHOUSE_ID="<LAKEHOUSE_RT_WAREHOUSE_ID>"
export DATABRICKS_HTTP_PATH="/sql/1.0/warehouses/<LAKEHOUSE_RT_WAREHOUSE_ID>"
export DATABRICKS_CONTROL_WAREHOUSE_ID="<STANDARD_CONTROL_WAREHOUSE_ID>"
export ZEROBUS_ENDPOINT="https://<WORKSPACE_ID>.zerobus.<REGION>.<CLOUD_SUFFIX>"

export BENCHMARK_RUN_ID="<UNIQUE_FULL_RUN_ID>"
export RUN_ROOT="/absolute/path/to/databricks-runs/${BENCHMARK_RUN_ID}"
export FROZEN_ENV="${RUN_ROOT}/qualification-frozen.env"
export RUNTIME_ENV="${RUN_ROOT}/runtime.env"
export WINDOW_ENV="${RUN_ROOT}/measurement-window.env"
export WINDOW_JSON="${RUN_ROOT}/measurement-window.json"
export DDL_OUTPUT_DIR="${RUN_ROOT}/ddl"
export PREFLIGHT_OUTPUT="${RUN_ROOT}/preflight_report.json"
export INGEST_OUTPUT_DIR="${RUN_ROOT}/ingest"
export INGEST_PROGRESS="${INGEST_OUTPUT_DIR}/ingest_progress.json"
export INGEST_MANIFEST="${INGEST_OUTPUT_DIR}/source_manifest.json"
export INGEST_METRICS="${INGEST_OUTPUT_DIR}/ingest_metrics.jsonl"
export DASHBOARD_OUTPUT="${RUN_ROOT}/dashboard.jsonl"
export DRILLDOWN_OUTPUT="${RUN_ROOT}/drilldown.jsonl"
export FRESHNESS_OUTPUT="${RUN_ROOT}/freshness.jsonl"
export FRESHNESS_PROGRESS="${RUN_ROOT}/freshness_progress.json"
export EVIDENCE_INITIAL_DIR="${RUN_ROOT}/evidence-initial"
export EVIDENCE_SETTLED_DIR="${RUN_ROOT}/evidence-settled"
export COST_SUMMARY="${RUN_ROOT}/cost_summary.json"
export VALIDATION_REPORT="${RUN_ROOT}/validation_report.json"
export FRESHNESS_INTERVAL_SECONDS="30"
export TMUX_SESSION="dbx-${BENCHMARK_RUN_ID}"
EOF

chmod 600 /absolute/path/to/databricks-full-run.env
. /absolute/path/to/databricks-full-run.env
mkdir -p "$RUN_ROOT"
```

If the licensed capture is staged from private S3, complete it before
qualification and preserve its metadata inventory:

```sh
export S3_SOURCE_URI='s3://<licensed-source-bucket>/<source-prefix>'
export SOURCE_INVENTORY_OUTPUT=/absolute/path/to/source_inventory.json
"$DBX_BENCH_DIR/stage_source_from_s3.sh"
```

Stop unless that command reports `status: complete` and
`actual_rows: 113219565734`. The producer independently creates the canonical
file-hash and row-group manifest when the measured run starts.

Do not put a PAT, OAuth access token, client secret, cloud secret, or source
credential in that file.

Control and Zerobus authorization use distinct OAuth token requests but may
use the same dedicated benchmark service principal, as in this deployment:

```sh
unset DATABRICKS_TOKEN
export DATABRICKS_CLIENT_ID='<BENCHMARK_SP_CLIENT_ID>'
export DATABRICKS_CLIENT_SECRET='<BENCHMARK_SP_SECRET>'
```

Those are variable contracts, not values to paste into shell history. In the
actual terminals, fetch from a secret manager or use `read -s` as shown below.
Workspace/control clients request a standard workspace OAuth token. The
Zerobus client requests its separate workspace-specific
`zerobusDirectWriteApi` resource token with explicit Unity Catalog
authorization details. A deployment may instead use separate principals or a
short-lived `DATABRICKS_TOKEN` for control access, but the chosen identity
model must be frozen and disclosed.

## 2. Prove qualification and extract frozen settings

This gate also proves that the qualified host, Zerobus endpoint, and
Lakehouse//RT warehouse match this run. It writes only non-secret settings.

```sh
QUALIFICATION_REPORT="$QUALIFICATION_REPORT" \
FROZEN_ENV="$FROZEN_ENV" \
DATABRICKS_HOST="$DATABRICKS_HOST" \
ZEROBUS_ENDPOINT="$ZEROBUS_ENDPOINT" \
DATABRICKS_RT_WAREHOUSE_ID="$DATABRICKS_RT_WAREHOUSE_ID" \
"$DATABRICKS_RUNNER_PYTHON" - <<'PY'
import json
import os
import shlex
from pathlib import Path

report_path = Path(os.environ["QUALIFICATION_REPORT"]).resolve()
report = json.loads(report_path.read_text(encoding="utf-8"))
if report.get("qualified") is not True:
    raise SystemExit(f"STOP: {report_path} does not contain qualified=true")

frozen = report.get("frozen_settings")
if not isinstance(frozen, dict):
    raise SystemExit("STOP: qualified report omits frozen_settings")
producer = frozen.get("producer")
rt = frozen.get("rt_warehouse")
if not isinstance(producer, dict) or not isinstance(rt, dict):
    raise SystemExit("STOP: qualified report omits producer or RT settings")

required_producer = (
    "stream_count",
    "batch_size",
    "queue_capacity",
    "compression",
    "host_description",
    "network_capacity_gbps",
    "maximum_host_cpu_percent",
    "maximum_network_utilization_percent",
)
missing = [name for name in required_producer if producer.get(name) in (None, "")]
if missing:
    raise SystemExit(f"STOP: frozen producer settings omit {missing}")

host = os.environ["DATABRICKS_HOST"].rstrip("/")
endpoint = os.environ["ZEROBUS_ENDPOINT"].rstrip("/")
if str(producer.get("host", "")).rstrip("/") != host:
    raise SystemExit("STOP: qualified producer host differs from DATABRICKS_HOST")
if str(producer.get("endpoint", "")).rstrip("/") != endpoint:
    raise SystemExit("STOP: qualified Zerobus endpoint differs from ZEROBUS_ENDPOINT")
if rt.get("warehouse_id") != os.environ["DATABRICKS_RT_WAREHOUSE_ID"]:
    raise SystemExit("STOP: qualified RT warehouse ID differs from this run")
if report.get("selected_query_size") != rt.get("query_size"):
    raise SystemExit("STOP: selected and frozen RT query sizes disagree")
if frozen.get("target_eps") != 1_000_000:
    raise SystemExit("STOP: qualification did not freeze exactly 1,000,000 EPS")
if frozen.get("dashboard_interval_seconds") != 600:
    raise SystemExit("STOP: qualification did not freeze dashboard interval 600")
if frozen.get("drilldown_interval_seconds") != 3600:
    raise SystemExit("STOP: qualification did not freeze drill-down interval 3600")

values = {
    "FROZEN_STREAM_COUNT": producer["stream_count"],
    "FROZEN_BATCH_SIZE": producer["batch_size"],
    "FROZEN_QUEUE_CAPACITY": producer["queue_capacity"],
    "FROZEN_COMPRESSION": producer["compression"],
    "FROZEN_PRODUCER_HOST_DESCRIPTION": producer["host_description"],
    "FROZEN_PRODUCER_NETWORK_CAPACITY_GBPS": producer["network_capacity_gbps"],
    "FROZEN_MAX_PRODUCER_CPU_PERCENT": producer["maximum_host_cpu_percent"],
    "FROZEN_MAX_PRODUCER_NETWORK_PERCENT": producer[
        "maximum_network_utilization_percent"
    ],
    "FROZEN_TARGET_EPS": frozen["target_eps"],
    "FROZEN_QUERY_SIZE": rt["query_size"],
    "FROZEN_RT_MIN_CLUSTERS": rt["min_num_clusters"],
    "FROZEN_RT_MAX_CLUSTERS": rt["max_num_clusters"],
    "FROZEN_RT_SERVERLESS": rt["enable_serverless_compute"],
}
output = Path(os.environ["FROZEN_ENV"])
if output.exists():
    raise SystemExit(f"STOP: refusing to overwrite {output}")
output.write_text(
    "".join(f"export {key}={shlex.quote(str(value))}\n" for key, value in values.items()),
    encoding="utf-8",
)
print(f"qualified=true; frozen settings written to {output}")
PY

. "$FROZEN_ENV"
```

Planning examples such as 16 streams, 50,000-row batches, and `NONE`
compression are placeholders only. They are not frozen for a full run unless
the qualified report selected those exact values. The launch command reads the
actual values from `qualification-frozen.env`.

## 3. Create the fresh full-run schema as administrator

This happens outside the measured window. The `costbench` catalog must already
exist from the qualification infrastructure setup with an explicit,
non-default managed location. Verify it and its inherited Predictive
Optimization setting:

```sql
DESCRIBE CATALOG EXTENDED costbench;
ALTER CATALOG costbench ENABLE PREDICTIVE OPTIMIZATION;
```

For every full attempt, create only its new schema. It inherits the explicit
catalog managed location:

```sql
CREATE SCHEMA costbench.`rt_full_<UNIQUE_FULL_RUN_ID>`;
```

Do not use `IF NOT EXISTS` for the full-run schema: a collision means the
target is not proven fresh. Record the administrator statement IDs, object
metadata, resolved managed locations, grants, and creation times. Grant the
benchmark principal only the privileges required by the contract.

`apply_ddl.py` intentionally cannot create catalogs, schemas, external
locations, storage credentials, or managed storage.

## 4. Apply the checked-in DDL

`create.sql` drops and recreates the raw table and materialized view. Confirm
the exact new target. `DDL_OUTPUT_DIR` must not already exist; a retry requires
a new output directory and a newly reviewed target.

Use only workspace/control credentials:

```sh
. /absolute/path/to/databricks-full-run.env
. "$FROZEN_ENV"
cd "$DBX_BENCH_DIR"

read -rsp 'Workspace PAT or OAuth access token: ' DATABRICKS_TOKEN; echo
export DATABRICKS_TOKEN
unset DATABRICKS_CLIENT_ID DATABRICKS_CLIENT_SECRET

"$DATABRICKS_RUNNER_PYTHON" apply_ddl.py \
  --host "$DATABRICKS_HOST" \
  --control-warehouse-id "$DATABRICKS_CONTROL_WAREHOUSE_ID" \
  --catalog "$DATABRICKS_CATALOG" \
  --schema "$DATABRICKS_SCHEMA" \
  --confirm-destructive-target "$DATABRICKS_CATALOG.$DATABRICKS_SCHEMA" \
  --output-dir "$DDL_OUTPUT_DIR"

unset DATABRICKS_TOKEN
```

The directory preserves the complete rendered SQL and an atomic report with
input/rendered/statement hashes, statement IDs, timestamps, statuses, and
redacted errors. Stop unless all four statements succeeded:

```sh
"$DATABRICKS_RUNNER_PYTHON" - "$DDL_OUTPUT_DIR/apply_ddl_report.json" <<'PY'
import json
import sys
from pathlib import Path

path = Path(sys.argv[1])
report = json.loads(path.read_text(encoding="utf-8"))
ok = (
    report.get("status") == "succeeded"
    and report.get("statement_count") == 4
    and len(report.get("statements", [])) == 4
    and all(item.get("status") == "succeeded" and item.get("statement_id")
            for item in report.get("statements", []))
)
if not ok:
    raise SystemExit(f"STOP: DDL report is not complete and successful: {path}")
print("DDL gate passed")
PY
```

## 5. Run online preflight and fail closed

Online preflight uses the shared benchmark principal to check both standard
workspace OAuth and the distinct resource-bound Zerobus path:

```sh
unset DATABRICKS_TOKEN
read -rp 'Benchmark service-principal client ID: ' DATABRICKS_CLIENT_ID
export DATABRICKS_CLIENT_ID
read -rsp 'Benchmark service-principal secret: ' DATABRICKS_CLIENT_SECRET; echo
export DATABRICKS_CLIENT_SECRET

"$DATABRICKS_RUNNER_PYTHON" preflight.py \
  --online \
  --output "$PREFLIGHT_OUTPUT" \
  --runner-python "$DATABRICKS_RUNNER_PYTHON" \
  --zerobus-python "$DATABRICKS_ZEROBUS_PYTHON" \
  --host "$DATABRICKS_HOST" \
  --cloud "$DATABRICKS_CLOUD" \
  --region "$DATABRICKS_REGION" \
  --workspace-id "$DATABRICKS_WORKSPACE_ID" \
  --catalog "$DATABRICKS_CATALOG" \
  --schema "$DATABRICKS_SCHEMA" \
  --raw-table "$DATABRICKS_RAW_TABLE" \
  --mv-table "$DATABRICKS_MV_TABLE" \
  --rt-warehouse "$DATABRICKS_RT_WAREHOUSE_ID" \
  --control-warehouse "$DATABRICKS_CONTROL_WAREHOUSE_ID" \
  --zerobus-endpoint "$ZEROBUS_ENDPOINT"

QUALIFICATION_REPORT="$QUALIFICATION_REPORT" \
PREFLIGHT_OUTPUT="$PREFLIGHT_OUTPUT" \
"$DATABRICKS_RUNNER_PYTHON" - <<'PY'
import json
import os
from pathlib import Path

preflight_path = Path(os.environ["PREFLIGHT_OUTPUT"])
qualification_path = Path(os.environ["QUALIFICATION_REPORT"])
preflight = json.loads(preflight_path.read_text(encoding="utf-8"))
qualification = json.loads(qualification_path.read_text(encoding="utf-8"))

if preflight.get("mode") != "online":
    raise SystemExit("STOP: preflight report is not online")
summary = preflight.get("summary", {})
online = preflight.get("online", {})
if summary.get("error_count") != 0 or summary.get("status") not in ("ok", "warning"):
    raise SystemExit(f"STOP: preflight summary failed: {summary}")
if online.get("status") not in ("ok", "warning") or online.get("errors") != []:
    raise SystemExit("STOP: online preflight contains errors")

frozen = qualification["frozen_settings"]["rt_warehouse"]
actual = online["warehouses"]["lakehouse_rt"]["metadata"]
checks = {
    "warehouse_id": (actual.get("id"), frozen.get("warehouse_id")),
    "query_size": (actual.get("cluster_size"), frozen.get("query_size")),
    "min_num_clusters": (actual.get("min_num_clusters"), frozen.get("min_num_clusters")),
    "max_num_clusters": (actual.get("max_num_clusters"), frozen.get("max_num_clusters")),
    "enable_serverless_compute": (
        actual.get("enable_serverless_compute"),
        frozen.get("enable_serverless_compute"),
    ),
}
drift = {name: pair for name, pair in checks.items() if pair[0] != pair[1]}
if drift:
    raise SystemExit(f"STOP: Lakehouse//RT configuration drift: {drift}")
if online["tables"]["raw_storage_assumption"].get("status") != "ok":
    raise SystemExit("STOP: non-default managed storage was not proven")
if online.get("zerobus_service_principal", {}).get("status") != "ok":
    raise SystemExit("STOP: Zerobus service-principal check failed")
predictive = online.get("predictive_optimization", {})
for table in ("raw", "materialized_view"):
    effective = predictive.get(table, {})
    if (
        effective.get("status") != "ok"
        or effective.get("effective_value") != "ENABLE"
        or effective.get("effectively_enabled") is not True
    ):
        raise SystemExit(
            f"STOP: Predictive Optimization is not effectively enabled for {table}"
        )
print("online preflight and frozen-configuration gates passed")
PY

unset DATABRICKS_TOKEN DATABRICKS_CLIENT_ID DATABRICKS_CLIENT_SECRET
```

Warnings must be read and retained. A warning is not permission to ignore an
error. Any error, package mismatch, unsupported SQL, inaccessible system
table, storage failure, endpoint failure, permission failure, or frozen
configuration drift stops the run.

## 6. Prepare four tmux terminals

Create one tmux session with four named windows. `remain-on-exit` preserves the
process status and terminal output:

```sh
tmux new-session -d -s "$TMUX_SESSION" -n ingest
tmux new-window -t "$TMUX_SESSION" -n freshness
tmux new-window -t "$TMUX_SESSION" -n dashboard
tmux new-window -t "$TMUX_SESSION" -n drilldown
tmux set-option -t "$TMUX_SESSION" remain-on-exit on

for window in ingest freshness dashboard drilldown; do
  tmux send-keys -t "$TMUX_SESSION:$window" \
    '. /absolute/path/to/databricks-full-run.env; . "$FROZEN_ENV"; cd "$DBX_BENCH_DIR"' Enter
done
```

Attach to each window before launch and load the shared benchmark principal
without placing its secret in shell history:

```sh
unset DATABRICKS_TOKEN
read -rp 'Benchmark service-principal client ID: ' DATABRICKS_CLIENT_ID
export DATABRICKS_CLIENT_ID
read -rsp 'Benchmark service-principal secret: ' DATABRICKS_CLIENT_SECRET; echo
export DATABRICKS_CLIENT_SECRET
```

Repeat in `ingest`, `freshness`, `dashboard`, and `drilldown`. Standard
workspace clients request ordinary OAuth tokens; the ingest client requests
the resource-bound Zerobus token. If a deployment uses separate principals,
load only the role-appropriate identity in each window and record that
deviation.

Detach after all four terminals are authenticated. Confirm that no output path
already exists:

```sh
for path in \
  "$INGEST_OUTPUT_DIR" "$DASHBOARD_OUTPUT" "$DRILLDOWN_OUTPUT" \
  "$FRESHNESS_OUTPUT" "$FRESHNESS_PROGRESS" \
  "$EVIDENCE_INITIAL_DIR" "$EVIDENCE_SETTLED_DIR" \
  "$COST_SUMMARY" "$VALIDATION_REPORT" "$WINDOW_ENV" "$WINDOW_JSON"; do
  if [ -e "$path" ]; then
    printf 'STOP: output already exists: %s\n' "$path" >&2
    exit 1
  fi
done
```

## 7. Launch in canonical order

Record one UTC start immediately before launching and source it into all four
terminals:

```sh
export RUN_STARTED_AT="$(
  "$DATABRICKS_RUNNER_PYTHON" -c 'from datetime import datetime, timezone; print(datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z"))'
)"
printf 'export RUN_STARTED_AT=%q\n' "$RUN_STARTED_AT" > "$RUNTIME_ENV"

for window in ingest freshness dashboard drilldown; do
  tmux send-keys -t "$TMUX_SESSION:$window" '. "$RUNTIME_ENV"' Enter
done
```

Launch in this exact order: ingest first, then immediately freshness,
dashboard, and drill-down. The latter three run concurrently with ingest.

```sh
tmux send-keys -t "$TMUX_SESSION:ingest" \
  'exec "$DATABRICKS_ZEROBUS_PYTHON" ingest_zerobus.py --dir "$SOURCE_DIR" --expected-rows 113219565734 --host "$DATABRICKS_HOST" --endpoint "$ZEROBUS_ENDPOINT" --catalog "$DATABRICKS_CATALOG" --schema "$DATABRICKS_SCHEMA" --table "$DATABRICKS_RAW_TABLE" --count-warehouse "$DATABRICKS_CONTROL_WAREHOUSE_ID" --workers "$FROZEN_STREAM_COUNT" --batch-size "$FROZEN_BATCH_SIZE" --queue-capacity "$FROZEN_QUEUE_CAPACITY" --target-eps "$FROZEN_TARGET_EPS" --compression "$FROZEN_COMPRESSION" --metrics-interval 5 --output-dir "$INGEST_OUTPUT_DIR" --run-id "$BENCHMARK_RUN_ID"' Enter

tmux send-keys -t "$TMUX_SESSION:freshness" \
  'exec "$DATABRICKS_RUNNER_PYTHON" monitor_freshness.py --host "$DATABRICKS_HOST" --control-warehouse-id "$DATABRICKS_CONTROL_WAREHOUSE_ID" --catalog "$DATABRICKS_CATALOG" --schema "$DATABRICKS_SCHEMA" --raw-table "$DATABRICKS_RAW_TABLE" --mv-table "$DATABRICKS_MV_TABLE" --producer-progress "$INGEST_PROGRESS" --since "$RUN_STARTED_AT" --interval "$FRESHNESS_INTERVAL_SECONDS" --output "$FRESHNESS_OUTPUT" --progress-json "$FRESHNESS_PROGRESS" --run-id "$BENCHMARK_RUN_ID"' Enter

tmux send-keys -t "$TMUX_SESSION:dashboard" \
  'exec "$DATABRICKS_RUNNER_PYTHON" run_dashboard.py --interval 600 --output "$DASHBOARD_OUTPUT" --host "$DATABRICKS_HOST" --rt-warehouse-id "$DATABRICKS_RT_WAREHOUSE_ID" --http-path "$DATABRICKS_HTTP_PATH" --control-warehouse-id "$DATABRICKS_CONTROL_WAREHOUSE_ID" --catalog "$DATABRICKS_CATALOG" --schema "$DATABRICKS_SCHEMA" --mv-table "$DATABRICKS_MV_TABLE" --producer-progress "$INGEST_PROGRESS" --freshness-monitor "$FRESHNESS_PROGRESS" --system "Databricks" --machine "$FROZEN_QUERY_SIZE" --cluster-size "$FROZEN_RT_MAX_CLUSTERS" --run-id "$BENCHMARK_RUN_ID"' Enter

tmux send-keys -t "$TMUX_SESSION:drilldown" \
  'exec "$DATABRICKS_RUNNER_PYTHON" run_drilldown.py --interval 3600 --output "$DRILLDOWN_OUTPUT" --host "$DATABRICKS_HOST" --rt-warehouse-id "$DATABRICKS_RT_WAREHOUSE_ID" --http-path "$DATABRICKS_HTTP_PATH" --control-warehouse-id "$DATABRICKS_CONTROL_WAREHOUSE_ID" --catalog "$DATABRICKS_CATALOG" --schema "$DATABRICKS_SCHEMA" --mv-table "$DATABRICKS_MV_TABLE" --producer-progress "$INGEST_PROGRESS" --freshness-monitor "$FRESHNESS_PROGRESS" --system "Databricks" --machine "$FROZEN_QUERY_SIZE" --cluster-size "$FROZEN_RT_MAX_CLUSTERS" --run-id "$BENCHMARK_RUN_ID"' Enter
```

Do not resize, restart, repoint, or reconfigure either warehouse while the
window is live.

## 8. Monitor and abort gates

Monitor all four windows plus the atomic progress files:

```sh
tmux attach -t "$TMUX_SESSION"
```

Abort the measured run on any of the following:

- producer `terminal_error`, non-empty `errors`, `ambiguous=true`,
  `safe_to_resume=false`, unexplained replay/reconnect, fallback rows,
  unacknowledged batches after a terminal stream failure, or nonzero exit;
- source manifest count other than 113,219,565,734 or any source membership,
  canonical-order, signed-range, or empty-target failure;
- after the declared startup grace, freshness errors, a
  failed/full/non-incremental materialized-view refresh, an unexplained refresh
  gap, or a declared raw/MV observational-age gate violation; an initial
  missing progress/freshness file while the ingest process creates its first
  atomic journal is startup state, not durability evidence;
- active-ingest runner query errors, a missing canonical query-history record,
  result-cache evidence other than exact false, a cache origin,
  queue/capacity wait, cadence/self-overlap failure, or changed warehouse
  metadata;
- service, warehouse, permission, network, storage, or region drift.

On abort, preserve everything. Stop `dashboard`, then `drilldown`, then
`freshness` with one `Ctrl-C` each; then send one `Ctrl-C` to `ingest` and let
it flush. Never force-kill unless required to protect the host. A clean
checkpoint is not an accepted full result. If recovery is ambiguous, do not
use `--resume`; create another fresh namespace, rerun DDL with a new output
directory, and start a new run ID.

Raw and materialized-view ages shown by the monitor are observational ages
from sample time to provider timestamps. They are useful gates and context,
not exact per-record latency measurements.

## 9. Finish only after provider durability

Do not stop ingest because all source rows were read or queued. Let it finish
naturally only after Zerobus durability acknowledgments cover every row. Then
run this gate:

```sh
"$DATABRICKS_RUNNER_PYTHON" - "$INGEST_PROGRESS" <<'PY'
import json
import sys
from pathlib import Path

path = Path(sys.argv[1])
progress = json.loads(path.read_text(encoding="utf-8"))
streams = progress.get("stream_status", {})
source = progress.get("source", {})
task_count = source.get("task_count")
ok = (
    progress.get("finished") is True
    and progress.get("running") is False
    and source.get("total_rows") == 113_219_565_734
    and progress.get("provider_committed_rows") == 113_219_565_734
    and progress.get("logical_raw_rows") == 113_219_565_734
    and progress.get("submitted_rows") == 113_219_565_734
    and progress.get("baseline_table_rows") == 0
    and progress.get("baseline_table_rows_unknown") is False
    and progress.get("pending_batches") == 0
    and progress.get("pending_rows") == 0
    and progress.get("partial_task_rows") == {}
    and isinstance(task_count, int)
    and task_count > 0
    and progress.get("completed_tasks") == task_count
    and progress.get("completed_task_ordinals") == list(range(task_count))
    and progress.get("clean_checkpoint") is True
    and progress.get("safe_to_resume") is True
    and progress.get("stopped_early") is False
    and progress.get("ambiguous") is False
    and progress.get("terminal_error") in (None, "")
    and progress.get("errors") == []
    and isinstance(streams, dict)
    and len(streams) == progress.get("config", {}).get("workers")
    and all(
        item.get("close_succeeded") is True
        and item.get("unacked_inspection_succeeded") is True
        and item.get("unacked_batches") == 0
        for item in streams.values()
    )
)
if not ok:
    raise SystemExit(f"STOP: producer durability gate failed: {path}")
print("producer durability gate passed")
PY
```

Allow one final freshness interval to complete, then stop and wait for each
process in this exact order:

```sh
sleep "$FRESHNESS_INTERVAL_SECONDS"

tmux send-keys -t "$TMUX_SESSION:dashboard" C-c
while [ "$(tmux display-message -p -t "$TMUX_SESSION:dashboard" '#{pane_dead}')" != 1 ]; do sleep 1; done

tmux send-keys -t "$TMUX_SESSION:drilldown" C-c
while [ "$(tmux display-message -p -t "$TMUX_SESSION:drilldown" '#{pane_dead}')" != 1 ]; do sleep 1; done

tmux send-keys -t "$TMUX_SESSION:freshness" C-c
while [ "$(tmux display-message -p -t "$TMUX_SESSION:freshness" '#{pane_dead}')" != 1 ]; do sleep 1; done
```

Now define and preserve the exact UTC evidence/cost window. Its start is the
timestamp captured immediately before launch; its end is after all measured
readers and the monitor have stopped. The producer finish timestamp remains a
separate boundary:

```sh
export MEASURED_UNTIL="$(
  "$DATABRICKS_RUNNER_PYTHON" -c 'from datetime import datetime, timezone; print(datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z"))'
)"

RUN_STARTED_AT="$RUN_STARTED_AT" \
MEASURED_UNTIL="$MEASURED_UNTIL" \
INGEST_PROGRESS="$INGEST_PROGRESS" \
WINDOW_ENV="$WINDOW_ENV" \
WINDOW_JSON="$WINDOW_JSON" \
BENCHMARK_RUN_ID="$BENCHMARK_RUN_ID" \
"$DATABRICKS_RUNNER_PYTHON" - <<'PY'
import json
import os
import shlex
from pathlib import Path

progress = json.loads(Path(os.environ["INGEST_PROGRESS"]).read_text(encoding="utf-8"))
finished = progress.get("finished_at")
if not isinstance(finished, str) or not finished:
    raise SystemExit("STOP: ingest progress omits finished_at")
window = {
    "run_id": os.environ["BENCHMARK_RUN_ID"],
    "since": os.environ["RUN_STARTED_AT"],
    "until": os.environ["MEASURED_UNTIL"],
    "producer_finished_at": finished,
}
window_json = Path(os.environ["WINDOW_JSON"])
window_env = Path(os.environ["WINDOW_ENV"])
window_json.write_text(json.dumps(window, indent=2, sort_keys=True) + "\n", encoding="utf-8")
window_env.write_text(
    "".join(
        f"export {name}={shlex.quote(value)}\n"
        for name, value in (
            ("MEASURED_SINCE", window["since"]),
            ("MEASURED_UNTIL", window["until"]),
            ("PRODUCER_FINISHED_AT", window["producer_finished_at"]),
        )
    ),
    encoding="utf-8",
)
print(json.dumps(window, indent=2, sort_keys=True))
PY

. "$WINDOW_ENV"
```

## 10. Collect initial and settled evidence

Collection is read-only and uses only workspace/control credentials. Ensure
the Zerobus service-principal variables are not present:

```sh
read -rsp 'Workspace PAT or OAuth access token: ' DATABRICKS_TOKEN; echo
export DATABRICKS_TOKEN
unset DATABRICKS_CLIENT_ID DATABRICKS_CLIENT_SECRET

"$DATABRICKS_RUNNER_PYTHON" collect_evidence.py \
  --host "$DATABRICKS_HOST" \
  --control-warehouse-id "$DATABRICKS_CONTROL_WAREHOUSE_ID" \
  --rt-warehouse-id "$DATABRICKS_RT_WAREHOUSE_ID" \
  --catalog "$DATABRICKS_CATALOG" \
  --schema "$DATABRICKS_SCHEMA" \
  --raw-table "$DATABRICKS_RAW_TABLE" \
  --mv-table "$DATABRICKS_MV_TABLE" \
  --since "$MEASURED_SINCE" \
  --until "$MEASURED_UNTIL" \
  --run-id "$BENCHMARK_RUN_ID" \
  --output-dir "$EVIDENCE_INITIAL_DIR"

unset DATABRICKS_TOKEN
```

Preserve this initial collection even if billing rows are incomplete.
`system.billing.usage` can lag actual usage by up to 24 hours. After billing
has settled, rerun the same bounded collection against the same immutable UTC
window and run ID, using the fresh settled directory:

```sh
. /absolute/path/to/databricks-full-run.env
. "$WINDOW_ENV"
cd "$DBX_BENCH_DIR"

read -rsp 'Workspace PAT or OAuth access token: ' DATABRICKS_TOKEN; echo
export DATABRICKS_TOKEN
unset DATABRICKS_CLIENT_ID DATABRICKS_CLIENT_SECRET

"$DATABRICKS_RUNNER_PYTHON" collect_evidence.py \
  --host "$DATABRICKS_HOST" \
  --control-warehouse-id "$DATABRICKS_CONTROL_WAREHOUSE_ID" \
  --rt-warehouse-id "$DATABRICKS_RT_WAREHOUSE_ID" \
  --catalog "$DATABRICKS_CATALOG" \
  --schema "$DATABRICKS_SCHEMA" \
  --raw-table "$DATABRICKS_RAW_TABLE" \
  --mv-table "$DATABRICKS_MV_TABLE" \
  --since "$MEASURED_SINCE" \
  --until "$MEASURED_UNTIL" \
  --run-id "$BENCHMARK_RUN_ID" \
  --output-dir "$EVIDENCE_SETTLED_DIR"

unset DATABRICKS_TOKEN
```

If required billing rows still have not settled, preserve that directory and
repeat later into another new directory. Do not overwrite or splice
collections. Do not run final cost or validation against the initial,
unsettled collection.

Export the producer/cloud bill for the same exact UTC boundaries into a
separate, immutable external subledger under `RUN_ROOT`. It must identify
producer compute, source-storage reads, network egress, and applicable cloud
storage/request charges. These costs are not present in Databricks system
billing and `summarize_run.py` does not invent or silently zero them.

## 11. Build cost ledger and validate

These commands are offline:

```sh
"$DATABRICKS_RUNNER_PYTHON" costs/summarize_run.py \
  --evidence-dir "$EVIDENCE_SETTLED_DIR" \
  --since "$MEASURED_SINCE" \
  --until "$MEASURED_UNTIL" \
  --rt-warehouse-id "$DATABRICKS_RT_WAREHOUSE_ID" \
  --control-warehouse-id "$DATABRICKS_CONTROL_WAREHOUSE_ID" \
  --producer-finished-at "$PRODUCER_FINISHED_AT" \
  --output "$COST_SUMMARY"

"$DATABRICKS_RUNNER_PYTHON" validate_run.py \
  --source-manifest "$INGEST_MANIFEST" \
  --ingest-progress "$INGEST_PROGRESS" \
  --ingest-metrics "$INGEST_METRICS" \
  --dashboard "$DASHBOARD_OUTPUT" \
  --drilldown "$DRILLDOWN_OUTPUT" \
  --freshness "$FRESHNESS_OUTPUT" \
  --evidence-summary "$EVIDENCE_SETTLED_DIR/evidence_summary.json" \
  --cost-summary "$COST_SUMMARY" \
  --expected-rows 113219565734 \
  --output "$VALIDATION_REPORT"
```

Both commands must exit zero, and acceptance must be exact JSON boolean true:

```sh
"$DATABRICKS_RUNNER_PYTHON" - "$COST_SUMMARY" "$VALIDATION_REPORT" <<'PY'
import json
import sys
from pathlib import Path

cost_path, validation_path = map(Path, sys.argv[1:])
cost = json.loads(cost_path.read_text(encoding="utf-8"))
validation = json.loads(validation_path.read_text(encoding="utf-8"))
if cost.get("complete") is not True:
    raise SystemExit(f"STOP: cost ledger is incomplete: {cost_path}")
if validation.get("accepted") is not True:
    raise SystemExit(f"STOP: validation accepted is not true: {validation_path}")
print("accepted=true")
PY
```

`accepted=true` is necessary, not sufficient for publication by itself. The
contract review must also confirm the separately preserved producer/cloud
subledger and every manual infrastructure/release-stage disclosure. It is not
permission to fabricate, backfill, or merge online results.

## 12. Publish accepted artifacts

First build a review copy outside the repository. The publisher fails unless
validation acceptance and cost completeness are exact JSON booleans `true`.
It selects only validator-approved active-ingest observations, caps presented
query and freshness samples at 100 billion rows, matches equal ClickHouse and
Databricks query counts monotonically by observed row progress, and keeps
complete-ingest cost at all 113,219,565,734 rows.

```sh
export PUBLICATION_REVIEW_OUTPUT="${RUN_ROOT}/publication-review"
export CLICKHOUSE_INGEST_COST="$DBX_BENCH_DIR/../clickhouse-cloud/costs/out_t2/ingest.json"

"$DATABRICKS_RUNNER_PYTHON" publish_results.py \
  --validation-report "$VALIDATION_REPORT" \
  --cost-summary "$COST_SUMMARY" \
  --dashboard "$DASHBOARD_OUTPUT" \
  --drilldown "$DRILLDOWN_OUTPUT" \
  --freshness "$FRESHNESS_OUTPUT" \
  --clickhouse-ingest-cost "$CLICKHOUSE_INGEST_COST" \
  --output-dir "$PUBLICATION_REVIEW_OUTPUT"

uv run visualizations/plot_publication.py \
  --publication-manifest "$PUBLICATION_REVIEW_OUTPUT/publication_manifest.json" \
  --output-dir "$PUBLICATION_REVIEW_OUTPUT/charts"
```

Review the publication manifest, source hashes, selected indices, matching
report, cost allocations, charts, external producer/cloud subledger, and Beta
disclosure. The review directory is not an accepted global result.

Only after that review succeeds, publish into a new repository result
directory and activate Databricks in the global manifest in the same command.
The publisher writes the manifest last and never overwrites an existing
publication artifact.

```sh
export GLOBAL_MANIFEST="$DBX_BENCH_DIR/../global/visualizations/manifest.json"
export FINAL_PUBLICATION_OUTPUT="$DBX_BENCH_DIR/results/$BENCHMARK_RUN_ID/publication"

"$DATABRICKS_RUNNER_PYTHON" publish_results.py \
  --validation-report "$VALIDATION_REPORT" \
  --cost-summary "$COST_SUMMARY" \
  --dashboard "$DASHBOARD_OUTPUT" \
  --drilldown "$DRILLDOWN_OUTPUT" \
  --freshness "$FRESHNESS_OUTPUT" \
  --clickhouse-ingest-cost "$CLICKHOUSE_INGEST_COST" \
  --output-dir "$FINAL_PUBLICATION_OUTPUT" \
  --global-manifest "$GLOBAL_MANIFEST" \
  --apply-global-manifest
```

Do not hand-edit accepted output or add Databricks directly to the global
manifest. Run the commands in `../global/visualizations/_commands.txt` only
after activation; those renderers ignore Databricks until the publisher adds
the accepted provider entry and required labels.

## 13. Cleanup, only after settlement and acceptance

Keep the workspace objects and both warehouses available until settled
evidence has been captured and the final report is accepted. Copy the complete
run root to durable, access-controlled storage before cleanup. Preserve source
manifest, producer ledgers, runner/freshness JSONL, rendered DDL and report,
preflight, both evidence collections, UTC window, cost summary, validation,
infrastructure records, and billing exports without hand edits.

Then stop the producer instance and warehouses according to the recorded
infrastructure procedure. An administrator may drop only the exact fresh run
objects after independently confirming the catalog/schema names and that no
other objects are present:

```sql
DROP MATERIALIZED VIEW `<NEW_CATALOG>`.`<NEW_SCHEMA>`.`quotes_daily`;
DROP TABLE `<NEW_CATALOG>`.`<NEW_SCHEMA>`.`quotes`;
DROP SCHEMA `<NEW_CATALOG>`.`<NEW_SCHEMA>`;
DROP CATALOG `<NEW_CATALOG>`;
```

Do not aim `create.sql`, cleanup SQL, cloud deletion, or result-directory
commands at historical targets or checked-in historical artifacts. Historical
files remain untouched and are never evidence for this run.
