# BigQuery slots and a controlled N-slot benchmark run

Last verified against the Google Cloud documentation on 2026-08-10.

This note separates two different benchmark configurations:

- the current T1 run uses BigQuery **on-demand** compute;
- a future controlled-capacity rerun can use an **Enterprise PAYG reservation
  with N baseline slots**.

Do not create or assign a reservation in the middle of the current T1 run. A
reservation assignment changes the execution and billing model for new jobs,
so any reservation test must use a fresh run ID, fresh dataset, and separate
results directory.

## What the current on-demand evidence means

The current run returned:

```text
reservation_id = NULL
edition        = NULL
jobs           = 28
failed_jobs    = 0
max single-job average slots = 72.3
max queue time                = 373 ms

observed peak project slots   = 320.5
observed p95 project slots    = 53.0
max running jobs              = 2
max pending jobs              = 1
seconds with runnable units   = 17.5%
```

This confirms that the jobs ran on demand. It does **not** reveal a configured
project maximum. For on-demand analysis queries, Google documents a nominal
soft limit of 2,000 concurrent slots per project and 20,000 slots per
organization. BigQuery dynamically decides how many slots a query receives; a
project can temporarily burst above the limit and can receive fewer slots when
regional capacity is constrained. There is no guaranteed minimum.

The observed 320.5-slot peak is therefore a measurement of this workload during
that sample, not the engine's maximum and not proof that 320.5 slots were always
available. Likewise, 72.3 is the highest **average slots used by one completed
job**, not the maximum number of slots that job might have used at an instant.

On-demand query charges are based on billed bytes, but slot availability still
affects latency. `total_slot_ms`, queue time, runnable units, concurrency, and
the reservation ID should therefore remain part of the benchmark evidence.

Official references:

- [BigQuery workload-management models](https://docs.cloud.google.com/bigquery/docs/reservations-intro)
- [BigQuery slots, autoscaling, baselines, and limits](https://docs.cloud.google.com/bigquery/docs/slots)
- [BigQuery quotas and limits](https://docs.cloud.google.com/bigquery/quotas)
- [BigQuery pricing](https://cloud.google.com/bigquery/pricing)

## What “guaranteed N slots” means

For this benchmark, use this configuration:

| Setting | Value | Reason |
|---|---:|---|
| Edition | `ENTERPRISE` | Baseline slots are supported and commitments are optional. |
| Baseline slots | `N` | This is the capacity that is always allocated and billed. |
| Autoscaling slots | `0` | Prevents the workload from scaling above the declared reservation. |
| Ignore idle slots | `true` | Prevents borrowing idle capacity from other reservations. |
| Target job concurrency | `0` | Lets BigQuery schedule concurrency automatically inside the N-slot pool. |
| Assignment type | project `QUERY` | Routes dashboard, drill-down, setup, and other query jobs in the project to the reservation. |
| Capacity commitment | none | A short benchmark should use PAYG; a one- or three-year commitment is unnecessary. |

Use a multiple of 50 for `N`. Without excess committed capacity, BigQuery
requires reservation changes in 50-slot increments. The current observation of
320.5 peak project slots makes **500 baseline slots** a reasonable first
controlled run: it is above the observed peak and leaves some headroom. This is
a benchmark choice, not a claim that 500 BigQuery slots equal 500 CPU cores or
the capacity of another vendor's warehouse.

There is an important difference between these two designs:

| Configuration | Guaranteed minimum | Configured capacity |
|---|---:|---:|
| `baseline=N`, autoscaling off | N | N |
| `baseline=0`, autoscaling maximum N | 0 | up to N, subject to capacity availability |

The second design is a capped autoscaling run, not a guaranteed-N run. The
commands below implement the first design.

Even with a fixed reservation, N slots are shared by concurrent jobs. N is not
guaranteed separately to every query. Google also notes that observed slot usage
can occasionally exceed configured capacity; the reservation settings are the
declared resource envelope, while system-table measurements remain the source
of truth for actual use.

## Scope and benchmark accounting

The `QUERY` assignment controls BigQuery query compute. It does not govern the
Storage Write API data path, so ingestion throughput and Write API charges must
still be measured separately.

Automatic materialized-view maintenance must also remain a separate accounting
component. Inspect its `job_type`, `reservation_id`, `edition`, and
`total_slot_ms` in `JOBS_BY_PROJECT`; do not assume it used the query
reservation. If automatic maintenance appears as `BACKGROUND`, decide before a
new run whether to add a `BACKGROUND` assignment to the same pool, and document
that decision. Do not change the assignment policy after measurement begins.

A project-level `QUERY` assignment affects every new query job in the project,
not just jobs with this benchmark's `run_id` label. A dedicated benchmark
project is best. If the existing project is used, stop unrelated queries or
report their slot consumption separately.

Under this capacity model:

- `billed_bytes` remains useful scan-volume evidence but is no longer the query
  compute charge;
- `total_slot_ms / 1000` remains workload slot-seconds, not necessarily billed
  slot-seconds;
- baseline capacity is billed for the entire time the reservation exists,
  including idle time;
- the approximate query-compute charge is `N * reservation_hours * the current
  Enterprise PAYG slot-hour price`;
- Storage Write API, storage, network, and materialized-view maintenance costs
  remain separate.

## One-time prerequisites

Run these only after the current on-demand run is complete and before creating
the fresh reservation-backed run.

The acting principal needs `roles/bigquery.resourceEditor` on both the
reservation administration project and the assigned project. When they are the
same project, one project-level grant is sufficient. The API name is
`bigqueryreservation.googleapis.com`. The reservation and assignee must be in
the same organization and location, and the administration project needs at
least N slots of regional reservation quota. The create request fails rather
than silently supplying fewer baseline slots when quota is insufficient.

```bash
export GOOGLE_CLOUD_PROJECT=pmm-project-377716
export BQ_LOCATION=US
export BQ_ADMIN_PROJECT="$GOOGLE_CLOUD_PROJECT"
export BQ_SLOT_COUNT=500
export BQ_RESERVATION="fpra-n${BQ_SLOT_COUNT}-$(date -u +%Y%m%dt%H%M%sz)"
export BQ_REGION_QUALIFIER="region-$(printf '%s' "$BQ_LOCATION" | tr '[:upper:]' '[:lower:]')"

gcloud services enable bigqueryreservation.googleapis.com \
  --project "$BQ_ADMIN_PROJECT"
```

If an administrator must grant the role and the project policy already has
conditional bindings, use an explicit unconditional binding:

```bash
export BQ_USER_EMAIL=tom@clickhouse.com

gcloud projects add-iam-policy-binding "$BQ_ADMIN_PROJECT" \
  --member="user:$BQ_USER_EMAIL" \
  --role="roles/bigquery.resourceEditor" \
  --condition=None
```

Before creating anything, list direct query assignments already owned by the
administration project. If this returns a row for the benchmark project, stop
and understand that assignment rather than creating a conflicting one.

```bash
bq query \
  --use_legacy_sql=false \
  --location="$BQ_LOCATION" \
  "SELECT
     assignment_id,
     reservation_name,
     assignee_id,
     assignee_type,
     job_type
   FROM \`$BQ_ADMIN_PROJECT\`.\`$BQ_REGION_QUALIFIER\`.INFORMATION_SCHEMA.ASSIGNMENTS_BY_PROJECT
   WHERE assignee_id = '$GOOGLE_CLOUD_PROJECT'
     AND job_type = 'QUERY'"
```

## Create the fixed PAYG reservation

This creates N Enterprise baseline slots, disables idle-slot borrowing, leaves
autoscaling unconfigured/off, and does **not** purchase a capacity commitment.
Baseline slots are charged immediately, so create the reservation shortly
before the run and delete it promptly afterward.

```bash
bq mk \
  --project_id="$BQ_ADMIN_PROJECT" \
  --location="$BQ_LOCATION" \
  --reservation \
  --slots="$BQ_SLOT_COUNT" \
  --ignore_idle_slots=true \
  --edition=ENTERPRISE \
  --target_job_concurrency=0 \
  "$BQ_RESERVATION"
```

Verify the reservation before assigning the project:

```bash
bq query \
  --use_legacy_sql=false \
  --location="$BQ_LOCATION" \
  "SELECT
     reservation_name,
     edition,
     slot_capacity,
     ignore_idle_slots,
     target_job_concurrency,
     IFNULL(autoscale.max_slots, 0) AS autoscale_max_slots
   FROM \`$BQ_ADMIN_PROJECT\`.\`$BQ_REGION_QUALIFIER\`.INFORMATION_SCHEMA.RESERVATIONS_BY_PROJECT
   WHERE reservation_name = '$BQ_RESERVATION'"
```

The required result is:

```text
edition                ENTERPRISE
slot_capacity          N
ignore_idle_slots       true
target_job_concurrency  0
autoscale_max_slots     0
```

If `autoscale_max_slots` is not zero, stop and correct the reservation before
the benchmark. Do not rely on an autoscaling maximum as guaranteed capacity.

Official references:

- [Create and manage reservations](https://docs.cloud.google.com/bigquery/docs/reservations-tasks)
- [Capacity commitments are optional](https://docs.cloud.google.com/bigquery/docs/reservations-commitments)
- [`RESERVATIONS_BY_PROJECT` schema](https://docs.cloud.google.com/bigquery/docs/information-schema-reservations)

## Assign QUERY jobs and wait for propagation

```bash
bq mk \
  --project_id="$BQ_ADMIN_PROJECT" \
  --location="$BQ_LOCATION" \
  --reservation_assignment \
  --reservation_id="$BQ_RESERVATION" \
  --assignee_type=PROJECT \
  --assignee_id="$GOOGLE_CLOUD_PROJECT" \
  --job_type=QUERY
```

Google's reservation tutorial says to wait at least five minutes after creating
an assignment before starting queries; otherwise an early query can still be
billed on demand.

```bash
sleep 300
```

Verify the active assignment:

```bash
bq query \
  --use_legacy_sql=false \
  --location="$BQ_LOCATION" \
  "SELECT
     assignment_id,
     reservation_name,
     assignee_id,
     assignee_type,
     job_type
   FROM \`$BQ_ADMIN_PROJECT\`.\`$BQ_REGION_QUALIFIER\`.INFORMATION_SCHEMA.ASSIGNMENTS_BY_PROJECT
   WHERE reservation_name = '$BQ_RESERVATION'
     AND assignee_id = '$GOOGLE_CLOUD_PROJECT'
     AND job_type = 'QUERY'"
```

Official references:

- [Manage reservation assignments](https://docs.cloud.google.com/bigquery/docs/reservations-assignments)
- [`ASSIGNMENTS_BY_PROJECT` schema](https://docs.cloud.google.com/bigquery/docs/information-schema-assignments)

## Prove that a job actually used the reservation

Do not start the benchmark merely because the resource exists. Run a uniquely
named probe and verify the job's provider-side metadata:

```bash
export BQ_PROBE_JOB_ID="fpra_reservation_probe_$(date -u +%Y%m%dT%H%M%SZ)"

bq query \
  --use_legacy_sql=false \
  --use_cache=false \
  --location="$BQ_LOCATION" \
  --job_id="$BQ_PROBE_JOB_ID" \
  "SELECT COUNT(*) AS value_count
   FROM UNNEST(GENERATE_ARRAY(1, 1000000))"

bq query \
  --use_legacy_sql=false \
  --location="$BQ_LOCATION" \
  "SELECT
     job_id,
     reservation_id,
     edition,
     total_slot_ms,
     TIMESTAMP_DIFF(start_time, creation_time, MILLISECOND) AS queue_ms,
     error_result
   FROM \`$GOOGLE_CLOUD_PROJECT\`.\`$BQ_REGION_QUALIFIER\`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
   WHERE job_id = '$BQ_PROBE_JOB_ID'"
```

The probe passes only when:

```text
reservation_id = BQ_ADMIN_PROJECT:BQ_LOCATION.BQ_RESERVATION
edition        = ENTERPRISE
error_result   = NULL
```

If `reservation_id` is `NULL`, do not start. Wait longer and inspect the
assignment. If it names a different reservation, check inherited and more
specific assignments.

## Record the reservation in the fresh run

Create a new run using `REAL_RUN.md`, but identify the capacity model correctly
instead of reusing `on_demand` metadata:

```bash
export BQ_PRICING_MODEL=capacity_enterprise_payg_fixed
export BQ_SLOT_COUNT=500
export BQ_RESERVATION=THE_VERIFIED_RESERVATION_NAME
export BQ_ADMIN_PROJECT=pmm-project-377716
```

Append these variables to the new run's `run.env`:

```bash
printf 'export BQ_PRICING_MODEL=%q\nexport BQ_SLOT_COUNT=%q\nexport BQ_RESERVATION=%q\nexport BQ_ADMIN_PROJECT=%q\n' \
  "$BQ_PRICING_MODEL" \
  "$BQ_SLOT_COUNT" \
  "$BQ_RESERVATION" \
  "$BQ_ADMIN_PROJECT" \
  >> "$BQ_RUN_ENV"
```

Use truthful runner metadata for both dashboard and drill-down invocations:

```bash
"BigQuery (GCP)" "Enterprise PAYG reservation" "${BQ_SLOT_COUNT} slots" \
  "fixed ${BQ_SLOT_COUNT}-slot reservation" 0
```

After `$BQ_RESULTS` exists, capture the provider configuration outside the
timed measurement window:

```bash
bq query \
  --use_legacy_sql=false \
  --location="$BQ_LOCATION" \
  --format=json \
  "SELECT
     reservation_name,
     edition,
     slot_capacity,
     ignore_idle_slots,
     target_job_concurrency,
     IFNULL(autoscale.max_slots, 0) AS autoscale_max_slots
   FROM \`$BQ_ADMIN_PROJECT\`.\`$BQ_REGION_QUALIFIER\`.INFORMATION_SCHEMA.RESERVATIONS_BY_PROJECT
   WHERE reservation_name = '$BQ_RESERVATION'" \
  > "$BQ_RESULTS/reservation_config.json"

bq query \
  --use_legacy_sql=false \
  --location="$BQ_LOCATION" \
  --format=json \
  "SELECT
     assignment_id,
     reservation_name,
     assignee_id,
     assignee_type,
     job_type
   FROM \`$BQ_ADMIN_PROJECT\`.\`$BQ_REGION_QUALIFIER\`.INFORMATION_SCHEMA.ASSIGNMENTS_BY_PROJECT
   WHERE reservation_name = '$BQ_RESERVATION'" \
  > "$BQ_RESULTS/reservation_assignment.json"
```

## Verify every measured query during and after the run

The definitive check is the job metadata, not the configuration command:

```bash
bq query \
  --use_legacy_sql=false \
  --location="$BQ_LOCATION" \
  "SELECT
     (SELECT value FROM UNNEST(labels) WHERE key = 'component') AS component,
     reservation_id,
     edition,
     COUNT(*) AS jobs,
     COUNTIF(error_result IS NOT NULL) AS failed_jobs,
     ROUND(MAX(SAFE_DIVIDE(
       total_slot_ms,
       TIMESTAMP_DIFF(end_time, start_time, MILLISECOND)
     )), 1) AS max_single_job_average_slots,
     MAX(TIMESTAMP_DIFF(start_time, creation_time, MILLISECOND)) AS max_queue_ms
   FROM \`$GOOGLE_CLOUD_PROJECT\`.\`$BQ_REGION_QUALIFIER\`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
   WHERE creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 HOUR)
     AND state = 'DONE'
     AND EXISTS (
       SELECT 1
       FROM UNNEST(labels)
       WHERE key = 'run_id'
         AND value = '$BQ_RUN_ID'
     )
   GROUP BY component, reservation_id, edition
   ORDER BY component"
```

All dashboard and drill-down jobs must show the declared reservation and
Enterprise edition. Any `NULL` or different reservation ID is a configuration
failure and must be reported; do not silently mix those jobs into the reserved
run.

Continue collecting `JOBS_TIMELINE_BY_PROJECT` metrics. Under a fixed N-slot
pool, sustained runnable units plus growing queue time indicate that N is
constraining latency. That is a valid benchmark result, not a reason to resize
the reservation after the run starts.

## Clean up immediately after evidence collection

First stop the runners and allow all jobs using the reservation to finish.
Deleting a reservation while jobs are executing causes those jobs to fail.

Resolve the exact assignment ID from the system view:

```bash
export BQ_ASSIGNMENT_ID="$(
  bq query \
    --use_legacy_sql=false \
    --location="$BQ_LOCATION" \
    --format=json \
    "SELECT assignment_id
     FROM \`$BQ_ADMIN_PROJECT\`.\`$BQ_REGION_QUALIFIER\`.INFORMATION_SCHEMA.ASSIGNMENTS_BY_PROJECT
     WHERE reservation_name = '$BQ_RESERVATION'
       AND assignee_id = '$GOOGLE_CLOUD_PROJECT'
       AND job_type = 'QUERY'" \
  | jq -r 'if length == 1 then .[0].assignment_id else empty end'
)"

test -n "$BQ_ASSIGNMENT_ID"
printf 'Deleting assignment %s.%s\n' "$BQ_RESERVATION" "$BQ_ASSIGNMENT_ID"
```

Only after confirming those exact values, delete the assignment and then the
reservation:

```bash
bq rm -f \
  --project_id="$BQ_ADMIN_PROJECT" \
  --location="$BQ_LOCATION" \
  --reservation_assignment "$BQ_RESERVATION.$BQ_ASSIGNMENT_ID"

bq rm -f \
  --project_id="$BQ_ADMIN_PROJECT" \
  --location="$BQ_LOCATION" \
  --reservation "$BQ_RESERVATION"
```

Because this procedure never creates a capacity commitment, there is no
long-lived commitment to cancel. Deleting the reservation ends its PAYG
baseline allocation. If a commitment is ever added later, deleting a
reservation does **not** cancel the commitment or its charges.

Finally, create a new probe job and verify whether it returns to on-demand
(`reservation_id IS NULL`) or inherits some parent reservation. Assignment
changes apply only to new jobs; jobs already running retain their original
assignment.

## Benchmark interpretation checklist

- [ ] Reservation created before the run, never resized during it.
- [ ] Enterprise edition, baseline N, autoscaling 0, idle borrowing disabled.
- [ ] No capacity commitment created for the short run.
- [ ] Assignment allowed at least five minutes to propagate.
- [ ] Pre-run probe named the expected reservation.
- [ ] Every measured dashboard and drill-down job named that reservation.
- [ ] Reservation configuration and assignment JSON saved with the run.
- [ ] Ingestion, MV maintenance, and query compute accounted separately.
- [ ] No unrelated project queries shared the reservation, or their use was
      measured and reported.
- [ ] Assignment removed and reservation deleted after in-flight jobs and final
      evidence collection completed.
