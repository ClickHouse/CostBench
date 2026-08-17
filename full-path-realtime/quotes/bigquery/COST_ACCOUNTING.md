# BigQuery benchmark cost accounting

Last verified against the Google Cloud documentation on 2026-08-10.

This benchmark keeps each cost-producing mechanism in a separate ledger. Do
not report only dashboard/drill-down query cost: continuous ingestion, raw and
materialized storage, and materialized-view maintenance are part of the
full-path system cost.

## Short answer

- **Materialized-view refresh is charged.** “Zero maintenance” means that
  BigQuery operates automatic refreshes without user orchestration; it does not
  mean zero billing.
- **Materialized-view storage is charged.** The persisted aggregate occupies
  BigQuery-managed storage.
- **Queries against the MV are charged.** Without `max_staleness`, this can
  include both the persisted MV and the unmaterialized base-table delta.
- **Automatic reclustering is a free BigQuery operation** and Google states
  that it has no effect on query capacity.
- **The clustered table is not free.** Its stored bytes and queries retain the
  normal BigQuery storage and query charges. Storage Write API ingestion is
  also a separate ledger.

## Cost ledger for this benchmark

| Component | On-demand project | Capacity/reservation project | Evidence |
|---|---|---|---|
| Storage Write API ingest | Priced ingestion bytes, subject to the current free allowance and contract | Same separate ingestion charge | `WRITE_API_TIMELINE_BY_PROJECT.total_input_bytes`; Cloud Billing export is authoritative |
| Raw `quotes` storage | Stored bytes over time | Same | Final table metadata plus Cloud Billing export |
| Automatic reclustering | No separate compute charge | No query-capacity consumption | Table clustering configuration; no cost job should be added |
| MV automatic refresh | Bytes processed by refresh | Slots/capacity consumed by refresh | `mv_refresh_jobs.jsonl` |
| `quotes_daily` storage | Stored MV bytes over time | Same | MV metadata plus Cloud Billing export |
| Dashboard queries | `total_bytes_billed` | Reservation/autoscale capacity cost; `total_slot_ms` is attribution evidence | Dashboard JSONL and query jobs |
| Drill-down queries | `total_bytes_billed` | Reservation/autoscale capacity cost; `total_slot_ms` is attribution evidence | Drill-down JSONL and query jobs |
| Harness/evidence queries | Separate measurement overhead | Separate measurement overhead | Jobs labelled `component=monitor` or `component=evidence` |

Free-tier allowances, negotiated discounts, credits, currency conversion, and
taxes are account-specific. Provider usage fields establish consumption; the
Cloud Billing export or invoice establishes the final currency amount.

## Collect the provider evidence

This is the same bounded collection sequence used in Step 6 of `REAL_RUN.md`
and `_commands.txt`. Immediately after the final measured query, stop the
runners consistently and run the following in a normal shell:

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
export BQ_EVIDENCE_DIR="$BQ_RESULTS/evidence"

printf 'since=%s\nuntil=%s\noutput=%s\n' \
  "$BQ_EVIDENCE_SINCE" "$BQ_EVIDENCE_UNTIL" "$BQ_EVIDENCE_DIR"

python3 collect_evidence.py \
  --project "$GOOGLE_CLOUD_PROJECT" \
  --dataset "$BQ_DATASET" \
  --location "$BQ_LOCATION" \
  --since "$BQ_EVIDENCE_SINCE" \
  --until "$BQ_EVIDENCE_UNTIL" \
  --run-id "$BQ_RUN_ID" \
  --output-dir "$BQ_EVIDENCE_DIR"

jq '.errors' "$BQ_EVIDENCE_DIR/evidence_summary.json"
```

The collector writes:

| File | Ledger |
|---|---|
| `write_api_timeline.jsonl` | Storage Write API usage |
| `mv_refresh_jobs.jsonl` | Target MV automatic-refresh jobs |
| `query_and_mv_jobs.jsonl` | Query-job reconciliation |
| `table_storage.jsonl` | Raw-table and MV storage snapshot |
| `evidence_summary.json` | Collection window, table metadata, collector overhead, and errors |

Require an explicit `--since` at least one minute before ingestion started.
The explicit `--until` prevents later refreshes or writes from leaking into the
final evidence manifest. For an active-run diagnostic, omit `--until`; the
collector fixes it to its own start time.

Validate the collection before calculating costs:

```bash
jq '{since, until, project, dataset, errors}' \
  "$BQ_EVIDENCE_DIR/evidence_summary.json"

wc -l \
  "$BQ_EVIDENCE_DIR/write_api_timeline.jsonl" \
  "$BQ_EVIDENCE_DIR/mv_refresh_jobs.jsonl" \
  "$BQ_EVIDENCE_DIR/query_and_mv_jobs.jsonl" \
  "$BQ_EVIDENCE_DIR/table_storage.jsonl"
```

## 1. Storage Write API ingestion

### What is charged

The Storage Write API is a separate ingestion SKU. As of 2026-08-10, list
pricing is $0.025/GiB and the first 2 TiB per month per billing account is free.
The allowance is not per project or per benchmark run.

There is no query-style `total_bytes_billed` field for this path.
`WRITE_API_TIMELINE_BY_PROJECT.total_input_bytes` is the provider-observed
number of bytes in rows sent through the API. Treat successful `OK` buckets as
run-attributed ingestion usage and use Cloud Billing for the final charge.

### How to get the data

```bash
jq -s '{
  minute_buckets: (map(.start_timestamp) | unique | length),
  successful_requests:
    (map(select(.error_code == "OK") | (.total_requests // 0)) | add // 0),
  successful_rows:
    (map(select(.error_code == "OK") | (.total_rows // 0)) | add // 0),
  provider_input_bytes:
    (map(select(.error_code == "OK") | (.total_input_bytes // 0)) | add // 0),
  provider_input_GiB:
    ((map(select(.error_code == "OK") | (.total_input_bytes // 0))
      | add // 0) / 1073741824),
  provider_input_TiB:
    ((map(select(.error_code == "OK") | (.total_input_bytes // 0))
      | add // 0) / 1099511627776),
  list_price_usd_before_free_tier:
    (((map(select(.error_code == "OK") | (.total_input_bytes // 0))
       | add // 0) / 1073741824) * 0.025),
  already_exists_requests:
    (map(select(.error_code == "ALREADY_EXISTS") |
      (.total_requests // 0)) | add // 0),
  unexpected_error_requests:
    (map(select(.error_code != "OK" and .error_code != "ALREADY_EXISTS") |
      (.total_requests // 0)) | add // 0),
  non_ok_buckets:
    (map(select(.error_code != "OK") |
      {start_timestamp, error_code, total_requests, total_rows,
       total_input_bytes}))
}' "$BQ_EVIDENCE_DIR/write_api_timeline.jsonl"
```

`list_price_usd_before_free_tier` is a transparent list-price estimate, not an
invoice. Reconcile provider rows with the ingester's `acknowledged_rows` and
`expected_input_rows`. A matching `ALREADY_EXISTS` request is valid only when
it reconciles to a client-recorded recovered exact-offset append; require zero
other error codes.

## 2. Materialized-view refresh

BigQuery documents three chargeable MV components:

1. querying the MV;
2. maintaining it, including automatic or manual refresh;
3. storing the materialized data.

For automatic refresh, the project containing the MV is billed. For manual
refresh, the project that runs the manual refresh job is billed.

Under on-demand analysis pricing, refresh maintenance is based on bytes
processed during refresh. Under capacity pricing, refresh consumes slots. The
automatic jobs can be identified in `JOBS_BY_PROJECT` by the
`materialized_view_refresh` text in their job ID. This repository's
`collect_evidence.py` exports the relevant jobs to:

```text
evidence/mv_refresh_jobs.jsonl
```

These system-generated jobs have `destination_table = NULL`. The collector
uses `referenced_tables` to restrict the export to the benchmark's exact
project, dataset, and MV rather than including unrelated project refreshes.

### How to get the data

Summarize the target MV's provider-reported usage:

```bash
jq -s '{
  refresh_jobs: length,
  failed_jobs: (map(select(.error_result != null)) | length),
  processed_bytes: (map(.total_bytes_processed // 0) | add // 0),
  billed_bytes: (map(.total_bytes_billed // 0) | add // 0),
  billed_GiB:
    ((map(.total_bytes_billed // 0) | add // 0) / 1073741824),
  billed_TiB:
    ((map(.total_bytes_billed // 0) | add // 0) / 1099511627776),
  slot_seconds: (map((.total_slot_ms // 0) / 1000) | add // 0),
  first_refresh: (map(.creation_time) | min),
  last_refresh: (map(.creation_time) | max)
}' "$BQ_EVIDENCE_DIR/mv_refresh_jobs.jsonl"
```

For the current on-demand run, apply the effective on-demand analysis price to
the refresh jobs' billed usage. Retain both `total_bytes_processed` and
`total_bytes_billed`: Google specifically recommends processed bytes and slot
milliseconds for monitoring refresh cost, while billed bytes is the direct
on-demand job billing field.

Do not add refresh `slot_seconds` to the on-demand dollar cost. They describe
engine work, but billed bytes are the on-demand billing basis. Conversely, do
not price every job's slot-seconds independently under a fixed reservation: the
reservation/autoscale capacity is the purchased product, and job slot time is
used to attribute that capacity.

## 3. Dashboard and drill-down queries

### What is charged

Under on-demand pricing, each query is charged using
`total_bytes_billed`. Under capacity pricing, queries consume the assigned
reservation/autoscale capacity; `total_slot_ms` attributes resource use but is
not a second independent charge.

The runner disables the result cache and stores aligned `result`,
`billed_bytes`, and `billed_slot_sec` arrays plus full `query_jobs` records.
Dashboard and drill-down must remain separate ledgers.

### How to get the data

```bash
for ledger in mv raw; do
  jq -s --arg ledger "$ledger" '
    [.[].query_jobs[]?] as $jobs
    | {
        ledger: $ledger,
        query_jobs: ($jobs | length),
        failed_jobs: ($jobs | map(select(.error != null)) | length),
        cache_hits: ($jobs | map(select(.cache_hit == true)) | length),
        processed_bytes:
          ($jobs | map(.total_bytes_processed // 0) | add // 0),
        billed_bytes:
          ($jobs | map(.total_bytes_billed // 0) | add // 0),
        billed_GiB:
          (($jobs | map(.total_bytes_billed // 0) | add // 0)
            / 1073741824),
        billed_TiB:
          (($jobs | map(.total_bytes_billed // 0) | add // 0)
            / 1099511627776),
        slot_seconds:
          (($jobs | map(.total_slot_ms // 0) | add // 0) / 1000)
      }
  ' "$BQ_RESULTS/$ledger"/*.jsonl
done
```

Use the runner JSONL as the measured-query source. Use
`query_and_mv_jobs.jsonl` to reconcile job IDs and provider fields; do not add
the same jobs from both files.

### Materialized-view query nuance

On-demand MV query charges include the bytes read from the materialized data
and any necessary portions of the base table. In the current benchmark,
`max_staleness` is unset, so BigQuery can combine the persisted MV with newer
base-table changes. A lagging refresh watermark can therefore move work from
background refresh into the dashboard query rather than making that work free.

This is why the benchmark reports all of the following separately:

- dashboard `billed_bytes` and `billed_slot_sec`;
- automatic-refresh billed/processed bytes and slot time;
- refresh-watermark lag.

`max_staleness` is out of scope for this benchmark because the workload queries
newly ingested events immediately and requires current results. There is no
`max_staleness` cost variant to account for.

## Unbilled automatic reclustering

`CLUSTER BY sym, t` does not add a clustering surcharge. BigQuery charges for
the table's storage and for queries, while automatic reclustering itself is a
free operation. Google also states that automatic reclustering has no effect
on query capacity.

Clustering still changes economics indirectly: block pruning can reduce bytes
scanned under on-demand pricing and can reduce query work under either model.
For clustered tables, the final bytes scanned are known after execution, so use
the completed job's `total_bytes_billed` rather than an up-front estimate.

The free-operation statement applies to BigQuery's automatic reclustering. A
user-run `UPDATE`, `CREATE TABLE AS SELECT`, or other query that rewrites data
to impose a layout is still a query/DML job and must be accounted for normally.
This benchmark creates the table clustered from the start and does not add a
manual rewrite job.

## 4. Raw-table and materialized-view storage

### What is charged

The raw table and MV are separately stored and both are charged. The applicable
bytes depend on the dataset's logical or physical storage billing model.
Storage is accumulated over time and billed in GiB-month or TiB-month, so an
instantaneous snapshot is evidence of size, not the exact prorated invoice.

### How to get the data

`collect_evidence.py` exports both objects from
`TABLE_STORAGE_BY_PROJECT`. Inspect logical, current physical, time-travel, and
fail-safe bytes:

```bash
jq -s '
  map({
    table: .table_name,
    table_type,
    rows: .total_rows,
    logical_GiB: ((.total_logical_bytes // 0) / 1073741824),
    current_physical_GiB:
      ((.current_physical_bytes // 0) / 1073741824),
    time_travel_physical_GiB:
      ((.time_travel_physical_bytes // 0) / 1073741824),
    fail_safe_physical_GiB:
      ((.fail_safe_physical_bytes // 0) / 1073741824),
    storage_last_modified_time
  })
' "$BQ_EVIDENCE_DIR/table_storage.jsonl"
```

Check the dataset's effective storage model:

```bash
bq show --format=json "$GOOGLE_CLOUD_PROJECT:$BQ_DATASET" \
  | jq '{
      storage_billing_model:
        (.storageBillingModel // "LOGICAL (default)")
    }'
```

The table metadata snapshots in `evidence_summary.json` are a second
reconciliation source. Use Cloud Billing export for the actual bytes-over-time
charge.

Do not treat `WRITE_API_TIMELINE.total_input_bytes` as table storage bytes. It
is provider-observed input on the ingestion path. Compression and BigQuery's
logical or physical storage billing model make storage a different quantity.

## Final currency reconciliation

System tables provide the cleanest run-attributed usage. Cloud Billing provides
the actual account currency, effective pricing, free-tier allocation, credits,
and taxes, but it can arrive hours later and commonly has hourly rather than
run-level granularity.

If billing export is enabled, query the standard or detailed export table for
the benchmark project and UTC window, then group by SKU. Replace the table name
with the actual billing-export table:

```bash
export BQ_BILLING_EXPORT_TABLE="BILLING_PROJECT.BILLING_DATASET.gcp_billing_export_v1_ACCOUNT_ID"
export BQ_BILLING_LOCATION="US"  # location of the billing-export dataset

bq query --use_legacy_sql=false --location="$BQ_BILLING_LOCATION" \
  "SELECT
     service.description AS service,
     sku.description AS sku,
     usage.pricing_unit,
     SUM(usage.amount_in_pricing_units) AS usage_in_pricing_units,
     SUM(cost) AS gross_cost,
     SUM((SELECT COALESCE(SUM(credit.amount), 0)
          FROM UNNEST(credits) AS credit)) AS credits,
     SUM(cost) + SUM((SELECT COALESCE(SUM(credit.amount), 0)
                      FROM UNNEST(credits) AS credit)) AS net_cost,
     ANY_VALUE(currency) AS currency
   FROM \`$BQ_BILLING_EXPORT_TABLE\`
   WHERE project.id = '$GOOGLE_CLOUD_PROJECT'
     AND usage_start_time < TIMESTAMP('$BQ_EVIDENCE_UNTIL')
     AND usage_end_time > TIMESTAMP('$BQ_EVIDENCE_SINCE')
     AND service.description = 'BigQuery'
   GROUP BY service, sku, usage.pricing_unit
   ORDER BY net_cost DESC"
```

Do not expect the Cloud Billing window to isolate this run perfectly if the
project had concurrent workloads. Reconcile each SKU with the system-table
usage and disclose any shared free-tier or account-level allocation.

## Measurement overhead

`INFORMATION_SCHEMA` queries are themselves chargeable. On-demand projects
incur the normal minimum bytes per query; capacity projects consume slots, and
the results are not cached. `evidence_summary.json` records the collector's
query jobs. Keep monitor/evidence overhead separate from workload query cost.

## Reporting rule

For each published run, report at least:

```text
ingestion
+ raw storage
+ automatic MV refresh maintenance
+ MV storage
+ dashboard queries
+ drill-down queries
= full-path BigQuery cost
```

Report automatic reclustering as **$0 separate maintenance charge**, with the
normal raw-table storage and query charges still present. Report harness and
evidence-query overhead separately and exclude it from the workload total only
when the exclusion is explicit and consistently applied across systems.

## Official references

- [Materialized-view pricing](https://docs.cloud.google.com/bigquery/docs/materialized-views-intro#materialized_views_pricing)
- [Monitor automatic refresh jobs and their cost fields](https://docs.cloud.google.com/bigquery/docs/materialized-views-monitor#monitor_materialized_views)
- [Clustered-table pricing and automatic reclustering](https://docs.cloud.google.com/bigquery/docs/clustered-tables)
- [Storage Write API and its free allowance](https://docs.cloud.google.com/bigquery/docs/write-api)
- [Storage Write API timeline fields](https://docs.cloud.google.com/bigquery/docs/information-schema-write-api)
- [Table and MV storage fields](https://docs.cloud.google.com/bigquery/docs/information-schema-table-storage)
- [Cloud Billing export schema](https://docs.cloud.google.com/billing/docs/how-to/export-data-bigquery-tables/standard-usage)
- [Current BigQuery pricing](https://cloud.google.com/bigquery/pricing)
