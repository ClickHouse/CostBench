# Six-hour Zerobus and materialized-view run

This run characterizes the write and maintenance path while Lakehouse//RT is
unavailable. It is not a publishable full-path result because it has no
Lakehouse//RT dashboard or drill-down reads.

## September 16 run result

Run `ingest_mv_6h_20260916T164822Z` completed the six-hour window without a
terminal client or Zerobus error, but **did not pass the 1M-EPS gate**:

- producer-acknowledged rows: 11,539,474,122;
- provider table/system metadata rows: 11,539,549,122;
- unexplained provider excess: 75,000 rows;
- average provider-acknowledged rate: 534,103 rows/second;
- first 30-minute average: 1,000,083 rows/second;
- final 30-minute average: 309,663 rows/second;
- all 16 client streams closed cleanly with zero pending batches; and
- host CPU remained around 3%, memory remained healthy, and network output
  declined with throughput.

The throughput decline is attributable to the evidence-journal
implementation, not established as a Databricks capacity limit. Every batch
called `_refresh_locked()`, which scanned and sorted the entire growing
completed-task map while holding one lock. Completed row-group tasks grew from
13,744 after 30 minutes to 88,202 at the end, while the producer process
converged on one CPU core and network output fell from 76.7 MB/s to 23.7 MB/s.
The implementation has since been changed to update compact completion ranges
incrementally rather than rescanning completed tasks.

The 75,000-row provider/client mismatch is independently fatal for
publication. Zerobus opened 413 provider streams over the run despite 16
logical client workers, consistent with periodic SDK recovery/rotation.
Zerobus is at least once and Arrow recovery can replay data. The mismatch must
be explained or eliminated before another accepted endurance run.

The maintenance path otherwise behaved correctly:

- 330 incremental `GROUP_AGGREGATE` refreshes completed during ingestion;
- zero full recomputes occurred;
- median refresh completion cadence was 61.1 seconds and p95 was 118.1
  seconds;
- median `WAITING_FOR_RESOURCES` was 10.4 seconds;
- median incremental `RUNNING` time was 27.8 seconds;
- the final data refresh completed 49.1 seconds after the last raw commit;
- a subsequent `NO_OP` confirmed full MV catch-up;
- active MV rows-behind p50/p95/max were 24.2M, 66.5M, and 145.2M; and
- all 751 monitor observations were error-free.

Predictive Optimization performed four clustering operations and two ANALYZE
operations. The final raw table retained `CLUSTER BY (sym, t)` and occupied
96,971,982,760 bytes across 2,235 active files. The largest observed
clustering operation processed 164,736,452,264 bytes.

Initial, not-yet-settled list cost was USD 40.38 for Zerobus, USD 45.29 for MV
refresh, and USD 6.32 for Predictive Optimization: USD 91.98 for the
fresh-data path. The monitor warehouse added USD 18.33 of excluded
instrumentation cost. These values use account price history of USD 0.39/DBU
for Premium Jobs Serverless and USD 0.91/DBU for Premium Serverless SQL in
Ireland. Billing was only partially settled and must be recollected after 24
hours.

The initial collector incorrectly attempted `DESCRIBE DETAIL` and `DESCRIBE
HISTORY` on the MV, which Databricks exposes as a view for those commands.
This made the evidence summary incomplete even though the cost inputs were
collected. The settled collector now uses `DESCRIBE TABLE EXTENDED ... AS
JSON` for the MV and no longer requires unsupported MV history.

## Frozen workload

- Duration at target rate: 6 hours
- Target rate: 1,000,000 provider-acknowledged rows/second
- Nominal result at target rate: 21,600,000,000 rows
- Source-selection envelope: 22,000,000,000 rows; a wall-clock deadline stops
  ingestion at six hours
- Producer: prepared same-region `m6i.8xlarge`
- Zerobus: 16 Arrow Flight streams, 50,000-row batches, no IPC compression
- Durability accounting: wait after every Arrow batch
- Raw target:
  `costbench.rt_qualification.quotes_ingest_mv_6h_20260916`
- MV target:
  `costbench.rt_qualification.quotes_daily_ingest_mv_6h_20260916`
- Raw layout: liquid clustering inherited from the canonical table,
  `CLUSTER BY (sym, t)`
- Maintenance: inherited Predictive Optimization
- MV: strict incremental, `CLUSTER BY (sym, day)`,
  `TRIGGER ON UPDATE AT MOST EVERY INTERVAL 1 MINUTE`
- Freshness sampling: every 60 seconds during ingestion and for 15 minutes
  after the target duration
- Control warehouse: `<control-warehouse-name>`, serverless `2X-Small`,
  `<control-warehouse-id>`

The MV remains on the observed trigger-on-update serverless path. The
system-managed-job preview is enabled, but no Performance optimized control is
available for this MV type in the current workspace. Do not claim that mode.

## Create fresh targets

Run on `<control-warehouse-name>`:

```sql
CREATE TABLE costbench.rt_qualification.quotes_ingest_mv_6h_20260916
LIKE costbench.rt_qualification.quotes;

GRANT SELECT, MODIFY
ON TABLE costbench.rt_qualification.quotes_ingest_mv_6h_20260916
TO `<service-principal-application-id>`;

CREATE MATERIALIZED VIEW
costbench.rt_qualification.quotes_daily_ingest_mv_6h_20260916
USING DELTA
CLUSTER BY (sym, day)
REFRESH POLICY INCREMENTAL STRICT
TRIGGER ON UPDATE AT MOST EVERY INTERVAL 1 MINUTE
AS
SELECT
    sym,
    date_add(
        DATE '1970-01-01',
        CAST(
            floor(
                CAST(t AS DECIMAL(20, 0))
                / CAST(86400000 AS DECIMAL(20, 0))
            ) AS INT
        )
    ) AS day,
    count(*) AS n_quotes,
    min(bp) AS bp_min,
    max(bp) AS bp_max,
    min(ap) AS ap_min,
    max(ap) AS ap_max,
    sum(bs) AS bs_sum,
    sum(`as`) AS as_sum,
    sum(ap - bp) AS spread_sum
FROM costbench.rt_qualification.quotes_ingest_mv_6h_20260916
GROUP BY sym, day;
```

Before ingestion:

1. transfer the new MV's ownership to `<benchmark-service-principal>`;
2. retain the benchmark administrator's `MANAGE` and `SELECT` grants;
3. verify the raw table has zero active files and only Delta version 0;
4. verify liquid clustering and the three Delta source features were copied;
5. verify effective Predictive Optimization is enabled; and
6. verify the MV definition is incrementalizable and owned by the service
   principal.

The harness repeats the zero-file/version-0 metadata check and issues no
`SELECT COUNT(*)`.

### SQL verification gate

Run these metadata-only statements after creating the objects and transferring
MV ownership.

Prove that the raw table is pristine:

```sql
DESCRIBE DETAIL
costbench.rt_qualification.quotes_ingest_mv_6h_20260916;

DESCRIBE HISTORY
costbench.rt_qualification.quotes_ingest_mv_6h_20260916
LIMIT 2;
```

Require all of the following:

- `format = delta`;
- `numFiles = 0`;
- `sizeInBytes = 0`;
- `clusteringColumns = ["sym", "t"]`; and
- history returns exactly one row with `version = 0` and operation
  `CREATE TABLE`.

Prove the three source-table features copied:

```sql
SHOW TBLPROPERTIES
costbench.rt_qualification.quotes_ingest_mv_6h_20260916
('delta.enableDeletionVectors');

SHOW TBLPROPERTIES
costbench.rt_qualification.quotes_ingest_mv_6h_20260916
('delta.enableRowTracking');

SHOW TBLPROPERTIES
costbench.rt_qualification.quotes_ingest_mv_6h_20260916
('delta.enableChangeDataFeed');
```

Each value must be `true`.

Prove effective Predictive Optimization, including inheritance:

```sql
DESCRIBE CATALOG EXTENDED costbench;

DESCRIBE TABLE EXTENDED
costbench.rt_qualification.quotes_ingest_mv_6h_20260916;

DESCRIBE TABLE EXTENDED
costbench.rt_qualification.quotes_daily_ingest_mv_6h_20260916;
```

The `Predictive Optimization` field must report `ENABLE` for both objects. An
inherited value from `costbench` is valid; `DISABLE`, missing, or unknown is
not.

Prove the MV owner and grants:

```sql
SHOW GRANTS ON MATERIALIZED VIEW
costbench.rt_qualification.quotes_daily_ingest_mv_6h_20260916;

DESCRIBE TABLE EXTENDED
costbench.rt_qualification.quotes_daily_ingest_mv_6h_20260916;
```

The `Owner` field must be `<benchmark-service-principal>` (Application ID
`<service-principal-application-id>`). the benchmark administrator must retain explicit `MANAGE`
and `SELECT`.

Finally, rerun the explained canonical definition without creating another
object:

```sql
EXPLAIN CREATE MATERIALIZED VIEW
costbench.rt_qualification.quotes_daily_ingest_mv_6h_20260916_explain_only
USING DELTA
CLUSTER BY (sym, day)
REFRESH POLICY INCREMENTAL STRICT
TRIGGER ON UPDATE AT MOST EVERY INTERVAL 1 MINUTE
AS
SELECT
    sym,
    date_add(
        DATE '1970-01-01',
        CAST(
            floor(
                CAST(t AS DECIMAL(20, 0))
                / CAST(86400000 AS DECIMAL(20, 0))
            ) AS INT
        )
    ) AS day,
    count(*) AS n_quotes,
    min(bp) AS bp_min,
    max(bp) AS bp_max,
    min(ap) AS ap_min,
    max(ap) AS ap_max,
    sum(bs) AS bs_sum,
    sum(`as`) AS as_sum,
    sum(ap - bp) AS spread_sum
FROM costbench.rt_qualification.quotes_ingest_mv_6h_20260916
GROUP BY sym, day;
```

Require:

```text
The Materialized View can be incrementally refreshed.
No issues detected.
```

## Launch

On the EC2 producer:

```sh
tmux new -As ingest-mv-6h
/home/ubuntu/costbench/full-path-realtime/quotes/databricks/run_ingest_mv_6h.sh
```

Type the exact target when prompted, then enter the service-principal secret.
The secret is read without echo, inherited only by the live processes, and
unset on exit.

Before opening Zerobus streams, the producer enumerates row-group metadata for
a 22B-row source envelope. It does not read and hash the selected files on the
startup path. File identity, size, and modification time are frozen in the
manifest; SHA-256 hashes are computed after ingestion and post-ingest freshness
monitoring finish, outside the measurement window.

The wrapper runs:

- six wall-clock hours of Zerobus ingestion, expected to produce approximately
  21.6B rows at the target rate;
- compact range-based recovery state suitable for the long source selection;
- five-second throughput and producer-resource telemetry;
- 60-second metadata-only Zerobus/MV monitoring;
- 15 minutes of post-ingest monitoring to prove MV catch-up;
- immediate target-scoped provider evidence collection;
- post-run source-file hashing; and
- an initial cost summary, which may be incomplete while billing settles.

The wrapper requires a clean, duration-driven stop, average committed EPS
within ±10% of 1M, no pending or ambiguous batches, error-free freshness
samples, final MV catch-up within the monitor window, exact client/provider
record reconciliation, and no full MV refresh.

## Pricing and cost boundary

Public AWS list pricing observed September 16, 2026:

- Zerobus Premium: USD 0.050 per GB sent to the ingestion API
- Zerobus Enterprise: USD 0.064 per GB sent to the ingestion API
- Zerobus translation: 0.143 DBU per GB; underlying service compute included
- Serverless Lakeflow Pipelines Premium: USD 0.35 per DBU
- Serverless Lakeflow Pipelines Enterprise: USD 0.45 per DBU
- Serverless Jobs used by Predictive Optimization Premium: USD 0.35 per DBU
- Serverless Jobs used by Predictive Optimization Enterprise: USD 0.45 per DBU

The 600M-row characterization committed 43,841,095,016 bytes, or approximately
73.07 bytes per row. At the same shape, 21.6B rows would send approximately
1.578 TB and cost about USD 78.91 at Premium list price or USD 101.01 at
Enterprise list price. This is an estimate only; canonical cost uses settled
`system.billing.usage` and the overlapping
`system.billing.list_prices` version.

Report separately:

- Zerobus volume usage;
- MV serverless pipeline DBUs, matched by the MV pipeline ID;
- Predictive Optimization serverless Jobs DBUs and operation history;
- raw and MV managed-storage size;
- the excluded `<control-warehouse-name>` instrumentation cost;
- EC2 producer compute and EBS storage; and
- cloud data transfer and requests.

The source download from `us-east-2` is setup and remains outside the measured
window. Measured producer-to-Zerobus traffic is same-region.

## Settled recollection

Billing usage can take up to 24 hours to settle. At least 24 hours after the
run, execute:

```sh
/home/ubuntu/costbench/full-path-realtime/quotes/databricks/collect_ingest_mv_6h_settled.sh
```

The script reads the immutable window and targets from the latest six-hour run
context, prompts for the secret, writes a new settled evidence directory, and
produces a versioned cost summary. It never overwrites the initial evidence.
