# Six-hour Zerobus and MV retry

This retry uses a fresh target and the settings accepted by
`ingest_mv_40m_20260917T090123Z`:

- compact incrementally maintained task ranges;
- six wall-clock hours at a 1M-EPS target;
- Arrow Flight automatic recovery disabled;
- proactive ten-minute stream rotation;
- flush, close, and zero-unacknowledged proof before each replacement stream;
- fail-closed behavior on any unexpected interruption;
- 60-second metadata-only MV monitoring;
- 15 minutes of post-ingest MV catch-up monitoring;
- deferred post-measurement source hashing; and
- initial plus settled evidence/cost collection.

## September 17 stopped-run result

The operator intentionally stopped run
`ingest_mv_6h_r2_20260917T104305Z` after 4h08m to collect results and prepare
the canonical full baseline. The shortened run passed every applicable
ingest, recovery, MV, and evidence gate:

- client submitted and acknowledged 14,877,188,034 rows;
- provider metadata reported exactly 14,877,188,034 rows and
  1,086,746,940,112 committed bytes;
- average committed throughput was 999,930 EPS;
- 2,956 of 2,957 derived five-second intervals were within ±10%; the sole
  exception was the deliberately shortened final interval;
- every completed 30-minute window averaged approximately 1M EPS;
- all 16 workers completed 24 proactive rotations with zero unacknowledged
  batches;
- automatic recovery remained disabled;
- provider and client error counts were zero;
- 184 MV refreshes were incremental and zero were full;
- final MV source rows exactly matched the raw provider count;
- final MV catch-up completed about 195 seconds after producer shutdown;
- source hashes completed for 39 selected files; and
- initial target-scoped evidence collection completed without errors.

Initial billing is provisional. It currently reports USD 28.08 Zerobus, USD
14.50 MV refresh, and USD 1.74 Predictive Optimization: USD 44.31 fresh-path
cost. The excluded monitor warehouse currently reports USD 4.02. Recollect
after at least 24 hours before using any cost result.

Because the run was stopped before six hours, it is a successful long-run
characterization rather than the originally requested six-hour completion.

## Create the fresh target

Run on `<control-warehouse-name>`:

```sql
CREATE TABLE costbench.rt_qualification.quotes_ingest_mv_6h_r2_20260917
LIKE costbench.rt_qualification.quotes;

GRANT SELECT, MODIFY
ON TABLE costbench.rt_qualification.quotes_ingest_mv_6h_r2_20260917
TO `<service-principal-application-id>`;

CREATE MATERIALIZED VIEW
costbench.rt_qualification.quotes_daily_ingest_mv_6h_r2_20260917
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
FROM costbench.rt_qualification.quotes_ingest_mv_6h_r2_20260917
GROUP BY sym, day;
```

Transfer the MV owner to `<benchmark-service-principal>` and retain the benchmark administrator's `MANAGE` and
`SELECT`. Run the SQL verification gate from
[SIX_HOUR_INGEST_MV.md](SIX_HOUR_INGEST_MV.md), substituting the retry object
names. Do not continue unless the raw target is pristine version 0 with zero
files, the Delta features and clustering copied, Predictive Optimization is
effectively enabled, and the MV is incrementally refreshable.

## Launch

```sh
tmux new -As ingest-mv-6h-r2
/home/ubuntu/costbench/full-path-realtime/quotes/databricks/run_ingest_mv_6h_r2.sh
```

Expected nominal output is approximately 21.6B rows. Actual acceptance is
duration-based: average committed throughput must remain within 900k–1.1M EPS
for the six-hour window.

The run also requires:

- client submitted/acknowledged rows exactly equal provider records and the
  final MV source snapshot;
- all 16 logical streams complete at least 35 clean proactive rotations;
- every rotation and final close has zero unacknowledged batches;
- provider stream errors and client ambiguity remain zero;
- all data-changing MV updates are incremental;
- final MV rows-behind reaches zero; and
- target-scoped evidence collection completes.

Billing can take up to 24 hours to settle. Recollect using the exact run root
printed at completion:

```sh
/home/ubuntu/costbench/full-path-realtime/quotes/databricks/collect_ingest_mv_6h_settled.sh \
  /data/databricks-qualification/ingest_mv_6h_r2_<RUN_TIMESTAMP>
```
