# Forty-minute Zerobus recovery qualification

This fresh-target run validates two fixes exposed by the September 16
six-hour characterization:

1. completed-task evidence is maintained with incremental ordinal ranges
   rather than rescanning all prior row groups under one lock; and
2. SDK automatic recovery is disabled. Each logical worker proactively rotates
   its stream every ten minutes only after flushing, closing, and proving zero
   unacknowledged Arrow batches.

Any unexpected stream interruption fails closed. Nothing is replayed onto the
measured target.

## September 17 result

Run `ingest_mv_40m_20260917T090123Z` passed the recovery qualification:

- 2,400,607,668 rows were submitted and durably acknowledged by the client;
- `system.lakeflow.zerobus_ingest` reported exactly 2,400,607,668 rows and
  175,385,857,760 committed bytes;
- provider/client row difference was exactly zero;
- 2,401.61-second duration and 999,582 average committed EPS;
- all 478 derived five-second intervals were within 900k–1.1M EPS;
- 10-minute window averages were 1,000,094, 1,000,136, 999,731, and 1,000,252
  EPS, showing no downward trend;
- producer process CPU p95 was 0.371 cores and median transmit throughput was
  76.65 MB/s;
- all 16 logical workers completed three proactive rotations;
- provider metadata showed exactly 64 streams: 16 at startup and 16 at each
  ten-minute rotation;
- every rotation and final close had zero unacknowledged batches;
- automatic recovery remained disabled and provider stream errors were zero;
- 34 MV refreshes were incremental and none was a full recompute;
- active MV rows-behind p50/p95/max were 62.1M, 134.6M, and 139.3M;
- final MV source rows exactly matched the provider raw-row total; and
- final MV catch-up completed approximately 248 seconds after the last
  provider raw commit.

The target-scoped evidence collection completed without errors. Source hashes
also completed after the measurement window. Billing had not populated yet,
so cost remains pending the settled recollection.

This result validates the client journal fix and the fail-closed proactive
rotation strategy over three rotation boundaries. It does not by itself
replace a six-hour endurance run; the same frozen settings must now pass on a
fresh six-hour target.

## Create fresh targets

```sql
CREATE TABLE costbench.rt_qualification.quotes_ingest_mv_40m_20260917
LIKE costbench.rt_qualification.quotes;

GRANT SELECT, MODIFY
ON TABLE costbench.rt_qualification.quotes_ingest_mv_40m_20260917
TO `<service-principal-application-id>`;

CREATE MATERIALIZED VIEW
costbench.rt_qualification.quotes_daily_ingest_mv_40m_20260917
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
FROM costbench.rt_qualification.quotes_ingest_mv_40m_20260917
GROUP BY sym, day;
```

Transfer MV ownership to `<benchmark-service-principal>` and retain the benchmark administrator's `MANAGE` and
`SELECT`. Apply the same metadata-only verification gate documented in
[SIX_HOUR_INGEST_MV.md](SIX_HOUR_INGEST_MV.md), substituting the 40-minute
object names.

## Run

```sh
tmux new -As ingest-mv-40m
/home/ubuntu/costbench/full-path-realtime/quotes/databricks/run_ingest_mv_40m.sh
```

The run lasts 40 minutes, targets 1M EPS, and monitors MV freshness for ten
additional minutes.

## Acceptance

- average committed EPS is 900k–1.1M and does not trend down with task count;
- all logical workers complete at least three proactive stream rotations;
- automatic recovery is disabled;
- every rotation and final close reports zero unacknowledged batches;
- producer submitted/acknowledged rows exactly equal
  `system.lakeflow.zerobus_ingest` records and the MV source snapshot;
- no terminal, ambiguous, stream, or freshness errors occur;
- every data-changing MV refresh is incremental; and
- the MV catches up and a subsequent no-op confirms no remaining source
  changes.

Do not proceed to another six-hour run if any row discrepancy remains. Zerobus
does not expose a server-side idempotency key for this canonical 12-column
schema; a persistent mismatch must be escalated as SDK/service evidence rather
than corrected in the measured table.
