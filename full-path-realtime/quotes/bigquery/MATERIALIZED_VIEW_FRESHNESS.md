# BigQuery materialized-view refresh and staleness

Last verified against the Google Cloud documentation on 2026-08-11.

## Default BigQuery behavior: MV queries return current results

By default—when `max_staleness` is unset—a direct query against a BigQuery
incremental materialized view returns current results. The persisted MV may be
minutes behind, but BigQuery compensates at query time:

1. It reads the persisted MV result.
2. It reads newer inserts and changes from the base table that are not yet in
   that persisted result.
3. It merges that delta into the answer returned by the query.

Recently streamed rows are therefore read by the query even when the automatic
MV refresh has not incorporated them yet. A lagging refresh watermark can make
the query slower and more expensive, but it does not make the default direct-MV
answer stale.

## Central refresh contract: best effort, not a schedule

BigQuery automatic MV refresh is **best effort**. Setting
`refresh_interval_minutes = 1` does not mean that BigQuery refreshes the MV
once per minute. The option is only a frequency cap: it prevents automatic
refreshes from running more often than once per minute.

In particular, the one-minute setting provides:

- no guarantee that a refresh starts every minute;
- no guarantee that an eligible refresh starts within one minute;
- no guarantee about when a refresh completes; and
- no upper bound on persisted-MV refresh lag.

Google documents that BigQuery attempts to start an eligible automatic refresh
within approximately five minutes of a base-table change, but explicitly does
not guarantee that start time or the completion time. Automatic refresh is
treated similarly to batch-priority work. It can be delayed when project
capacity is unavailable, and expensive refreshes or many MVs can cause an
individual MV to lag significantly.

This is a persisted-cache maintenance property, not necessarily query-result
staleness. With `max_staleness` unset, direct queries still reflect the latest
base-table state by reading and merging changes that the persisted MV has not
yet incorporated. A lagging persisted refresh can therefore increase query
latency, bytes processed, and slot consumption even though the returned result
remains current.

Official source: [Manage materialized views: frequency cap and best-effort
refresh](https://docs.cloud.google.com/bigquery/docs/materialized-views-manage#frequency_cap).

## Three different concepts

The following settings and measurements are related, but they are not
interchangeable:

| Concept | Meaning |
|---|---|
| `refresh_interval_minutes` | A cap on the maximum frequency of automatic persisted-MV refreshes. |
| `refresh_watermark` | The base-table point through which data has been incorporated into the persisted MV cache. |
| `max_staleness` | An opt-in bound on the permitted staleness of a direct MV result. BigQuery evaluates it against the time of the last persisted MV refresh. If that refresh is within `X`, BigQuery can return only the persisted MV; if it is outside `X`, BigQuery performs query-time delta reconciliation. It is unset in this benchmark. |

The monitor's `watermark_lag_sec` measures persisted-cache lag. It is not
automatically the staleness of the answer returned by a dashboard query.

## Current benchmark configuration

`create.sql` currently creates the MV with:

```sql
OPTIONS (
    enable_refresh = TRUE,
    refresh_interval_minutes = 1
)
```

No `max_staleness` value is set.

The defaults and current choices are:

| Option | BigQuery default | Current benchmark |
|---|---:|---:|
| `enable_refresh` | `TRUE` | Explicitly `TRUE` |
| `refresh_interval_minutes` | 30 minutes | Explicitly 1 minute |
| `max_staleness` | Disabled | Unset/disabled |

The current dashboard queries reference `quotes_daily` directly. With
`max_staleness` disabled, BigQuery keeps direct MV query results consistent with
the latest base-table state. When the persisted MV cache is behind, BigQuery can
combine it with unprocessed base-table changes at query time. This runtime delta
work can increase latency, bytes processed, and slot consumption, but it avoids
intentionally returning a stale answer.

## Hard minimum: `refresh_interval_minutes = 1`

**One minute is BigQuery's documented hard minimum for
`refresh_interval_minutes`.** Seven days is the maximum. The default is 30
minutes. This benchmark explicitly uses the hard minimum: `1`.

The value `1` means that automatic refresh is allowed to run at most once per
minute. It is not a request to refresh every minute, a refresh-start SLA, a
refresh-completion SLA, or a bound on persisted-MV lag. See the central refresh
contract above.

Manual refresh is not subject to this frequency cap, but inserting manual
refresh calls into the benchmark would create a different maintenance policy
and must be declared as a separate experiment.

## Interpretation of the observed waveform

The attached monitor output shows the expected sawtooth pattern:

1. `refresh_watermark` remains fixed between persisted refresh completions.
2. The one-minute monitor adds approximately 60 seconds to
   `watermark_lag_sec` on each sample.
3. When a refresh completes, the watermark jumps forward and lag falls.

After the initial interval, the observed watermark advances were mostly about
309 to 322 seconds apart, or approximately 5.1 to 5.4 minutes. The first
observed gap was approximately eight minutes. This is consistent with
best-effort automatic refresh; it does not indicate that the configured
one-minute cap was ignored.

`last_refresh_status = None` is healthy. BigQuery populates
`last_refresh_status` when the last automatic refresh failed; `NULL`/`None`
means no failure was reported for that refresh.

For this current run:

```text
watermark lag             = persisted MV-cache lag
dashboard answer staleness = no intentional staleness; runtime delta merge can apply
```

## `max_staleness`: how refresh age controls query-time delta reconciliation

Start with the default: without `max_staleness`, querying the MV returns current
results. BigQuery reads and merges base-table inserts that are newer than the
persisted MV.

`max_staleness` changes that rule. BigQuery compares the configured interval
with the time at which the last persisted MV refresh occurred:

1. **The last MV refresh occurred within `X`:** BigQuery returns the persisted
   MV without reading the base table. Inserts and changes made after that
   refresh can be absent.
2. **The last MV refresh occurred outside `X`:** BigQuery reads the persisted
   MV and combines it with changes from the base table. The combined result can
   still be stale by as much as `X`; it is not guaranteed to be fully current.

Equivalently, define:

```text
refresh_age = query_time - last_successful_refresh_time
X           = max_staleness
```

Then:

```text
refresh_age <= X  -> skip the delta merge; return the persisted MV
refresh_age >  X  -> perform query-time delta reconciliation
```

Skipping the delta merge does **not** mean the result is current. It means the
persisted MV is recent enough to satisfy the explicitly permitted staleness.
The returned result can be `refresh_age` old.

### Concrete example

```text
Last successful MV refresh: 12:00
Direct MV query:             12:20
max_staleness:               30 minutes
refresh_age:                 20 minutes
```

Because `20 minutes <= 30 minutes`, BigQuery can skip the query-time delta
merge and return the persisted 12:00 MV state. At 12:20, that result is 20
minutes stale even though it satisfies the configured 30-minute bound.

If the same MV were queried at 12:40, its refresh age would be 40 minutes.
Because `40 minutes > 30 minutes`, BigQuery would reconcile the persisted MV
with base-table changes at query time. Even then, the returned result is only
required to fall within the configured 30-minute staleness bound; it is not
guaranteed to be fully current.

The comparison is therefore easy to invert accidentally: BigQuery skips the
delta merge when the **refresh age is less than or equal to**
`max_staleness`. If `max_staleness` is less than the refresh age, BigQuery does
not skip the reconciliation.

This is why the option is unacceptable for this benchmark: the benchmark must
query inserts immediately, not allow the latest inserts to remain absent for a
configured interval.

This setting is independent of `refresh_interval_minutes`:

- `refresh_interval_minutes = 1` is the hard minimum frequency cap for
  automatic background refresh. It allows at most one automatic refresh per
  minute; it is not a one-minute completion SLA.
- `max_staleness` is evaluated against the last persisted MV refresh time and
  controls the permitted staleness of a direct MV result.
- Using the one-minute refresh cap does not implicitly enable staleness.
- Leaving `max_staleness` unset means direct MV queries must account for newer
  base-table data even when the persisted refresh watermark is behind.

This benchmark ingests approximately one million events per second and queries
newly ingested events immediately. A bounded-stale answer does not satisfy that
real-time contract. Therefore `max_staleness` is unset and there is no
`max_staleness` run, variant, or sensitivity study in this benchmark.

## Verify the effective current settings

This metadata request does not run a BigQuery query job:

```bash
bq show \
  --format=json \
  "$GOOGLE_CLOUD_PROJECT:$BQ_DATASET.quotes_daily" \
  | jq '{
  enable_refresh: .materializedView.enableRefresh,
  refresh_interval_ms: .materializedView.refreshIntervalMs,
  refresh_interval_minutes:
    ((.materializedView.refreshIntervalMs | tonumber) / 60000),
  max_staleness: (.maxStaleness // null)
}'
```

Expected for the current run:

```json
{
  "enable_refresh": true,
  "refresh_interval_ms": "60000",
  "refresh_interval_minutes": 1,
  "max_staleness": null
}
```

Because `create.sql` uses `CREATE MATERIALIZED VIEW IF NOT EXISTS`, this check
is important if a dataset is ever reused: the DDL does not update the options
of an MV that already exists. Real benchmark runs should continue to use fresh
datasets.

## Pricing implications

Automatic refresh is operated by BigQuery, but it is not free. Under on-demand
pricing, bytes processed during refresh are charged to the project containing
the MV. Under capacity pricing, refresh consumes slots. The persisted MV also
incurs storage charges.

Direct queries of the MV are charged separately. In this benchmark,
`max_staleness` is unset, so a query can read both persisted MV data and
necessary base-table changes to return current results.

Use `collect_evidence.py` to export automatic refresh jobs and summarize them
separately from dashboard jobs. The full ledger and ready-to-run `jq` command
are in `COST_ACCOUNTING.md`.

## Official references

- [Create materialized views and `max_staleness` behavior](https://docs.cloud.google.com/bigquery/docs/materialized-views-create#data_staleness)
- [Manage automatic refresh, its frequency cap, and best-effort behavior](https://docs.cloud.google.com/bigquery/docs/materialized-views-manage#frequency_cap)
- [Query materialized views and incremental updates](https://docs.cloud.google.com/bigquery/docs/materialized-views-use)
- [Materialized-view DDL options](https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language)
- [BigQuery table and materialized-view REST fields](https://docs.cloud.google.com/bigquery/docs/reference/rest/v2/tables)
- [Monitor materialized views](https://docs.cloud.google.com/bigquery/docs/materialized-views-monitor)
- [Materialized-view pricing](https://docs.cloud.google.com/bigquery/docs/materialized-views-intro#materialized_views_pricing)
