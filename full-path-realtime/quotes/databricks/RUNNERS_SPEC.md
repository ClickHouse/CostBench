# Dashboard + Drilldown runner spec

Two long-running scripts that execute in parallel to the ingest process. They sample query performance throughout the ingest so we can correlate query latency against data volume (raw row count, MV row count) over time. This doc describes what they do so equivalent scripts can be built for Snowflake and Databricks.

## Purpose

We're benchmarking three systems (ClickHouse Cloud, Snowflake, Databricks) under sustained 1M EPS ingest with a Materialized View attached. To characterize each system we need:

- **Dashboard query latency** over time, as the dataset grows. Run frequently (every ~10 minutes), against the MV target table. Simulates a UI dashboard auto-refreshing.
- **Drilldown query latency** over time, against the raw base table. Run occasionally (every ~60 minutes). Simulates an ad-hoc investigation.

Each system implements these two runners against its own query set (queries already translated per system) and writes structured output we can compare across systems.

## Cadence and lifecycle

- **Dashboard runner**: defaults to 600 seconds between iterations.
- **Drilldown runner**: defaults to 3600 seconds between iterations.
- Both **run indefinitely until Ctrl-C**. They're started shortly after the ingest begins and killed shortly after it ends. No "run N iterations" mode.

## Per-iteration algorithm

Both runners do the same thing on each iteration:

1. Record an ISO-8601 UTC start timestamp.
2. Query `SELECT count() FROM <database>.<raw_table>` → `raw_rows`.
3. Query `SELECT count() FROM <database>.<mv_table>` → `mv_rows`. (Both runners log both counts so the JSONL lines can be correlated and graphed together.)
4. For each query in the queries file (in order):
   - Run the query **once** (single try per iteration; cross-iteration sampling provides the variance signal).
   - Measure **server-side query duration in seconds** (not client wall-clock).
   - On error, log `null` and continue with the next query.
5. Record an ISO-8601 UTC end timestamp.
6. Append one JSON object as a single line to the output file (JSONL format).
7. Sleep until the next iteration.

## Output: JSONL file

One JSON object per line, appended. Each line is a self-contained record. Output filename defaults to `<runner>_<utc_timestamp>.jsonl` inside an output directory that's auto-created (recursively) if it doesn't exist.

### JSON object schema (per line)

| Field | Type | Description |
|---|---|---|
| `iteration` | int | 1-based iteration counter for this runner's lifetime. |
| `iteration_started_at` | ISO-8601 UTC string (`Z`-suffixed) | Wall-clock at iteration start. |
| `iteration_finished_at` | ISO-8601 UTC string (`Z`-suffixed) | Wall-clock at iteration end. |
| `raw_rows` | int | Count of rows in the raw base table when iteration started. |
| `mv_rows` | int | Count of rows in the MV target table when iteration started. |
| `system` | string | System label (e.g. `"Snowflake (AWS)"`, `"Databricks (AWS)"`). |
| `version` | string | Engine version string. |
| `machine` | string | Compute tier / warehouse size label (e.g. `"X-Small"`, `"2X-Large"`, `"236GiB"`). |
| `cluster_size` | int | Cluster/replica count if applicable; 1 otherwise. |
| `comment` | string | Free-form annotation (e.g. `"10B rows (dashboard)"`). |
| `tags` | string array | System tags (`["managed","aws","snowflake"]` etc.). |
| `result` | array of arrays of (float\|null) | One inner array per query, in queries-file order. Each inner array has one element (since we do one try per iteration). |

### Example line

```json
{"iteration":7,"iteration_started_at":"2026-06-04T14:23:01Z","iteration_finished_at":"2026-06-04T14:23:14Z","raw_rows":234567890,"mv_rows":12345,"system":"Snowflake (AWS)","version":"8.27.1","machine":"X-Small","cluster_size":1,"comment":"10B rows (dashboard)","tags":["snowflake","managed","aws"],"result":[[0.123],[0.456],[0.022],[1.834]]}
```

`result[0][0]` is the duration in seconds of the first query in the queries file. `[null]` means that query errored.

## Timing methodology

**Use server-side query duration**, not client wall-clock. We want to measure the engine's work, not network latency or driver overhead. Each system exposes this differently:

- **ClickHouse**: `clickhouse-client --time` emits server-side duration on stderr.
- **Snowflake**: query history (`SELECT EXECUTION_TIME FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY()) WHERE QUERY_ID = LAST_QUERY_ID()`), or the Python connector's `cursor.execute()` returns a query_id you can look up; or use `RESULT_SCAN(LAST_QUERY_ID()).EXECUTION_TIME`.
- **Databricks**: query history API (`/api/2.0/sql/history/queries`) or SQL warehouse query metrics. The `cursor.description` of the SQL connector also exposes execution stats.

In all cases, capture duration in **seconds as a float**.

## Error handling

If any query fails (timeout, syntax error, transient connectivity issue), log `null` in `result` at that query's position and continue to the next query. The runner must not crash on individual query failures — long-running scripts have to tolerate hiccups. Print the error to stderr so it's visible in logs.

Connection establishment failures at the start of an iteration are allowed to fail the whole iteration (log a row of all-`null` results, continue to the next iteration after sleeping).

## CLI shape (suggested)

```
./run_dashboard.<ext> \
    --database <db> \
    [--queries <file>] \
    [--interval <sec>] \
    [--output <file>] \
    [--output-dir <dir>] \
    <system> <machine_desc> <cluster_size> <base_comment> <extra_flag>
```

- `--database`: required. Database/schema name.
- `--queries`: defaults to `queries_mv.sql` (dashboard) / `queries_raw.sql` (drilldown). Queries split on `;`.
- `--interval`: seconds between iterations. Defaults 600 / 3600.
- `--output`: full output filepath. Overrides auto-generated name.
- `--output-dir`: directory for auto-generated filename. Created recursively if missing.
- Positional args (same flavor as ClickBench `run.sh`): `<system> <machine> <cluster_size> <comment> <extra_flag>`. The runner copies these into each JSONL line's metadata fields.

Connection credentials should be passed via environment variables (system-specific) rather than CLI args — same pattern as the existing ClickHouse runner uses `FQDN` and `PASSWORD`.

## What does NOT change across systems

- Output JSON schema (so we can use one analysis pipeline).
- Cadence defaults (600s dashboard, 3600s drilldown).
- One try per query per iteration.
- Logging `null` on error.
- Logging both `raw_rows` and `mv_rows` in both runners.
- ISO-8601 UTC timestamps with `Z` suffix.
- JSONL append (not JSON array, not multiple files).

## What WILL differ per system

- Client / SDK used to talk to the engine.
- How server-side timing is captured.
- Exact `count()` syntax (trivial but check — DBX uses `count(*)`).
- Auth env vars.
- Naming of the raw vs MV tables (use the names your `create.sql` equivalent established).

## ClickHouse implementations as reference

The CH versions of these scripts (`run_dashboard.sh` and `run_drilldown.sh`) are committed alongside this spec. They're shell scripts based on the existing ClickBench `run.sh` pattern, and they're the authoritative example of what each line should look like. The Python equivalents for Snowflake (using `snowflake-connector-python`) and Databricks (using `databricks-sql-connector`) should produce byte-identical JSONL structure given the same input metadata.
