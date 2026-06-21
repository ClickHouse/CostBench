# Next Benchmark Runbook — Snowflake drilldown re-runs (T0 / T1 / T2)

> **Audience:** a colleague (and their Claude Code) continuing this work while Lionel is away.
> **Goal:** re-run the **drilldown** workload with the *new two-query drilldown* across three
> Snowflake architectures, collect the per-iteration latency JSONL, and regenerate the charts —
> all comparable against the ClickHouse Cloud baseline.
>
> Written to be driven by **Claude Code**: explicit, copy-pasteable commands; decision points called
> out. Read it once before starting. Original working folder (source of these scripts):
> `/Users/lio/Clickhouse/dev/benchmark/bench2cost/stockhouse/snowflake` → being consolidated into
> `quotes/snowflake/`.

---

## ⚠️ Operating policy — no destructive actions by the assistant

When driving this with Claude Code (or any assistant): **the assistant must NOT run destructive
operations** — `DROP`, `TRUNCATE`, `DELETE`, `REMOVE`, `ALTER ... DROP`, deleting files/result dirs,
or `CREATE OR REPLACE` over populated objects. It may *propose* them and show the exact SQL/command,
but a **human runs the destructive step**. Setup/ingest/query/read and non-destructive `ALTER`
(e.g. re-pointing a warehouse) are fine. Example: the redundant standard `QUOTES_DAILY` MV in
`STOCKHOUSE_T1` is dropped by a person, not the assistant.

## 0. The drilldown queries (shared by all three benchmarks)

The drilldown is now **two queries** (the runner times each → `result: [[q1],[q2]]`):
- **Q1 — Hourly OHLCV bars:** per-hour OHLC + VWAP + volume + volatility + spread (high-cardinality `GROUP BY`).
- **Q2 — Risk & liquidity profile ("B7"):** single-row microstructure panel — realized volatility,
  spread distribution + tail risk (skew/kurtosis/p95/p99), order-book imbalance, spread-vs-depth corr.

**Schema convention:** each benchmark runs in its own schema — **`STOCKHOUSE_T0`** (T0 standard),
**`STOCKHOUSE_T1`** (T1 interactive), **`STOCKHOUSE_T2`** (T2 streaming). The original `STOCKHOUSE`
(first run) is left intact. Drive every step with `SF_SCHEMA=STOCKHOUSE_T{0,1,2}`.

Query files (keep ClickHouse and Snowflake in lock-step; originals kept as `*_v1.sql`):

| Benchmark | Schema | Snowflake query file | ClickHouse query file |
|---|---|---|---|
| T0 standard | `STOCKHOUSE_T0` | `quotes/snowflake/queries_raw.sql` (FROM `QUOTES`) | `quotes/clickhouse-cloud/queries_raw.sql` |
| T1 interactive | `STOCKHOUSE_T1` | `quotes/snowflake/queries_raw_it.sql` (FROM `QUOTES_IT`) | same CH file |
| T2 streaming | `STOCKHOUSE_T2` | `quotes/snowflake/t2/queries_raw_it.sql` (FROM `QUOTES_IT`) | same CH file |

> All three query files are updated to the new two-query drilldown (originals kept as `*_v1.sql`).

**Finding so far (AAPL, caches disabled, warm):** ClickHouse wins ~2–3× on these and is far more
stable; the Snowflake interactive table is slower and high-variance on grouped aggregations.

---

## 1. Connect to the boxes  ⚠️ FILL IN — never commit secrets

Both EC2 boxes are Snowflake clients (`/home/ubuntu/bench`, key-pair auth via `keys/rsa_key.p8`,
`.sfenv`). ClickHouse Cloud is reached via `clickhouse-client` from any host.

```bash
# Paris box — ORIGINAL Snowflake account (eu-west-3). Used by T0 + T1 (COPY ingest + runners).
ssh -i <FILL-IN paris.pem> ubuntu@<FILL-IN paris-host>
cd /home/ubuntu/bench && source .sfenv && source .venv/bin/activate

# London box — NEW Snowflake account region. Used by T2 (Snowpipe Streaming client).
ssh -i <FILL-IN london.pem> ubuntu@<FILL-IN london-host>
cd /home/ubuntu/bench && source .sfenv && source .venv/bin/activate

# ClickHouse (baseline, run from any host with clickhouse-client):
export FQDN=<FILL-IN clickhouse-cloud-host>   # e.g. xxxx.us-east-2.aws.clickhouse-staging.com
export PASSWORD=<FILL-IN>                       # do NOT commit
```
`.sfenv` exports (per box/account): `SF_ACCOUNT`, `SF_USER`, `SF_KEY=/home/ubuntu/bench/keys/rsa_key.p8`,
`SF_SCHEMA`, `SF_WAREHOUSE` (the warehouse to MEASURE), `SF_TRACK_WAREHOUSE` (counts/timing lookups).

---

## 1b. Onboarding a new colleague (SSH + Snowflake access)

Two independent "keys" are involved — don't confuse them:
- **SSH key** → logs into the EC2 box (shared `ubuntu` user).
- **Snowflake RSA key** (`SF_KEY` → `keys/rsa_key.p8`) → authenticates to Snowflake as a user.

### a. Box access (SSH)
Append the colleague's SSH **public** key to `ubuntu`'s `authorized_keys` (repeat on each box):
```bash
mkdir -p ~/.ssh && chmod 700 ~/.ssh
echo 'ssh-ed25519 AAAA... colleague@clickhouse.com' >> ~/.ssh/authorized_keys
chmod 600 ~/.ssh/authorized_keys
sort -u ~/.ssh/authorized_keys -o ~/.ssh/authorized_keys   # de-dupe if re-run
```
Also ensure the instance's **security group** allows inbound TCP 22 from their IP — that, not the
key, is the usual cause of a hanging SSH.

### b. Snowflake access — reuse the one key pair, one role grant per user
We share the box and the single Snowflake private key, so **register the existing public key on each
new user** rather than minting per-user keys. Per-person Snowflake users still give per-person
attribution in `QUERY_HISTORY`.

```bash
# 1. derive the existing public key from the private key on the box
openssl rsa -in /home/ubuntu/bench/keys/rsa_key.p8 -pubout 2>/dev/null \
  | grep -v 'PUBLIC KEY' | tr -d '\n'; echo
```
```sql
-- 2. register that public key on each user, and grant the benchmark role
ALTER USER MARK SET RSA_PUBLIC_KEY='<that string>';
ALTER USER TOM  SET RSA_PUBLIC_KEY='<that string>';
GRANT ROLE ACCOUNTADMIN TO USER MARK;      -- or a least-priv BENCH_ADMIN role (see below)
GRANT ROLE ACCOUNTADMIN TO USER TOM;
ALTER USER MARK SET DEFAULT_ROLE = ACCOUNTADMIN DEFAULT_WAREHOUSE = BENCH2COST_SMALL_GEN2;
ALTER USER TOM  SET DEFAULT_ROLE = ACCOUNTADMIN DEFAULT_WAREHOUSE = BENCH2COST_SMALL_GEN2;
```
Each person's `.sfenv` then differs **only** in the user — same key for everyone:
```bash
export SF_KEY=/home/ubuntu/bench/keys/rsa_key.p8   # shared
export SF_USER=MARK                                # or TOM / LIONEL
```
Verify: `DESC USER MARK;` shows `RSA_PUBLIC_KEY_FP` set; then
`python3 -c "import runner_common as r; r.connect('BENCH2COST'); print('OK')"` on the box.

> **Least-privilege alternative:** instead of `ACCOUNTADMIN`, create a `BENCH_ADMIN` role granted
> `CREATE WAREHOUSE ON ACCOUNT`; `USAGE`/`CREATE TABLE|MATERIALIZED VIEW|DYNAMIC TABLE|PIPE|STAGE`
> on schemas `STOCKHOUSE`+`STREAMING`; `SELECT,INSERT,TRUNCATE` on their tables; and
> `IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE` (for the `mv_billing.sh` ACCOUNT_USAGE lookups). Then
> change the scripts' `use role ACCOUNTADMIN` → `use role BENCH_ADMIN`. Interactive-table / Snowpipe
> Streaming privileges (T1/T2) are newer — if you hit "insufficient privileges", add the grant named
> in the error or fall back to `ACCOUNTADMIN` (fine for a dedicated benchmark account).

> **Trade-off of the shared key:** simple, but you can't revoke one person without rotating for all,
> and "who ran it" isn't cryptographically guaranteed. Acceptable for a short-lived internal account;
> give each person their own key if it outlives the benchmark.

---

## 2. T0 — Standard baseline (schema `STOCKHOUSE_T0`)

**What:** the original architecture — standard warehouse, standard table `QUOTES`, standard
materialized view `QUOTES_DAILY` — re-run in its own fresh schema `STOCKHOUSE_T0` (the original
`STOCKHOUSE` is left intact). Only the new drilldown queries are re-measured (MV-query latency was
already captured in the first benchmark).

**Topology:** `BENCH2COST.STOCKHOUSE_T0` · `QUOTES` (`CLUSTER BY (sym,t)`) · `QUOTES_DAILY` (standard
MV — kept, it's the T0 architecture) · reads on a standard warehouse.

**Run (Paris box):**
```bash
cd /home/ubuntu/bench && source .sfenv && source .venv/bin/activate

# 1. create the fresh STANDARD schema (WITH the standard QUOTES_DAILY MV — no T1 flag)
SF_SCHEMA=STOCKHOUSE_T0 bash ops/setup_schema.sh

# 2. ingest into STOCKHOUSE_T0.QUOTES   (parallel rgpi maxfiles target_rps ingest_wh)
SF_SCHEMA=STOCKHOUSE_T0 bash ops/run.sh 8 8 all 1000000 BENCH2COST_GEN2_XSMALL

# 3. drilldown runner against the STANDARD table, new 2-query file.
#    NOTE: a standard table must be read by a STANDARD warehouse (an interactive
#    warehouse rejects it: "Querying non-interactive table ... not supported").
SF_SCHEMA=STOCKHOUSE_T0 SF_WAREHOUSE=BENCH2COST_GEN2_XSMALL \
SF_RAW_TABLE=QUOTES SF_MV_TABLE=QUOTES_DAILY \
python3 run_drilldown.py --database BENCH2COST \
    --queries queries_raw.sql --interval 3600 --output-dir out_t0 \
    "Snowflake (AWS)" "Standard" 1 "T0 standard drilldown" 0
```
- Output → `out_t0/drilldown_<UTC>.jsonl`, `result: [[hourly],[b7]]`.
- No need to re-run the dashboard (already captured). Stop with `bash ops/stop_experiment.sh`.

**Collect & charts (T0).** Dashboard, MV-lag and storage are unchanged from the first run and already
pre-populated in `quotes/snowflake/results/t0/`, so you only collect the **new drilldown**:
```bash
# laptop: download the re-run's drilldown into results/t0/, then chart
scp -i <key.pem> 'ubuntu@<paris-host>:/home/ubuntu/bench/out_t0/drilldown_*.jsonl' quotes/snowflake/results/t0/
cd quotes/_viz && bash make_charts.sh t0          # -> _out/t0/
```
`make_charts.sh` renders whatever's present (it already works before the drilldown lands — the
Snowflake drilldown line just fills in once collected). Details/debugging in §5.

---

## 3. T1 — Interactive tables (the architecture we just designed for)

**What:** ingest via `COPY INTO` into a **standard** row table `QUOTES`, then two interactive tables:
- `QUOTES_IT` — interactive **copy of the row table** (`CLUSTER BY (sym,t)`), kept in sync by a refresh wh.
- `QUOTES_DAILY_IT` — interactive table **replacing the MV** (the (sym,day) rollup), refreshed by its own wh.

**Refresh-warehouse sizing (key learning from the last run — apply this time):**
| Interactive table | Role | Last run | **Use this run** | Why |
|---|---|---|---|---|
| `QUOTES_DAILY_IT` | aggregate rollup | Small | **Medium** | Small lagged behind the target lag; Medium keeps up |
| `QUOTES_IT` | raw copy | Small | **X-Small** | Small was overkill; XS keeps it in sync fine |

> `ops/setup_interactive.sh` now defaults to **Medium** (agg) + **X-Small** (raw); override via
> `AGG_WH`/`RAW_WH` + `AGG_SIZE`/`RAW_SIZE`. Target lags: `QUOTES_DAILY_IT` = 1 min, `QUOTES_IT` = 10 min.
> **Gen2-create caveat (Snowflake ≥10.21):** creating a *new* Gen2 warehouse with the old
> `resource_constraint=STANDARD_GEN_2` clause errors — so pass `AGG_WH`/`RAW_WH` that ALREADY EXIST
> (the `if not exists` then skips the bad clause), or `ALTER INTERACTIVE TABLE … SET WAREHOUSE=…`
> afterwards (see `ops/reconfig_it.sh`). On the benchmark account we used `BENCH2COST_GEN2_MEDIUM` (agg)
> + `BENCH2COST_GEN2_XSMALL_2` (raw, X-Small, pre-created).

**Preflight (validate before the full run):**
```bash
cd /home/ubuntu/bench && source .sfenv && source .venv/bin/activate
bash ops/preflight.sh T1     # checks QUOTES + QUOTES_IT/QUOTES_DAILY_IT + warehouse, runs both drilldown queries once
```
Expect `OK` for the base table, both interactive tables (with their refresh wh + lag + state), and
`q1/q2 executed`. A `FAIL` on an interactive table means run `ops/setup_interactive.sh` first.

**Run (Paris box) — fresh schema `STOCKHOUSE_T1`:**
```bash
cd /home/ubuntu/bench && source .sfenv && source .venv/bin/activate

# 1. create the schema WITHOUT the standard QUOTES_DAILY MV (T1=1 -> create_stockhouse_t1.sql):
#    QUOTES_DAILY_IT replaces the MV; a standard MV would only add redundant maintenance cost.
SF_SCHEMA=STOCKHOUSE_T1 T1=1 bash ops/setup_schema.sh

# 2. ingest into STOCKHOUSE_T1.QUOTES (the ITs derive from it)
SF_SCHEMA=STOCKHOUSE_T1 bash ops/run.sh 8 8 all 1000000 BENCH2COST_GEN2_XSMALL

# 3. create the interactive tables (Medium agg + X-Small raw refresh wh; both must already exist)
SF_SCHEMA=STOCKHOUSE_T1 \
AGG_WH=BENCH2COST_GEN2_MEDIUM   AGG_SIZE=MEDIUM AGG_LAG='1 minute' \
RAW_WH=BENCH2COST_GEN2_XSMALL_2 RAW_SIZE=XSMALL RAW_LAG='10 minutes' \
bash ops/setup_interactive.sh

# 4. drilldown runner against the INTERACTIVE table
#    NOTE: QUOTES_IT must be read by an INTERACTIVE warehouse (BENCH2COST_IT_SMALL).
SF_SCHEMA=STOCKHOUSE_T1 SF_WAREHOUSE=BENCH2COST_IT_SMALL \
SF_RAW_TABLE=QUOTES_IT SF_MV_TABLE=QUOTES_DAILY_IT \
python3 run_drilldown.py --database BENCH2COST \
    --queries queries_raw_it.sql --interval 3600 --output-dir out_t1 \
    "Snowflake (AWS)" "IT Small read" 1 "T1 interactive drilldown" 0
```
- The measured **read** warehouse is `SF_WAREHOUSE`; the agg/raw warehouses above are the
  **refresh/maintenance** whs whose credits you track separately (`ops/it_refresh.sh`, `mv_billing.sh`).
- Output → `out_t1/drilldown_<UTC>.jsonl`, `result: [[hourly],[b7]]`.

**Dashboard (vs the interactive rollup `QUOTES_DAILY_IT`)** — 4 queries in `queries_mv_it.sql`, every 600s:
```bash
SF_SCHEMA=STOCKHOUSE_T1 SF_WAREHOUSE=BENCH2COST_IT_SMALL \
SF_RAW_TABLE=QUOTES SF_MV_TABLE=QUOTES_DAILY_IT \
python3 run_dashboard.py --database BENCH2COST \
    --queries queries_mv_it.sql --interval 600 --output-dir out_t1 \
    "Snowflake IT (AWS)" "IT Small read" 1 "T1 interactive dashboard" 0
```

**Or run the whole IT suite at once (dashboard + drilldown + refresh tracker, detached):**
```bash
SF_SCHEMA=STOCKHOUSE_T1 bash ops/start_runners_it.sh "T1 interactive" "IT Small" 1
# dashboard (600s) + drilldown (3600s) + it_refresh (60s); writes to out_t1/ (derived from SF_SCHEMA).
# Stop everything with: bash ops/stop_experiment.sh
```
This is the easiest path for T1 — it launches both workloads with the right warehouses/tracking wired in.

**Collect & charts (T1).** At the end of the run:
```bash
# on the Paris box — stop (optional; you can collect while still running) + produce supplementary files
bash ops/stop_experiment.sh
SF_SCHEMA=STOCKHOUSE_T1 bash ops/collect_it_refresh.sh out_t1/it_refresh.csv
SF_SCHEMA=STOCKHOUSE_T1 SF_RAW_TABLE=QUOTES_IT SF_MV_TABLE=QUOTES_DAILY_IT \
  bash ops/collect_storage.sh out_t1/storage.json
# on your laptop — download everything for T1, then chart
scp -i <key.pem> 'ubuntu@<paris-host>:/home/ubuntu/bench/out_t1/*' quotes/snowflake/results/t1/
cd quotes/_viz && bash make_charts.sh t1          # -> _out/t1/
```
`out_t1/` already holds the drilldown + dashboard JSONL (+ live refresh tracker); the two collect
scripts add `it_refresh.csv` + `storage.json`. `storage.json` has a ClickHouse placeholder to fill by
hand from CH `system.parts` (see §5). Details/debugging in §5.

---

## 4. T2 — Snowpipe Streaming → interactive table + interactive MV  (validated 2026-06-17)

**What:** the new architecture — **no standard table, no ingest warehouse**. Stream rows directly into
the interactive table `QUOTES_IT`, and roll up with an **interactive materialized view** on it (an IT
can't source another IT, so the rollup is an interactive MV). Run from the **London** box (co-located
with the new account's region, eu-west-2). Source code: `quotes/snowflake/t2/`.

**Topology:** `BENCH2COST.STOCKHOUSE_T2` ·
- `QUOTES_IT` — interactive table, **Snowpipe Streaming target** (`CLUSTER BY (sym,t)`), fed by pipe `QUOTES_IT_PIPE`.
- `QUOTES_DAILY_IMV` — **interactive materialized view** on `QUOTES_IT` = the (sym,day) rollup (serverless).
- Warehouses: **ingest is SERVERLESS** (no warehouse). Reads on an interactive wh **`SNOWPIPES_IT_READ_SMALL`**; tracking on **`BENCH`**. The IMV + its source IT are attached to the read wh via `ALTER WAREHOUSE … ADD TABLES (…)` in `setup_streaming.sql`.
- Cost = streaming credits (`METERING_HISTORY SERVICE_TYPE='SNOWPIPE_STREAMING'`) + IMV maintenance + read wh.

> **Validated 2026-06-17:** `t2/setup_streaming.sql` creates the IT + pipe + interactive MV; a short
> `stream_quotes.py` run streamed **~119M rows in ~2 min (~1M EPS)** into `QUOTES_IT`, and
> `QUOTES_DAILY_IMV` materialized the rollup. All four prior open items are resolved (end of section).

**Preflight:**
```bash
cd /home/ubuntu/bench && source .sfenv && source .venv/bin/activate
SF_SCHEMA=STOCKHOUSE_T2 SF_WAREHOUSE=SNOWPIPES_IT_READ_SMALL SF_TRACK_WAREHOUSE=BENCH \
  bash ops/preflight.sh T2   # checks SDK import + QUOTES_IT + QUOTES_DAILY_IMV + pipe
```

**Run (London box):**
```bash
cd /home/ubuntu/bench && source .sfenv && source .venv/bin/activate
export SF_SCHEMA=STOCKHOUSE_T2 SF_WAREHOUSE=SNOWPIPES_IT_READ_SMALL SF_TRACK_WAREHOUSE=BENCH
export SF_RAW_TABLE=QUOTES_IT SF_MV_TABLE=QUOTES_DAILY_IMV
# (deps incl. snowpipe-streaming are pre-installed in the venv. If ever missing, use
#  `uv pip install snowpipe-streaming` — a uv venv has NO `pip`. Verify: python -c "import snowflake.ingest.streaming")

# 1. create schema + interactive table + pipe + interactive MV (serverless; creates NO warehouses)
bash ops/setup_streaming.sh            # runs t2/setup_streaming.sql via the connector

# 2. stream the dataset — SERVERLESS (profile.json is auto-written from SF_* env; port is a string)
python3 t2/stream_quotes.py --dir ~/data/stockhouse --schema STOCKHOUSE_T2 \
    --pipe QUOTES_IT_PIPE --profile profile.json --parallel 8 --target-rps 1000000

# 3. read runners (dashboard 600s + drilldown 3600s), detached -> out_t2/
bash ops/start_runners_t2.sh "T2 streaming" "IT (stream)" 1
```
- `SF_KEY` defaults to `keys/rsa_key.p8` if unset. Stop the stream with Ctrl-C / `pkill -f stream_quotes.py`;
  stop the runners with `bash ops/stop_experiment.sh`.
- `ops/start_runners_t2.sh` is the T2 analogue of `start_runners_it.sh` — it wires the runners to
  `QUOTES_DAILY_IMV` / `QUOTES_IT` on `SNOWPIPES_IT_READ_SMALL` (read) + `BENCH` (tracking). To run a
  single workload instead: `python3 run_drilldown.py … --queries t2/queries_raw_it.sql` (or
  `run_dashboard.py … --queries t2/queries_mv_imv.sql`) with the same `SF_*` env.

**Collect & charts (T2):**
```bash
# London box — stop the streamer, then supplementary data
pkill -f stream_quotes.py
SF_SCHEMA=STOCKHOUSE_T2 SF_RAW_TABLE=QUOTES_IT SF_MV_TABLE=QUOTES_DAILY_IMV SF_TRACK_WAREHOUSE=BENCH \
  bash ops/collect_storage.sh out_t2/storage.json
# refresh lag: QUOTES_DAILY_IMV is a materialized view -> use MATERIALIZED_VIEW_REFRESH_HISTORY
#   (metrics_reference.sql §4), NOT collect_it_refresh.sh (that reads INTERACTIVE_TABLE_REFRESH_HISTORY).
# laptop — download + chart
scp -i <london.pem> 'ubuntu@<london-host>:/home/ubuntu/bench/out_t2/*' quotes/snowflake/results/t2/
cd quotes/_viz && bash make_charts.sh t2          # -> _out/t2/
```
`storage.json`'s ClickHouse entry is a `null` placeholder to fill (see §5).

**Open items — RESOLVED (2026-06-17):**
1. ✅ **Streaming works** in eu-west-2 and the pipe targets the interactive table (~1M EPS, 119M rows).
2. ✅ **Interactive MV**: `CREATE INTERACTIVE MATERIALIZED VIEW … AS SELECT … GROUP BY` (serverless) —
   **no `CLUSTER BY`** (with it, Snowflake silently makes a plain VIEW), then attach via
   `ALTER WAREHOUSE <iv_wh> ADD TABLES (QUOTES_IT, QUOTES_DAILY_IMV)` (**parentheses required**).
3. ✅ **`profile.json`**: the SDK needs `port` as a **string** (`"443"`); `stream_quotes.py` now writes that.
4. ✅ **Pipe DDL**: streaming pipe = `COPY INTO … FROM (SELECT $1:<field>::<type> … FROM TABLE(DATA_SOURCE(TYPE => 'STREAMING')))`
   (the bare `FROM TABLE(...) MATCH_BY_COLUMN_NAME` form is rejected). IMV refresh history is under `MATERIALIZED_VIEW_REFRESH_HISTORY`.

---

## 5. Collect & charts — reference & debugging

The per-experiment **Collect & charts** blocks (§2 / §3 / §4) are the happy path. This section is the
shared reference: what each input is, what it feeds, and how to drive the pieces by hand.

> **Metric-query cheat sheet:** every Snowflake metric query the scripts run — query latency, IT/MV
> refresh lag, storage size, clustering depth, credits/cost — is collected with descriptions in
> [`metrics_reference.sql`](./metrics_reference.sql). Use it to query Snowflake directly without
> digging through the scripts.

**Per-benchmark inputs** (`make_charts.sh` stages the newest of each from `quotes/snowflake/results/t{n}/`;
everything is optional — a chart whose inputs are absent is simply skipped):

| File | Feeds | If absent |
|---|---|---|
| `drilldown_*.jsonl` | drilldown latency charts | drilldown charts skipped |
| `dashboard_*.jsonl` | dashboard latency charts | dashboard charts skipped |
| `it_refresh.csv` | `it_lag` freshness chart (T1/T2) | `it_lag` skipped |
| `storage.json` | `storage` chart | `storage` skipped |
| ClickHouse baseline (`quotes/clickhouse-cloud/results/{raw,mv}/`) | the CH line on every chart | no CH line |

`make_charts.sh <tn>` skips any chart whose inputs are absent and prints which. Chart set per architecture:
**t0** → `query_latency dashboard* drilldown* mv_lag storage` (vendor: CH vs Snowflake); **t1/t2** →
`it_query_latency it_dashboard_smooth it_drilldown_smooth it_lag storage` (CH vs Snowflake IT).
`uv run generate.py --list` lists all chart names; outputs land in `quotes/_viz/_out/t{n}/`.

**Refresh-lag CSV — under the hood** (what `collect_it_refresh.sh` runs; the chart needs the
`STALENESS_AT_DONE_SEC` column + the timestamp format below):
```sql
SELECT name,
       TO_CHAR(refresh_end_time, 'YYYY-MM-DD HH24:MI:SS.FF3 TZHTZM')      AS refresh_end_time,
       DATEDIFF('second', data_timestamp,    refresh_end_time)           AS staleness_at_done_sec,
       DATEDIFF('second', refresh_start_time, refresh_end_time)          AS duration_sec,
       state
FROM TABLE(INFORMATION_SCHEMA.INTERACTIVE_TABLE_REFRESH_HISTORY())
WHERE database_name='BENCH2COST' AND schema_name='STOCKHOUSE_T1'
ORDER BY refresh_end_time;     -- export as CSV (header row)
```

**Storage — under the hood** (what `collect_storage.sh` runs; Snowflake only):
```sql
SELECT m.table_name, m.active_bytes, t.row_count
FROM BENCH2COST.INFORMATION_SCHEMA.TABLE_STORAGE_METRICS m
JOIN BENCH2COST.INFORMATION_SCHEMA.TABLES t USING (table_schema, table_name)
WHERE m.table_schema='STOCKHOUSE_T1' AND m.table_name IN ('QUOTES_IT','QUOTES_DAILY_IT');
```
→ `storage.json` (Snowflake filled; **ClickHouse is written as a `null` placeholder you fill manually**):
```json
{ "raw": [ {"system":"ClickHouse (AWS)","bytes":null,"rows":null},
           {"system":"Snowflake IT","bytes":N,"rows":N} ],
  "mv":  [ {"system":"ClickHouse (AWS)","bytes":null,"rows":null},
           {"system":"Snowflake IT","bytes":N,"rows":N} ],
  "note": "active on-disk size, compressed; Snowflake filled, ClickHouse = fill manually" }
```
**ClickHouse storage** is not auto-collected (different engine; `collect_storage.sh` is Snowflake-only).
Fill the two `null` ClickHouse entries by hand from CH `system.parts` (run via `clickhouse-client`):
```sql
SELECT table, sum(bytes_on_disk) AS bytes, sum(rows) AS rows
FROM system.parts WHERE active AND table IN ('quotes','quotes_daily') GROUP BY table;
```
`raw` ← the `quotes` table, `mv` ← the `quotes_daily` rollup. The CH numbers are the same baseline for
T0/T1/T2 (collect once, reuse). Placeholders with `null` bytes are **skipped** by the chart until
filled, and `make_charts.sh` prints a `⚠` reminder when it sees them.

**Render one chart by hand** (debugging a single plot):
```bash
cd quotes/_viz
uv run render_latency.py _test/drilldown_*.jsonl --out _out/drilldown.png --smooth 5 --no-raw \
    --query-labels "Hourly OHLCV bars;Risk & liquidity profile (B7)"
```

**Gotchas:** the drilldown is two queries → latency charts draw two subplots (2-item `--query-labels`).
If `collect_storage.sh` prints `null`, `TABLE_STORAGE_METRICS` may be lagging (updates on a delay) — re-run
later. `collect_it_refresh.sh` needs the history still in retention (or the `it_refresh` tracker to have
run during the benchmark). Don't mix old 1-element drilldown JSONL with the new 2-element in one `_test/`.

---

## 6. Validation status (2026-06-17)

Ran `ops/preflight.sh` on both boxes:
- **T1 (Paris): ✅ runnable.** `QUOTES` 95.6B rows; `QUOTES_IT` 95.6B (ACTIVE, 10-min lag);
  `QUOTES_DAILY_IT` 1.55M (ACTIVE, 1-min lag). Both drilldown queries executed on
  `BENCH2COST_IT_SMALL` — q1 hourly **1.93s**, q2 B7 **1.39s** (client wall-time).
- **T2 (London): ✅ validated.** `STOCKHOUSE_T2` set up (IT + pipe + interactive MV); a short
  `stream_quotes.py` run streamed ~119M rows ≈ **~1M EPS** into `QUOTES_IT`, `QUOTES_DAILY_IMV`
  materialized. Reads on `SNOWPIPES_IT_READ_SMALL`, tracking on `BENCH`; streaming + IMV serverless.
  Fixes that got it working are in §4 (pipe `SELECT`-wrapper DDL, IMV no-`CLUSTER BY` + `ADD TABLES`
  association, `profile.json` port-as-string).
- **Warehouse types matter:** read **standard** tables (T0 `QUOTES`) with a **standard** wh
  (e.g. `BENCH2COST_GEN2_XSMALL`); read **interactive** tables (T1 `QUOTES_IT`) with an
  **interactive** wh (`BENCH2COST_IT_SMALL`). Mixing them errors. The run commands above set
  `SF_WAREHOUSE` accordingly. The boxes' `.sfenv` does **not** set `SF_WAREHOUSE`, which is why each
  benchmark sets it inline.
- **Account warehouses present:** `BENCH`, `BENCH2COST_GEN2_{MEDIUM,SMALL_1,SMALL_2,XSMALL}`,
  `BENCH2COST_IT_SMALL`, `BENCH2COST_SMALL_1`, `SNOWPIPES_GEN2_XSMALL_2`, `SNOWPIPES_IT_READ_SMALL`.

## 7. Cleaning up / tearing down a run (human-run — see Operating policy)

Destructive — the assistant proposes these; a **person runs them**.

```bash
# 1. stop runners on the box
bash ~/bench/ops/stop_experiment.sh
```
```sql
-- 2. drop the schema and ALL its objects (QUOTES, QUOTES_DAILY MV, QUOTES_IT,
--    QUOTES_DAILY_IT, QUOTES_INT_STAGE) in one statement:
USE ROLE ACCOUNTADMIN;
DROP SCHEMA IF EXISTS BENCH2COST.STOCKHOUSE_T1;

-- 3. (optional) drop the dedicated raw-IT refresh warehouse made for this run
--    (suspended => costs nothing if kept; leave the shared warehouses):
DROP WAREHOUSE IF EXISTS BENCH2COST_GEN2_XSMALL_2;
```
```bash
# 4. (optional) remove captured drilldown output on the box
rm -rf ~/bench/out_t1
```
Notes: `DROP SCHEMA` stops compute immediately; dropped data still occupies Time-Travel/Fail-safe
storage for the retention window, then auto-reclaims. Shared warehouses
(`BENCH2COST_GEN2_MEDIUM/_XSMALL/_SMALL_*`, `BENCH2COST_IT_SMALL`) are used elsewhere — don't drop them.

- **Re-run T1 clean:** `SF_SCHEMA=STOCKHOUSE_T1 T1=1 bash ops/setup_schema.sh` → `setup_interactive.sh` → `run.sh` → drilldown runner.
- **Keep schema, just restart ingest:** `SF_SCHEMA=STOCKHOUSE_T1 bash ops/reset.sh` (truncates `QUOTES` — also human-run).

## 8. Status / TODO
- ✅ Drilldown = 2 queries (hourly OHLCV + B7) for **all three** benchmarks: T0 (`queries_raw.sql`),
  T1 (`queries_raw_it.sql`), T2 (`t2/queries_raw_it.sql`), and CH (`clickhouse-cloud/queries_raw.sql`). Originals kept as `*_v1.sql`.
- ✅ **T1 warehouse sizing**: `ops/setup_interactive.sh` now creates Medium (agg) + X-Small (raw); override via `AGG_*/RAW_*` env (see `.env.example`).
- ✅ **Consolidated**: `quotes/snowflake/` is self-contained — `ops/*.sh` (run/reset/start_runners/clustering/billing/…), `ingest.py`, runners, queries, `t2/`, `RESULTS.md`, plus `.gitignore` (secrets) and `.env.example`.
  - Deliberately NOT copied from `dev/`: secrets (`_credentials_aws.txt`, `keys/`), dialect dupes (`queries_*_snowflake.sql` = the repo's `queries_*.sql`), old DDL variants (`create (1).sql`, `create_quotes.sql`), the superseded `stream_ingest.py` (use `t2/stream_quotes.py`), and the CH-side `ingest_parquet_dir.py`/`parquet.thrift` (belong in `clickhouse-cloud/`).
- ⏳ **T2** is in progress — verify the 4 open items in §4 against the new account.
- 🔒 Connection secrets stay as `<FILL-IN>` placeholders / `.env.example`; never commit hosts, `.pem` keys, `SF_ACCOUNT`, or CH `FQDN`/`PASSWORD`.
