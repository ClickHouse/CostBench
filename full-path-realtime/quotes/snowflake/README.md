# Stockhouse — Snowflake Ingestion

End-to-end steps to spin up an EC2 box, download the Stockhouse dataset, ingest it into
Snowflake at a sustained **1M events/sec (EPS)** with a materialized-view rollup attached,
and run the dashboard/drilldown latency benchmark — the Snowflake peer of the
ClickHouse and Databricks tracks in this repo.

## Architecture

- **Ingest path:** a Python client on EC2 reads Parquet row groups, re-encodes each to a
  small Parquet file, `PUT`s it to an **internal stage**, then runs `COPY INTO ... FORCE=TRUE`
  on a running warehouse. This is the "client pushing data to a running warehouse" model
  (continuous COPY), *not* Snowpipe Streaming. Throughput is **concurrency-bound**: ~8
  parallel COPY streams clear 1M EPS; bigger warehouses do **not** go faster (see Results).
- **Rollup:** `QUOTES_DAILY` is a **Materialized View** — the faithful peer of the ClickHouse
  *incremental* MV (always query-consistent, serverless background maintenance). A Dynamic
  Table variant (peer of a ClickHouse *refreshable* MV) is kept commented in `create.sql`.
- **Region:** Snowflake account + EC2 box both in **AWS eu-west-3 (Paris)** so the measured
  `PUT`/`COPY` cycle is region-local. The source S3 bucket (`pme-internal`) is in **us-east-2**;
  the cross-region download of the dataset to EC2 is one-time, unmeasured prep.
- **Warehouses:** ingest COPY → `BENCH2COST_GEN2_XSMALL`; reader (dashboard/drilldown queries)
  → `BENCH2COST_SMALL_GEN2`; MV maintenance → serverless (no warehouse to assign).

## Key findings

A running log of the interesting things this benchmark surfaced about Snowflake. Measured
numbers are from the 29.6h clustered run at ~1M EPS (105.5B rows) unless noted.

### Ingest throughput & cost
- **Throughput is concurrency-bound, not warehouse-size-bound.** A warehouse-size sweep at
  fixed parallelism (8) gave ~1.1M EPS on X-Small, Small, Medium *and* Large — identical. A
  bigger warehouse does not go faster; more concurrent COPY streams or bigger batches do.
- **A single COPY underutilizes the warehouse** (~350–420K rows/s). Throughput scales
  near-linearly with concurrent COPYs: c=4 → ~1.4M/s, c=8 → ~2.3M/s (the engine ceiling).
- **Cost-optimal sustained 1M EPS ≈ X-Small Gen2 ≈ ~$1 / billion rows** — ~8× cheaper than
  Large for identical throughput. AWS **Gen2 = 1.35× Gen1** credits/hr (XS 1.35, S 2.7, M 5.4, L 10.8).
- **The `i ARRAY` column was a red herring.** COPY with vs without it was identical (~360K/s);
  the apparent slowness was under-parallelization, not the semi-structured column.

### Materialized view behaviour
- **MV maintenance is serverless and decoupled from the ingest warehouse** → attaching the MV
  caused *no* ingest slowdown, but the MV falls behind: `behind_by` sawtooths up to **~72 min**
  at 1M EPS (partially catching up each refresh cycle). See the MV-lag chart in `../_viz`.
- **The MV is always query-consistent.** Querying it merges materialized rows with a live scan
  of un-merged base rows, so results are *never* stale even when `behind_by` is high. The cost
  of lag shows up as **rising dashboard query latency** + maintenance credits — not wrong answers.
- **MV freshness is observable only via `behind_by`.** A row-count backlog is meaningless for an
  MV (because of the query-time merge, `count(*)` on the MV equals `count(*)` on the base). Row
  backlog is valid only for a Dynamic Table.
- **`SHOW`/metadata rows ≠ `SELECT count(*)` for the MV.** `SHOW MATERIALIZED VIEWS` (and
  `TABLE_STORAGE_METRICS`) report **physical materialized fragments** (14.88M), while
  `count(*)` returns the **logical `(sym, day)` groups** (1.72M) — ~8.6× fragmentation, because
  the MV is maintained by *appending* partial-aggregate fragments per ingested micro-partition
  and only consolidating them via a lazy background compaction. The base table shows no such gap
  (`SHOW` rows == `count(*)` == 105,546,024,533) since it has no aggregation. See
  `results/storage-size-mv.md`.

### Storage (vs ClickHouse on the same dataset)
- **Raw table is ~1.6× larger on disk than ClickHouse** — 583 GiB vs 361 GiB (~5.9 vs ~3.4
  bytes/row at comparable row counts) — *plus* ~1.06 TiB time-travel and ~787 GiB fail-safe
  retained on top, which ClickHouse does not carry by default.
- **MV is ~8× larger on disk** (456 MiB vs 58 MiB), driven by the un-compacted physical
  fragments above; the *logical* cardinality is actually comparable (1.72M vs 1.88M groups).
- **`CREATE OR REPLACE` MV churn leaves residual fail-safe storage** across the dropped versions
  (~686 MB here) that ages out over the fail-safe window.

### Query latency
- **Full-MV-scan dashboard queries scale with volume** (top-movers, daily-activity → ~5–9s at
  105B rows). **Sym-filtered queries stay fast** via prefix-pruning on the `(sym, day)` cluster
  key. The drilldown on the raw table reaches ~12s at ~104B rows.

### Operational gotchas
- **`ABORT_DETACHED_QUERY` defaults OFF** → a client-killed COPY keeps running server-side
  (zombie warehouse usage). Set `abort_detached_query = true` and cancel via `system$cancel_query`.
- **In-memory Parquet `PUT` fails** with `253007` (connector can't classify the stream) → write
  the re-encoded row group to `/dev/shm` and `PUT` the file with `AUTO_COMPRESS=FALSE`.
- **Key-pair (JWT) auth, not PAT.** A PAT requires a network policy (lock-out risk); `ALTER USER
  ... SET RSA_PUBLIC_KEY` doesn't touch IP/UI login and is reversible via `UNSET`.
- **`INFORMATION_SCHEMA.QUERY_HISTORY()` needs a database in context** (`USE DATABASE`), and the
  `datediff` date-part must be unquoted.

## Repo layout

| Path | Purpose |
|---|---|
| `create.sql` | `QUOTES` table + `QUOTES_DAILY` materialized view (DDL, with clustering keys). |
| `download_stockhouse.sh` | Download dataset from S3 via AWS CLI (portable). |
| `download_all.py` | Parallel, resume-friendly boto3 download (the variant actually used). |
| `ingest.py` | Parallel per-row-group ingester (read → PUT → `COPY INTO FORCE=TRUE`). |
| `queries_mv.sql` | 4 dashboard queries (run vs the MV). |
| `queries_raw.sql` | 1 drilldown query (run vs the raw table). |
| `run_dashboard.py` / `run_drilldown.py` | Query-latency runners (interval 600s / 3600s). |
| `runner_common.py` | Shared loop / server-side timing / JSONL schema for both runners. |
| `RUNNERS_SPEC.md` | Cross-system runner spec (identical schema for CH/SF/DBX). |
| `ops/` | Operational scripts for the box (reset, run, start/stop, clustering, billing). |
| `results/` | JSONL + logs from the 29.6h clustered run (see Results). |

`ops/` and the top-level Python/SQL files all deploy **flat** to `/home/ubuntu/bench/` on the
box (the ops scripts assume a flat working dir and `cd /home/ubuntu/bench`).

## 1. Spin up an EC2 instance (eu-west-3, co-located with Snowflake)

A 32 vCPU / ~123 GB box with a large data disk. The dataset is ~650 GB, so format and mount
a separate volume at `/data`. (Standard `aws ec2 run-instances` in `eu-west-3` — see the
Databricks README in this repo for the full key-pair / security-group recipe.)

## 2. Set up Python on EC2

```bash
sudo apt-get update -q && sudo apt-get install -y python3.12-venv unzip -q
python3 -m venv ~/bench/.venv
~/bench/.venv/bin/pip install snowflake-connector-python pyarrow boto3 cryptography
# AWS CLI for the download step
curl -s 'https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip' -o awscliv2.zip
unzip -q awscliv2.zip && sudo ./aws/install
```

## 3. Key-pair (JWT) auth

The connector authenticates with an RSA key pair (no network policy required, unlike a PAT):

```bash
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8 -nocrypt
openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub
# In Snowflake (one-time, reversible via UNSET):
#   ALTER USER <your_user> SET RSA_PUBLIC_KEY='<contents of rsa_key.pub, no header/footer>';
```

Place `rsa_key.p8` at `/home/ubuntu/bench/keys/rsa_key.p8` — the ops scripts read it from
there. **This key file is effectively the credential; keep it off the repo.**

The account and user are read from the environment (no values are hardcoded in the repo).
Export them once per shell — every script and runner inherits them, and fails fast if unset:

```bash
export SF_ACCOUNT=ORG-ACCT          # your Snowflake account identifier
export SF_USER=MYUSER               # your Snowflake username
export SF_KEY=/home/ubuntu/bench/keys/rsa_key.p8   # optional; this is the default
# Tip: put these in ~/bench/.sfenv and `source` it (keep that file out of git).
```

## 4. Download the dataset

```bash
export AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=... AWS_SESSION_TOKEN=...
# Parallel + resumable (recommended; writes to /data/quotes):
python3 download_all.py
# or portable AWS-CLI variant (writes to ~/data/stockhouse):
bash download_stockhouse.sh        # all files
bash download_stockhouse.sh 10     # first 10 files only
```

Files are `quotes_YYYY-MM-DD.parquet` (`quotes_0.parquet`, the 10B-row historical file, is
excluded). Full set = 232 files / ~650 GB. Some daily files are empty (market-closed days).

### Dataset facts

| | |
|---|---|
| Rows/day file | ~808M (e.g. `quotes_2025-10-16`: 808M rows, 4.59 GB ZSTD) |
| Columns | `sym, bx, bp, bs, ax, ap, as, c, i, t, q, z` (12) |
| Row size | ~63 B logical, ~5.7 B/row compressed |
| `t` | unix **millis**, monotonic; `as` is a reserved word → quoted `"AS"`; `i` is `ARRAY` |
| 1 daily file | 808M rows ≈ 13.5 min at 1M EPS |

## 5. Create the schema, table, and MV

Run `create.sql` against `BENCH2COST.STOCKHOUSE`. It creates the raw `QUOTES` table
(`CLUSTER BY (sym, t)`) and the `QUOTES_DAILY` materialized view (`CLUSTER BY (sym, day)`,
`day = TO_DATE(TO_TIMESTAMP_NTZ(t, 3))`). For the **unclustered baseline**, omit the
`CLUSTER BY` lines (or use `ops/drop_clustering.sh`).

## 6. Run ingestion

```bash
# ops/run.sh <parallel> <row_groups_per_insert> <max_files|all> <target_rps> <warehouse>
bash ops/run.sh 8 8 all 1000000 BENCH2COST_GEN2_XSMALL
```

This launches `ingest.py` detached (`setsid`+`nohup`, survives SSH drops) and writes
`ingest.log`. `--target-rps 1000000` is a global limiter that pins the rolling average at 1M.

### Key parameters

| Flag | Default | Description |
|---|---|---|
| `--parallel` | 8 | Worker processes (8 ≈ the smallest count that clears 1M EPS). |
| `--row-groups-per-insert` | — | Row groups batched per COPY (8 ≈ ~1M-row batches). |
| `--target-rps` | 0 (unlimited) | Global rows/s ceiling across all workers. |
| `--max-files` | all | Limit to first N files. |
| `--live-eps-interval` | 15s | Seconds between live row-count samples. |

`COPY ... FORCE=TRUE` defeats load-history dedup so files can be replayed to sustain the rate
past the end of the dataset. `enumerate_tasks` skips unreadable/partial files so ingest can
coexist with an in-progress download.

## 7. Run the query-latency benchmark

Start both runners + the MV-lag tracker (all detached):

```bash
# ops/start_runners.sh [comment] [machine] [cluster_size]
bash ops/start_runners.sh "24h clustered" "Gen2 Small" 1
```

- **dashboard** runner → `out/dashboard_<ts>.jsonl` every 600s (4 queries vs `QUOTES_DAILY`).
- **drilldown** runner → `out/drilldown_<ts>.jsonl` every 3600s (1 query vs raw `QUOTES`).
- **mv_latency** tracker → `out/mv_latency_<ts>.jsonl` every 60s (`SHOW MATERIALIZED VIEWS`
  → `behind_by`, metadata-only, ~free).

Each JSONL line follows `RUNNERS_SPEC.md` (identical schema across CH/SF/DBX), with
**server-side** query duration (`EXECUTION_TIME` from `QUERY_HISTORY`, not client wall-clock).

Stop everything with `bash ops/stop_experiment.sh`.

> **MV lag note:** a Snowflake MV is *always query-consistent* — queries merge the materialized
> rows with a live scan of un-merged base rows, so results never go stale. The cost of lag
> shows up as **rising dashboard query latency** (+ MV maintenance credits), and the lag itself
> is observable **only** via `behind_by` — *not* via a row-count backlog (that method is valid
> only for a Dynamic Table). See `ops/lag.sh`.

## 8. Clustering experiment

Run **after** `ops/reset.sh` (empty `QUOTES`, so the MV rebuild is instant):

```bash
bash ops/reset.sh            # stop ingest, cancel in-flight COPYs, truncate, recreate MV
bash ops/add_clustering.sh   # QUOTES -> CLUSTER BY (sym,t); QUOTES_DAILY -> CLUSTER BY (sym,day)
bash ops/run.sh 8 8 all 1000000 BENCH2COST_GEN2_XSMALL
bash ops/start_runners.sh "24h clustered" "Gen2 Small" 1
# ... let it run ...
bash ops/stop_experiment.sh
bash ops/mv_billing.sh 30    # run ~3h after stop (ACCOUNT_USAGE settles)
```

Automatic Clustering reclusters continuously during ingest — a **separate serverless cost**
reported by `ops/mv_billing.sh` from `ACCOUNT_USAGE.AUTOMATIC_CLUSTERING_HISTORY`, alongside
ingest / MV-maintenance / reader credits. Use `ops/drop_clustering.sh` for the unclustered A/B.

### Tracking clustering lag (must be captured live)

Clustering **quality over time** — `average_depth` / `average_overlaps` from
`SYSTEM$CLUSTERING_INFORMATION`, the analogue of the MV's `behind_by` — is **point-in-time only**;
Snowflake never historizes it, so it **cannot be reconstructed after the run**. (Only the
reclustering *cost* — bytes/rows reclustered, credits — is retained, in
`AUTOMATIC_CLUSTERING_HISTORY`.) To get the lag-over-time curve you must sample it during the run,
which `start_runners.sh` now does via **`ops/clustering_lag.sh`** (polls `QUOTES` (sym,t) and
`QUOTES_DAILY` (sym,day) every 300s → `out/clustering_lag_<ts>.jsonl`). It runs **without a
warehouse** (cloud-services only) so it doesn't perturb the reader-warehouse query timings.
`average_depth` rises as time-ordered ingest lands `(sym,t)`-disordered partitions and falls as
AC catches up — a sawtooth, like the MV lag.

### Clustering re-run in a fresh schema (STOCKHOUSE_2)

The original `STOCKHOUSE` tables carry time-travel + fail-safe baggage and a `CREATE OR REPLACE`-
churned MV, and the clustering-lag depth signal was never captured live. To get a clean run whose
clustering lag we track, use a fresh schema. The whole toolchain is parameterized by **`SF_SCHEMA`**
(default `STOCKHOUSE`) — set it once and every script, the ingester, and the runners follow.

```bash
# 1) One-time: create the schema, clustered table, internal stage, clustered MV.
#    Run create_stockhouse_2.sql in Snowsight (or: snow sql -f create_stockhouse_2.sql).

# 2) Point the whole flow at the new schema, then run as usual (no reset/add_clustering needed —
#    create_stockhouse_2.sql already makes everything clustered).
export SF_SCHEMA=STOCKHOUSE_2
bash ops/run.sh 8 8 all 1000000 BENCH2COST_GEN2_XSMALL     # ingest -> STOCKHOUSE_2.QUOTES
bash ops/start_runners.sh "clustered STOCKHOUSE_2" "Gen2 Small" 1
#    start_runners launches dashboard + drilldown + mv_latency + clustering_lag (every 300s,
#    -> out/clustering_lag_<ts>.jsonl) — the clustering-depth-over-time signal.
# ... let it run ...
bash ops/stop_experiment.sh
bash ops/mv_billing.sh 30    # ~3h after stop
```

`clustering_lag.sh` polls `SYSTEM$CLUSTERING_INFORMATION` for `STOCKHOUSE_2.QUOTES` (sym,t) and
`QUOTES_DAILY` (sym,day) — `average_depth`/`average_overlaps` over time, the sawtooth that the
completed `STOCKHOUSE` run never recorded. (Note: `mv_billing.sh` attributes Automatic-Clustering
credits by table name and is not yet schema-filtered, so run the cost pull while only one schema's
tables exist, or filter its output by `SCHEMA_NAME`.)

## Results — 29.6h clustered run @ ~1M EPS (X-Small Gen2)

Ingest: `parallel=8, rgpi=8, all files, target_rps=1,000,000, BENCH2COST_GEN2_XSMALL`.

| Metric | Value |
|---|---|
| Duration | 106,584 s (~29.6 h) |
| Rows ingested | **105.5 billion** |
| Average throughput | **~990k rows/s** (target 1M; throttle shed ~1%) |
| Ingest warehouse | X-Small Gen2 (~1.35 credits/hr) |
| MV `behind_by` at end | ~14m28s (serverless MV maintenance lag at 105B rows) |

### Dashboard query latency vs data volume (vs MV, clustered)

| Raw rows | Q1 single-sym | Q2 watchlist | Q3 top-movers | Q4 daily series |
|---|---|---|---|---|
| 0 | 0.012s | 0.012s | 0.013s | 0.015s |
| 105.4B | **1.55s** | **0.832s** | **5.12s** | **8.91s** |

Q1/Q2 are `sym`-filtered → prefix-pruned by the `(sym, day)` cluster key, so they stay fast.
Q3/Q4 are full-MV scans → latency scales with the total data volume.

### Drilldown query latency (vs raw `QUOTES`, clustered)

| Raw rows | drilldown (single sym) |
|---|---|
| 0 | 0.016s |
| ~104B | **11.94s** |

See [Key findings](#key-findings) for the cost, sizing, and MV-behaviour takeaways. When
reporting cost, keep ingest / MV-maintenance / Automatic-Clustering / reader credits
**separate** — don't conflate them when stating "cost to sustain 1M EPS".

Raw data for all of the above is in `results/` (`dashboard_*.jsonl`, `drilldown_*.jsonl`,
`mv_latency_*.jsonl`, `ingest.log.gz`); storage sizes in `results/storage-size-{raw,mv}.md`.

## 9. Tear down

Suspend or drop the warehouses, stop the box, and **rotate any AWS keys** shared in plaintext
during setup.
