# Quotes data — Snowflake Ingestion

End-to-end steps to spin up an EC2 box, download the quotes dataset, ingest it into
Snowflake at a sustained **1M events/sec (EPS)** with a materialized-view rollup attached,
and run the dashboard/drilldown latency benchmark — the Snowflake peer of the
ClickHouse and Databricks tracks in this repo.

## What this runs

The Snowflake peer of the ClickHouse / Databricks ingest benchmark in this repo: ingest market-quote
Parquet at a sustained **~1M events/sec** with a rollup attached, then measure **dashboard + drilldown
query latency** as the data grows. Two variants, each in its own schema (details in
[Running the benchmark](#running-the-benchmark-t0-and-t1)):

- **T0 — standard:** raw `QUOTES` table + `QUOTES_DAILY` **materialized view**.
- **T1 — interactive tables:** `QUOTES_IT` + `QUOTES_DAILY_IT` (Snowflake interactive tables).

## Repo layout

| Path | Purpose |
|---|---|
| `create.sql` | `QUOTES` table + `QUOTES_DAILY` materialized view (DDL, with clustering keys). |
| `download_stockhouse.sh` | Download dataset from S3 via AWS CLI (portable). |
| `download_all.py` | Parallel, resume-friendly boto3 download (the variant actually used). |
| `ingest.py` | Parallel per-row-group ingester (read → PUT → `COPY INTO FORCE=TRUE`). |
| `queries_mv.sql` / `queries_mv_it.sql` | 4 dashboard queries vs the MV (T0) / interactive `QUOTES_DAILY_IT` (T1). |
| `queries_raw.sql` / `queries_raw_it.sql` | 2 drilldown queries (hourly OHLCV bars + "B7" risk/liquidity profile) vs raw `QUOTES` (T0) / interactive `QUOTES_IT` (T1). |
| `create_stockhouse_t1.sql` · `ops/setup_interactive.sh` · `ops/start_runners_it.sh` | T1 interactive-table variant (no-MV schema, interactive tables, IT runners). |
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

## 4. Get the dataset

⚠️ **The quotes dataset is not shipped with this repo.**  See the parent
**[README → "Data source & licensing"](../README.md#data-source--licensing)** for how to obtain it.

Once you have `quotes_YYYY-MM-DD.parquet` files staged in your own location (an S3 bucket or `/data`),
point the download/ingest at your copy — the scripts are otherwise unchanged:

### Dataset facts

| | |
|---|---|
| Rows/day file | ~808M (e.g. `quotes_2025-10-16`: 808M rows, 4.59 GB ZSTD) |
| Columns | `sym, bx, bp, bs, ax, ap, as, c, i, t, q, z` (12) |
| Row size | ~63 B logical, ~5.7 B/row compressed |
| `t` | unix **millis**, monotonic; `as` is a reserved word → quoted `"AS"`; `i` is `ARRAY` |
| 1 daily file | 808M rows ≈ 13.5 min at 1M EPS |

## Running the benchmark (T0 and T1)

Two variants, each in its **own schema** — set `SF_SCHEMA` once and every script + runner follows it
(the toolchain refuses to target the original `STOCKHOUSE`). Export your account/user/key first (§3),
then pick **T0** or **T1**. Both follow the same shape:

> **set up the schema → start ingest → start the read runners + a lag tracker → (at the end) pull the cost.**

The read runners write one JSONL line per iteration (`RUNNERS_SPEC.md` schema, server-side query
time): **dashboard** every 600s (4 queries) + **drilldown** every 3600s (2 queries). Cost comes from
`ACCOUNT_USAGE`, which lags up to ~3h — so **run the billing step a few hours after you stop.**

### T0 — standard (materialized view)

Raw `QUOTES` table + `QUOTES_DAILY` **materialized view** (serverless background maintenance).

```bash
export SF_SCHEMA=STOCKHOUSE_T0

# 1. create the schema: clustered QUOTES + internal stage + QUOTES_DAILY MV
bash ops/setup_schema.sh

# 2. start ingest at ~1M EPS (detached; survives SSH drops, writes ingest.log)
bash ops/run.sh 8 8 all 1000000 BENCH2COST_GEN2_XSMALL

# 3. start the read runners + trackers (detached)
bash ops/start_runners.sh "T0 standard" "Gen2 Small" 1
#    dashboard (600s, vs QUOTES_DAILY)  +  drilldown (3600s, vs QUOTES)
#    + mv_latency tracker (60s -> behind_by)  + clustering_lag tracker (300s)

# ... let it run for your window, then stop everything ...
bash ops/stop_experiment.sh

# 4. COST -- run ~3h AFTER stop (ACCOUNT_USAGE settles):
bash ops/mv_billing.sh 30
#    -> ingest wh + serverless MV maintenance + reader wh + auto-clustering credits
```

- **MV freshness lag** is tracked by the `mv_latency` runner (`behind_by`, every 60s). The MV is always
  query-consistent — lag shows up as rising dashboard latency, not stale answers.
- Clustering is on by default (`CLUSTER BY` in the schema); depth-over-time is tracked by
  `clustering_lag` and its serverless cost is in the billing pull. Unclustered A/B: `ops/drop_clustering.sh`.

### T1 — interactive tables

Ingest into the standard `QUOTES`, then two **interactive tables** that replace the MV — each
maintained by its own refresh warehouse on a target lag:

| Interactive table | Role | Target lag | Refresh warehouse |
|---|---|---|---|
| `QUOTES_DAILY_IT` | `(sym, day)` rollup (MV peer) | **1 minute** | `BENCH2COST_GEN2_MEDIUM` |
| `QUOTES_IT` | live copy of the raw table | **10 minutes** | `BENCH2COST_GEN2_XSMALL` |

```bash
export SF_SCHEMA=STOCKHOUSE_T1

# 1. create the schema: clustered QUOTES + stage, WITHOUT the standard MV (the ITs replace it)
T1=1 bash ops/setup_schema.sh

# 2. start ingest at ~1M EPS (detached) -- same ingester as T0
bash ops/run.sh 8 8 all 1000000 BENCH2COST_GEN2_XSMALL

# 3. create the interactive tables (QUOTES_DAILY_IT 1-min lag, QUOTES_IT 10-min lag)
bash ops/setup_interactive.sh

# 4. start the read runners + IT refresh tracker (detached) on an interactive read wh
bash ops/start_runners_it.sh "T1 interactive" "IT Small" 1
#    dashboard (600s, vs QUOTES_DAILY_IT)  +  drilldown (3600s, vs QUOTES_IT)
#    + it_refresh tracker (60s -> per-IT refresh duration / staleness)

# ... let it run, then stop everything ...
bash ops/stop_experiment.sh

# 5. COST -- run ~3h AFTER stop:
bash ops/mv_billing.sh 30
#    -> ingest wh + the two IT REFRESH warehouses (Medium + X-Small) + interactive reader wh credits
#    (warehouse credits = WAREHOUSE_METERING_HISTORY; if your warehouse names differ from the
#     script defaults, use the query in metrics_reference.sql)
```

- **Interactive-table refresh lag** is tracked by the `it_refresh` runner (per-IT refresh duration +
  staleness, every 60s) — the IT analogue of the MV's `behind_by`.
- **Cost** here is warehouse credits: the ingest wh + the two IT **refresh** warehouses + the
  interactive **reader** wh (there is no serverless MV maintenance in this variant).

## Tear down

Suspend or drop the warehouses, stop the EC2 box, and **rotate any AWS keys** shared in plaintext
during setup. Drop the schema to fully reclaim storage — note Snowflake retains time-travel +
fail-safe for the retention window after a drop.
