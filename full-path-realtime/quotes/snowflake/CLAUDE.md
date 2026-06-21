# Claude Code guide — Snowflake quotes benchmark (T0 / T1 / T2)

You are helping a colleague **run the Snowflake side of the quotes drilldown benchmark** and produce
results, while the project owner is away. **Read [`NEXT_BENCHMARK_RUNBOOK.md`](./NEXT_BENCHMARK_RUNBOOK.md)
first — it is the source of truth** and walks through everything below in detail.

## Your job
Guide the colleague through, in order:
1. **Connect** to the right box — Paris (T0, T1) or London (T2) — and ClickHouse for the baseline (runbook §1). Secrets are placeholders; see below.
2. **Onboard** if needed (SSH key + Snowflake access, runbook §1b).
3. **Preflight** the target benchmark (`ops/preflight.sh T1` / `T2`) before any long run (runbook §3/§4).
4. **Run** T0, T1, or T2 — each in its own schema (runbook §2/§3/§4). Each benchmark has **two query
   workloads**: the **drilldown** (vs the raw table, hourly) and the **dashboard** (vs the rollup, every
   600s). For T1/T2, `SF_SCHEMA=STOCKHOUSE_T{1,2} bash ops/start_runners_it.sh` launches **both** plus the
   refresh tracker, detached.
5. **Stop**, **collect**, and **produce charts** — folded into each experiment's "Collect & charts" block
   (runbook §2/§3/§4); shared reference/debugging in §5, teardown in §7.

Ask the colleague which benchmark they want to run, then follow that section step by step. Confirm
the preflight passes before kicking off a multi-hour ingest.

## Stopping a run & collecting results
- **Stop** the ingest + runners on the box (non-destructive):
  ```bash
  bash ~/bench/ops/stop_experiment.sh          # kills ingest.py + run_drilldown/dashboard + monitors
  ```
- **Collect** the per-iteration JSONL the runner wrote (output dir is per benchmark: `out_t0/` /
  `out_t1/` / `out_t2/`), e.g. from your laptop:
  ```bash
  scp -i <key.pem> ubuntu@<host>:/home/ubuntu/bench/out_t1/drilldown_*.jsonl ./_collected/
  ```
  Commit runs under `quotes/snowflake/results/t{0,1,2}/` (SF, per benchmark) and
  `quotes/clickhouse-cloud/results/raw/` (CH baseline).
- **Supplementary data** (runbook §5): besides the latency JSONL, collect into `results/t{n}/`:
  the **IT refresh** history (`out_t{n}/it_refresh_*.jsonl` + an `it_refresh.csv` dump for the lag chart)
  and **storage** (`storage.json` — Snowflake raw + rollup bytes/rows from `TABLE_STORAGE_METRICS`/`TABLES`).
- **Charts**: one command per benchmark renders the full set into `_out/t{n}`:
  `cd quotes/_viz && bash make_charts.sh t1` (t0/t1/t2 the same way — it stages `results/t{n}/` + the
  CH baseline and picks the right chart set; missing inputs are skipped). `uv run generate.py --list` lists charts.
- **Teardown** (DROP SCHEMA etc.) is destructive → **propose, let the colleague run it** (runbook §7).

## Hard rules
- ⚠️ **NEVER run destructive operations** (`DROP`, `TRUNCATE`, `DELETE`, `REMOVE`, `ALTER … DROP`,
  `CREATE OR REPLACE` over a populated object, deleting result dirs). **Propose** the exact command and
  let the colleague run it. (See the Operating policy in the runbook.) Setup / ingest / query / read
  and non-destructive `ALTER` (e.g. re-pointing a refresh warehouse) are fine to run.
- 🔒 **Never commit secrets.** Hosts, `.pem` keys, `SF_ACCOUNT`, ClickHouse `FQDN`/`PASSWORD` are
  `<FILL-IN>` placeholders. The colleague copies `.env.example` → `.sfenv` on the box and fills it in
  (`.gitignore` already excludes secrets, `out/`, `results*`).

## Conventions
- **Schemas:** `STOCKHOUSE_T0` (standard), `STOCKHOUSE_T1` (interactive), `STOCKHOUSE_T2` (streaming).
  The original `STOCKHOUSE` is the first run — leave it intact. Drive every step with `SF_SCHEMA=…`.
- **Drilldown = two queries** (hourly OHLCV bars + "B7" risk/liquidity profile) → `result: [[q1],[q2]]`.
- **Warehouse types:** standard tables need a standard wh; interactive tables need an interactive wh
  (`BENCH2COST_IT_SMALL`). Mixing errors.
- **Deploying code to a box:** edits live in this repo; push them to a box with
  `bash ops/sync_box.sh <key.pem> <user@host> --apply` (code only; excludes secrets/results).
- The box runs from `/home/ubuntu/bench` (a flat copy of this folder); scripts are under `ops/`.
- **Metric queries** (latency, IT/MV refresh lag, storage, clustering depth, credits) are collected
  with descriptions in `metrics_reference.sql` — point colleagues there for direct Snowflake queries.

## Current state (2026-06-17)
- T1 validated runnable on Paris; T0 ready to run; **T2 validated** (Snowpipe Streaming → `QUOTES_IT`
  ~1M EPS, interactive MV `QUOTES_DAILY_IMV`; reads on `SNOWPIPES_IT_READ_SMALL`, tracking `BENCH`).
- A prior `STOCKHOUSE_T1` trial run was stopped; teardown/cleanup steps are in runbook §7.
