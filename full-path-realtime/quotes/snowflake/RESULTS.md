# Snowflake Ingestion Benchmark — Quotes (1M EPS)

**Date:** 2026-06-05 · **Goal:** reach and sustain **1,000,000 events/sec (EPS)** ingesting market-quote
data into Snowflake, and find the **cost** to do so. Part of the `bench2cost` series (ClickHouse, Databricks,
Snowflake on the same dataset & methodology).

---

## TL;DR

- ✅ **Snowflake sustains 1M EPS** ingesting quotes via continuous `COPY INTO`, measured end-to-end from the
  client (read → upload → load → queryable).
- 🔑 It took **8 concurrent client workers** to reach 1M (vs **2** for ClickHouse on the same data).
- 💸 **Cost is ~$1 per billion rows** at the cost-optimal config (an **X-Small** warehouse).
- 🧠 **Warehouse size does not change throughput** for this pattern — it's bound by *client concurrency × batch
  size*, not warehouse compute. So you pick the **smallest** warehouse and tune the client → same 1M EPS at
  **1/8th the cost** of a Large.

| Config (parallel 8, ~1M-row batches) | Sustained EPS | Gen2 credits/hr | credits/billion @ 1M | ~$/billion @ 1M* |
|---|---:|---:|---:|---:|
| X-Small Gen2 | ~1.11 M | ~1.35 | ~0.375 | **~$1.1** |
| Small Gen2 | ~1.08 M | ~2.7 | ~0.75 | ~$2.3 |
| Medium Gen2 | ~1.11 M | ~5.4 | ~1.5 | ~$4.5 |
| Large Gen2 | ~1.16 M | ~10.8 | ~3.0 | ~$9.0 |

\* Warehouses are **Gen2** (AWS Gen2 = 1.35× Gen1 credits/hr). Credits are the durable metric; $ assumes
Enterprise on-demand ≈ $3/credit (adjust to your contract rate). Gen2 Large rate was metered (~10 cr/hr,
consistent with the 10.8 list rate); smaller sizes derived via the 1.35× ratio.

---

## Dataset

Market quotes (Polygon-style), exported from a ClickHouse `stockhouse.quotes` table as daily Parquet files.

- **Per daily file:** ~808 M rows, ~4.6 GB compressed (ZSTD), 12 columns, ~131 K rows/row-group.
- **Whole dataset:** 223 daily files, ~653 GB in S3.
- **Row size:** ~63 bytes logical (the figure Snowflake bills on); 8.4 B parquet-encoded; 5.7 B compressed.
- **Schema:** `sym` (string), `bx ax c z` (uint8), `bp ap` (double), `bs as` (uint64), `i` (array<uint8>),
  `t` (uint64, unix-ms), `q` (uint64). `t` kept as raw NUMBER to avoid per-row timestamp conversion.
  Snowflake types: uint8→`NUMBER(3,0)`, uint64→`NUMBER(20,0)`, double→`FLOAT`, string→`VARCHAR`,
  array→`ARRAY`. Note `"AS"` is a reserved word and must be quoted.

---

## Environment & regions

| Component | Location |
|---|---|
| S3 bucket (`pme-internal`) | AWS **us-east-2** (Ohio) |
| Snowflake account | AWS **eu-west-3** (Paris), Enterprise, Gen2 warehouses |
| EC2 client | AWS **eu-west-3** (Paris) — **co-located with Snowflake**: 32 vCPU, 123 GB RAM, 2 TB disk |

**Why co-locate the client with Snowflake:** we measure the *client-perceived* ingest rate, which includes the
upload (`PUT`). A cross-region client would measure the transatlantic network, not Snowflake. The source data
(Ohio) crossed to Paris **once, unmeasured**, into a Snowflake internal stage; from there everything is
region-local, so the measured numbers reflect Snowflake, not the WAN.

---

## Methodology

Same skeleton as the ClickHouse and Databricks ingesters (one task per `(file, row_group)`, a pool of
parallel workers, a global rows/sec limiter, and a `count(*)`-polling live monitor). Per task, each worker:

1. **Read** N row groups from a local Parquet file (pyarrow).
2. **Encode** them to a fresh Parquet buffer on `/dev/shm` (RAM — effectively in-memory).
3. **`PUT`** the file to a Snowflake **internal stage** (region-local).
4. **`COPY INTO`** the table (`MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE`, `FORCE=TRUE` for replay).
5. **`REMOVE`** the staged file.
6. **Throttle** against a shared atomic counter if the aggregate exceeds `--target-rps`.

**EPS metric:** `avg = (count(*) − start_count) / elapsed_seconds` — rows actually committed and queryable,
i.e. the true end-to-end client rate (not "rows sent").

> Snowpipe Streaming (the real-time best-practice path) was evaluated and working, but **dropped by choice** in
> favor of the client-driven `COPY` path to mirror the ClickHouse/Databricks "client pushes to a warehouse"
> comparison. Worth revisiting separately — Streaming is serverless (no warehouse) and bills ~0.0037
> credits/uncompressed-GB.

---

## Results

### 1. Throughput scales with client workers (Large Gen2, ~1M-row batches)

| Workers | Sustained EPS |
|---:|---:|
| 2 | ~300 K |
| 4 | ~550 K |
| 6 | ~835 K |
| **8** | **~1.16 M** ✅ |

8 workers is the smallest count that clears 1M at this batch size.

### 2. Batch size matters (parallel 8)

| Batch size | Sustained EPS |
|---|---:|
| ~1 M rows (8 row groups) | ~1.16 M |
| ~6.5 M rows (50 row groups) | **~2.3 M** |

Bigger batches amortize fixed per-`COPY` overhead (planning/commit/metadata), nearly doubling throughput.

### 3. Held steady at exactly 1M (the goal)

`parallel 8`, ~1M-row batches, `--target-rps 1,000,000` → average **pinned at ~1.00 M EPS**, with the limiter
sleeping ~1.3 s/batch to shed the ~16% natural surplus. Sustained, no errors.

### 4. Warehouse-size sweep — size is irrelevant (parallel 8, ~1M-row batches)

| Warehouse | EPS | Gen2 credits/hr |
|---|---:|---:|
| X-Small | ~1.11 M | ~1.35 |
| Small | ~1.08 M | ~2.7 |
| Medium | ~1.11 M | ~5.4 |
| Large | ~1.16 M | ~10.8 |

**All sizes deliver the same ~1.1 M EPS.** (Each run's warehouse was verified via `query_history`.)

---

## Why warehouse size doesn't change throughput

`COPY` parallelizes by spreading files/splits across the warehouse's cores. Our workload is **8 concurrent
single-file `COPY`s of ~1 M-row (~50 MB) files ≈ ~8 cores of work** — which already ~saturates an X-Small
(~8 cores). A Large (~64 cores) gets the *same* 8 small jobs and leaves ~56 cores idle. And each `COPY`'s
~6.5 s is mostly **fixed overhead** (planning, commit, micro-partition metadata) that doesn't shrink with more
nodes. So:

```
throughput ≈ concurrent_COPYs × rows_per_COPY / per_COPY_time ≈ 8 × 1M / 6.5s ≈ 1.1 M/s
```

Every term is independent of warehouse size. The levers that *do* work are **more workers** (raises
concurrency) and **bigger batches** (amortizes overhead). Size *would* matter if a single `COPY` loaded a
large/many-file payload (fans out across nodes) or if concurrency were high enough to saturate the small
warehouse (note: `MAX_CONCURRENCY_LEVEL` defaults to 8 on every size).

---

## Cost

- These are **Gen2** warehouses. On AWS, **Gen2 = 1.35× Gen1 credits/hr** → X-Small 1.35, Small 2.7,
  Medium 5.4, Large 10.8 cr/hr. (The Large was **metered at ~10 cr/hr**, consistent with the 10.8 list rate —
  a slightly-under-full-hour bucket; smaller sizes derived via the 1.35× ratio.)
- At 1M EPS = **3.6 B rows/hour**, so credits-per-billion-rows = `(credits/hr) ÷ 3.6`.
- **Cost-optimal:** X-Small (~1.35 cr/hr) → **~0.375 credits/billion rows ≈ ~$1.1/B** at $3/credit — and it
  still delivers ~1.1 M EPS. That's **8× cheaper than a Large** for the identical result.

> Cost here is purely the warehouse run-time. Snowflake's *streaming* ingest (not used here) bills per
> uncompressed GB instead, which would be a separate, much-lower ingest line item if revisited.

---

## ClickHouse comparison (same dataset)

| | ClickHouse | Snowflake (COPY path) |
|---|---|---|
| Sustained 1M EPS | ✅ | ✅ |
| Client workers to reach 1M | **2** | **8** |
| Bottleneck | — | client concurrency / per-COPY overhead |
| Ingest mechanism | native async insert (zero-decode parquet bytes over HTTP) | re-encode → PUT → COPY |

Snowflake reaches the same 1M EPS but requires ~4× the client parallelism, because each `COPY` carries fixed
per-statement overhead that ClickHouse's async-insert path avoids.

---

## Reproduce

On the co-located eu-west-3 client (`/home/ubuntu/bench`, Python 3.12 venv, key-pair auth):

```bash
# bash run.sh <parallel> <row_groups_per_insert> <max_files> <target_rps> <warehouse>

# Cost-optimal sustained 1M EPS:
bash run.sh 8 8 4 1000000 BENCH2COST_GEN2_XSMALL

# Max throughput (~2.3M):
bash run.sh 8 50 4 0 BENCH2COST_GEN2_LARGE

# Watch live:
tail -f /home/ubuntu/bench/ingest.log
```

Target objects: `BENCH2COST.STOCKHOUSE.QUOTES` (unclustered) loaded from internal stage `@QUOTES_INT_STAGE`.

---

## Caveats

- The warehouse-size sweep used the **~1M-row-batch** config; the big-batch (2.3M) result was only confirmed on
  Medium & Large, so an X-Small might not hold 2.3M with big batches (it holds ~1.1M with 1M-row batches).
- `$` figures assume **Enterprise ≈ $3/credit**; substitute your contract rate. Credits are the durable metric.
- Instantaneous EPS oscillates because each `COPY` commits its whole batch atomically; the **average** is the
  sustained rate.
- Table is **unclustered** to keep the ingest measurement clean (Automatic Clustering is a separate serverless
  cost; measure it independently if the table needs clustering).
- Source data (us-east-2) was staged into Snowflake (eu-west-3) once, unmeasured; measured ingest is
  region-local.

---

## Suggested next steps

- Official long (30–60 min) throttled-1M run on X-Small to lock the headline number + exact credits.
- Throughput beyond 2.3M: more workers (raise `MAX_CONCURRENCY_LEVEL` / multi-cluster) and/or bigger batches.
- Evaluate **Snowpipe Streaming** as a serverless, per-GB-billed alternative (no warehouse).
- Separately measure clustering cost if the target table needs `(sym, t)` clustering.
