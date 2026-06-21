# Streaming → Interactive Tables (Option B)

Snowpipe Streaming writes raw rows **directly** into an interactive table (no ingest warehouse,
no refresh), and an **interactive materialized view** maintains the `(sym, day)` rollup. Goal:
measure the cost saving vs the warehouse-based path (COPY ingest + dynamic-IT refresh).

Runs in a **fresh schema + separate warehouses** so it never collides with the warehouse-based
benchmark in `BENCH2COST.STOCKHOUSE`.

## Objects (all in `BENCH2COST.STOCKHOUSE_T2`)
| Object | What |
|---|---|
| `QUOTES_IT` | interactive table, `CLUSTER BY (sym,t)` — **streaming target** (no TARGET_LAG) |
| `QUOTES_IT_PIPE` | pipe: `COPY INTO QUOTES_IT FROM TABLE(DATA_SOURCE(TYPE=>'STREAMING'))` |
| `QUOTES_DAILY_IMV` | interactive **materialized view** on `QUOTES_IT` = the rollup (an IT can't source another IT, so the aggregate must be an IMV) |

## Separate warehouses (distinct names from the live run)
| Warehouse | Role | Notes |
|---|---|---|
| _(none)_ | ingest | **serverless** — Snowpipe Streaming, no warehouse |
| `BENCH2COST_IT_STREAM` | interactive read | serves dashboard/drilldown |
| `BENCH2COST_GEN2_SMALL_STREAM` | IMV maintenance | only if the IMV needs a maintenance wh (see open items) |
| `BENCH_STREAM` | tracking | counts / refresh / cost lookups, isolated |

## Run sequence (when the account is free)
```bash
cd ~/bench && source .sfenv && source .venv/bin/activate
export SF_SCHEMA=STOCKHOUSE_T2 SF_TRACK_WAREHOUSE=BENCH_STREAM SF_WAREHOUSE=BENCH2COST_IT_STREAM
export SF_RAW_TABLE=QUOTES_IT SF_MV_TABLE=QUOTES_DAILY_IMV
pip install snowpipe-streaming                 # one-time (adds the streaming SDK)

# 1. create schema + IT + pipe + IMV + warehouses (one-time)
snow sql -f t2/setup_streaming.sql       # or run via the connector

# 2. stream the dataset (serverless; no ingest warehouse)
python t2/stream_quotes.py --dir /data/quotes --schema STOCKHOUSE_T2 \
    --pipe QUOTES_IT_PIPE --profile profile.json --parallel 8 --target-rps 1000000

# 3. read runners (timed queries on the interactive wh, support on BENCH_STREAM)
python run_dashboard.py  --database BENCH2COST --queries t2/queries_mv_imv.sql  --output-dir out  "Snowflake (AWS)" "IT Small (stream)" 1 "streaming" 0
python run_drilldown.py  --database BENCH2COST --queries t2/queries_raw_it.sql  --output-dir out  "Snowflake (AWS)" "IT Small (stream)" 1 "streaming" 0
```

## Cost tracking (the whole point)
| Cost | Source |
|---|---|
| **Streaming ingest** | `SNOWFLAKE.ACCOUNT_USAGE.METERING_HISTORY WHERE SERVICE_TYPE='SNOWPIPE_STREAMING'` (≈0.0037 cr/uncompressed GB) |
| **Rollup (IMV) maintenance** | `MATERIALIZED_VIEW_REFRESH_HISTORY` (+ serverless `METERING_HISTORY`) if serverless; else `WAREHOUSE_METERING_HISTORY` for `BENCH2COST_GEN2_SMALL_STREAM` |
| **Read** | `WAREHOUSE_METERING_HISTORY` for `BENCH2COST_IT_STREAM` |
| Query latency / timeouts | dashboard/drilldown runners (`"timeout"` captured) |

Compare against the warehouse-based ledger: COPY-ingest wh + 2 refresh whs + read wh.

## Open items to verify against the account (none run yet — live benchmark in progress)
1. **Snowpipe Streaming HPA available in eu-west-2** and the pipe can target an interactive table.
2. **`CREATE INTERACTIVE MATERIALIZED VIEW` exact syntax** — whether it accepts `CLUSTER BY`, and
   whether maintenance is **serverless** (ideal — keeps the "no warehouse" win) or needs
   `WAREHOUSE = BENCH2COST_GEN2_SMALL_STREAM`. This determines whether the rollup is truly
   warehouse-free. (`setup_streaming.sql` currently omits the warehouse clause — add it if creation errors.)
3. **`profile.json` schema** for the installed `snowpipe-streaming` SDK version (keys/host) — the
   streamer writes a best-effort profile from env; adjust if the SDK rejects it.
4. **IMV refresh-history**: confirm `INTERACTIVE_TABLE_REFRESH_HISTORY` vs `MATERIALIZED_VIEW_REFRESH_HISTORY`
   covers the IMV, so the existing `ops/it_refresh.sh` (or an MV variant) tracks it.
