# Ingest Benchmark: ClickHouse Cloud vs Databricks vs Snowflake

Benchmark comparing ingest performance across three systems using a full ingest path: a raw data table plus one Materialized View (MV).

## Dataset

NBBO-style stock market **quotes** (bid/ask snapshots) — a narrow schema with a small row
size and minimal columns. Stored as daily Parquet files (one per trading day), ZSTD-compressed.

### Schema (vendor-agnostic)

| Column | Type | Description |
|---|---|---|
| `sym` | string | Ticker symbol (~3–4 chars) |
| `bx` | uint8 | Bid exchange code |
| `bp` | float64 | Bid price |
| `bs` | uint64 | Bid size |
| `ax` | uint8 | Ask exchange code |
| `ap` | float64 | Ask price |
| `as` | uint64 | Ask size |
| `c` | uint8 | Quote condition code |
| `i` | array&lt;uint8&gt; | Indicator flags (usually empty) |
| `t` | uint64 | Timestamp — Unix epoch, milliseconds (monotonic) |
| `q` | uint64 | Sequence number |
| `z` | uint8 | Tape / exchange group |

Mapped per engine in each system's `create.sql` (e.g. `string`→`String`/`VARCHAR`/`STRING`,
`uint8`→`UInt8`/`NUMBER(3,0)`/`SMALLINT`, `array<uint8>`→`Array(UInt8)`/`ARRAY`). `as` is a
reserved word and is quoted everywhere.

### Size

| Metric | Value |
|---|---|
| Total rows | ~113 billion |
| Total size | ~651 GB (Parquet, ZSTD) |
| Files | 232 daily files (`quotes_YYYY-MM-DD.parquet`); empty on market-closed days |
| Avg file size | ~2.8 GB across all files; a typical trading day is ~4.6 GB / ~808M rows |
| Row group | ~130K rows |
| Row size | ~5.7 bytes/row compressed (~63 bytes uncompressed) |

To sustain ingest past the dataset's end, files are replayed; each run reaches ~100B+ rows.

### Data source & licensing

The quotes data comes from **[Massive](https://massive.com/)**, a US market-data API provider
(REST + WebSocket + bulk "flat files"). We access it under a partnership that **does not permit
redistribution**, so this repo ships only the ingest/query scripts and the aggregated results —
**not** the dataset itself. To reproduce the benchmark you bring your own Massive data.

**Getting the data:**

The dataset is a **capture of Massive's real-time quotes WebSocket**, recorded to daily Parquet
files — not a historical pull. To reproduce it:

1. Get a Massive plan with **real-time stock quotes** access (per the docs, Stocks Advanced for
   personal use, or Business + Expansion) and an API key.
2. Connect to the quotes stream
   ([`WS /stocks/Q`](https://massive.com/docs/websocket/stocks/quotes)) and subscribe to **all
   tickers** (`ticker=*`). Each message is one NBBO quote whose fields map 1:1 to the schema
   above (`sym, bx, bp, bs, ax, ap, as, c, i, t, q, z`).
3. Record the stream and batch messages into one Parquet file per trading day
   (`quotes_YYYY-MM-DD.parquet`), then point each system's `download_*` script at your copy (the
   rest of the pipeline is unchanged). Because WebSocket is a live feed, you capture **going
   forward** over a comparable multi-day window — you can't pull a past window from the socket.

For a fixed *historical* window instead (same data, different path/format), use the REST
[`Quotes`](https://massive.com/docs/rest/stocks/trades-quotes/quotes) endpoint (paged per ticker)
or [Flat Files](https://massive.com/docs/flat-files/stocks/quotes) (bulk daily CSV).

**License you'll need to reproduce it:**

- **Personal / non-commercial:** an individual plan with **real-time access** (Stocks Advanced)
  lets a non-professional user stream the full US quotes feed for personal use — enough to
  re-run this benchmark yourself. See [pricing](https://massive.com/pricing) and the
  [Individuals Terms of Service](https://massive.com/legal/individuals-terms-of-service).
- **Commercial / business:** requires a [business plan](https://massive.com/business); under US
  market-data rules, real-time exchange data also carries **exchange licensing agreements and
  fees** on top of the subscription. See the
  [Market Data Terms of Service](https://massive.com/legal/market-data-terms-of-service).
- **Redistribution** (republishing the raw data or sharing the dataset) is **not allowed on
  standard plans** — it needs a separate redistribution agreement with Massive
  ([details](https://massive.com/knowledge-base/article/how-can-i-redistribute-massives-market-data)).
  That restriction is why the dataset is not included here.

Plan names, limits, and fees change — confirm the current terms on Massive's pricing and legal
pages before relying on them.

## Ingest setup

All three systems ingested at the same throughput rate with the same batch size, reading from the same Parquet files using equivalent ingest script logic.

| Parameter | Value |
|---|---|
| Target throughput | 1M events/sec |
| Batch size | ~1M rows |
| Source | Parquet files |
| Ingest path | Raw table + 1 MV |

Smallest hardware configuration that could sustain 1M eps was selected for each platform.

| System | Write hardware |
|---|---|
| ClickHouse Cloud | 2 nodes × 2 vCPUs / 8 GiB RAM |
| Databricks | 2X-Small Warehouse — 8 vCPUs / 61 GiB RAM |
| Snowflake | Gen2 X-Small Warehouse — 8 vCPUs / 16 GB RAM |

## Read setup

Read and write compute are separated (compute-compute separation) so query workloads run on isolated hardware. Read hardware was aligned on CPU core count across platforms.

| System | Read hardware |
|---|---|
| ClickHouse Cloud | 1 node × 16 vCPUs / 64 GiB RAM |
| Databricks | X-Small Warehouse — 16 vCPUs / 122 GiB RAM |
| Snowflake | Gen2 Small Warehouse — 16 vCPUs / 32 GB RAM |

Read query pattern:

- Every 10 minutes: 4 queries against the MV simulating a live dashboard
- Every hour: 1 drill-down query against the raw data table
