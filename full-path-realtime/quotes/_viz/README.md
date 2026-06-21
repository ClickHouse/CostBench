# Quotes ingest benchmark — charts

Renders **query latency vs data volume** for the quotes ingest benchmark (sustained
~1M EPS with a rollup attached). One line per system, faceted one subplot per query.
This is the latency-over-time view; the ClickBench-style storage/cost charts live in the
repo-root `_viz` / `_viz2`.

Style matches `../../_viz2` (dark theme, Inter font, shared `VENDOR_COLOR` map).

## Input

Each system's runner writes a JSONL time series (see `quotes/<system>/RUNNERS_SPEC.md`):
one record per iteration, with `raw_rows` (data volume at that moment) and `result`
(per-query latency in seconds). Drop the files into `_test/`:

```
_test/dashboard_<system>.jsonl    # 4 queries vs the MV
_test/drilldown_<system>.jsonl    # 1 query vs the raw table
```

Currently staged: `clickhouse`, `snowflake`, `databricks`. Adding a system is just dropping
its `dashboard_<system>.jsonl` / `drilldown_<system>.jsonl` into `_test/` and re-rendering —
no code change (the system is auto-detected from each record's `system` field and matched to
the shared `VENDOR_COLOR` map).

## Charts

- **`render_query_latency.py`** — query-latency summary as grouped bars: one representative
  single-query latency per system, per workload (dashboard / drilldown), on a log y-axis. The
  headline "how fast" comparison. `--agg` picks the statistic over all per-query samples in the
  run: `median` (default), `mean`, `p90`, `p95`, or `p99` (the committed chart uses p99 — it
  exposes the full-scan tail, e.g. Snowflake's dashboard p99 is ~30s vs ~2s median). Inputs are
  the same `_test/{dashboard,drilldown}_*.jsonl`; pass workloads via repeatable
  `--workload "LABEL=GLOB"`.
- **`render_query_cost.py`** — query-cost summary: total compute cost (USD) to run a workload
  over the run, one bar per system. **One chart per workload** — the driver emits
  `query_cost_dashboard` and `query_cost_drilldown` (plus `_linear` variants). Pass one
  `--workload` per chart; reads the staged `_test/cost_{dashboard,drilldown}_*.json` (from
  `<vendor>/costs/`). `--tier` picks the pricing tier (default `enterprise`; a system lacking it
  falls back to its cheapest, and each bar is labelled with the tier used). `--yscale log|linear`
  (default log; linear exaggerates the magnitude gap). Finding: on the dashboard workload
  Snowflake costs ~$10.8 vs ClickHouse ~$0.01 and Databricks ~$0.87; on the drilldown the gap
  narrows (CH ~$0.04, SF ~$0.43, DBX ~$0.44).
- **`render_ingest_cost.py`** — ingest-cost composition as **stacked** bars (linear y), one bar
  per system, segmented into **Ingest → Clustering → MV refresh** (clustering raw + MV merged
  into one layer — the renderer sums files mapping to the same vendor). Same component labels and
  colours as `render_cost_over_time.py` (Ingest blue, Clustering orange, MV refresh green).
  Reads the staged `_test/cost_{ingest,clustering_raw,clustering_mv,mv_refresh}_*.json` (from
  `<vendor>/costs/`); `--component "LABEL=GLOB[,GLOB]"` is repeatable and stacks bottom→top,
  `--tier` picks the pricing tier. ClickHouse sorts + rolls up *at ingest*, so it has a single
  bar in **ClickHouse yellow**, legend-labelled **"Everything (ingest + sort + MV)"**, so it
  isn't mistaken for just the ingest slice of the others. Finding (enterprise tier, ~100B rows):
  ClickHouse **~$21** vs Snowflake **~$283** (ingest $109 + clustering $162 + refresh $11) vs
  Databricks **~$254** (ingest $76 + clustering $101 + refresh $78) — ~12× cheaper, because the
  separate clustering + refresh cost is the bulk of the others' bill.
- **`render_cost_over_time.py`** (+ `build_cost_timeline.py`) — cumulative compute cost over
  time, one panel per system, **stacked by component** (ingest → clustering → MV refresh, with
  the same labels, colours, and ClickHouse-yellow "Everything" treatment as
  `render_ingest_cost.py`). The ingest layer plateaus at its billed window while
  clustering + MV refresh keep climbing — the "lag tail" of getting data query-ready.
  `build_cost_timeline.py` first normalizes the captured system-table detail files (Snowflake
  hourly `CREDITS_USED`; Databricks per-operation DBU + per-day refresh DBU) into
  `_test/cost_timeline_<vendor>.json`, anchoring elapsed time to each run's **measured** start
  (the dashboard runner's first `iteration_started_at`) and dropping pre-benchmark ops. Finding
  (enterprise tier): ~40% of Snowflake's cost (~$114) and ~$43 of Databricks' lands *after* the
  27h ingest window as clustering/refresh catch up; ClickHouse has no such tail. (Totals here
  run a few $ under the `ingest_cost` bars because that chart sums all-history ops while this
  view excludes the pre-benchmark ones.)
- **`render_latency.py`** — query latency vs data volume (dashboard 2×2, drilldown 1 panel),
  one line per system. The detailed view (latency as the table grows).
- **`render_storage.py`** — storage size comparison, two panels (raw table / MV), one bar per
  system labelled with size and total row count. Reads `_test/storage.json` (current/active
  compressed on-disk size; excludes Snowflake time-travel + fail-safe and Databricks
  time-travel — see the `note` field). Finding: ClickHouse's raw table is the smallest (362 GiB)
  despite holding the most rows (113B), vs Snowflake 583 GiB / Databricks 656 GiB; the Snowflake
  MV is ~8× larger than ClickHouse/Databricks due to un-compacted physical fragments.
- **`render_mv_lag.py`** — materialized-view freshness lag, comparing how stale each system's
  rollup gets under sustained ~1M EPS. Per-system inputs (vendor inferred from filename):
  - **Snowflake** — `mv_latency_snowflake.jsonl` (`SHOW MATERIALIZED VIEWS` → `behind_by`, the
    serverless MV's actual lag, sampled every 60s → a sawtooth peaking ~72 min).
  - **Databricks** — `mv_lag_databricks.csv` (`completed_at`, `seconds_since_prev_refresh`; the
    refresh interval = worst-case staleness per cycle for its refresh-triggered MV, ~8–10 min).
  - **ClickHouse** — none needed; its incremental MV is synchronous → flat 0s baseline.

  Elapsed time is measured per system from **its own ingest start**, so runs that began at
  different wall-clock times still line up at "t hours into ingest". X-axis modes:
  - default → elapsed ingest time (hours);
  - `--volume-from FILE` → base-table row count (each lag poll's timestamp mapped to
    `raw_rows`, interpolated from the dashboard runner JSONL), `--xscale log|linear`;
  - `--volume-line FILE` → keeps elapsed-time x and overlays growing data volume as a line on
    a second (right) y-axis (dual-axis view).

  Other options: `--xmax N` (clip the x-axis, in x-units — e.g. `--xmax 24` for a 24h window),
  `--smooth W`, `--no-baseline`, `--title`/`--no-title`. Finding: Snowflake's serverless
  MV sawtooths up to **~72 min behind** then partially catches up each refresh cycle;
  Databricks' refresh-triggered MV stays bounded at **~8–10 min**; ClickHouse stays at **0s**
  — all while rows grow linearly at ~1M EPS.

## Render

Use the **`generate.py`** driver — it runs every renderer with the right input files and is the
single place to choose which vendors and which charts to produce. Run it with **uv** — deps
(matplotlib/numpy) come from each script's inline PEP 723 metadata, so there's no venv to set up
and the folder is self-contained (copy it anywhere). Outputs land in `_out/`.

```bash
uv run generate.py                                    # all vendors, all charts (default)
uv run generate.py --vendors clickhouse snowflake     # only those two vendors
uv run generate.py --charts query_latency storage     # only those charts
uv run generate.py --list                             # list chart + vendor names, then exit
uv run generate.py --out-dir /tmp/preview --dpi 150   # render elsewhere / at lower DPI
```

| Flag | Default | Description |
|---|---|---|
| `--vendors V...` | all | Subset of `clickhouse snowflake databricks`. The driver picks each vendor's `_test/` files (and filters `storage.json`); a chart whose required vendor isn't selected is skipped (logged). |
| `--charts C...` | all | Subset of chart names (see `--list`). |
| `--out-dir DIR` | `_out` | Output directory (created if missing). |
| `--dpi N` | renderer default (300) | Override render DPI. |
| `--list` | — | Print chart and vendor names and exit. |

Chart names: `query_latency`, `query_cost_dashboard` / `query_cost_drilldown` (+ `_linear`
variants), `ingest_cost`, `cost_over_time`, `dashboard` /
`dashboard_smooth` / `dashboard_smooth_linear`, `drilldown` / `drilldown_smooth` /
`drilldown_smooth_linear`, `mv_lag`, `mv_lag_time_volume`, `storage`.

Vendor-filtering notes: `mv_lag` is Snowflake-specific (skipped unless `snowflake` is selected);
`mv_lag_time_volume` needs at least one of Snowflake/Databricks and uses ClickHouse as the flat-0
baseline only when `clickhouse` is selected; `storage` reads all vendors from one
`_test/storage.json` and writes a filtered copy to a temp file for subsets.

You can still call an individual renderer directly for one-off tweaks — `_calls.txt` lists the
exact per-chart commands, and each renderer's own options are below.

## Options

| Flag | Default | Description |
|---|---|---|
| `files...` | (required) | One JSONL per system. Globs fine (`_test/dashboard_*.jsonl`). |
| `--out PATH` | show window | Output PNG. |
| `--title STR` / `--no-title` | generic | Figure title. |
| `--query-labels "A;B;C"` | `Query N` | `;`-separated subplot titles, in queries-file order. |
| `--smooth W` | 0 (off) | Rolling-median window (odd, e.g. 7). Raw drawn faint behind the median (hide with `--no-raw`). |
| `--no-raw` | off | With `--smooth`, draw only the rolling-median trend (no faint raw line). |
| `--full-range` | off | Plot each system's full range. Default trims the x-axis to the row-count window where **all** systems have data. |
| `--xscale log\|linear` | log | X (row-count) axis scale. |
| `--yscale log\|linear` | log | Y (latency) axis scale. Linear y exaggerates the magnitude gap (e.g. ClickHouse pinned near 0 vs Snowflake towering). |
| `--min-rows N` | 1 | Drop points below N rows (the empty-table warm-up; log x can't show 0). |
| `--dpi N` | 300 | Output resolution. |

The raw series are noisy per-iteration (cache effects, MV-merge cost) — use `--smooth 7`
for a readable trend. Add `--no-raw` to drop the faint raw underlay and show the median only
(the committed smooth charts use this).

By default the x-axis is trimmed to the **common volume window** across systems (the
overlap of each system's `raw_rows` range), so a system that logged from a much lower
volume — e.g. ClickHouse from ~8M rows vs Snowflake/Databricks from ~600M — doesn't leave
the others with empty space. Within that window **every line is anchored to the same start/end
x** (its value at the window edges is interpolated from neighbouring samples), so all vendors
begin and end together rather than at their own first/last sample. Pass `--full-range` to show
each system's full extent instead (lines then start/end at their own data, un-anchored).

## Setup

No setup needed beyond [uv](https://docs.astral.sh/uv/) — each script carries its
dependencies in a PEP 723 inline metadata block, **pinned for reproducible renders**
(`matplotlib==3.10.9`, plus `numpy==2.4.6` for `render_query_latency.py`), and `uv run`
resolves them into a cached ephemeral env on first use. The folder is self-contained: no
shared/parent venv, copy it anywhere and `uv run generate.py` works. (To bump a pin later, edit
the `# dependencies = [...]` line in the script; uv re-resolves on the next run.)
The `Inter` font is optional — it falls back to DejaVu Sans (install via `brew install
font-inter` to match `_viz2` exactly).

## What the charts show (~100B rows; CH 64GiB ×1, SF Gen2 Small, DBX X-Small)

- **ClickHouse** is ~2–3 orders of magnitude faster on the dashboards (~5–50ms vs 1–30s) and
  stays roughly flat on the drilldown (~1s from 10M → 100B rows). It also logged from a much
  lower starting volume, so its line spans the full x-range.
- **Dashboard, full-MV-scan queries** (Top movers, Daily activity): Snowflake climbs to
  ~10–30s as volume grows; Databricks stays roughly flat ~1s; ClickHouse ~10–50ms.
- **Dashboard, sym-filtered queries** (Single-symbol, Watchlist): Snowflake/Databricks both
  ~1s (clustering on `sym` helps); ClickHouse ~5–50ms.
- **Drilldown** (raw table, single symbol): ClickHouse ~1s flat; Snowflake climbs ~1s→~10s;
  Databricks noisier, ~10–20s.

> Caveat: these are each system's own run, not a controlled A/B — tiers differ (64GiB ×1 vs
> X-Small) and the Snowflake series is from its *clustered* run. The chart compares what each
> system actually did.
