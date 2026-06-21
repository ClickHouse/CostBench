# Clustering experiment — STOCKHOUSE_2 re-run (2026-06-12)

A clean-schema clustered re-run whose purpose was to capture the **clustering lag over time**
(the original `STOCKHOUSE` run never sampled it live) and to confirm the headline numbers
reproduce. Raw `QUOTES` clustered `(sym, t)`, MV `QUOTES_DAILY` clustered `(sym, day)`, sustained
~1M EPS into a fresh `BENCH2COST.STOCKHOUSE_2`. Auto-stopped at 30.6h. Chart:
`../../_viz/_out/clustering_lag.png` (`render_clustering_lag.py`).

## Clustering lag is a ramp on the raw table, a sawtooth on the MV

Sampled `SYSTEM$CLUSTERING_INFORMATION.average_depth` every 300s (`ops/clustering_lag.sh`).
Depth ~1 is ideal; higher = more overlapping partitions per key = worse pruning (read
amplification). It is **point-in-time** — Snowflake never historizes it, so it can only be
captured live during the run.

| object | depth trajectory over the 30h |
|---|---|
| raw `QUOTES` (sym,t) | **monotonic ramp 0 → ~2,140** (peak 2,277), never recovers during ingest |
| MV `QUOTES_DAILY` (sym,day) | **sawtooth**: climbs ~2 → ~90, AC fully reclusters the small MV, crashes back to ~2, repeats (~3 cycles) |

Time-ordered ingest (`t` monotonic) means every new partition spans all `sym`s, maximally
disordering the `sym`-leading key. AC can fully recluster the *small* MV periodically (sawtooth),
but on the *large* raw table it can only tread water — depth climbs in lockstep with partition
count (~5% of partitions) and only collapses **after** ingest stops.

## Automatic Clustering cost more than the ingest

From `ACCOUNT_USAGE.AUTOMATIC_CLUSTERING_HISTORY` (`STOCKHOUSE_2`, settled):

| object | credits | rows reclustered | shape |
|---|---|---|---|
| raw `QUOTES` | **50.28** | 240B (≈2.2× the table) | steady ~0.69 cr/hr during ingest, then a **~28-credit catch-up spike** in the hour after ingest stopped |
| MV `QUOTES_DAILY` | 0.02 | 96M | sporadic, negligible |

Ingest itself on X-Small Gen2 ≈ 1.35 cr/hr × 30.6h ≈ **~41 credits**. So **Automatic Clustering
(50.3 cr) cost more than the ingest (~41 cr)** — and ~30 of those 50 were the post-ingest
catch-up burst. AC was *active the whole time* (not idle); it is rate-limited during ingest and
only sprints once the table stabilizes. MV clustering was effectively free — and, per the first
run's pruning history, bought ~no pruning anyway.

## Reproducibility vs the original STOCKHOUSE run

The re-run confirms the original numbers (both ~1M EPS, ~105–109B rows).

| metric | STOCKHOUSE (orig) | STOCKHOUSE_2 (new) | |
|---|---|---|---|
| avg ingest EPS | 990,187/s | 991,228/s | ✅ |
| rows | 105.5B (29.6h) | 109.2B (30.6h) | ✅ |
| MV `behind_by` median / p90 / max | 31.9 / 56.9 / 71.9 min | 32.5 / 57.2 / 72.1 min | ✅ ~identical |
| Dashboard Q3 top-movers (median) | 9.51s | 9.71s | ✅ ~identical |
| Dashboard Q4 daily (median) | 16.58s | 16.81s | ✅ ~identical |
| Dashboard Q1 single-sym (median) | 1.31s | 0.82s | ~ clustering-dependent |
| Dashboard Q2 watchlist (median) | 1.31s | 1.13s | ~ |
| Drilldown vs raw (median) | 6.57s | 5.08s | ~ clustering-dependent |

- The **full-MV-scan dashboards (Q3/Q4)** — which scale with volume and dominate — match within
  **~1–2%**, as do **MV lag** and **ingest EPS**. The benchmark is reproducible.
- Differences are confined to the **clustering-sensitive queries** (sym-filtered Q1/Q2,
  drilldown), expected since the two runs had different clustering histories. Not a controlled
  A/B for those.
- Compare **medians**, not single end-of-run samples — the latter are noisy (one iteration).

## Method note for next time

The scheduled stop killed `clustering_lag.sh` at the same instant as ingest, but AC's depth-
collapsing catch-up ran in the *following* hour — so the depth series captures the **ramp but not
the recovery** (the recovery is only visible in the AC credits spike). Next run: keep
`clustering_lag.sh` running ~2h **after** stopping ingest to capture the depth collapse.

Data: `../results_stockhouse2_2026-06-12/` (clustering_lag, mv_latency, dashboard, drilldown
JSONL) + the AC-credits CSV staged at `../../_viz/_test/clustering_credits_snowflake.csv`.
