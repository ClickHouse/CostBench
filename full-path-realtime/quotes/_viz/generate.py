#!/usr/bin/env python3
# /// script
# requires-python = ">=3.10"
# dependencies = ["matplotlib==3.10.9", "numpy==2.4.6"]   # for the renderers it spawns (run under this interpreter)
# ///
"""
Generate all quotes-benchmark charts, optionally filtered to a subset of vendors.

This is the single entry point — it drives the individual renderers
(render_query_latency.py, render_latency.py, render_mv_lag.py) with the right input files
for the vendors you select, so you don't hand-assemble file lists.

Examples:
  python3 generate.py                                   # all vendors, all charts
  python3 generate.py --vendors clickhouse snowflake    # only those two
  python3 generate.py --charts query_latency mv_lag     # only those charts
  python3 generate.py --list                            # list chart names and exit

Inputs live in _test/ as <workload>_<vendor>.{jsonl,csv} (e.g. dashboard_clickhouse.jsonl,
mv_lag_databricks.csv). Outputs land in _out/. Run with uv from this directory — deps
(matplotlib/numpy) come from the inline script metadata above, so no venv setup is needed:
  uv run generate.py
The spawned renderers run under this same interpreter, so they inherit the resolved env.
"""
import sys
import json
import tempfile
import argparse
import subprocess
from pathlib import Path

HERE = Path(__file__).resolve().parent
TEST = HERE / "_test"
PY = sys.executable

ALL_VENDORS = ["clickhouse", "snowflake", "databricks"]
DASH_LABELS = "Single-symbol summary;Watchlist summary;Top movers;Daily activity"
DRILL_LABELS = "Symbol drilldown (raw)"

# Vendors whose materialized view actually produces a lag signal (ClickHouse is the always-0
# baseline; it has no lag file). File name per vendor:
LAG_FILE = {"snowflake": "mv_latency_snowflake.jsonl", "databricks": "mv_lag_databricks.csv"}


def _existing(paths):
    return [str(p) for p in paths if Path(p).exists()]


def _dash(vs):
    return _existing(TEST / f"dashboard_{v}.jsonl" for v in vs)


def _drill(vs):
    return _existing(TEST / f"drilldown_{v}.jsonl" for v in vs)


# Standalone Interactive-Table comparison (ClickHouse vs Snowflake IT). Fixed inputs — these
# charts always pit ClickHouse against the IT run, independent of --vendors.
IT_DASH = [TEST / "dashboard_clickhouse.jsonl", TEST / "dashboard_snowflake_it.jsonl"]
IT_DRILL = [TEST / "drilldown_clickhouse.jsonl", TEST / "drilldown_snowflake_it.jsonl"]
IT_REFRESH = TEST / "it_refresh_snowflake_it.csv"   # INTERACTIVE_TABLE_REFRESH_HISTORY dump
IT_VOL = TEST / "dashboard_snowflake_it.jsonl"      # IT run's raw_rows = right-axis volume


def _cost_dash(vs):
    return _existing(TEST / f"cost_dashboard_{v}.json" for v in vs)


def _cost_drill(vs):
    return _existing(TEST / f"cost_drilldown_{v}.json" for v in vs)


def _lag(vs):
    return _existing(TEST / LAG_FILE[v] for v in vs if v in LAG_FILE)


def _cost_component(component, vs):
    """Existing cost_<component>_<vendor>.json files for the selected vendors."""
    return _existing(TEST / f"cost_{component}_{v}.json" for v in vs)


def _vtok(system):
    """Map a storage.json `system` string to its vendor token (clickhouse/snowflake/...)."""
    s = (system or "").lower()
    for v in ALL_VENDORS:
        if v in s:
            return v
    return s


# Each builder returns the renderer argv (list) for the chosen vendors, or None to skip
# (e.g. the required input vendors aren't selected). `out`/`dpi` come from the CLI.
def build(name, vs, out_dir, dpi):
    out = out_dir
    dpi_args = ["--dpi", str(dpi)] if dpi else []
    cluster_x = TEST / "dashboard_snowflake.jsonl"   # Snowflake volume source for MV-lag x/right-axis
    sf_in = "snowflake" in vs
    ch_in = "clickhouse" in vs

    if name == "clustering_lag":
        # Snowflake-specific (STOCKHOUSE_2 re-run): depth ramp + AC credits/hr.
        lagf = TEST / "clustering_lag_snowflake.jsonl"
        if not (sf_in and lagf.exists()):
            return None
        argv = ["render_clustering_lag.py", str(lagf)]
        credf = TEST / "clustering_credits_snowflake.csv"
        if credf.exists():
            argv += ["--credits", str(credf)]
        return argv + ["--stop-hours", "30.6", "--depth-scale", "linear",
                       "--credits-scale", "linear", "--raw-only",
                       "--out", str(out / "clustering_lag.png"), *dpi_args,
                       "--title", "Snowflake Automatic-Clustering lag, raw table (STOCKHOUSE_2, ~1M EPS, 109B rows)"]

    if name == "storage":
        sj = TEST / "storage.json"
        if not sj.exists():
            return None
        if set(vs) == set(ALL_VENDORS):
            data_path = str(sj)
        else:  # filter raw/mv arrays to the selected vendors, write a temp JSON
            d = json.load(open(sj))
            for key in ("raw", "mv"):
                d[key] = [r for r in d.get(key, []) if _vtok(r.get("system", "")) in vs]
            if not d.get("raw") and not d.get("mv"):
                return None
            fd, data_path = tempfile.mkstemp(prefix="storage_filtered_", suffix=".json")
            with open(fd, "w") as f:
                json.dump(d, f)
        return ["render_storage.py", data_path, "--out", str(out / "storage.png"), *dpi_args,
                "--title", "Storage size — raw table vs MV (active, compressed on disk)"]

    if name == "query_latency":
        d, r = _dash(vs), _drill(vs)
        if not d and not r:
            return None
        argv = ["render_query_latency.py", "--agg", "p99"]
        if d:
            argv += ["--workload", "Dashboard (vs MV)=" + ",".join(d)]
        if r:
            argv += ["--workload", "Drilldown (vs raw)=" + ",".join(r)]
        return argv + ["--out", str(out / "query_latency.png"), *dpi_args,
                       "--title", "p99 query latency by workload (~100B rows, ~1M EPS ingest)"]

    # Query cost: one chart per workload (dashboard / drilldown), in log and linear variants.
    QCOST = {
        "query_cost_dashboard":        ("Dashboard (vs MV)",  _cost_dash, False),
        "query_cost_drilldown":        ("Drilldown (vs raw)", _cost_drill, False),
        "query_cost_dashboard_linear": ("Dashboard (vs MV)",  _cost_dash, True),
        "query_cost_drilldown_linear": ("Drilldown (vs raw)", _cost_drill, True),
    }
    if name in QCOST:
        label, getter, linear = QCOST[name]
        files = getter(vs)
        if not files:
            return None
        argv = ["render_query_cost.py", "--tier", "enterprise",
                "--workload", f"{label}=" + ",".join(files)]
        if linear:
            argv += ["--yscale", "linear"]
        short = label.split(" (")[0]   # "Dashboard" / "Drilldown"
        title = f"{short} query cost — enterprise tier" + (" — linear" if linear else "")
        return argv + ["--out", str(out / f"{name}.png"), *dpi_args, "--title", title]

    if name == "dashboard":
        d = _dash(vs)
        return None if not d else \
            ["render_latency.py", *d, "--out", str(out / "dashboard.png"), *dpi_args,
             "--title", "Dashboard query latency vs data volume (vs MV, ~1M EPS)",
             "--query-labels", DASH_LABELS]

    if name == "dashboard_smooth":
        d = _dash(vs)
        return None if not d else \
            ["render_latency.py", *d, "--out", str(out / "dashboard_smooth.png"),
             "--smooth", "7", "--no-raw", *dpi_args,
             "--title", "Dashboard query latency vs volume (vs MV) — 7-pt median",
             "--query-labels", DASH_LABELS]

    if name == "dashboard_smooth_linear":
        d = _dash(vs)
        return None if not d else \
            ["render_latency.py", *d, "--out", str(out / "dashboard_smooth_linear.png"),
             "--smooth", "7", "--no-raw", "--yscale", "linear", *dpi_args,
             "--title", "Dashboard query latency vs volume (vs MV) — 7-pt median, linear y",
             "--query-labels", DASH_LABELS]

    if name == "drilldown":
        r = _drill(vs)
        return None if not r else \
            ["render_latency.py", *r, "--out", str(out / "drilldown.png"), *dpi_args,
             "--title", "Drilldown query latency vs data volume (vs raw table, ~1M EPS)",
             "--query-labels", DRILL_LABELS]

    if name == "drilldown_smooth":
        r = _drill(vs)
        return None if not r else \
            ["render_latency.py", *r, "--out", str(out / "drilldown_smooth.png"),
             "--smooth", "5", "--no-raw", *dpi_args,
             "--title", "Drilldown query latency vs volume (vs raw) — 5-pt median",
             "--query-labels", DRILL_LABELS]

    if name == "drilldown_smooth_linear":
        r = _drill(vs)
        return None if not r else \
            ["render_latency.py", *r, "--out", str(out / "drilldown_smooth_linear.png"),
             "--smooth", "5", "--no-raw", "--yscale", "linear", *dpi_args,
             "--title", "Drilldown query latency vs volume (vs raw) — 5-pt median, linear y",
             "--query-labels", DRILL_LABELS]

    if name == "mv_lag":
        # Snowflake serverless-MV lag vs base-table row count (x interpolated from SF volume).
        # SF-specific (one volume file); ClickHouse is the flat-0 baseline.
        if not sf_in:
            return None
        argv = ["render_mv_lag.py", str(TEST / LAG_FILE["snowflake"]),
                "--volume-from", str(cluster_x),
                "--out", str(out / "mv_lag.png"), "--smooth", "9", "--no-raw", *dpi_args,
                "--title", "MV freshness lag vs data volume: Snowflake vs ClickHouse (~1M EPS)"]
        if not ch_in:
            argv.append("--no-baseline")
        return argv

    if name == "mv_lag_time_volume":
        # All systems over the first 24h; right axis = Snowflake rows ingested.
        lag = _lag(vs)
        if not lag:
            return None
        argv = ["render_mv_lag.py", *lag]
        if sf_in:  # right-axis volume line is Snowflake's
            argv += ["--volume-line", str(cluster_x)]
        argv += ["--xmax", "24", "--out", str(out / "mv_lag_time_volume.png"),
                 "--smooth", "9", "--no-raw", *dpi_args,
                 "--title", "MV freshness lag over first 24h, with data volume (~1M EPS)"]
        if not ch_in:
            argv.append("--no-baseline")
        return argv

    if name == "ingest_cost":
        # Stacked composition: ingest + clustering (raw + MV merged) + MV refresh — same
        # components/colours as cost_over_time. ClickHouse has only ingest (its single bar is
        # "everything"; sort + rollup happen at ingest).
        comps = [("Ingest", ["ingest"]),
                 ("Clustering", ["clustering_raw", "clustering_mv"]),
                 ("MV refresh", ["mv_refresh"])]
        argv = ["render_ingest_cost.py", "--tier", "enterprise"]
        any_data = False
        for label, keys in comps:
            files = [f for k in keys for f in _cost_component(k, vs)]
            if files:
                any_data = True
                argv += ["--component", f"{label}=" + ",".join(files)]
        if not any_data:
            return None
        return argv + ["--out", str(out / "ingest_cost.png"), *dpi_args,
                       "--title", "Ingest cost composition "
                                  "(enterprise tier, ~100B rows @ ~1M EPS, 27h)"]

    if name == "cost_over_time":
        # Cumulative cost over time, stacked by component (ingest / clustering / MV refresh).
        # Needs the normalized cost_timeline_<vendor>.json (built by build_cost_timeline.py,
        # run as a prep step in main()). ClickHouse has only the ingest layer.
        files = _existing(TEST / f"cost_timeline_{v}.json" for v in vs)
        if not files:
            return None
        return ["render_cost_over_time.py", *files,
                "--out", str(out / "cost_over_time.png"), *dpi_args,
                "--title", "Cumulative ingest + prep cost over time "
                           "(enterprise tier, ~100B rows)"]

    # --- Standalone Interactive-Table charts: ClickHouse vs "Snowflake IT" --------------------
    # Distinct purple/pink "Snowflake IT" labels keep these separable from a later MV comparison.
    # They ignore --vendors (always CH-vs-IT) and are skipped if their inputs aren't staged.
    if name == "it_query_latency":
        d, r = _existing(IT_DASH), _existing(IT_DRILL)
        if not d and not r:
            return None
        argv = ["render_query_latency.py", "--agg", "median"]
        if d:
            argv += ["--workload", "Dashboard (vs aggregate)=" + ",".join(d)]
        if r:
            argv += ["--workload", "Drilldown (vs raw)=" + ",".join(r)]
        return argv + ["--out", str(out / "it_query_latency.png"), *dpi_args,
                       "--title", "Query latency: ClickHouse vs Snowflake Interactive Tables (~100B rows)"]

    if name == "it_dashboard_smooth":
        d = _existing(IT_DASH)
        return None if not d else \
            ["render_latency.py", *d, "--out", str(out / "it_dashboard_smooth.png"),
             "--smooth", "7", "--no-raw", "--yscale", "linear", *dpi_args,
             "--title", "Dashboard latency vs volume: ClickHouse vs Snowflake IT — 7-pt median (linear)",
             "--query-labels", DASH_LABELS]

    if name == "it_drilldown_smooth":
        r = _existing(IT_DRILL)
        return None if not r else \
            ["render_latency.py", *r, "--out", str(out / "it_drilldown_smooth.png"),
             "--smooth", "5", "--no-raw", "--yscale", "linear", *dpi_args,
             "--title", "Drilldown latency vs volume: ClickHouse vs Snowflake IT — 5-pt median (linear)",
             "--query-labels", DRILL_LABELS]

    if name == "it_lag":
        # IT refresh freshness lag over the first 24h (staleness_at_done per IT), mirroring the
        # MV-lag chart: ClickHouse = flat-0 baseline, right axis = rows ingested.
        if not IT_REFRESH.exists():
            return None
        argv = ["render_mv_lag.py", str(IT_REFRESH)]
        if IT_VOL.exists():
            argv += ["--volume-line", str(IT_VOL)]
        return argv + ["--xmax", "24", "--smooth", "7", "--no-raw", *dpi_args,
                       "--out", str(out / "it_lag_time_volume.png"),
                       "--ylabel", "IT freshness lag behind base table (minutes)\n↓ lower is fresher",
                       "--title", "Interactive-table freshness lag under ~1M EPS ingest"]

    raise SystemExit(f"unknown chart: {name}")


CHARTS = [
    "query_latency",
    "query_cost_dashboard", "query_cost_drilldown",
    "query_cost_dashboard_linear", "query_cost_drilldown_linear",
    "ingest_cost",
    "cost_over_time",
    "dashboard", "dashboard_smooth", "dashboard_smooth_linear",
    "drilldown", "drilldown_smooth", "drilldown_smooth_linear",
    "mv_lag", "mv_lag_time_volume",
    "storage",
    "clustering_lag",
    # Standalone ClickHouse-vs-Snowflake-IT comparison (fixed CH+IT inputs, ignore --vendors).
    "it_query_latency", "it_dashboard_smooth", "it_drilldown_smooth", "it_lag",
]


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--vendors", nargs="+", choices=ALL_VENDORS, default=ALL_VENDORS,
                    metavar="V", help=f"Vendors to include (default: all). Choices: {ALL_VENDORS}.")
    ap.add_argument("--charts", nargs="+", choices=CHARTS, default=CHARTS,
                    metavar="C", help="Charts to render (default: all). See --list.")
    ap.add_argument("--out-dir", default=str(HERE / "_out"), help="Output directory (default _out).")
    ap.add_argument("--dpi", type=int, default=None, help="Override render DPI.")
    ap.add_argument("--list", action="store_true", help="List chart names and exit.")
    args = ap.parse_args()

    if args.list:
        print("charts:", " ".join(CHARTS))
        print("vendors:", " ".join(ALL_VENDORS))
        return

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    print(f"vendors: {', '.join(args.vendors)}", file=sys.stderr)

    # cost_over_time consumes normalized timelines parsed from the captured system tables.
    if "cost_over_time" in args.charts:
        subprocess.run([PY, str(HERE / "build_cost_timeline.py")], cwd=HERE, check=True)

    rendered, skipped = 0, []
    for name in args.charts:
        argv = build(name, args.vendors, out_dir, args.dpi)
        if argv is None:
            skipped.append(name)
            print(f"  skip {name} (selected vendors lack the required input)", file=sys.stderr)
            continue
        subprocess.run([PY, str(HERE / argv[0]), *argv[1:]], cwd=HERE, check=True)
        rendered += 1

    print(f"\ndone: {rendered} chart(s) -> {out_dir}"
          + (f"; skipped: {', '.join(skipped)}" if skipped else ""), file=sys.stderr)


if __name__ == "__main__":
    main()
