#!/usr/bin/env python3
# /// script
# requires-python = ">=3.10"
# ///
"""
Build normalized per-vendor cost timelines for the cost-over-time chart.

Reads each vendor's cost JSONs (ingest totals + per-tier unit price) and the captured
system-table detail files (per-period clustering / MV-refresh usage), and writes
_test/cost_timeline_<vendor>.json with, per component, a list of (elapsed_hours, usd_increment)
events plus the flat ingest rate. Elapsed time is measured from each vendor's earliest
clustering/refresh timestamp (≈ start of ingest activity) so the three runs overlay on a
common virtual axis. The renderer (render_cost_over_time.py) consumes these.

Component cost = period_units × unit_price, where unit_price = (component total cost at the
chosen tier) / (component total units) — so each curve's endpoint matches the ingest-cost bar.

ClickHouse has only the ingest component (it sorts + rolls up at ingest).
"""
import re
import sys
import json
import math
from pathlib import Path
from datetime import datetime, timedelta

HERE = Path(__file__).resolve().parent
TEST = HERE / "_test"
QUOTES = HERE.parent

TIER = {"snowflake": "enterprise", "databricks": "premium", "clickhouse": "enterprise"}
SYSTEMS = {"clickhouse": "ClickHouse", "snowflake": "Snowflake", "databricks": "Databricks"}

SF_SYS = "snowflake/results/system_tables"
DBX_SYS = "databricks/results/system_tables"


def parse_ts(s):
    """Handle Snowflake '2026-06-10 16:00:00.000 -0700' and Databricks
    '2026-06-11T08:36:16.159+00:00' (and trailing Z)."""
    s = s.strip().replace("Z", "+00:00")
    m = re.search(r"\s*([+-]\d{2}):?(\d{2})$", s)   # split off tz offset (maybe space-sep)
    tz = ""
    if m:
        tz = f"{m.group(1)}:{m.group(2)}"
        s = s[:m.start()].strip()
    return datetime.fromisoformat(s.replace(" ", "T") + tz)


def read_md_table(path):
    """Parse the first markdown table in a file -> list of {header: cell} dicts."""
    rows, header = [], None
    for line in open(path):
        line = line.strip()
        if not line.startswith("|"):
            continue
        cells = [c.strip() for c in line.strip("|").split("|")]
        if all(set(c) <= set("-: ") for c in cells):   # separator row
            continue
        if header is None:
            header = [c.lower() for c in cells]
            continue
        rows.append(dict(zip(header, cells)))
    return rows


def cost_entry(jpath, tier):
    d = json.load(open(jpath))
    costs = d.get("costs", [])
    for c in costs:
        if tier.lower() in str(c.get("tier", "")).lower():
            return c
    return min(costs, key=lambda c: c.get("total_cost_usd", c.get("total_compute_cost_usd", 0)))


def unit_price(jpath, tier, units_key):
    """USD per credit/DBU = component total cost / total units (so totals match the bar)."""
    d = json.load(open(jpath))
    c = cost_entry(jpath, tier)
    usd = c.get("total_cost_usd", c.get("total_compute_cost_usd"))
    units = d.get(units_key) or c.get(units_key)
    return usd / units if units else 0.0


def sf_events(detail_md, jpath, tier):
    """Snowflake clustering/refresh: hourly rows with END_TIME + CREDITS_USED."""
    up = unit_price(jpath, tier, "total_credits")
    ev = []
    for r in read_md_table(QUOTES / detail_md):
        cr = r.get("credits_used")
        if cr in (None, "", "null"):
            continue
        ev.append((parse_ts(r["end_time"]), float(cr) * up))
    return ev


def dbx_clustering_events(detail_md, jpath, tier):
    """Databricks clustering: per-operation rows with end_time + usage_quantity (DBU)."""
    up = unit_price(jpath, tier, "total_dbu")
    ev = []
    for r in read_md_table(QUOTES / detail_md):
        q = r.get("usage_quantity")
        if q in (None, "", "null"):
            continue
        ev.append((parse_ts(r["end_time"]), float(q) * up))
    return ev


def dbx_refresh_events(detail_md, jpath, tier, t0, t_max):
    """Databricks MV refresh: per-DAY dbus. Spread each day's cost evenly across the hours of
    that day that fall within [t0, t_max] (no intra-day timing is captured)."""
    up = unit_price(jpath, tier, "total_dbu")
    ev = []
    for r in read_md_table(QUOTES / detail_md):
        d = r.get("dbus")
        if d in (None, "", "null"):
            continue
        usd = float(d) * up
        day = datetime.fromisoformat(r["usage_date"].strip() + "T00:00:00+00:00")
        lo = max(t0, day)
        hi = min(t_max, day + timedelta(days=1))
        if hi <= lo:
            ev.append((max(lo, t0), usd))     # window fell outside the run; pin at the edge
            continue
        n = max(1, math.ceil((hi - lo).total_seconds() / 3600))
        for i in range(n):
            ev.append((lo + timedelta(hours=i + 1), usd / n))
    return ev


def benchmark_t0(vendor):
    """Measured benchmark start = first dashboard-runner iteration_started_at (the runner
    starts right after ingest begins). Used to anchor the timeline and drop pre-benchmark
    system-table ops (e.g. Databricks ANALYZE/COMPACTION from earlier table setup)."""
    p = TEST / f"dashboard_{vendor}.jsonl"
    ts = []
    for line in open(p):
        line = line.strip()
        if line:
            s = json.loads(line).get("iteration_started_at")
            if s:
                ts.append(parse_ts(s))
    return min(ts) if ts else None


def elapsed(ev, t0):
    """(elapsed_hours, usd) for events at/after t0; pre-t0 events dropped."""
    return sorted(((t - t0).total_seconds() / 3600.0, usd) for t, usd in ev if t >= t0)


def build(vendor):
    tier = TIER[vendor]
    ingest = cost_entry(TEST / f"cost_ingest_{vendor}.json", tier)
    ing_d = json.load(open(TEST / f"cost_ingest_{vendor}.json"))
    ingest_usd = ingest.get("total_compute_cost_usd", ingest.get("total_cost_usd"))
    ingest_hours = ing_d.get("duration_hours", 27)

    out = {"system": SYSTEMS[vendor], "tier": ingest.get("tier"),
           "components": {"Ingest": {"type": "flat", "total_usd": round(ingest_usd, 4),
                                     "hours": ingest_hours}}}

    if vendor == "clickhouse":
        out["max_elapsed_h"] = ingest_hours
        return out

    t0 = benchmark_t0(vendor)   # measured ingest start; anchors the timeline

    if vendor == "snowflake":
        clu = sf_events(f"{SF_SYS}/clustering-raw_table-details.md",
                        TEST / "cost_clustering_raw_snowflake.json", tier)
        clu += sf_events(f"{SF_SYS}/clustering-mv_table-details.md",
                         TEST / "cost_clustering_mv_snowflake.json", tier)
        ref = sf_events(f"{SF_SYS}/refresh-mv_table-details.md",
                        TEST / "cost_mv_refresh_snowflake.json", tier)
        out["components"]["Clustering"] = {"type": "events", "events": elapsed(clu, t0)}
        out["components"]["MV refresh"] = {"type": "events", "events": elapsed(ref, t0)}
    else:  # databricks
        clu = dbx_clustering_events(f"{DBX_SYS}/clustering-raw_table-details.md",
                                    TEST / "cost_clustering_raw_databricks.json", tier)
        clu += dbx_clustering_events(f"{DBX_SYS}/clustering-mv_table-details.md",
                                     TEST / "cost_clustering_mv_databricks.json", tier)
        t_max = max(t for t, _ in clu if t >= t0)
        ref = dbx_refresh_events(f"{DBX_SYS}/refresh-mv_table-details.md",
                                 TEST / "cost_mv_refresh_databricks.json", tier, t0, t_max)
        out["components"]["Clustering"] = {"type": "events", "events": elapsed(clu, t0)}
        out["components"]["MV refresh"] = {"type": "events", "events": elapsed(ref, t0)}

    ev_max = max((c["events"][-1][0] for c in out["components"].values()
                  if c["type"] == "events" and c["events"]), default=ingest_hours)
    out["t0"] = t0.isoformat()
    out["max_elapsed_h"] = round(max(ev_max, ingest_hours), 2)
    return out


def main():
    for vendor in SYSTEMS:
        tl = build(vendor)
        (TEST / f"cost_timeline_{vendor}.json").write_text(json.dumps(tl, indent=2))
        comps = tl["components"]
        print(f"\n{tl['system']}  (tier={tl['tier']}, span={tl.get('max_elapsed_h')}h)")
        ing = comps["Ingest"]
        print(f"  Ingest      flat  ${ing['total_usd']:>10,.2f}  over {ing['hours']}h")
        for name in ("Clustering", "MV refresh"):
            if name in comps:
                ev = comps[name]["events"]
                tot = sum(u for _, u in ev)
                last = ev[-1][0] if ev else 0
                after = sum(u for h, u in ev if h > ing["hours"])
                print(f"  {name:11} {len(ev):>3} ev  ${tot:>10,.2f}  last@{last:5.1f}h"
                      f"  (${after:,.2f} after {ing['hours']}h)")
        total = ing["total_usd"] + sum(sum(u for _, u in comps[n]["events"])
                                       for n in ("Clustering", "MV refresh") if n in comps)
        print(f"  TOTAL                  ${total:>10,.2f}")


if __name__ == "__main__":
    main()
