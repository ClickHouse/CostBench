#!/usr/bin/env python3
"""
Remote-read percentage per query, the metric Snowflake's interactive-table docs point at:

  "Because cache warming depends on your queries, the best way to monitor whether the cache is
   warm is to review the remote read percentage in Snowsight Query Profile. For programmatic
   access to query operator statistics, see GET_QUERY_OPERATOR_STATS. In ideal execution
   scenarios, low-latency queries should have a remote read percentage of 0%."

NOTE this is NOT `percentage_scanned_from_cache` from QUERY_HISTORY. That is a share of BYTES
served from the local cache. The docs' "remote read percentage" is the Query Profile's
"Remote disk I/O" — a share of execution TIME. A query can scan 40% of its bytes remotely and
still spend ~0% of its time on remote I/O if the fetch overlaps compute. Only the time-based
figure is what the 0% target refers to, and only GET_QUERY_OPERATOR_STATS exposes it.

Query-level remote read % = PLAIN SUM over operators of
    execution_time_breakdown:remote_disk_io
The sub-values are ALREADY fractions of total query time, so weighting by overall_percentage
double-discounts them. Verified against the UI on 01c5dcb8-0003-a84a-0003-a22e01126496:
plain sum = 56.5% (matches the profile pane), weighted = 34.9% (wrong).

Run ON THE BOX (needs SF creds):
    cd ~/bench && source .sfenv && source .venv/bin/activate
    python ops/remote_read_check.py --arm t2_drilldown

GET_QUERY_OPERATOR_STATS only covers COMPLETED warehouse queries from the last 14 DAYS, and a
query killed at a statement timeout has no operator tree at all — so timed-out queries are
skipped and counted separately, not silently dropped.
"""
import argparse
import json
import os
import statistics as st
import sys

import snowflake.connector as sc

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import runner_common as rc

# The RUN8 arms that ran on the INTERACTIVE warehouse — the ones where the local-cache question
# actually applies. Windows and hashes from analysis/extract_query_history.sql.
ARMS = {
    "t2_drilldown": dict(
        wh="SNOWPIPES_IT_READ_SMALL", schema="STOCKHOUSE_T2_RUN8",
        hashes=["81bfa7bfde17377a3c0dd2f00ea3883f", "98a25b7faf10ec93061165a1efbd79dd"],
        lo="2026-07-20 12:15:08", hi="2026-07-22 06:36:55",
        note="sym+time filtered, 0 timeouts -> unbiased sample"),
    "t2_dashboard_raw_iv": dict(
        wh="SNOWPIPES_IT_READ_SMALL", schema="STOCKHOUSE_T2_RUN8",
        hashes=["49f0fd2569c4079e89daa0b84f877f1d", "7c0f8300c30ea00f6fdf768e653b1045",
                "120292e60f2c28d5fd83f1c198caa296", "da2f485cec29a5f67e49a05ad1cfa1b7"],
        lo="2026-07-20 12:15:08", hi="2026-07-22 07:24:01",
        note="unfiltered scans of QUOTES_IT; 523/968 timed out -> SURVIVOR BIAS"),
    "t2_dashboard_imv_iv": dict(
        wh="SNOWPIPES_IT_READ_SMALL", schema="STOCKHOUSE_T2_RUN8",
        hashes=["4138da05ca5b8b3b12d91e3c60b451c2", "032fd86ef70df033fb74f37a3a6ff43a",
                "198a3ec3a02513cbd5e088271fcf95ba", "2adb24066ebed97154fb420445501a46"],
        lo="2026-07-20 12:15:08", hi="2026-07-22 07:25:59",
        note="unfiltered scans of the IMV; 763/956 timed out -> SURVIVOR BIAS"),
}


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--arm", choices=sorted(ARMS), default="t2_drilldown")
    ap.add_argument("--limit", type=int, default=0, help="sample only the first N queries (0 = all)")
    ap.add_argument("--out", default=None, help="write per-query rows to this JSONL")
    args = ap.parse_args()
    arm = ARMS[args.arm]

    con = sc.connect(account=os.environ["SF_ACCOUNT"], user=os.environ["SF_USER"],
                     private_key=rc._pkb(), database="BENCH2COST", login_timeout=30)
    cur = con.cursor()
    cur.execute(f"USE WAREHOUSE {os.environ.get('SF_TRACK_WAREHOUSE', 'BENCH')}")
    cur.execute("ALTER SESSION SET TIMEZONE = 'UTC'")

    hashes = ", ".join(f"'{h}'" for h in arm["hashes"])
    cur.execute(f"""
        SELECT query_id, execution_status, error_code, total_elapsed_time, execution_time,
               bytes_scanned, percentage_scanned_from_cache, start_time
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
        WHERE warehouse_name = '{arm["wh"]}' AND schema_name = '{arm["schema"]}'
          AND query_hash IN ({hashes})
          AND start_time >= '{arm["lo"]} +0000'::TIMESTAMP_TZ
          AND start_time <  '{arm["hi"]} +0000'::TIMESTAMP_TZ
        ORDER BY start_time
    """)
    qs = cur.fetchall()
    print(f"arm {args.arm}: {len(qs)} queries in window   ({arm['note']})")

    todo = [q for q in qs if q[1] == "SUCCESS"]
    skipped = len(qs) - len(todo)
    if args.limit:
        todo = todo[:args.limit]
    print(f"  {len(todo)} SUCCESS to inspect, {skipped} skipped (no operator tree on a killed query)\n")

    rows, unavailable = [], 0
    for i, (qid, _st, _ec, tot, ex, bs, cache, t0) in enumerate(todo, 1):
        try:
            cur.execute(f"SELECT operator_type, execution_time_breakdown, operator_statistics "
                        f"FROM TABLE(GET_QUERY_OPERATOR_STATS('{qid}'))")
            ops = cur.fetchall()
        except Exception:
            unavailable += 1
            continue
        if not ops:
            unavailable += 1
            continue
        rr = scan_rr = 0.0
        for otype, etb, ostat in ops:
            e = json.loads(etb) if isinstance(etb, str) else (etb or {})
            share = float(e.get("overall_percentage") or 0)
            remote = float(e.get("remote_disk_io") or 0)
            rr += remote
            if otype and "TableScan" in otype:
                scan_rr = max(scan_rr, remote)
        rows.append(dict(query_id=qid, start_time=str(t0), remote_read_pct=100 * rr,
                         tablescan_remote_pct=100 * scan_rr,
                         elapsed_ms=tot, exec_ms=ex, bytes_scanned=bs,
                         pct_from_cache=float(cache or 0)))
        if i % 25 == 0:
            print(f"    … {i}/{len(todo)}")

    if not rows:
        print("no operator stats available (older than 14 days?)")
        return

    rr = [r["remote_read_pct"] for r in rows]
    print(f"\n=== remote read % (share of execution TIME, the docs' metric; target 0%) ===")
    print(f"  n={len(rows)}   at exactly 0%: {sum(1 for v in rr if v == 0)}"
          f"   under 1%: {sum(1 for v in rr if v < 1)}   under 5%: {sum(1 for v in rr if v < 5)}")
    print(f"  min {min(rr):.2f}%   median {st.median(rr):.2f}%   p95 {sorted(rr)[int(.95*(len(rr)-1))]:.2f}%"
          f"   max {max(rr):.2f}%")
    if unavailable:
        print(f"  ({unavailable} queries had no operator stats and are excluded)")

    cb = [r["pct_from_cache"] for r in rows]
    print(f"\n=== for contrast, bytes-based percentage_scanned_from_cache ===")
    print(f"  median {100*st.median(cb):.1f}% of bytes from local cache "
          f"(=> {100*(1-st.median(cb)):.1f}% of BYTES read remotely)")
    print("  the two answer different questions: bytes read remotely vs time spent waiting on it")

    if args.out:
        with open(args.out, "w") as f:
            for r in rows:
                f.write(json.dumps(r) + "\n")
        print(f"\nwrote {args.out}")
    cur.close()
    con.close()


if __name__ == "__main__":
    main()
