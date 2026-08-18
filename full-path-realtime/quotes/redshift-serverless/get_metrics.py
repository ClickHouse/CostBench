#!/usr/bin/env python3
"""
Redshift Serverless COST + timing puller for the streaming benchmark — the piece the read-runner
and lag-monitor don't capture. Modeled on the prior query-side-only benchmark's get_metrics.sh
(query-side-only/redshift-serverless/.../get_metrics.sh), which reads SYS_SERVERLESS_USAGE.

Both source views are HISTORIZED (retained a few days), so unlike SYS_STREAM_SCAN_STATES this can be
pulled AFTER the run — give it the run's [--since, --until] window.

Reports, over the window as an optional post-run audit:
  * Workgroup usage: compute_seconds, charged_seconds, and max compute_capacity from
    SYS_SERVERLESS_USAGE. The published T2 query-cost pipeline does NOT depend on this pull: it
    consumes the committed hourly allocation embedded in the JSONL results and CSV evidence.
  * Query timing summary: count + median elapsed/compile/exec (µs->s) from SYS_QUERY_HISTORY,
               optionally filtered to the dashboard/drilldown reads by --query-like.

RUN IT ONCE PER WORKGROUP. Serverless bills RPU-seconds per workgroup, and T2 uses two:
  * WRITER  (cb-quotes-rt-wg)        streaming ingestion + quotes_typed + quotes_daily refreshes
  * READER  (cb-quotes-rt-reader-wg) dashboard + drilldown queries over the datashare
Point RS_HOST at each in turn; the two RPU numbers are separate published lines.

  Redshift T2 fresh-data path = declared writer capacity x producer uptime
                                + MSK broker-hours + MSK storage

Per-MV refresh counts/durations/incremental-vs-full are reported (mv_refreshes) but NOT per-MV
dollars: billing is aggregated per workgroup per minute while ingest and both refreshes overlap,
so the writer is represented by one shared capacity-time path, not decomposed per operation.

Auth/env: same as runner_redshift.py (RS_HOST/RS_PORT/RS_DB/RS_USER/RS_PASSWORD).
  python get_metrics.py --since '2026-08-10 14:00:00' --until '2026-08-10 18:00:00' --price 0.375
Requires: pip install redshift_connector
"""
import argparse
import json
import os
import sys

try:
    import redshift_connector
except ImportError:
    sys.exit("ERROR: pip install redshift_connector")

HOST = os.environ["RS_HOST"]; PORT = int(os.environ.get("RS_PORT", "5439"))
DB = os.environ.get("RS_DB", "quotes"); USER = os.environ["RS_USER"]; PASSWORD = os.environ["RS_PASSWORD"]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--since", required=True, help="window start, e.g. '2026-08-10 14:00:00' (UTC)")
    ap.add_argument("--until", default="", help="window end (default: now)")
    # Redshift Serverless price is region-specific — VERIFY for eu-west-2 before trusting the $.
    ap.add_argument("--price", type=float, default=0.467, help="$ per RPU-hour (eu-west-2)")
    ap.add_argument("--query-like", default="quotes_daily",
                    help="substring to filter the read queries in SYS_QUERY_HISTORY")
    ap.add_argument("--schema", default="public", help="schema holding quotes_raw / quotes_daily (MVs)")
    ap.add_argument("--storage-price", type=float, default=0.025,
                    help="$ per GB-month of Redshift Managed Storage (eu-west-2; evidence only)")
    # MSK cost is not in any SQL view — computed from the cluster spec x uptime. Defaults match our
    # cluster (3x kafka.m7g.xlarge, 500GB/broker). KEY: inter-broker (cross-AZ) REPLICATION is FREE on
    # MSK provisioned (AWS MSK FAQ) — only CLIENT cross-AZ (producer/consumer) is billed (~$0.01/GB,
    # tiny at ~30 MB/s). So broker-hours + storage dominate. VERIFY eu-west-2 prices before trusting $.
    ap.add_argument("--msk-hours", type=float, default=0.0, help="hours the MSK cluster was up (0 -> skip)")
    ap.add_argument("--msk-brokers", type=int, default=3)
    ap.add_argument("--msk-broker-price", type=float, default=0.47175,
                    help="$ per broker-hour (kafka.m7g.xlarge; eu-west-2)")
    ap.add_argument("--msk-storage-gb", type=float, default=1500.0, help="total MSK EBS GB (3x500)")
    ap.add_argument("--msk-storage-price", type=float, default=0.116, help="$ per GB-month MSK storage (eu-west-2)")
    ap.add_argument("--msk-xaz-gb", type=float, default=0.0,
                    help="client cross-AZ GB (producer+consumer) over the run; replication is free; 0 -> skip")
    ap.add_argument("--msk-xaz-price", type=float, default=0.01, help="$ per GB client cross-AZ transfer")
    ap.add_argument("--include-client-cross-az", action="store_true",
                    help="opt in to client cross-AZ; excluded by the CostBench comparison contract")
    args = ap.parse_args()
    until = args.until or "now"

    con = redshift_connector.connect(host=HOST, port=PORT, database=DB, user=USER, password=PASSWORD)
    con.autocommit = True
    cur = con.cursor()

    # ---- RPU cost from SYS_SERVERLESS_USAGE ----
    cur.execute(
        "SELECT COALESCE(SUM(compute_seconds),0), COALESCE(SUM(charged_seconds),0), "
        "       COALESCE(MAX(compute_capacity),0) "
        "FROM SYS_SERVERLESS_USAGE "
        "WHERE start_time >= %s AND end_time <= %s", (args.since, until))
    compute_s, charged_s, max_rpu = cur.fetchone()
    rpu_hours = float(charged_s) / 3600.0
    cost = rpu_hours * args.price
    usage = {
        "window": {"since": args.since, "until": until},
        "compute_seconds": float(compute_s),
        "charged_seconds": float(charged_s),
        "max_compute_capacity_rpu": int(max_rpu),
        "rpu_hours_charged": round(rpu_hours, 3),
        "price_per_rpu_hour": args.price,
        "compute_cost_usd": round(cost, 2),
    }

    # ---- read-query timing summary from SYS_QUERY_HISTORY (µs -> s) ----
    # This is a NICE-TO-HAVE (the runners already record per-query timings): wrapped so a failure
    # here can never lose the cost numbers above.
    # Two Redshift gotchas encoded below:
    #   * `query_text ILIKE %s` with a '%...%' parameter fails to bind — the wildcards collide with
    #     the connector's paramstyle. POSITION() does the same job with a wildcard-free parameter.
    #   * MEDIAN() is an ordered-set aggregate; several of them over DIFFERENT columns in one SELECT
    #     is rejected with "within group ORDER BY clauses for aggregate functions must be the same".
    #     So each median is its own statement.
    queries = {"matched_query_like": args.query_like}
    try:
        where = ("FROM SYS_QUERY_HISTORY "
                 "WHERE query_type = 'SELECT' AND start_time >= %s AND start_time <= %s "
                 "  AND POSITION(LOWER(%s) IN LOWER(query_text)) > 0")
        params = (args.since, until, args.query_like)
        cur.execute(f"SELECT COUNT(*), MAX(elapsed_time)/1e6 {where}", params)
        n, max_elapsed = cur.fetchone()
        queries["count"] = int(n or 0)
        queries["max_elapsed_s"] = round(float(max_elapsed), 3) if max_elapsed is not None else None
        for key, col in (("median_elapsed_s", "elapsed_time"),
                         ("median_compile_s", "compile_time"),
                         ("median_execution_s", "execution_time")):
            cur.execute(f"SELECT MEDIAN({col})/1e6 {where}", params)
            v = cur.fetchone()[0]
            queries[key] = round(float(v), 3) if v is not None else None
    except Exception as exc:
        queries["error"] = str(exc)[:200]

    # ---- storage from SVV_TABLE_INFO (point-in-time; snapshot BEFORE dropping the schema) ----
    # SVV_TABLE_INFO.size is the table size in 1MB blocks -> MB. Sum the schema (incl. MV backing tables).
    cur.execute('SELECT "table", size FROM SVV_TABLE_INFO WHERE schema = %s ORDER BY size DESC', (args.schema,))
    rows = cur.fetchall()
    total_mb = sum(int(r[1]) for r in rows if r[1] is not None)
    gb = total_mb / 1024.0
    storage = {
        "schema": args.schema,
        "total_gb": round(gb, 3),
        "price_per_gb_month": args.storage_price,
        "storage_cost_usd_per_month": round(gb * args.storage_price, 2),
        "by_table_mb": {str(r[0]).strip(): int(r[1]) for r in rows[:10] if r[1] is not None},
    }

    # ---- per-MV refresh summary from SYS_MV_REFRESH_HISTORY ----
    # Reported per MV: how many refreshes ran, how long they took, how many failed, and CRUCIALLY
    # how many were incremental vs full recompute. A silent switch to full recompute is the failure
    # mode that would invalidate the design at 113B rows, so it must be visible in the output.
    # NOTE: we deliberately do NOT split $ per MV — Serverless bills per workgroup at one-minute
    # granularity while ingest and both refreshes overlap, so a per-MV dollar figure would be fiction.
    # Do not split the writer into per-MV dollars. Published fresh-path cost uses the declared
    # writer capacity-time model; this view remains an optional usage audit.
    refreshes = {}
    try:
        cur.execute(
            "SELECT TRIM(mv_name), COALESCE(TRIM(refresh_type),'unknown'), COUNT(*), "
            "       SUM(CASE WHEN status ILIKE '%%success%%' THEN 1 ELSE 0 END), "
            "       AVG(duration)/1e6, MAX(duration)/1e6, SUM(duration)/1e6, "
            "       SUM(CASE WHEN POSITION('incrementally' IN LOWER(status)) > 0 THEN 1 ELSE 0 END) "
            "FROM SYS_MV_REFRESH_HISTORY "
            "WHERE start_time >= %s AND start_time <= %s "
            "GROUP BY 1, 2 ORDER BY 1, 2", (args.since, until))
        for mv, rtype, n, ok, avg_s, max_s, tot_s, incremental in cur.fetchall():
            entry = refreshes.setdefault(str(mv).strip(), {"by_type": {}, "total": 0, "succeeded": 0,
                                                           "total_seconds": 0.0})
            entry["by_type"][str(rtype).strip()] = {
                "count": int(n),
                "succeeded": int(ok or 0),
                "avg_seconds": round(float(avg_s), 3) if avg_s is not None else None,
                "max_seconds": round(float(max_s), 3) if max_s is not None else None,
                "incremental": int(incremental or 0),
            }
            entry["total"] += int(n)
            entry["succeeded"] += int(ok or 0)
            entry["total_seconds"] = round(entry["total_seconds"] + float(tot_s or 0), 2)
        for mv, e in refreshes.items():
            e["failed"] = e["total"] - e["succeeded"]
            # the acceptance signal, surfaced explicitly rather than buried in by_type
            e["incremental"] = sum(int(item["incremental"]) for item in e["by_type"].values())
            e["all_incremental"] = e["incremental"] == e["total"]
    except Exception as exc:
        refreshes = {"error": str(exc)[:200]}

    cur.close(); con.close()

    out = {"rpu_usage": usage, "read_queries": queries, "storage": storage,
           "mv_refreshes": refreshes}

    # ---- MSK cluster cost: computed from spec x uptime (NOT in any SQL view) ----
    if args.msk_hours > 0:
        broker_cost = args.msk_brokers * args.msk_broker_price * args.msk_hours
        storage_cost = args.msk_storage_gb * args.msk_storage_price * (args.msk_hours / 730.0)  # GB-month prorated
        xaz_cost = args.msk_xaz_gb * args.msk_xaz_price if args.include_client_cross_az else 0.0
        out["msk"] = {
            "hours_up": args.msk_hours, "brokers": args.msk_brokers,
            "broker_cost_usd": round(broker_cost, 2),
            "storage_cost_usd": round(storage_cost, 2),
            "client_xaz_cost_usd": round(xaz_cost, 2),
            "client_xaz_included": args.include_client_cross_az,
            "client_xaz_gb_supplied": args.msk_xaz_gb,
            "msk_total_usd": round(broker_cost + storage_cost + xaz_cost, 2),
            "note": "CostBench includes broker-hours + storage; client cross-AZ is excluded unless explicitly opted in; inter-broker replication is free",
        }

    print(json.dumps(out, indent=2))


if __name__ == "__main__":
    main()
