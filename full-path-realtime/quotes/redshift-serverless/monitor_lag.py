#!/usr/bin/env python3
"""
Redshift T2 refresh controller + streaming freshness monitor.

Runs ONE process against the WRITER workgroup with three independent threads:

  lag      — samples SYS_STREAM_SCAN_STATES for quotes_streamed every --lag-interval seconds
  typed    — REFRESH MATERIALIZED VIEW quotes_typed RESTRICT, continuous best-effort
             (next refresh starts --typed-delay seconds after the previous one FINISHES)
  daily    — REFRESH MATERIALIZED VIEW quotes_daily RESTRICT, fixed-rate --daily-interval

WHY THIS SHAPE
--------------
quotes_streamed is the only AUTO REFRESH MV. Redshift disallows AUTO REFRESH on an MV defined on
another MV, so both children must be refreshed explicitly. They are a FORK (both defined directly
on quotes_streamed), not a cascade, so they are scheduled and measured independently:
  * RESTRICT (the default) is passed explicitly — never CASCADE, which would also refresh the
    streaming MV underneath and conflate ingest with child maintenance.
  * Each MV is single-flight: a refresh never overlaps another refresh of the SAME MV. Siblings may
    overlap (use --serialize-refresh to force them to take turns if that harms ingest keep-up).
  * Failures are logged with bounded backoff and recorded as failures — never counted as success,
    never silently advancing freshness.

WHY IT MUST RUN DURING THE RUN
------------------------------
SYS_STREAM_SCAN_STATES is effectively point-in-time — per-partition freshness is gone if not
sampled live. SYS_MV_REFRESH_HISTORY is historized, but we join it per refresh anyway so each
JSONL record carries the server's own verdict (refresh_type = Incremental vs Full, duration).

WHICH LAG ACTUALLY MATTERS
--------------------------
The publishable freshness number is the staleness of the object a QUERY READS — quotes_daily for the
dashboard, quotes_typed for the drilldown. That is the direct analogue of Snowflake's `behind_by`
(which measures QUOTES_DAILY_IMV lagging its source QUOTES_IT) and of ClickHouse's synchronous MV.

The streaming hop is still recorded, because Redshift has an EXTRA hop the others don't:
    Snowflake:  Snowpipe --> QUOTES_IT --> IMV          (behind_by covers only the last arrow)
    ClickHouse: INSERT   --> raw       --> MV           (synchronous, ~0)
    Redshift:   MSK --> quotes_streamed --> child       (TWO lag hops)
So honest end-to-end freshness = child hop + streaming hop. Dropping the streaming hop would
flatter Redshift by hiding a real component of its latency.

OUTPUT (JSONL, one file per stream)
  lag_<ts>.jsonl      {ts, raw_rows, raw_rows_source, max_latency_s, total_lag_rows, partitions[]}
  refresh_<ts>.jsonl  {target_mv, scheduled_at, started_at, finished_at, client_duration_seconds,
                       status, error, server_refresh_type, server_status, server_duration_seconds,
                       streamed_latency_before_s, streamed_latency_after_s, lag_before, lag_after,
                       child_freshness_s, end_to_end_freshness_s, target_rows}

  python monitor_lag.py --typed-delay 2 --daily-interval 60 --lag-interval 60
  python monitor_lag.py --no-typed            # daily only (component calibration phase)
  python monitor_lag.py --no-typed --no-daily # pure lag sampling

Auth/env: RS_HOST/RS_PORT/RS_DB/RS_USER/RS_PASSWORD (writer endpoint).
Requires: pip install redshift_connector
"""
import argparse
import json
import os
import signal
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

try:
    import redshift_connector
except ImportError:
    sys.exit("ERROR: pip install redshift_connector")

HOST = os.environ["RS_HOST"]; PORT = int(os.environ.get("RS_PORT", "5439"))
DB = os.environ.get("RS_DB", "quotes"); USER = os.environ["RS_USER"]; PASSWORD = os.environ["RS_PASSWORD"]
STREAM_MV = os.environ.get("RS_STREAM_MV", "quotes_streamed")
TYPED_MV  = os.environ.get("RS_TYPED_MV", "quotes_typed")
DAILY_MV  = os.environ.get("RS_DAILY_MV", "quotes_daily")

# Stable SQL comment tags so refresh statements are identifiable in SYS_QUERY_HISTORY and can be
# excluded from (or attributed in) the cost/timing analysis.
REFRESH_TAG = {TYPED_MV: "/* costbench:refresh:typed */", DAILY_MV: "/* costbench:refresh:daily */"}

_STOP = threading.Event()
signal.signal(signal.SIGINT,  lambda *_: _STOP.set())
signal.signal(signal.SIGTERM, lambda *_: _STOP.set())

_write_lock = threading.Lock()
_serialize_lock = threading.Lock()   # only used with --serialize-refresh


def _now_iso():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def connect():
    con = redshift_connector.connect(host=HOST, port=PORT, database=DB, user=USER, password=PASSWORD)
    con.autocommit = True          # each REFRESH MATERIALIZED VIEW is its own transaction
    return con


def _emit(path, record):
    line = json.dumps(record, separators=(",", ":"), default=str)
    with _write_lock:
        with open(path, "a") as f:
            f.write(line + "\n")


def scalar(cur, q, params=None):
    try:
        cur.execute(q, params) if params else cur.execute(q)
        r = cur.fetchone()
        return r[0] if r else None
    except Exception as exc:
        print(f"  WARN {q[:60]!r}: {exc}", file=sys.stderr, flush=True)
        return None


def poll_stream_state(cur):
    """Latest scan state per partition for the streaming MV -> (max_latency_s, total_lag_rows, rows)."""
    cur.execute(
        "SELECT partition_id, lag_from_latest, max_latency_s, latest_position "
        "FROM ( SELECT partition_id, lag_from_latest, max_latency_s, latest_position, "
        "              ROW_NUMBER() OVER (PARTITION BY partition_id ORDER BY record_time DESC) AS rn "
        "       FROM SYS_STREAM_SCAN_STATES WHERE TRIM(mv_name) = %s ) WHERE rn = 1", (STREAM_MV,))
    parts = []
    for pid, lag, lat, pos in cur.fetchall():
        parts.append({"partition_id": str(pid).strip(),
                      "lag_from_latest": int(lag) if lag is not None else None,
                      "max_latency_s": int(lat) if lat is not None else None,
                      "latest_position": str(pos).strip()})
    max_lat = max((p["max_latency_s"] for p in parts if p["max_latency_s"] is not None), default=None)
    tot_lag = sum((p["lag_from_latest"] for p in parts if p["lag_from_latest"] is not None), 0)
    return max_lat, tot_lag, parts


def stream_summary(cur):
    """(max_latency_s, total_lag_rows) for the streaming MV — cheap (system view only)."""
    try:
        lat, tot, _ = poll_stream_state(cur)
        return lat, tot
    except Exception:
        return None, None


def child_freshness_s(cur, mv):
    """How stale is the CHILD's data, in seconds — the direct analogue of Snowflake's `behind_by`.

    Computed SERVER-side (GETDATE()) so there's no client/Redshift clock skew.

    Only quotes_daily is probed: its watermark column is an aggregate over ~10k rollup rows, so
    MAX() is trivially cheap. We deliberately DO NOT probe quotes_typed the same way — a
    MAX(kafka_timestamp) there is a full scan of a table heading for 113B rows, i.e. the harness
    would add real cost to the very workgroup whose cost we're measuring. The typed branch's
    staleness is instead derived post-hoc from this journal: time since the previous typed refresh
    finished, plus that refresh's duration, plus the streamed latency recorded alongside it.

    NOTE for cross-vendor comparison: this is the lag of the child vs its SOURCE, exactly like
    Snowflake's behind_by. Redshift's END-TO-END freshness is this PLUS streamed_latency_s, because
    Redshift has an extra hop (MSK -> streaming MV) that Snowpipe/ClickHouse inserts don't.
    """
    if mv != DAILY_MV:
        return None
    try:
        cur.execute(f"SELECT DATEDIFF(second, MAX(watermark_kafka_ts), GETDATE()) FROM {mv}")
        r = cur.fetchone()
        return int(r[0]) if r and r[0] is not None else None
    except Exception:
        return None


def server_refresh_info(cur, mv):
    """The server's own verdict on the most recent refresh: Incremental vs Full, status, duration.
    This is the acceptance signal — a silent switch to full recompute must be visible."""
    try:
        cur.execute(
            "SELECT refresh_type, status, duration/1e6 FROM SYS_MV_REFRESH_HISTORY "
            "WHERE TRIM(mv_name) = %s ORDER BY start_time DESC LIMIT 1", (mv,))
        row = cur.fetchone()
        if not row:
            return None, None, None
        return str(row[0]).strip(), str(row[1]).strip(), (float(row[2]) if row[2] is not None else None)
    except Exception as exc:
        print(f"  WARN refresh-history lookup for {mv}: {exc}", file=sys.stderr, flush=True)
        return None, None, None


def refresh_loop(mv, out_path, fixed_rate, period, serialize, probe_rows=False, stop=_STOP):
    """Single-flight refresh loop for ONE materialized view.

    fixed_rate=True  -> fire at loop_start + N*period (daily: a steady cadence)
    fixed_rate=False -> start the next refresh `period` seconds after the previous one FINISHED
                        (typed: continuous best effort)
    Either way the loop is inherently single-flight: it is one thread issuing one refresh at a time.
    """
    con = cur = None
    backoff = 1.0
    next_fire = time.monotonic()
    while not stop.is_set():
        if fixed_rate:
            delay = next_fire - time.monotonic()
            if delay > 0 and stop.wait(delay):
                break
        scheduled_at = _now_iso()

        if serialize:
            # Siblings take turns: hold the shared lock for the duration of this refresh.
            _serialize_lock.acquire()
            if stop.is_set():
                _serialize_lock.release()
                break

        try:
            if con is None:
                con = connect(); cur = con.cursor()

            lat_before, lag_before = stream_summary(cur)
            started_at = _now_iso(); t0 = time.monotonic()
            # RESTRICT explicitly: refresh ONLY this MV, never the streaming MV beneath it.
            cur.execute(f"{REFRESH_TAG.get(mv, '')} REFRESH MATERIALIZED VIEW {mv} RESTRICT")
            client_secs = time.monotonic() - t0
            finished_at = _now_iso()

            rtype, rstatus, rsecs = server_refresh_info(cur, mv)
            lat_after, lag_after = stream_summary(cur)
            fresh = child_freshness_s(cur, mv)
            rec = {
                "target_mv": mv, "scheduled_at": scheduled_at, "started_at": started_at,
                "finished_at": finished_at, "client_duration_seconds": round(client_secs, 3),
                "status": "ok", "error": None,
                "server_refresh_type": rtype, "server_status": rstatus,
                "server_duration_seconds": rsecs,
                # the streaming hop (Redshift-only extra hop vs Snowflake/ClickHouse)
                "streamed_latency_before_s": lat_before, "streamed_latency_after_s": lat_after,
                "lag_before": lag_before, "lag_after": lag_after,
                # child_freshness_s is ALREADY END-TO-END: it is now() minus the KAFKA ARRIVAL
                # timestamp of the newest row in the child, so it spans MSK -> streaming MV -> child.
                # (An earlier version added streamed_latency_s on top and DOUBLE-COUNTED the streaming
                # hop. streamed_latency_s is for ATTRIBUTING which hop the staleness sits in, not for
                # summing.) Runs before 2026-08-13 have an inflated end_to_end_freshness_s — recompute
                # from child_freshness_s, which is journalled separately.
                "child_freshness_s": fresh,
                "end_to_end_freshness_s": fresh,
                "target_rows": scalar(cur, f"SELECT COUNT(*) FROM {mv}") if probe_rows else None,
            }
            _emit(out_path, rec)
            print(f"  [{mv}] {client_secs:6.2f}s type={rtype} "
                  f"child_fresh={fresh}s e2e={rec['end_to_end_freshness_s']}s "
                  f"streamed_lat {lat_before}->{lat_after}s lag {lag_before}->{lag_after}",
                  file=sys.stderr, flush=True)
            backoff = 1.0
        except Exception as exc:
            # A failed refresh is recorded as a FAILURE: never counted as success, never advances
            # freshness. Drop the connection so the next attempt reconnects cleanly.
            _emit(out_path, {
                "target_mv": mv, "scheduled_at": scheduled_at, "started_at": scheduled_at,
                "finished_at": _now_iso(), "client_duration_seconds": None,
                "status": "error", "error": str(exc)[:300],
                "server_refresh_type": None, "server_status": None, "server_duration_seconds": None,
                "streamed_latency_before_s": None, "streamed_latency_after_s": None,
                "lag_before": None, "lag_after": None,
                "child_freshness_s": None, "end_to_end_freshness_s": None, "target_rows": None,
            })
            print(f"  [{mv}] REFRESH ERROR: {str(exc)[:160]}", file=sys.stderr, flush=True)
            try:
                if con is not None:
                    con.close()
            except Exception:
                pass
            con = cur = None
            if stop.wait(backoff):
                break
            backoff = min(backoff * 2, 60.0)   # bounded backoff
            continue
        finally:
            if serialize:
                try:
                    _serialize_lock.release()
                except RuntimeError:
                    pass

        if fixed_rate:
            next_fire += period
            now = time.monotonic()
            if next_fire <= now:
                print(f"  [{mv}] WARN: refresh exceeded the {period}s cadence by "
                      f"{now - next_fire:.1f}s; next fires immediately.", file=sys.stderr, flush=True)
                next_fire = now
        else:
            if stop.wait(period):   # continuous best-effort: delay AFTER completion
                break

    try:
        if con is not None:
            con.close()
    except Exception:
        pass
    print(f"  [{mv}] refresh loop stopped.", file=sys.stderr, flush=True)


def lag_loop(out_path, interval, probe_rows=False, stop=_STOP):
    """Samples per-partition streaming freshness. Must run during the run (point-in-time telemetry)."""
    con = cur = None
    next_fire = time.monotonic()
    while not stop.is_set():
        delay = next_fire - time.monotonic()
        if delay > 0 and stop.wait(delay):
            break
        rec = {"ts": _now_iso()}
        try:
            if con is None:
                con = connect(); cur = con.cursor()
            max_lat, tot_lag, parts = poll_stream_state(cur)
            rec["max_latency_s"] = max_lat
            rec["total_lag_rows"] = tot_lag
            rec["partitions"] = parts
            # raw_rows from the CONSUMED OFFSETS (free — already in the system view above) rather
            # than COUNT(*) on the streaming MV, which grows into a 113B-row scan billed to the
            # workgroup we're measuring. The topic is recreated per run so offsets start at 0,
            # making the offset sum equal the rows consumed. --probe-rows forces the exact COUNT(*).
            rec["raw_rows"] = (scalar(cur, f"SELECT COUNT(*) FROM {STREAM_MV}") if probe_rows else
                               sum(int(p["latest_position"]) for p in parts
                                   if str(p["latest_position"]).isdigit()))
            rec["raw_rows_source"] = "count" if probe_rows else "offsets"
            print(f"  [lag] {rec['ts']} raw={rec['raw_rows']} behind={max_lat}s lag_rows={tot_lag}",
                  file=sys.stderr, flush=True)
        except Exception as exc:
            rec["error"] = str(exc)[:200]
            print(f"  [lag] POLL ERROR: {exc}", file=sys.stderr, flush=True)
            try:
                if con is not None:
                    con.close()
            except Exception:
                pass
            con = cur = None
        _emit(out_path, rec)
        next_fire += interval
        if next_fire <= time.monotonic():
            next_fire = time.monotonic()
    try:
        if con is not None:
            con.close()
    except Exception:
        pass
    print("  [lag] monitor stopped.", file=sys.stderr, flush=True)


def main(argv=None):
    ap = argparse.ArgumentParser(prog="monitor_lag.py", description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--lag-interval", type=int, default=60, help="freshness sampling period (s)")
    ap.add_argument("--typed-delay", type=float, default=2.0,
                    help="seconds to wait AFTER a quotes_typed refresh finishes before the next")
    ap.add_argument("--daily-interval", type=int, default=60,
                    help="fixed-rate cadence (s) for the quotes_daily refresh")
    ap.add_argument("--no-typed", action="store_true", help="disable the typed refresh loop")
    ap.add_argument("--no-daily", action="store_true", help="disable the daily refresh loop")
    ap.add_argument("--serialize-refresh", action="store_true",
                    help="never let the typed and daily refreshes overlap (use if they hurt ingest)")
    ap.add_argument("--probe-rows", action="store_true",
                    help="also COUNT(*) the refreshed MV each cycle. OFF by default: at 113B rows that "
                         "scan is harness overhead landing on the workgroup we're measuring.")
    ap.add_argument("--output-dir", default="./out_redshift")
    args = ap.parse_args(argv)

    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    Path(args.output_dir).mkdir(parents=True, exist_ok=True)
    lag_path = os.path.join(args.output_dir, f"lag_{ts}.jsonl")
    ref_path = os.path.join(args.output_dir, f"refresh_{ts}.jsonl")

    print(f"writer {HOST}:{PORT}/{DB}\n"
          f"  lag     -> {lag_path}   (every {args.lag_interval}s, mv={STREAM_MV})\n"
          f"  refresh -> {ref_path}\n"
          f"    {TYPED_MV}: {'DISABLED' if args.no_typed else f'continuous, {args.typed_delay}s after completion'}\n"
          f"    {DAILY_MV}: {'DISABLED' if args.no_daily else f'fixed-rate {args.daily_interval}s'}\n"
          f"  serialize siblings: {args.serialize_refresh}. Ctrl-C to stop.", file=sys.stderr)

    threads = [threading.Thread(target=lag_loop,
                               args=(lag_path, args.lag_interval, args.probe_rows), daemon=True)]
    if not args.no_typed:
        threads.append(threading.Thread(
            target=refresh_loop,
            args=(TYPED_MV, ref_path, False, args.typed_delay, args.serialize_refresh,
                  args.probe_rows), daemon=True))
    if not args.no_daily:
        threads.append(threading.Thread(
            target=refresh_loop,
            args=(DAILY_MV, ref_path, True, args.daily_interval, args.serialize_refresh,
                  args.probe_rows), daemon=True))

    for t in threads:
        t.start()
    try:
        while any(t.is_alive() for t in threads) and not _STOP.is_set():
            time.sleep(0.5)
    except KeyboardInterrupt:
        _STOP.set()
    _STOP.set()
    for t in threads:
        t.join(timeout=120)
    print("\nController stopped.", file=sys.stderr)


if __name__ == "__main__":
    main()
