#!/usr/bin/env python3
"""
Backfill T2 RUN8 result JSONLs with the server-side timings the runners failed to record.

The runners captured EXECUTION_TIME only. On a streaming-fed interactive MV that excluded
the dominant cost: for the dashboard_mv_std arm, median compilation was 10516ms against
1057ms of execution, so the published latency was 12.4x lower than TOTAL_ELAPSED_TIME.
This rewrites each arm's JSONL from an ACCOUNT_USAGE.QUERY_HISTORY extract:

    "result"            -> TOTAL_ELAPSED_TIME  (compile + queue + execute), seconds
                           ... or the string "timeout" where the interactive warehouse
                           aborted the query at its 5s cap (a censored observation, not a
                           latency measurement — kept as-is, by decision)
    "compilation_time"  -> COMPILATION_TIME, seconds, same [[v], ...] shape
    "execution_time"    -> EXECUTION_TIME, seconds, same shape (null where the query was
                           killed during compilation and never executed)

Input CSVs come from analysis/extract_query_history.sql (one block per arm). Usage:

    python3 t2/backfill_timings.py ~/Downloads/*.csv            # dry run, validates only
    python3 t2/backfill_timings.py ~/Downloads/*.csv --write     # emit *.backfilled.jsonl

ALIGNMENT: the arm is identified from WAREHOUSE_NAME + the set of QUERY_HASHes, not from
the filename. `q` and `iteration` are recomputed here from the hash order and start_time
rather than trusting the extract's own columns, then cross-checked against them. Nothing is
written unless every positional check passes — a misaligned backfill would silently attach
the wrong timings to the wrong iteration instead of failing loudly.
"""

import argparse
import csv
import json
import statistics as st
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
RESULTS = HERE.parent / "results"          # per-arm subdirectory comes from ARMS["dir"]

# Timeout error codes: 000630 = statement/warehouse timeout, 000604 = statement canceled.
TIMEOUT_CODES = {"000630", "000604"}

# Per-arm identity + the expectations validated against ACCOUNT_USAGE on 2026-07-27.
# hashes are listed in queries-file order, so index+1 == the query's position q.
ARMS = [
    {
        "name": "dashboard_mv_std",
        "warehouse": "BENCH2COST_GEN2_SMALL_DASH",
        "hashes": ["4138da05ca5b8b3b12d91e3c60b451c2", "032fd86ef70df033fb74f37a3a6ff43a",
                   "198a3ec3a02513cbd5e088271fcf95ba", "2adb24066ebed97154fb420445501a46"],
        "dir": "t2", "jsonl": "dashboard_mv_std_20260720T121508Z.jsonl",
        "rows": 872, "iterations": 218, "timeouts": 0,
    },
    {
        "name": "dashboard_imv_iv",
        "warehouse": "SNOWPIPES_IT_READ_SMALL",
        "hashes": ["4138da05ca5b8b3b12d91e3c60b451c2", "032fd86ef70df033fb74f37a3a6ff43a",
                   "198a3ec3a02513cbd5e088271fcf95ba", "2adb24066ebed97154fb420445501a46"],
        "dir": "t2", "jsonl": "dashboard_imv_iv_20260720T121508Z.jsonl",
        "rows": 956, "iterations": 239, "timeouts": 763,
    },
    {
        "name": "dashboard_raw_iv",
        "warehouse": "SNOWPIPES_IT_READ_SMALL",
        "hashes": ["49f0fd2569c4079e89daa0b84f877f1d", "7c0f8300c30ea00f6fdf768e653b1045",
                   "120292e60f2c28d5fd83f1c198caa296", "da2f485cec29a5f67e49a05ad1cfa1b7"],
        "dir": "t2", "jsonl": "dashboard_raw_iv_20260720T121508Z.jsonl",
        "rows": 968, "iterations": 242, "timeouts": 523,
    },
    {
        "name": "drilldown",
        "warehouse": "SNOWPIPES_IT_READ_SMALL",
        "hashes": ["81bfa7bfde17377a3c0dd2f00ea3883f", "98a25b7faf10ec93061165a1efbd79dd"],
        "dir": "t2", "jsonl": "drilldown_20260720T121508Z.jsonl",
        "rows": 86, "iterations": 43, "timeouts": 0,
    },
    # --- T1, London account (IXHMFWU-LONDONTEST). NOTE: the drilldown hashes are IDENTICAL
    # to T2's (both run byte-identical SQL against an unqualified FROM QUOTES_IT), so the
    # warehouse is the only thing separating those two arms here.
    {
        "name": "t1_dashboard",
        "warehouse": "BENCH2COST_IT_SMALL",
        "hashes": ["efc99dfb4e75c8090e7c5f15a5ab4890", "e8e2f176d29c0adbdc6a98665e8c7baa",
                   "a55ea2c89d70307c76537477a5c9b144", "bf634500f741e0a2f2ecdf96cbba3f3b"],
        "dir": "t1", "jsonl": "dashboard_20260617T154602Z.jsonl",
        "rows": 976, "iterations": 244, "timeouts": 0,
    },
    {
        "name": "t1_drilldown",
        "warehouse": "BENCH2COST_IT_SMALL",
        "hashes": ["81bfa7bfde17377a3c0dd2f00ea3883f", "98a25b7faf10ec93061165a1efbd79dd"],
        "dir": "t1", "jsonl": "drilldown_20260617T154602Z.jsonl",
        "rows": 82, "iterations": 41, "timeouts": 2,
    },
    {
        "name": "t0_drilldown",
        "warehouse": "BENCH2COST_GEN2_SMALL_T0",
        "hashes": ["bb4cf593cb383ec183b5e8cae4fba8f2", "664528618f5fcd871b5fa67ed24bbc17"],
        "dir": "t0", "jsonl": "drilldown_20260617T155914Z.jsonl",
        "rows": 80, "iterations": 40, "timeouts": 0,
    },
    # T0 dashboard lives on the PARIS account, in schema STOCKHOUSE on a standard Gen2 Small
    # warehouse — a different schema AND warehouse from the T0 drilldown arm above, which is
    # on London. The two T0 arms are not interchangeable.
    {
        "name": "t0_dashboard",
        "warehouse": "BENCH2COST_SMALL_GEN2",
        "hashes": ["5a1b576018ce7df649dc5e0ad6bd0d76", "c7ae3cc7962c11bc97381e6df56b0e3a",
                   "ba1f0eb3db5d08288aa50d5b8b311afa", "4023099af69dc938d721a9191a465449"],
        "dir": "t0", "jsonl": "dashboard_20260609T154304Z.jsonl",
        "rows": 652, "iterations": 163, "timeouts": 0,
    },
]


class Fail(Exception):
    """A validation failure. Always aborts the arm — never downgraded to a warning."""


def parse_ts(s):
    """'2026-07-20 12:15:12.423 Z' or '... -0700' or '... +0000' -> aware datetime.
    Snowflake renders START_TIME (TIMESTAMP_LTZ) in the session TIMEZONE, so an export
    may carry either form; both must sort identically."""
    body, _, tz = s.strip().rpartition(" ")
    if not body:
        raise Fail(f"unparseable START_TIME {s!r}")
    dt = datetime.strptime(body.strip(), "%Y-%m-%d %H:%M:%S.%f")
    if tz in ("Z", "UTC", "+0000"):
        return dt.replace(tzinfo=timezone.utc)
    sign = -1 if tz[0] == "-" else 1
    return (dt - sign * timedelta(hours=int(tz[1:3]), minutes=int(tz[3:5]))).replace(tzinfo=timezone.utc)


def ms_to_s(v):
    """QUERY_HISTORY ms -> seconds; empty/NULL -> None (JSON null)."""
    if v is None or str(v).strip() == "":
        return None
    return float(v) / 1000.0


def identify(rows, path):
    """Match the extract to exactly one arm -> (arm, provenance_verified).

    Preferred: (WAREHOUSE_NAME, set of QUERY_HASHes), which pins the arm to the exact
    queries. Extracts taken before those two columns were added to the SELECT list fall
    back to the (rows, query slots, timeouts) signature — unique across the four arms, but
    it cannot confirm WHICH query each slot is, so the caller must trust the extract's own
    server-side Q. That is reported, not silently accepted."""
    if "QUERY_HASH" in rows[0] and "WAREHOUSE_NAME" in rows[0]:
        whs = {r["WAREHOUSE_NAME"] for r in rows}
        hashes = {r["QUERY_HASH"] for r in rows}
        if len(whs) != 1:
            raise Fail(f"{path.name}: expected one warehouse, found {sorted(whs)}")
        wh = whs.pop()
        hits = [a for a in ARMS if a["warehouse"] == wh and set(a["hashes"]) == hashes]
        if len(hits) != 1:
            raise Fail(f"{path.name}: warehouse {wh} + {len(hashes)} hashes matched "
                       f"{len(hits)} arms (need exactly 1); hashes={sorted(hashes)}")
        return hits[0], True

    if "Q" not in rows[0]:
        raise Fail(f"{path.name}: no QUERY_HASH/WAREHOUSE_NAME and no Q column — "
                   f"cannot identify the arm; re-export with analysis/extract_query_history.sql")
    sig = (len(rows), len({int(r["Q"]) for r in rows}),
           sum(1 for r in rows if r["ERROR_CODE"].strip() in TIMEOUT_CODES))
    hits = [a for a in ARMS if (a["rows"], len(a["hashes"]), a["timeouts"]) == sig]
    if len(hits) != 1:
        raise Fail(f"{path.name}: signature rows/slots/timeouts={sig} matched {len(hits)} arms "
                   f"(need exactly 1); re-export with QUERY_HASH + WAREHOUSE_NAME")
    return hits[0], False


def index_rows(rows, arm, provenance):
    """Recompute (q, iteration) -> row and cross-check the extract's own Q / ITERATION.
    With provenance, q comes from the hash order (independent of the extract). Without it,
    q can only come from the extract's server-side DECODE."""
    if provenance:
        pos = {h: i + 1 for i, h in enumerate(arm["hashes"])}
        slot = lambda r: pos[r["QUERY_HASH"]]
    else:
        slot = lambda r: int(r["Q"])
    by_q = {}
    for r in rows:
        by_q.setdefault(slot(r), []).append(r)

    grid = {}
    for q, rs in by_q.items():
        rs.sort(key=lambda r: parse_ts(r["START_TIME"]))
        for i, r in enumerate(rs, start=1):
            grid[(q, i)] = r
            # The server-side columns are advisory; disagreement means the extract and this
            # script disagree about ordering, which is exactly the failure we must not paper over.
            if provenance and "Q" in r and r["Q"].strip() and int(r["Q"]) != q:
                raise Fail(f"{arm['name']}: extract Q={r['Q']} but hash order says q={q} "
                           f"(query_id {r.get('QUERY_ID')})")
            if "ITERATION" in r and r["ITERATION"].strip() and int(r["ITERATION"]) != i:
                raise Fail(f"{arm['name']}: extract ITERATION={r['ITERATION']} but start_time "
                           f"order says {i} (query_id {r.get('QUERY_ID')})")
    return by_q, grid


def validate(arm, rows, by_q, grid, records):
    """Every check that must hold before a single byte is written."""
    n_q = len(arm["hashes"])

    if len(rows) != arm["rows"]:
        raise Fail(f"{arm['name']}: {len(rows)} rows, expected {arm['rows']}")
    if len(records) != arm["iterations"]:
        raise Fail(f"{arm['name']}: JSONL has {len(records)} lines, expected {arm['iterations']}")
    counts = {q: len(rs) for q, rs in by_q.items()}
    if sorted(counts) != list(range(1, n_q + 1)):
        raise Fail(f"{arm['name']}: got query slots {sorted(counts)}, expected 1..{n_q}")
    if set(counts.values()) != {arm["iterations"]}:
        raise Fail(f"{arm['name']}: unbalanced arm, rows per query = {counts}")

    timeouts = sum(1 for r in rows if r["ERROR_CODE"].strip() in TIMEOUT_CODES)
    if timeouts != arm["timeouts"]:
        raise Fail(f"{arm['name']}: {timeouts} timeout rows, expected {arm['timeouts']}")

    # The decisive check: position by position, the extract must agree with what the runner
    # already recorded. A matching total with shifted positions would otherwise pass silently.
    mismatches, checked, matched_exec, matched_resid = [], 0, 0, 0
    for i, rec in enumerate(records, start=1):
        got = rec.get("result", [])
        if len(got) != n_q:
            raise Fail(f"{arm['name']} line {i}: result width {len(got)}, expected {n_q}")
        for q in range(1, n_q + 1):
            row = grid.get((q, i))
            if row is None:
                raise Fail(f"{arm['name']}: no extract row for q={q} iteration={i}")
            old = got[q - 1][0]
            is_to = row["ERROR_CODE"].strip() in TIMEOUT_CODES
            if old == "timeout":
                if not is_to:
                    mismatches.append(f"q{q} it{i}: JSONL timeout vs extract "
                                      f"{row['EXECUTION_STATUS']}/{row['ERROR_CODE'] or '-'}")
            elif is_to:
                mismatches.append(f"q{q} it{i}: JSONL {old}s vs extract timeout")
            elif old is not None:
                # The old runner stored whatever INFORMATION_SCHEMA.QUERY_HISTORY() called
                # EXECUTION_TIME at runtime. That equals ACCOUNT_USAGE's EXECUTION_TIME for
                # most rows, but on ~8% of the interactive-warehouse rows there is 1.5-4s of
                # time that is neither compilation, execution nor queueing, and INFORMATION_
                # SCHEMA folded it into execution while ACCOUNT_USAGE reports it outside both.
                # For those, the runner's value == TOTAL_ELAPSED_TIME - COMPILATION_TIME.
                # Accept either identity; requiring only the first would reject 51 correctly
                # aligned rows, and requiring only the second would reject the 5 mv_std rows
                # that carry QUEUED_PROVISIONING_TIME. Matching one of the two to sub-ms
                # precision is still far too tight for a positional shift to slip through.
                exec_s = ms_to_s(row["EXECUTION_TIME"])
                total_s = ms_to_s(row["TOTAL_ELAPSED_TIME"])
                comp_s = ms_to_s(row["COMPILATION_TIME"]) or 0.0
                checked += 1
                by_exec = exec_s is not None and abs(exec_s - old) <= 0.0005
                by_resid = total_s is not None and abs((total_s - comp_s) - old) <= 0.0015
                if by_exec:
                    matched_exec += 1
                elif by_resid:
                    matched_resid += 1
                else:
                    mismatches.append(f"q{q} it{i}: JSONL {old}s vs EXECUTION_TIME {exec_s}s "
                                      f"and vs elapsed-compile {None if total_s is None else round(total_s - comp_s, 3)}s")
    if mismatches:
        raise Fail(f"{arm['name']}: {len(mismatches)} positional mismatches, e.g.\n    "
                   + "\n    ".join(mismatches[:8]))
    return checked, timeouts, matched_exec, matched_resid


def rebuild(arm, grid, records):
    """New records: result = TOTAL_ELAPSED_TIME (or "timeout"), plus the two split fields.
    Original key order is preserved; the new keys land immediately after "result"."""
    n_q = len(arm["hashes"])
    out = []
    for i, rec in enumerate(records, start=1):
        result, comp, exe = [], [], []
        for q in range(1, n_q + 1):
            row = grid[(q, i)]
            is_to = row["ERROR_CODE"].strip() in TIMEOUT_CODES
            total_s = ms_to_s(row["TOTAL_ELAPSED_TIME"])
            # Decision: a query the interactive warehouse killed at its cap stays "timeout".
            # It is censored at 5s, not measured, so reporting 5.06s as a latency would overstate
            # what we know. Its compile/exec split is still recorded alongside.
            result.append(["timeout" if is_to else total_s])
            comp.append([ms_to_s(row["COMPILATION_TIME"])])
            exe.append([ms_to_s(row["EXECUTION_TIME"])])
        new = {}
        for k, v in rec.items():
            if k == "result":
                new["result"] = result
                new["compilation_time"] = comp
                new["execution_time"] = exe
            else:
                new[k] = v
        if "result" not in rec:            # defensive: keep the fields even if absent upstream
            new.update(result=result, compilation_time=comp, execution_time=exe)
        out.append(new)
    return out


def numeric(records, key="result"):
    return [v[0] for rec in records for v in rec.get(key, []) if isinstance(v[0], (int, float))]


def measured(new):
    """(result, compile, exec) triples for positions where result is a real latency, i.e.
    excluding "timeout". Medians must be taken over the SAME positions or they are not
    comparable: on the IWH arms most rows are timeouts with ~5s of compilation, so a compile
    median over all rows against a result median over successes only is meaningless."""
    out = []
    for rec in new:
        for r, c, e in zip(rec["result"], rec["compilation_time"], rec["execution_time"]):
            if isinstance(r[0], (int, float)):
                out.append((r[0], c[0], e[0]))
    return out


def process(path, write):
    rows = list(csv.DictReader(path.open()))
    if not rows:
        raise Fail(f"{path.name}: empty")
    rows = [{k.upper(): (v if v is not None else "") for k, v in r.items()} for r in rows]

    arm, provenance = identify(rows, path)
    jsonl = RESULTS / arm["dir"] / arm["jsonl"]
    if not jsonl.exists():
        raise Fail(f"{arm['name']}: missing {jsonl}")
    records = [json.loads(l) for l in jsonl.open() if l.strip()]

    by_q, grid = index_rows(rows, arm, provenance)
    checked, timeouts, m_exec, m_resid = validate(arm, rows, by_q, grid, records)
    new = rebuild(arm, grid, records)

    old_v, new_v = numeric(records), numeric(new)
    trip = measured(new)
    print(f"\n{arm['name']}  <- {path.name}")
    print(f"  validated : {len(rows)} rows, {arm['iterations']} iterations x {len(arm['hashes'])} "
          f"queries, {timeouts} timeouts, {checked} timing positions matched "
          f"({m_exec} via EXECUTION_TIME, {m_resid} via elapsed-compile)")
    if not provenance:
        print("  PROVENANCE: no QUERY_HASH/WAREHOUSE_NAME in this extract — arm identified by "
              "row/slot/timeout signature,\n              and q taken from the extract's own Q "
              "column rather than re-derived from the hash order.")
    if old_v and new_v:
        print(f"  reported  : median {st.median(old_v):8.3f}s -> {st.median(new_v):8.3f}s "
              f"({st.median(new_v) / st.median(old_v):.1f}x)   [over the {len(new_v)} "
              f"non-timeout positions]")
    if trip:
        # Median of each per-row SHARE, not a ratio of independent medians:
        # median(elapsed) != median(compile) + median(exec), so the latter reads as though
        # time were missing when it is not. These three sum to ~100% by construction.
        r = [t[0] for t in trip]
        cs = st.median([(t[1] or 0) / t[0] for t in trip if t[0] > 0])
        es = st.median([(t[2] or 0) / t[0] for t in trip if t[0] > 0])
        os_ = st.median([(t[0] - (t[1] or 0) - (t[2] or 0)) / t[0] for t in trip if t[0] > 0])
        print(f"  per row   : median elapsed {st.median(r):7.3f}s  ->  compile {100 * cs:4.1f}%  "
              f"exec {100 * es:4.1f}%  other {100 * os_:4.1f}%")
    n_to = sum(1 for rec in new for v in rec["result"] if v[0] == "timeout")
    if n_to:
        tc = [c[0] for rec in new for v, c in zip(rec["result"], rec["compilation_time"])
              if v[0] == "timeout" and c[0] is not None]
        print(f"  timeouts  : {n_to} censored at the 5s cap, median compile "
              f"{st.median(tc):.3f}s (never executed)")
    if write:
        out = jsonl.with_suffix(".backfilled.jsonl")
        with out.open("w") as f:
            for rec in new:
                f.write(json.dumps(rec, separators=(",", ":"), ensure_ascii=False) + "\n")
        print(f"  wrote     : {out.relative_to(RESULTS.parent)}")
    else:
        print("  dry run   : nothing written (pass --write)")


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("csv", nargs="+", type=Path, help="extract CSVs from analysis/extract_query_history.sql")
    ap.add_argument("--write", action="store_true",
                    help="emit <name>.backfilled.jsonl beside the original (originals untouched)")
    args = ap.parse_args()

    failures = 0
    for p in args.csv:
        try:
            process(p, args.write)
        except Fail as exc:
            failures += 1
            print(f"\nFAIL {p.name}\n  {exc}", file=sys.stderr)
    if failures:
        print(f"\n{failures} of {len(args.csv)} extracts failed validation; "
              f"no backfill written for those.", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
