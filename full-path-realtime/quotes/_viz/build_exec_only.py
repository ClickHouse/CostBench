#!/usr/bin/env python3
"""
Derive an EXECUTION-ONLY input from a backfilled runner JSONL: copy the record but replace
`result` with the `execution_time` array, i.e. Snowflake's EXECUTION_TIME with compilation
and queueing stripped out.

  python3 build_exec_only.py _test/dashboard_snowflake_it.jsonl _test/dash_exec_only.jsonl \
      --label "Interactive Small (execution only)"

WHAT THIS IS FOR: isolating scan cost from planning cost within one system, and reproducing
what the runners originally reported before the backfill.

WHAT IT IS NOT FOR: a fair cross-vendor comparison. The ClickHouse series in these charts is
`clickhouse-client --time`, i.e. client-side wall clock including ClickHouse's own parsing,
planning and network round-trip. Plotting Snowflake execution-only against that removes one
system's planning while keeping the other's — the exact asymmetry the backfill existed to
fix. Any chart built from this file has to say so in its title.

Timeouts stay "timeout" (censored, not measured). Positions whose execution_time is null —
a query killed during compilation never executed — become null rather than 0, so they are
absent from the plot instead of appearing as an implausibly fast query.
"""
import argparse
import json
import sys
from pathlib import Path


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("src")
    ap.add_argument("dst")
    ap.add_argument("--label", default=None,
                    help="overwrite `machine` so the legend distinguishes this series")
    args = ap.parse_args()

    out, n_to, n_null, n_val = [], 0, 0, 0
    for line in Path(args.src).read_text().splitlines():
        if not line.strip():
            continue
        rec = json.loads(line)
        if "execution_time" not in rec:
            sys.exit(f"{args.src} has no 'execution_time' — needs a backfilled results file "
                     f"(see snowflake/t2/backfill_timings.py).")
        new = []
        for res, ex in zip(rec["result"], rec["execution_time"]):
            if res[0] == "timeout":
                new.append(["timeout"]); n_to += 1
            elif ex[0] is None:
                new.append([None]); n_null += 1
            else:
                new.append([ex[0]]); n_val += 1
        rec["result"] = new
        if args.label:
            rec["machine"] = args.label
        out.append(json.dumps(rec, separators=(",", ":"), ensure_ascii=False))

    Path(args.dst).write_text("\n".join(out) + "\n")
    print(f"wrote {args.dst}  ({len(out)} records; {n_val} execution values, "
          f"{n_to} timeouts kept, {n_null} null)")


if __name__ == "__main__":
    main()
