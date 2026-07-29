#!/usr/bin/env python3
"""
Execute a .sql file statement-by-statement over the key-pair connection. Stand-in for
`snow sql -f`, which is not installed on the benchmark boxes.

    cd ~/bench && source .sfenv && source .venv/bin/activate
    python ops/exec_sql.py t2/setup_streaming_run9.sql

Prints each statement's first rows so SHOW/GET_DDL output in the file is visible, and stops at
the first error with a non-zero exit — a half-applied setup is worse than none.
Statements are split with runner_common.parse_queries (the same splitter the runners use).
"""
import argparse
import os
import sys

import snowflake.connector as sc

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import runner_common as rc


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("sqlfile")
    ap.add_argument("--database", default="BENCH2COST")
    ap.add_argument("--max-rows", type=int, default=6, help="rows to print per statement")
    args = ap.parse_args()

    stmts = rc.parse_queries(args.sqlfile)
    print(f"{args.sqlfile}: {len(stmts)} statement(s)\n")

    con = sc.connect(account=os.environ["SF_ACCOUNT"], user=os.environ["SF_USER"],
                     private_key=rc._pkb(), database=args.database, login_timeout=30)
    cur = con.cursor()
    try:
        for i, stmt in enumerate(stmts, 1):
            head = stmt[:110] + ("…" if len(stmt) > 110 else "")
            print(f"[{i}/{len(stmts)}] {head}")
            cur.execute(stmt)
            if cur.description:
                cols = [c[0] for c in cur.description]
                rows = cur.fetchmany(args.max_rows)
                if rows:
                    print("        " + " | ".join(cols[:8]))
                    for r in rows:
                        print("        " + " | ".join(str(v)[:28] for v in r[:8]))
            print()
    except Exception as exc:
        print(f"\nFAILED on statement {i}: {exc}", file=sys.stderr)
        sys.exit(1)
    finally:
        cur.close()
        con.close()
    print("all statements OK")


if __name__ == "__main__":
    main()
