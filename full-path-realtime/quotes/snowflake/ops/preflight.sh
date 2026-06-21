#!/bin/bash
# =============================================================================
# Preflight — validate a box can RUN a benchmark before launching the full ingest.
# Diagnostic: authenticates raw (key-pair), then probes warehouse / database /
# schema / objects INDIVIDUALLY so a missing piece is pinpointed (not a generic
# "object does not exist"). Read-only + cheap. Each check prints OK / WARN / FAIL.
#
# Run ON THE BOX after sourcing env + venv:
#   T1 (Paris):   source .sfenv && source .venv/bin/activate && bash ops/preflight.sh T1
#   T2 (London):  source .sfenv && source .venv/bin/activate && \
#                 SF_SCHEMA=STREAMING SF_WAREHOUSE=BENCH2COST_IT_STREAM \
#                 SF_TRACK_WAREHOUSE=BENCH_STREAM bash ops/preflight.sh T2
# =============================================================================
export PATH="$HOME/.local/bin:$PATH"
cd /home/ubuntu/bench && source .venv/bin/activate 2>/dev/null
MODE="${1:?usage: preflight.sh T1|T2}"
python - "$MODE" <<'PY'
import os, sys, time
MODE = sys.argv[1].upper()
DB   = os.environ.get("SF_DATABASE", "BENCH2COST")

def ok(m):   print(f"OK    {m}")
def warn(m): print(f"WARN  {m}")
def fail(m): print(f"FAIL  {m}")

print(f"=== PREFLIGHT {MODE} ===")
KEY     = os.environ.get("SF_KEY") or "/home/ubuntu/bench/keys/rsa_key.p8"
schema  = os.environ.get("SF_SCHEMA", "STOCKHOUSE")
want_wh = os.environ.get("SF_WAREHOUSE")
print(f"  SF_ACCOUNT={os.environ.get('SF_ACCOUNT','<unset>')}  SF_USER={os.environ.get('SF_USER','<unset>')}")
print(f"  SF_KEY={KEY}  SF_SCHEMA={schema}  SF_WAREHOUSE={want_wh or '<unset>'}")
(ok if os.path.exists(KEY) else fail)(f"key file exists: {KEY}")

import snowflake.connector as sc
try:
    import runner_common as rc          # reuse _pkb() (key loader) + parse_queries
except Exception as e:
    fail(f"import runner_common: {e}"); sys.exit(1)

# --- raw connect (no db/warehouse/schema) so each object can be probed separately ---
try:
    con = sc.connect(account=os.environ["SF_ACCOUNT"], user=os.environ["SF_USER"],
                     private_key=rc._pkb(), login_timeout=30)
    cur = con.cursor(); ok("authenticated (key-pair)")
except Exception as e:
    fail(f"authenticate: {e}"); sys.exit(1)

def q(sql):
    cur.execute(sql); return cur.fetchall()

try:
    print(f"  account={q('select current_account()')[0][0]}  "
          f"version={q('select current_version()')[0][0]}  role={q('select current_role()')[0][0]}")
except Exception as e:
    warn(f"version/account: {e}")

# --- warehouses ---
try:
    whs = [r[0] for r in q("show warehouses")]
    print(f"  warehouses present: {whs}")
except Exception as e:
    warn(f"show warehouses: {e}")
if want_wh:
    try: q(f"use warehouse {want_wh}"); ok(f"USE WAREHOUSE {want_wh}")
    except Exception as e: fail(f"USE WAREHOUSE {want_wh}: {e}")
else:
    warn("SF_WAREHOUSE unset — set it in .sfenv (runner would otherwise use a default that may not exist here)")

# --- database + schema ---
try: q(f"use database {DB}"); ok(f"USE DATABASE {DB}")
except Exception as e: fail(f"USE DATABASE {DB}: {e}")
try:
    print(f"  schemas in {DB}: {[r[1] for r in q(f'show schemas in database {DB}')]}")
except Exception as e:
    warn(f"show schemas: {e}")
schema_ok = False
try: q(f"use schema {DB}.{schema}"); ok(f"USE SCHEMA {DB}.{schema}"); schema_ok = True
except Exception as e: fail(f"USE SCHEMA {DB}.{schema}: {e}")

if not schema_ok:
    warn("schema not usable — create the schema/objects or fix SF_SCHEMA. Skipping object/query checks.")
    con.close(); print("=== PREFLIGHT DONE ==="); sys.exit(0)

def show(kind):
    rows = q(f"show {kind} in schema {DB}.{schema}")
    cols = [d[0].lower() for d in cur.description]
    return [dict(zip(cols, r)) for r in rows]

if MODE == "T1":
    qfile = "queries_raw_it.sql"
    try: ok(f"QUOTES rows = {q(f'select count(*) from {DB}.{schema}.QUOTES')[0][0]:,}")
    except Exception as e:
        if "interactive warehouse" in str(e).lower():
            warn("QUOTES (standard table) not queryable on an interactive warehouse — expected; "
                 "T0 needs a STANDARD warehouse. Skipping base-table count.")
        else:
            fail(f"QUOTES base table: {e}")
    try:
        its = {r['name'].upper(): r for r in show("interactive tables")}
        for t in ("QUOTES_IT", "QUOTES_DAILY_IT"):
            if t in its:
                d = its[t]
                ok(f"{t}: wh={d.get('warehouse')} lag={d.get('target_lag')} state={d.get('scheduling_state')} rows={d.get('rows')}")
            else:
                fail(f"{t} missing — run ops/setup_interactive.sh")
    except Exception as e:
        fail(f"show interactive tables: {e}")

elif MODE == "T2":
    qfile = "t2/queries_raw_it.sql"
    try:
        import snowflake.ingest.streaming  # noqa: F401
        ok("snowpipe-streaming SDK importable")
    except Exception as e:
        warn(f"snowpipe-streaming SDK not importable ({e}) — pip install snowpipe-streaming")
    try:
        its = {r['name'].upper(): r for r in show("interactive tables")}
        (ok if "QUOTES_IT" in its else fail)(
            f"QUOTES_IT (stream target) rows={its['QUOTES_IT'].get('rows')}" if "QUOTES_IT" in its
            else "QUOTES_IT missing — run t2/setup_streaming.sql")
        (ok if "QUOTES_DAILY_IMV" in its else warn)(
            f"QUOTES_DAILY_IMV present rows={its['QUOTES_DAILY_IMV'].get('rows')}" if "QUOTES_DAILY_IMV" in its
            else "QUOTES_DAILY_IMV not under SHOW INTERACTIVE TABLES — check SHOW MATERIALIZED VIEWS (open item #4)")
    except Exception as e:
        fail(f"show interactive tables: {e}")
    try:
        pipes = {r['name'].upper() for r in show("pipes")}
        (ok if "QUOTES_IT_PIPE" in pipes else fail)(
            "pipe QUOTES_IT_PIPE present" if "QUOTES_IT_PIPE" in pipes
            else "pipe QUOTES_IT_PIPE missing — run t2/setup_streaming.sql")
    except Exception as e:
        fail(f"show pipes: {e}")
else:
    fail(f"unknown mode {MODE} (use T1 or T2)"); con.close(); sys.exit(2)

# --- parse + execute each drilldown query once (client-timed) ---
try:
    qs = rc.parse_queries(qfile)
    ok(f"{qfile} parsed -> {len(qs)} queries")
    if want_wh:
        for i, qq in enumerate(qs, 1):
            t0 = time.time()
            try: cur.execute(qq); cur.fetchall(); ok(f"  q{i} executed in {time.time()-t0:.2f}s (client wall-time)")
            except Exception as e: fail(f"  q{i}: {str(e)[:160]}")
    else:
        warn("no SF_WAREHOUSE set — skipping query execution")
except Exception as e:
    fail(f"queries from {qfile}: {e}")

con.close()
print("=== PREFLIGHT DONE ===")
PY
