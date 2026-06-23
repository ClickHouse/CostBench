#!/bin/bash
# Create the two DYNAMIC interactive tables for the interactive-table benchmark, in $SF_SCHEMA.
# Per-table refresh warehouse so refresh cost is attributable. Sizing reflects the last run's
# finding (override via env if needed):
#   QUOTES_DAILY_IT (aggregate) -> BENCH2COST_GEN2_MEDIUM (MEDIUM) , TARGET_LAG=1 minute
#       (a Small warehouse LAGGED behind the 1-minute target; Medium keeps up)
#   QUOTES_IT       (raw copy)  -> BENCH2COST_GEN2_XSMALL (XSMALL) , TARGET_LAG=10 minutes
#       (Small was overkill; XSmall keeps the raw IT in sync fine)
# Warehouses are created IF NOT EXISTS at the size below (no-op if they already exist — if an
# existing warehouse is the wrong size, ALTER WAREHOUSE <name> SET WAREHOUSE_SIZE=... or use
# ops/reconfig_it.sh). ITs use CREATE OR REPLACE (cheap while empty).
#
# CAVEAT (Snowflake >= 10.21): CREATING a NEW Gen2 warehouse with
# `resource_constraint=STANDARD_GEN_2` errors ("Use the GENERATION property"). Workaround: pass
# AGG_WH/RAW_WH pointing at warehouses that ALREADY EXIST (the `if not exists` then skips the bad
# clause). E.g. AGG_WH=BENCH2COST_GEN2_MEDIUM RAW_WH=BENCH2COST_GEN2_XSMALL_2.
# To CREATE a fresh Gen2 warehouse in this version, the working form (per GET_DDL of an existing one)
# is QUOTED: `resource_constraint='STANDARD_GEN_2'`; `generation=2` is rejected ("invalid value").
# A bare create (no resource_constraint/generation) also defaults to Gen2 in this account.
export PATH="$HOME/.local/bin:$PATH"
cd /home/ubuntu/bench && source .venv/bin/activate
: "${SF_ACCOUNT:?set SF_ACCOUNT}"
: "${SF_USER:?set SF_USER}"
SCHEMA="${SF_SCHEMA:-STOCKHOUSE}"
AGG_WH="${AGG_WH:-BENCH2COST_GEN2_MEDIUM}";  AGG_SIZE="${AGG_SIZE:-MEDIUM}";  AGG_LAG="${AGG_LAG:-1 minute}"
RAW_WH="${RAW_WH:-BENCH2COST_GEN2_XSMALL}";  RAW_SIZE="${RAW_SIZE:-XSMALL}";  RAW_LAG="${RAW_LAG:-10 minutes}"
python - "$SCHEMA" "$AGG_WH" "$AGG_LAG" "$RAW_WH" "$RAW_LAG" "$AGG_SIZE" "$RAW_SIZE" <<'PY'
import os, sys, snowflake.connector as sc
from cryptography.hazmat.primitives import serialization
SCHEMA, AGG_WH, AGG_LAG, RAW_WH, RAW_LAG, AGG_SIZE, RAW_SIZE = sys.argv[1:8]
pk=serialization.load_pem_private_key(open('keys/rsa_key.p8','rb').read(),password=None)
pkb=pk.private_bytes(serialization.Encoding.DER,serialization.PrivateFormat.PKCS8,serialization.NoEncryption())
con=sc.connect(account=os.environ['SF_ACCOUNT'],user=os.environ['SF_USER'],private_key=pkb,login_timeout=25)
cur=con.cursor()
def run(sql, label):
    try:
        cur.execute(sql); print(f"OK  {label}"); return True
    except Exception as e:
        print(f"ERR {label} -> {str(e)[:200]}"); return False

for s in ["use role ACCOUNTADMIN","use database BENCH2COST",f"use schema {SCHEMA}"]:
    run(s, s)

# Per-warehouse size.
for wh, size in ((AGG_WH, AGG_SIZE), (RAW_WH, RAW_SIZE)):
    run(f"""create warehouse if not exists {wh} with warehouse_type=STANDARD
            resource_constraint=STANDARD_GEN_2 warehouse_size={size}
            auto_resume=TRUE initially_suspended=TRUE""", f"warehouse {wh} (Gen2 {size})")

run(f"""create or replace interactive table QUOTES_DAILY_IT
        cluster by (sym, day)
        target_lag='{AGG_LAG}'
        warehouse={AGG_WH}
        as select sym, TO_DATE(TO_TIMESTAMP_NTZ(t,3)) as day, count(*) as n_quotes,
                  min(bp) as bp_min, max(bp) as bp_max, min(ap) as ap_min, max(ap) as ap_max,
                  sum(bs) as bs_sum, sum("AS") as as_sum, sum(ap-bp) as spread_sum
           from QUOTES group by sym, TO_DATE(TO_TIMESTAMP_NTZ(t,3))""",
    f"QUOTES_DAILY_IT -> {AGG_WH} ({AGG_SIZE}), lag {AGG_LAG}")
run(f"""create or replace interactive table QUOTES_IT
        cluster by (sym, t)
        target_lag='{RAW_LAG}'
        warehouse={RAW_WH}
        as select * from QUOTES""",
    f"QUOTES_IT -> {RAW_WH} ({RAW_SIZE}), lag {RAW_LAG}")

print("=== SHOW INTERACTIVE TABLES ===")
if run(f"show interactive tables in schema BENCH2COST.{SCHEMA}", "show interactive tables"):
    cols=[d[0].lower() for d in cur.description]
    for r in cur.fetchall():
        d=dict(zip(cols,r))
        print("   ", {k:d.get(k) for k in ("name","rows","target_lag","warehouse","scheduling_state","cluster_by") if k in d})
con.close()
PY
