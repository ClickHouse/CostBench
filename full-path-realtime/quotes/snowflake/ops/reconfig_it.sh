#!/bin/bash
# Non-destructive reconfig of the interactive tables via ALTER (no replace / no data loss):
#   QUOTES_DAILY_IT -> warehouse BENCH2COST_GEN2_SMALL_1 (keep TARGET_LAG=1 minute)
#   QUOTES_IT       -> TARGET_LAG=10 minutes (keep warehouse BENCH2COST_GEN2_SMALL_2)
# Verifies warehouses exist, then SHOWs the resulting IT config. If ALTER isn't supported it
# prints the error (don't fall back to CREATE OR REPLACE automatically).
export PATH="$HOME/.local/bin:$PATH"
cd /home/ubuntu/bench && source .venv/bin/activate
: "${SF_ACCOUNT:?}"; : "${SF_USER:?}"; SCHEMA="${SF_SCHEMA:-STOCKHOUSE}"
python - "$SCHEMA" <<'PY'
import os, sys, snowflake.connector as sc
from cryptography.hazmat.primitives import serialization
SCHEMA=sys.argv[1]
pk=serialization.load_pem_private_key(open('keys/rsa_key.p8','rb').read(),password=None)
pkb=pk.private_bytes(serialization.Encoding.DER,serialization.PrivateFormat.PKCS8,serialization.NoEncryption())
con=sc.connect(account=os.environ['SF_ACCOUNT'],user=os.environ['SF_USER'],private_key=pkb,login_timeout=25)
cur=con.cursor()
def run(sql,label):
    try: cur.execute(sql); print("OK ",label); return True
    except Exception as e: print("ERR",label,"->",str(e)[:200]); return False
for s in ["use role ACCOUNTADMIN","use database BENCH2COST","use schema "+SCHEMA]: run(s,s)
cur.execute("show warehouses like 'BENCH2COST_GEN2_SMALL_%'")
print("refresh warehouses:", [r[0] for r in cur.fetchall()])
run("alter interactive table QUOTES_DAILY_IT set warehouse=BENCH2COST_GEN2_SMALL_1",
    "QUOTES_DAILY_IT -> warehouse BENCH2COST_GEN2_SMALL_1")
run("alter interactive table QUOTES_IT set target_lag='10 minutes'",
    "QUOTES_IT -> target_lag 10 minutes")
if run("show interactive tables in schema BENCH2COST."+SCHEMA,"show interactive tables"):
    cols=[d[0].lower() for d in cur.description]
    for r in cur.fetchall():
        d=dict(zip(cols,r))
        print("   ", {k:d.get(k) for k in ("name","target_lag","warehouse","scheduling_state") if k in d})
con.close()
PY
