#!/bin/bash
# T2 setup (Snowpipe Streaming): create BENCH2COST.<SF_SCHEMA> + interactive table QUOTES_IT +
# pipe QUOTES_IT_PIPE + interactive MV QUOTES_DAILY_IMV by running t2/setup_streaming.sql through the
# connector (no snow CLI needed). The T2 analogue of ops/setup_schema.sh. Reads SF_ACCOUNT/SF_USER/
# SF_KEY from .sfenv.
#
# The target schema is SF_SCHEMA (default STOCKHOUSE_T2); every STOCKHOUSE_T2 in the SQL is rewritten
# to it, so a fresh schema (e.g. STOCKHOUSE_T2_RUN_1) is created clean with NO destructive replace of
# any existing populated schema. NOTE: setup_streaming.sql uses CREATE OR REPLACE — pointing SF_SCHEMA
# at an ALREADY-POPULATED schema WIPES its QUOTES_IT. Streaming ingest + the IMV are serverless; the
# IMV is attached to the interactive read warehouse named in setup_streaming.sql (SNOWPIPES_IT_READ_SMALL).
#   SF_SCHEMA=STOCKHOUSE_T2_RUN_1 bash ops/setup_streaming.sh
export PATH="$HOME/.local/bin:$PATH"
cd /home/ubuntu/bench && source .venv/bin/activate
: "${SF_ACCOUNT:?set SF_ACCOUNT}"
: "${SF_USER:?set SF_USER}"
python - <<'PY'
import os, snowflake.connector as sc
from cryptography.hazmat.primitives import serialization as s
pk=s.load_pem_private_key(open(os.environ.get("SF_KEY","/home/ubuntu/bench/keys/rsa_key.p8"),"rb").read(),password=None)
kb=pk.private_bytes(s.Encoding.DER,s.PrivateFormat.PKCS8,s.NoEncryption())
con=sc.connect(account=os.environ["SF_ACCOUNT"],user=os.environ["SF_USER"],private_key=kb,login_timeout=30)
SCHEMA=os.environ.get("SF_SCHEMA","STOCKHOUSE_T2")
SETUP_SQL=os.environ.get("SF_SETUP_SQL","t2/setup_streaming.sql")   # T3: t3/setup_streaming_it.sql
sql=open(SETUP_SQL).read().replace("STOCKHOUSE_T2", SCHEMA)
print(f"-- setup sql: {SETUP_SQL}  -> target schema: BENCH2COST.{SCHEMA}")
ok=True
try:
    for cur in con.execute_string(sql):
        q=(cur.query or "").strip().split("\n")[0][:72]
        try: rows=cur.fetchall()
        except Exception: rows=None
        print(">>", q)
        if rows and len(rows)<=4:
            for r in rows: print("   ", r[:4] if isinstance(r,(list,tuple)) else r)
except Exception as e:
    ok=False; print("ERR:", str(e)[:240])
print("T2 setup complete" if ok else "T2 setup FAILED — see error above")
con.close()
PY
