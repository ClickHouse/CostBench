#!/bin/bash
# Create a FRESH clustered schema = $SF_SCHEMA (schema + clustered QUOTES + internal stage +
# clustered MV), by reusing create_stockhouse_2.sql with the schema name substituted. Lets each
# clustering run start from depth ~0 in its own schema.
#   export SF_SCHEMA=STOCKHOUSE_3 && bash setup_schema.sh
#
# T1 variant: set T1=1 to use create_stockhouse_t1.sql, which OMITS the standard QUOTES_DAILY
# materialized view (the interactive QUOTES_DAILY_IT created by ops/setup_interactive.sh replaces
# it; a standard MV would only add redundant maintenance cost and pollute T1 cost attribution).
#   export SF_SCHEMA=STOCKHOUSE_T1 T1=1 && bash setup_schema.sh
# (An explicit SQL file as $1 always overrides the T1 flag.)
#
# Refuses to target the original STOCKHOUSE (would clobber that MV via CREATE OR REPLACE).
export PATH="$HOME/.local/bin:$PATH"
cd /home/ubuntu/bench && source .venv/bin/activate
: "${SF_ACCOUNT:?set SF_ACCOUNT}"
: "${SF_USER:?set SF_USER}"
: "${SF_SCHEMA:?set SF_SCHEMA, e.g. export SF_SCHEMA=STOCKHOUSE_3}"
if [ "$SF_SCHEMA" = "STOCKHOUSE" ]; then
  echo "refusing: SF_SCHEMA=STOCKHOUSE is the original schema (use a fresh name)"; exit 1
fi
# Default DDL: T1=1 -> no-MV T1 variant; else the standard clustered DDL. Explicit $1 wins.
DEFAULT_SRC="create_stockhouse_2.sql"; [ "${T1:-0}" = "1" ] && DEFAULT_SRC="create_stockhouse_t1.sql"
SRC="${1:-$DEFAULT_SRC}"
# Canonical DDL is written for STOCKHOUSE_2; retarget it to the requested schema.
sed "s/STOCKHOUSE_2/${SF_SCHEMA}/g" "$SRC" > /tmp/_setup_${SF_SCHEMA}.sql
echo "creating schema ${SF_SCHEMA} from ${SRC} ..."
python - "/tmp/_setup_${SF_SCHEMA}.sql" <<'PY'
import sys, os, snowflake.connector as sc
from cryptography.hazmat.primitives import serialization
pk=serialization.load_pem_private_key(open('keys/rsa_key.p8','rb').read(),password=None)
pkb=pk.private_bytes(serialization.Encoding.DER,serialization.PrivateFormat.PKCS8,serialization.NoEncryption())
con=sc.connect(account=os.environ['SF_ACCOUNT'],user=os.environ['SF_USER'],private_key=pkb,login_timeout=30)
for cur in con.execute_string(open(sys.argv[1]).read()):
    q=(cur.query or '').strip().split('\n')[0][:72]
    try: rows=cur.fetchall()
    except Exception: rows=None
    print(f">> {q}")
    if rows and len(rows)<=2:
        for r in rows: print("   ", r[:4] if isinstance(r,(list,tuple)) else r)
print("schema setup complete")
con.close()
PY
