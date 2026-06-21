#!/bin/bash
# One-time: create the fresh clustered schema (default STOCKHOUSE_2) by executing
# create_stockhouse_2.sql through the connector — so setup is copy-paste on the box,
# no Snowsight needed. Creates schema + clustered QUOTES + internal stage + clustered MV.
#   bash setup_schema_2.sh [sql_file]   (default create_stockhouse_2.sql)
export PATH="$HOME/.local/bin:$PATH"
cd /home/ubuntu/bench && source .venv/bin/activate
: "${SF_ACCOUNT:?set SF_ACCOUNT, e.g. export SF_ACCOUNT=ORG-ACCT}"
: "${SF_USER:?set SF_USER, e.g. export SF_USER=MYUSER}"
SQLFILE="${1:-create_stockhouse_2.sql}"
python - "$SQLFILE" <<'PY'
import sys, os, snowflake.connector as sc
from cryptography.hazmat.primitives import serialization
pk=serialization.load_pem_private_key(open('keys/rsa_key.p8','rb').read(),password=None)
pkb=pk.private_bytes(serialization.Encoding.DER,serialization.PrivateFormat.PKCS8,serialization.NoEncryption())
con=sc.connect(account=os.environ['SF_ACCOUNT'],user=os.environ['SF_USER'],private_key=pkb,login_timeout=30)
sql=open(sys.argv[1]).read()
# execute_string splits the multi-statement DDL and runs each (handles -- comments).
for cur in con.execute_string(sql):
    q=(cur.query or '').strip().split('\n')[0][:72]
    try: rows=cur.fetchall()
    except Exception: rows=None
    print(f">> {q}")
    if rows and len(rows)<=2:
        for r in rows: print("   ", r[:4] if isinstance(r,(list,tuple)) else r)
print("schema setup complete")
con.close()
PY
