#!/bin/bash
# =============================================================================
# Dump INTERACTIVE_TABLE_REFRESH_HISTORY for $SF_SCHEMA to the CSV the it_lag chart consumes
# (columns NAME, REFRESH_END_TIME, STALENESS_AT_DONE_SEC, DURATION_SEC, STATE; timestamp formatted
# 'YYYY-MM-DD HH24:MI:SS.FF3 TZHTZM' to match render_mv_lag.py). Run on a box (key-pair auth),
# WITHIN Snowflake's refresh-history retention window. Tracking warehouse only (no measured load).
#
#   SF_SCHEMA=STOCKHOUSE_T1 bash ops/collect_it_refresh.sh [out_csv]
#   default out_csv: out_<tn>/it_refresh.csv   (tn = lowercased schema suffix, e.g. t1)
# Then copy it into the repo for charting:  cp out_t1/it_refresh.csv quotes/snowflake/results/t1/
# =============================================================================
export PATH="$HOME/.local/bin:$PATH"
cd /home/ubuntu/bench && source .venv/bin/activate
: "${SF_ACCOUNT:?set SF_ACCOUNT}"
: "${SF_USER:?set SF_USER}"
: "${SF_SCHEMA:?set SF_SCHEMA, e.g. export SF_SCHEMA=STOCKHOUSE_T1}"
TRACK_WH="${SF_TRACK_WAREHOUSE:-BENCH}"
TN="$(printf '%s' "${SF_SCHEMA##*_}" | tr 'A-Z' 'a-z')"
OUT="${1:-out_${TN}/it_refresh.csv}"
python - "$OUT" "$SF_SCHEMA" "$TRACK_WH" <<'PY'
import os, sys, csv
import snowflake.connector as sc
from cryptography.hazmat.primitives import serialization
OUT, SCHEMA, WH = sys.argv[1:4]
os.makedirs(os.path.dirname(OUT) or '.', exist_ok=True)
pk=serialization.load_pem_private_key(open('keys/rsa_key.p8','rb').read(),password=None)
pkb=pk.private_bytes(serialization.Encoding.DER,serialization.PrivateFormat.PKCS8,serialization.NoEncryption())
con=sc.connect(account=os.environ['SF_ACCOUNT'],user=os.environ['SF_USER'],private_key=pkb,login_timeout=30)
cur=con.cursor()
for q in ("use role ACCOUNTADMIN", f"use warehouse {WH}", "use database BENCH2COST", f"use schema {SCHEMA}"):
    cur.execute(q)
cur.execute(f"""
  select name,
         to_char(refresh_end_time, 'YYYY-MM-DD HH24:MI:SS.FF3 TZHTZM') as refresh_end_time,
         datediff('second', data_timestamp,    refresh_end_time) as staleness_at_done_sec,
         datediff('second', refresh_start_time, refresh_end_time) as duration_sec,
         state
  from table(information_schema.interactive_table_refresh_history(RESULT_LIMIT => 10000))
  where database_name='BENCH2COST' and schema_name='{SCHEMA}'
  order by refresh_end_time
""")
rows=cur.fetchall()
cols=[c[0].upper() for c in cur.description]
with open(OUT,'w',newline='') as f:
    w=csv.writer(f); w.writerow(cols)
    for r in rows: w.writerow(r)
ok=sum(1 for r in rows if str(r[-1]).upper()=='SUCCEEDED')
print(f"wrote {len(rows)} refresh rows ({ok} SUCCEEDED) -> {OUT}", file=sys.stderr)
if not rows:
    print("WARN: no rows — INTERACTIVE_TABLE_REFRESH_HISTORY may be empty/out of retention, "
          "or the it_refresh tracker wasn't running during the benchmark.", file=sys.stderr)
con.close()
PY
