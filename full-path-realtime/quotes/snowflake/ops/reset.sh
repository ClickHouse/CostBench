#!/bin/bash
# Reset to zero. Robust: stops any running ingest + cancels in-flight (detached)
# COPY queries FIRST (else they refill the table right after the truncate),
# then TRUNCATE QUOTES, recreate the empty MV, and verify the table stays at 0.
export PATH="$HOME/.local/bin:$PATH"
cd /home/ubuntu/bench && source .venv/bin/activate
: "${SF_ACCOUNT:?set SF_ACCOUNT, e.g. export SF_ACCOUNT=ORG-ACCT}"
: "${SF_USER:?set SF_USER, e.g. export SF_USER=MYUSER}"
# 1) stop any client-side ingest (it would refill QUOTES after truncate)
if pkill -9 -f ingest.py 2>/dev/null; then echo "stopped running ingest"; else echo "no ingest process running"; fi
sleep 2
python - <<'PY'
import os, snowflake.connector as sc, time
from cryptography.hazmat.primitives import serialization
pk=serialization.load_pem_private_key(open('keys/rsa_key.p8','rb').read(),password=None)
pkb=pk.private_bytes(serialization.Encoding.DER,serialization.PrivateFormat.PKCS8,serialization.NoEncryption())
con=sc.connect(account=os.environ['SF_ACCOUNT'],user=os.environ['SF_USER'],private_key=pkb,login_timeout=30);cur=con.cursor()
SCHEMA=os.environ.get('SF_SCHEMA','STOCKHOUSE')
for q in ['use role ACCOUNTADMIN','use database BENCH2COST',f'use schema {SCHEMA}','use warehouse BENCH2COST_GEN2_XSMALL']: cur.execute(q)
# 2) cancel any in-flight / detached COPY queries still committing into QUOTES
cur.execute("select query_id from table(information_schema.query_history(result_limit=>500)) "
            "where execution_status in ('RUNNING','QUEUED') and query_text ilike 'copy into%'")
ids=[r[0] for r in cur.fetchall()]
for qid in ids:
    try: cur.execute(f"select system$cancel_query('{qid}')")
    except Exception as e: print('  cancel warn:', str(e)[:50])
if ids:
    print(f'cancelled {len(ids)} in-flight COPY queries'); time.sleep(6)
# 3) truncate + recreate the (empty) MV
cur.execute('truncate table QUOTES')
cur.execute('''create or replace materialized view QUOTES_DAILY as
  select sym, TO_DATE(TO_TIMESTAMP_NTZ(t,3)) as day, count(*) as n_quotes,
         min(bp) as bp_min, max(bp) as bp_max, min(ap) as ap_min, max(ap) as ap_max,
         sum(bs) as bs_sum, sum("AS") as as_sum, sum(ap-bp) as spread_sum
  from QUOTES group by sym, TO_DATE(TO_TIMESTAMP_NTZ(t,3))''')
# 4) verify the table is empty AND stays empty (no stragglers refilling it)
cur.execute('select count(*) from QUOTES'); c1=cur.fetchone()[0]
time.sleep(6)
cur.execute('select count(*) from QUOTES'); c2=cur.fetchone()[0]
print(f'QUOTES after reset: {c1:,}   (+6s: {c2:,})')
print('RESET OK — table empty and stable.' if c2==0 else
      'WARNING: table is refilling — an ingest is STILL running somewhere.')
con.close()
PY
