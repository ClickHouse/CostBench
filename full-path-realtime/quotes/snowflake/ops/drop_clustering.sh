#!/bin/bash
# Revert to the UNCLUSTERED baseline: drop clustering on QUOTES, recreate MV plain.
export PATH="$HOME/.local/bin:$PATH"
cd /home/ubuntu/bench && source .venv/bin/activate
: "${SF_ACCOUNT:?set SF_ACCOUNT, e.g. export SF_ACCOUNT=ORG-ACCT}"
: "${SF_USER:?set SF_USER, e.g. export SF_USER=MYUSER}"
python - <<'PY'
import os, snowflake.connector as sc
from cryptography.hazmat.primitives import serialization
pk=serialization.load_pem_private_key(open('keys/rsa_key.p8','rb').read(),password=None)
pkb=pk.private_bytes(serialization.Encoding.DER,serialization.PrivateFormat.PKCS8,serialization.NoEncryption())
con=sc.connect(account=os.environ['SF_ACCOUNT'],user=os.environ['SF_USER'],private_key=pkb,login_timeout=30);cur=con.cursor()
SCHEMA=os.environ.get('SF_SCHEMA','STOCKHOUSE')
for q in ['use role ACCOUNTADMIN','use database BENCH2COST',f'use schema {SCHEMA}','use warehouse BENCH2COST_GEN2_XSMALL']: cur.execute(q)
cur.execute('alter table QUOTES drop clustering key'); print('QUOTES       -> clustering dropped')
cur.execute('''create or replace materialized view QUOTES_DAILY as
  select sym, TO_DATE(TO_TIMESTAMP_NTZ(t,3)) as day, count(*) as n_quotes,
         min(bp) as bp_min, max(bp) as bp_max, min(ap) as ap_min, max(ap) as ap_max,
         sum(bs) as bs_sum, sum("AS") as as_sum, sum(ap-bp) as spread_sum
  from QUOTES group by sym, TO_DATE(TO_TIMESTAMP_NTZ(t,3))''')
print('QUOTES_DAILY -> recreated UNCLUSTERED')
con.close()
PY
