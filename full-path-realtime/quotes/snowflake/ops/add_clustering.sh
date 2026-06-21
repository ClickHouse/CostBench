#!/bin/bash
# Add clustering for the clustering experiment:
#   QUOTES       -> CLUSTER BY (sym, t)    (mirrors CH ORDER BY (sym, t))
#   QUOTES_DAILY -> CLUSTER BY (sym, day)  (sym leading -> prefix pruning for sym-filtered dashboards)
# Best run right AFTER reset.sh (empty QUOTES) so the MV rebuild is instant.
# Automatic Clustering then maintains both during ingest -> a serverless cost;
# track it via SNOWFLAKE.ACCOUNT_USAGE.AUTOMATIC_CLUSTERING_HISTORY.
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
cur.execute('alter table QUOTES cluster by (sym, t)'); print('QUOTES       -> CLUSTER BY (sym, t)')
cur.execute('''create or replace materialized view QUOTES_DAILY cluster by (sym, day) as
  select sym, TO_DATE(TO_TIMESTAMP_NTZ(t,3)) as day, count(*) as n_quotes,
         min(bp) as bp_min, max(bp) as bp_max, min(ap) as ap_min, max(ap) as ap_max,
         sum(bs) as bs_sum, sum("AS") as as_sum, sum(ap-bp) as spread_sum
  from QUOTES group by sym, TO_DATE(TO_TIMESTAMP_NTZ(t,3))''')
print('QUOTES_DAILY -> CLUSTER BY (sym, day)')
def cl(show):
    cur.execute(show); d=dict(zip([c[0].lower() for c in cur.description], cur.fetchall()[0])); return d.get('cluster_by')
print('verify QUOTES.cluster_by       =', cl(f"show tables like 'QUOTES' in schema BENCH2COST.{SCHEMA}"))
print('verify QUOTES_DAILY.cluster_by =', cl(f"show materialized views like 'QUOTES_DAILY' in schema BENCH2COST.{SCHEMA}"))
con.close()
PY
