#!/bin/bash
# Freshness of the QUOTES_DAILY rollup vs raw QUOTES.
#
# IMPORTANT: if QUOTES_DAILY is a MATERIALIZED VIEW, query results are ALWAYS
# consistent (Snowflake merges the MV with un-merged base rows at query time), so
# the row-backlog below is ~0 BY DESIGN and tells you nothing about maintenance lag.
# For an MV the real lag signal is `behind_by` (and the cost shows up as slower
# dashboard queries). The row-backlog is only meaningful for a DYNAMIC TABLE
# (physical, returns its last-refreshed/stale state).
#   bash lag.sh [eps]   (eps used only to convert DT backlog rows -> seconds; default 1M)
export PATH="$HOME/.local/bin:$PATH"
cd /home/ubuntu/bench && source .venv/bin/activate
: "${SF_ACCOUNT:?set SF_ACCOUNT, e.g. export SF_ACCOUNT=ORG-ACCT}"
: "${SF_USER:?set SF_USER, e.g. export SF_USER=MYUSER}"
EPS="${1:-1000000}"
python - "$EPS" <<'PY'
import sys, os, snowflake.connector as sc
from cryptography.hazmat.primitives import serialization
eps=float(sys.argv[1])
pk=serialization.load_pem_private_key(open('keys/rsa_key.p8','rb').read(),password=None)
pkb=pk.private_bytes(serialization.Encoding.DER,serialization.PrivateFormat.PKCS8,serialization.NoEncryption())
con=sc.connect(account=os.environ['SF_ACCOUNT'],user=os.environ['SF_USER'],private_key=pkb,login_timeout=30);cur=con.cursor()
SCHEMA=os.environ.get('SF_SCHEMA','STOCKHOUSE')
for q in ['use role ACCOUNTADMIN','use database BENCH2COST',f'use schema {SCHEMA}','use warehouse BENCH2COST_SMALL_GEN2']: cur.execute(q)
def show_one(sql):
    try:
        cur.execute(sql); rows=cur.fetchall()
        return dict(zip([c[0].lower() for c in cur.description], rows[0])) if rows else None
    except Exception:
        return None
mv = show_one(f"show materialized views like 'QUOTES_DAILY' in schema BENCH2COST.{SCHEMA}")
dt = show_one(f"show dynamic tables like 'QUOTES_DAILY' in schema BENCH2COST.{SCHEMA}")
if mv:
    print("QUOTES_DAILY is a MATERIALIZED VIEW")
    print(f"  behind_by (the lag signal) : {mv.get('behind_by')}")
    print(f"  refreshed_on               : {mv.get('refreshed_on')}   invalid={mv.get('invalid')}")
    print( "  (query results are always consistent -> watch DASHBOARD QUERY LATENCY for the lag impact)")
elif dt:
    print("QUOTES_DAILY is a DYNAMIC TABLE")
    print(f"  target_lag                 : {dt.get('target_lag')}")
    print(f"  mean_lag_sec / max_lag_sec : {dt.get('mean_lag_sec')} / {dt.get('maximum_lag_sec')}")
    cur.execute('select count(*) from QUOTES'); raw=cur.fetchone()[0]
    cur.execute('select coalesce(sum(n_quotes),0) from QUOTES_DAILY'); reflected=int(cur.fetchone()[0])
    backlog=raw-reflected
    print(f"  raw rows / reflected in DT : {raw:,} / {reflected:,}")
    print(f"  backlog (rows)             : {backlog:,}   (~{backlog/eps:.1f}s @ {eps:,.0f} EPS)")
else:
    print("QUOTES_DAILY not found as MV or Dynamic Table.")
con.close()
PY
