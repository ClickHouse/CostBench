#!/bin/bash
# Poll SHOW MATERIALIZED VIEWS for the rollup MV every N seconds and append one
# JSON line per poll (all SHOW columns + polled_at UTC) to a JSONL file.
# SHOW is metadata-only (no warehouse), so this is ~free to run. `behind_by` is the lag signal
# and works for a regular MV (T0: QUOTES_DAILY) and an interactive MV (T2: QUOTES_DAILY_IMV).
# MV name comes from SF_MV_TABLE (default QUOTES_DAILY); schema from SF_SCHEMA.
#   SF_MV_TABLE=QUOTES_DAILY_IMV bash mv_latency.sh [interval_sec] [output_file]
#     interval_sec : default 60
#     output_file  : default out/mv_latency.jsonl
# Run detached for a long experiment:
#   setsid nohup bash mv_latency.sh 60 > out/mv_latency_run.log 2>&1 < /dev/null &
export PATH="$HOME/.local/bin:$PATH"
cd /home/ubuntu/bench && source .venv/bin/activate
: "${SF_ACCOUNT:?set SF_ACCOUNT, e.g. export SF_ACCOUNT=ORG-ACCT}"
: "${SF_USER:?set SF_USER, e.g. export SF_USER=MYUSER}"
INTERVAL="${1:-60}"; OUT="${2:-out/mv_latency.jsonl}"
python - "$INTERVAL" "$OUT" <<'PY'
import sys, json, time, signal, os
from datetime import datetime, timezone, date
from decimal import Decimal
import snowflake.connector as sc
from cryptography.hazmat.primitives import serialization
INTERVAL=float(sys.argv[1]); OUT=sys.argv[2]
os.makedirs(os.path.dirname(OUT) or '.', exist_ok=True)
stop={'v':False}
signal.signal(signal.SIGINT,  lambda s,f: stop.__setitem__('v',True))
signal.signal(signal.SIGTERM, lambda s,f: stop.__setitem__('v',True))
def jval(v):
    if isinstance(v,(datetime,date)): return v.isoformat()
    if isinstance(v,Decimal): return str(v)
    return v
def keypair():
    pk=serialization.load_pem_private_key(open('keys/rsa_key.p8','rb').read(),password=None)
    return pk.private_bytes(serialization.Encoding.DER,serialization.PrivateFormat.PKCS8,serialization.NoEncryption())
pkb=keypair()
def connect():
    c=sc.connect(account=os.environ['SF_ACCOUNT'],user=os.environ['SF_USER'],private_key=pkb,login_timeout=30)
    cur=c.cursor(); cur.execute('use role ACCOUNTADMIN'); return c,cur
con,cur=connect()
SCHEMA=os.environ.get('SF_SCHEMA','STOCKHOUSE')
MV=os.environ.get('SF_MV_TABLE','QUOTES_DAILY')
SQL=f"SHOW MATERIALIZED VIEWS LIKE '{MV}' IN SCHEMA BENCH2COST.{SCHEMA}"
print(f"Polling {MV} in {SCHEMA} every {INTERVAL:g}s -> {OUT} (Ctrl-C to stop)", file=sys.stderr, flush=True)
def slp(s):
    t=0.0
    while t<s and not stop['v']:
        d=min(0.5,s-t); time.sleep(d); t+=d
while not stop['v']:
    polled=datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
    rec={'polled_at':polled}
    try:
        cur.execute(SQL); cols=[c[0] for c in cur.description]; rows=cur.fetchall()
        if rows: rec.update({c:jval(v) for c,v in zip(cols,rows[0])})
        else:    rec['_no_rows']=True
    except Exception as e:
        rec['error']=str(e)[:300]
        try: con.close()
        except: pass
        try: con,cur=connect()
        except Exception: pass
    with open(OUT,'a') as f: f.write(json.dumps(rec, default=str)+'\n')
    print(f"{polled}  behind_by={rec.get('behind_by','?')}  rows={rec.get('rows','?')}  invalid={rec.get('invalid','?')}",
          file=sys.stderr, flush=True)
    slp(INTERVAL)
print("stopped.", file=sys.stderr)
con.close()
PY
