#!/bin/bash
# Track interactive-table REFRESH duration + lag over time. Polls
# INFORMATION_SCHEMA.INTERACTIVE_TABLE_REFRESH_HISTORY (purpose-built for interactive tables;
# returns the same columns/rows as the documented DYNAMIC_TABLE_REFRESH_HISTORY, which is the
# fallback if this synonym ever changes) and appends one JSONL line per *new*
# refresh event (deduped) with all columns + polled_at. Duration = refresh_end - refresh_start;
# lag adherence tells us if each refresh warehouse (BENCH2COST_GEN2_SMALL_1 / _2) is sized right.
#
# Runs on the tracking warehouse (BENCH) so it adds NO load/cost to the measured warehouses.
#   bash it_refresh.sh [interval_sec] [output_file]
#     interval_sec : default 60
#     output_file  : default out/it_refresh.jsonl
# Detached:  setsid nohup bash it_refresh.sh 60 out/it_refresh_<ts>.jsonl >log 2>&1 </dev/null &
export PATH="$HOME/.local/bin:$PATH"
cd /home/ubuntu/bench && source .venv/bin/activate
: "${SF_ACCOUNT:?set SF_ACCOUNT}"
: "${SF_USER:?set SF_USER}"
SCHEMA="${SF_SCHEMA:-STOCKHOUSE}"
TRACK_WH="${SF_TRACK_WAREHOUSE:-BENCH}"
INTERVAL="${1:-60}"; OUT="${2:-out/it_refresh.jsonl}"
python - "$INTERVAL" "$OUT" "$SCHEMA" "$TRACK_WH" <<'PY'
import sys, json, time, signal, os
from datetime import datetime, timezone, date
from decimal import Decimal
import snowflake.connector as sc
from cryptography.hazmat.primitives import serialization
INTERVAL=float(sys.argv[1]); OUT=sys.argv[2]; SCHEMA=sys.argv[3]; TRACK_WH=sys.argv[4]
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
    cur=c.cursor()
    for q in ("use role ACCOUNTADMIN", f"use warehouse {TRACK_WH}",
              "use database BENCH2COST", f"use schema {SCHEMA}"):
        cur.execute(q)
    return c,cur
con,cur=connect()
SQL=("select * from table(information_schema.interactive_table_refresh_history()) "
     f"where database_name='BENCH2COST' and schema_name='{SCHEMA}'")
def keyof(d):
    # Dedup by (refresh identity, STATE) so a refresh first seen EXECUTING is still logged
    # again when it flips to SUCCEEDED/FAILED — otherwise the final row (with refresh_end_time
    # and the real duration) is dropped for any refresh longer than the poll interval.
    qid=d.get('query_id')
    base = str(qid) if qid else f"{d.get('name')}|{d.get('refresh_start_time')}|{d.get('data_timestamp')}"
    return f"{base}|{d.get('state')}"
def dur(d):
    s,e=d.get('refresh_start_time'), d.get('refresh_end_time')
    if isinstance(s,datetime) and isinstance(e,datetime): return (e-s).total_seconds()
    return None
seen=set()
print(f"Polling IT refresh history every {INTERVAL:g}s on {TRACK_WH} -> {OUT} (Ctrl-C to stop)", file=sys.stderr, flush=True)
def slp(s):
    t=0.0
    while t<s and not stop['v']:
        d=min(0.5,s-t); time.sleep(d); t+=d
while not stop['v']:
    polled=datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
    try:
        cur.execute(SQL); cols=[c[0].lower() for c in cur.description]; rows=cur.fetchall()
        new=0
        for r in rows:
            d={c:v for c,v in zip(cols,r)}
            k=keyof(d)
            if k in seen: continue
            seen.add(k); new+=1
            rec={'polled_at':polled, 'duration_sec':dur(d)}
            rec.update({c:jval(v) for c,v in d.items()})
            with open(OUT,'a') as f: f.write(json.dumps(rec, default=str)+'\n')
        last=rows[-1] if rows else None
        ld={c:v for c,v in zip(cols,last)} if last else {}
        print(f"{polled}  +{new} new  (latest: {ld.get('name')} state={ld.get('state')} dur={dur(ld)}s)",
              file=sys.stderr, flush=True)
    except Exception as e:
        print(f"{polled}  ERROR {str(e)[:200]}", file=sys.stderr, flush=True)
        try: con.close()
        except: pass
        try: con,cur=connect()
        except Exception: pass
    slp(INTERVAL)
print("stopped.", file=sys.stderr)
con.close()
PY
