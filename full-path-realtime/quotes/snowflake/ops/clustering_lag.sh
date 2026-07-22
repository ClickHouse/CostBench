#!/bin/bash
# Track CLUSTERING LAG over time during a clustered re-run — the analogue of mv_latency.sh
# for the MV refresh lag. Polls SYSTEM$CLUSTERING_INFORMATION for the raw table (SF_RAW_TABLE,
# default QUOTES; T2: QUOTES_IT, clustered by (sym,t)) and the rollup (SF_MV_TABLE, default
# QUOTES_DAILY) every N seconds and appends one JSON line per poll with each object's
# average_depth / average_overlaps / total_partition_count + polled_at UTC.
# Set SF_CLUSTER_MV=0 to sample the raw only (T2: the interactive MV has no CLUSTER BY).
# Prefer running on the TRACKING warehouse (set SF_WAREHOUSE=$SF_TRACK_WAREHOUSE) so it does not
# perturb the measured read warehouse.
#
# WHY: clustering depth/overlap is the only "how far behind is Automatic Clustering" signal,
# and it is POINT-IN-TIME — Snowflake never historizes it (unlike ACCOUNT_USAGE.AUTOMATIC_
# CLUSTERING_HISTORY, which keeps the recluster *cost*). So you can only get the lag-over-time
# curve by sampling it live, like this. average_depth rises as time-ordered ingest lands
# (sym,t)-disordered partitions and falls as AC reclusters -> a sawtooth (lower = better).
#
# COST/NOTES: SYSTEM$CLUSTERING_INFORMATION samples partition metadata; on a ~100B-row table
# it is heavier than SHOW (seconds, not instant) though it runs without a virtual warehouse.
# Poll gently — default 300s (5 min), not 60s — so it doesn't perturb the benchmark.
#
#   bash clustering_lag.sh [interval_sec] [output_file]
#     interval_sec : default 300
#     output_file  : default out/clustering_lag.jsonl
# Detached for a long run:
#   setsid nohup bash clustering_lag.sh 300 > out/clustering_lag_run.log 2>&1 < /dev/null &
export PATH="$HOME/.local/bin:$PATH"
cd /home/ubuntu/bench && source .venv/bin/activate
: "${SF_ACCOUNT:?set SF_ACCOUNT, e.g. export SF_ACCOUNT=ORG-ACCT}"
: "${SF_USER:?set SF_USER, e.g. export SF_USER=MYUSER}"
INTERVAL="${1:-300}"; OUT="${2:-out/clustering_lag.jsonl}"
python - "$INTERVAL" "$OUT" <<'PY'
import sys, json, time, signal, os
from datetime import datetime, timezone
import snowflake.connector as sc
from cryptography.hazmat.primitives import serialization
INTERVAL=float(sys.argv[1]); OUT=sys.argv[2]
os.makedirs(os.path.dirname(OUT) or '.', exist_ok=True)
stop={'v':False}
signal.signal(signal.SIGINT,  lambda s,f: stop.__setitem__('v',True))
signal.signal(signal.SIGTERM, lambda s,f: stop.__setitem__('v',True))
def keypair():
    pk=serialization.load_pem_private_key(open('keys/rsa_key.p8','rb').read(),password=None)
    return pk.private_bytes(serialization.Encoding.DER,serialization.PrivateFormat.PKCS8,serialization.NoEncryption())
pkb=keypair()
def connect():
    c=sc.connect(account=os.environ['SF_ACCOUNT'],user=os.environ['SF_USER'],private_key=pkb,login_timeout=30)
    cur=c.cursor(); cur.execute('use role ACCOUNTADMIN')
    # SYSTEM$CLUSTERING_INFORMATION needs no virtual warehouse; set one only if provided.
    wh=os.environ.get('SF_WAREHOUSE')
    if wh: cur.execute(f'use warehouse {wh}')
    return c,cur
con,cur=connect()
# (object label, fully-qualified name) — clustering key is the table's DEFINED key, so omit it.
SCHEMA=os.environ.get('SF_SCHEMA','STOCKHOUSE')
RAW=os.environ.get('SF_RAW_TABLE','QUOTES')      # T2: QUOTES_IT (clustered by sym,t)
MV=os.environ.get('SF_MV_TABLE','QUOTES_DAILY')  # T2: QUOTES_DAILY_IMV is NOT clustered -> skip via SF_CLUSTER_MV=0
OBJS=[('raw',f'BENCH2COST.{SCHEMA}.{RAW}')]
if os.environ.get('SF_CLUSTER_MV','1')!='0' and MV:
    OBJS.append(('mv',f'BENCH2COST.{SCHEMA}.{MV}'))
def info(name):
    cur.execute(f"select system$clustering_information('{name}')")
    d=json.loads(cur.fetchone()[0])
    return (d.get('average_depth'), d.get('average_overlaps'),
            d.get('total_partition_count'), d.get('total_constant_partition_count'))
print(f"Polling clustering depth every {INTERVAL:g}s -> {OUT} (Ctrl-C to stop)", file=sys.stderr, flush=True)
def slp(s):
    t=0.0
    while t<s and not stop['v']:
        d=min(0.5,s-t); time.sleep(d); t+=d
while not stop['v']:
    polled=datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
    rec={'polled_at':polled}
    for label,name in OBJS:
        try:
            depth,overlaps,parts,const=info(name)
            rec[f'{label}_avg_depth']=depth
            rec[f'{label}_avg_overlaps']=overlaps
            rec[f'{label}_partitions']=parts
            rec[f'{label}_constant_partitions']=const
        except Exception as e:
            rec[f'{label}_error']=str(e)[:200]
            try: con.close()
            except: pass
            try: con,cur=connect()
            except Exception: pass
    with open(OUT,'a') as f: f.write(json.dumps(rec, default=str)+'\n')
    print(f"{polled}  raw_depth={rec.get('raw_avg_depth','?')}  mv_depth={rec.get('mv_avg_depth','?')}",
          file=sys.stderr, flush=True)
    slp(INTERVAL)
print("stopped.", file=sys.stderr)
con.close()
PY
