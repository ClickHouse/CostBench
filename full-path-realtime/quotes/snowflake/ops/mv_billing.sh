#!/bin/bash
# Track the benchmark's serverless + warehouse credits over a recent window:
#   - ingest warehouse (X-Small)          compute for COPY
#   - MV maintenance (QUOTES_DAILY)        serverless, always-fresh rollup
#   - Automatic Clustering (QUOTES + MV)   serverless reclustering (only when CLUSTER BY is set)
#   - reader warehouse (Small)             dashboard/drilldown queries
# Source = SNOWFLAKE.ACCOUNT_USAGE (latency up to ~3h, so the final total settles
# a few hours after the run ends).
#   bash mv_billing.sh [hours] [interval_sec]
#     hours        : lookback window (default 25 -> covers a 24h run)
#     interval_sec : 0 = one-shot (default); >0 = poll every N s, append out/mv_billing.jsonl
export PATH="$HOME/.local/bin:$PATH"
cd /home/ubuntu/bench && source .venv/bin/activate
: "${SF_ACCOUNT:?set SF_ACCOUNT, e.g. export SF_ACCOUNT=ORG-ACCT}"
: "${SF_USER:?set SF_USER, e.g. export SF_USER=MYUSER}"
HOURS="${1:-25}"; INTERVAL="${2:-0}"
python - "$HOURS" "$INTERVAL" <<'PY'
import sys, json, time, os
from datetime import datetime, timezone
import snowflake.connector as sc
from cryptography.hazmat.primitives import serialization
HOURS=float(sys.argv[1]); INTERVAL=float(sys.argv[2]); RATE=3.0  # $/credit (Enterprise on-demand; adjust)
pk=serialization.load_pem_private_key(open('keys/rsa_key.p8','rb').read(),password=None)
pkb=pk.private_bytes(serialization.Encoding.DER,serialization.PrivateFormat.PKCS8,serialization.NoEncryption())
con=sc.connect(account=os.environ['SF_ACCOUNT'],user=os.environ['SF_USER'],private_key=pkb,login_timeout=30);cur=con.cursor()
cur.execute('use role ACCOUNTADMIN'); cur.execute('use warehouse BENCH2COST_GEN2_XSMALL')
W=f"dateadd(hour,-{HOURS},current_timestamp())"
def snap():
    cur.execute(f"""select count(*), coalesce(sum(credits_used),0), max(end_time)
        from snowflake.account_usage.materialized_view_refresh_history
        where table_name='QUOTES_DAILY' and start_time >= {W}""")
    n,mvc,last=cur.fetchone()
    cur.execute(f"""select warehouse_name, coalesce(sum(credits_used),0)
        from snowflake.account_usage.warehouse_metering_history
        where start_time >= {W} and warehouse_name in ('BENCH2COST_GEN2_XSMALL','BENCH2COST_SMALL_GEN2') group by 1""")
    wh={name:float(c) for name,c in cur.fetchall()}
    ac={}
    try:
        cur.execute(f"""select table_name, coalesce(sum(credits_used),0)
            from snowflake.account_usage.automatic_clustering_history
            where start_time >= {W} and table_name in ('QUOTES','QUOTES_DAILY') group by 1""")
        ac={name:float(c) for name,c in cur.fetchall()}
    except Exception as e:
        print('  (auto-clustering history unavailable:', str(e)[:60], ')')
    return int(n), float(mvc), last, wh, ac
def report():
    n,mvc,last,wh,ac=snap()
    ingest=wh.get('BENCH2COST_GEN2_XSMALL',0.0); reader=wh.get('BENCH2COST_SMALL_GEN2',0.0)
    ac_q=ac.get('QUOTES',0.0); ac_mv=ac.get('QUOTES_DAILY',0.0)
    total=mvc+ingest+reader+ac_q+ac_mv; ts=datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
    print(f"--- {ts}  (last {HOURS:g}h; ACCOUNT_USAGE lags up to ~3h) ---")
    print(f"  Ingest WH (X-Small):            {ingest:.4f} credits")
    print(f"  MV maintenance (QUOTES_DAILY):  {mvc:.4f} credits   ({n} refreshes, last end {last})")
    print(f"  Auto-clustering QUOTES:         {ac_q:.4f} credits")
    print(f"  Auto-clustering QUOTES_DAILY:   {ac_mv:.4f} credits")
    print(f"  Reader WH (Small):              {reader:.4f} credits")
    print(f"  TOTAL: {total:.4f} credits  (~${total*RATE:.2f} at ${RATE:g}/credit)")
    return {"ts":ts,"window_hours":HOURS,"mv_refreshes":n,"mv_credits":round(mvc,4),
            "mv_last_refresh_end":str(last),"ingest_wh_credits":round(ingest,4),
            "reader_wh_credits":round(reader,4),"autoclust_quotes_credits":round(ac_q,4),
            "autoclust_quotes_daily_credits":round(ac_mv,4),"total_credits":round(total,4)}
if INTERVAL<=0:
    report()
else:
    os.makedirs('out',exist_ok=True)
    while True:
        rec=report()
        with open('out/mv_billing.jsonl','a') as f: f.write(json.dumps(rec)+"\n")
        time.sleep(INTERVAL)
con.close()
PY
