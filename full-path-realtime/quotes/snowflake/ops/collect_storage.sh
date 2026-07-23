#!/bin/bash
# =============================================================================
# Emit storage.json (active compressed bytes + row count, raw table + rollup) for the storage chart
# — SNOWFLAKE ONLY. ACTIVE_BYTES from INFORMATION_SCHEMA.TABLE_STORAGE_METRICS + ROW_COUNT from
# INFORMATION_SCHEMA.TABLES, for SF_RAW_TABLE and SF_MV_TABLE in SF_SCHEMA.
#
#   SF_SCHEMA=STOCKHOUSE_T1 SF_RAW_TABLE=QUOTES_IT SF_MV_TABLE=QUOTES_DAILY_IT \
#     bash ops/collect_storage.sh [out_json]
#   T2 (interactive MV — raw=QUOTES_IT, rollup=QUOTES_DAILY_IMV):
#   SF_SCHEMA=STOCKHOUSE_T2_RUN8 SF_RAW_TABLE=QUOTES_IT SF_MV_TABLE=QUOTES_DAILY_IMV \
#     bash ops/collect_storage.sh [out_json]
#   default out_json: out_<tn>/storage.json   (tn = lowercased schema suffix)
# Then: cp out_t1/storage.json quotes/snowflake/results/t1/
#
# NOTE: interactive tables should appear in TABLE_STORAGE_METRICS; if a value comes back null,
# verify the table name / that the metrics view has caught up (it can lag minutes), or use the
# manual SQL in runbook §5a. An interactive *materialized view* (e.g. QUOTES_DAILY_IMV, T2) often
# does NOT surface in TABLE_STORAGE_METRICS at all — this script falls back to SHOW MATERIALIZED
# VIEWS / SHOW TABLES (metadata-only bytes+rows) automatically when the metrics view returns null.
# =============================================================================
export PATH="$HOME/.local/bin:$PATH"
cd /home/ubuntu/bench && source .venv/bin/activate
: "${SF_ACCOUNT:?set SF_ACCOUNT}"
: "${SF_USER:?set SF_USER}"
: "${SF_SCHEMA:?set SF_SCHEMA, e.g. export SF_SCHEMA=STOCKHOUSE_T1}"
RAW_TABLE="${SF_RAW_TABLE:-QUOTES}"; MV_TABLE="${SF_MV_TABLE:-QUOTES_DAILY}"
TRACK_WH="${SF_TRACK_WAREHOUSE:-BENCH}"
TN="$(printf '%s' "${SF_SCHEMA##*_}" | tr 'A-Z' 'a-z')"
OUT="${1:-out_${TN}/storage.json}"
case "$RAW_TABLE" in *_IT|*_IMV) SF_LABEL="Snowflake IT";; *) SF_LABEL="Snowflake";; esac

python - "$OUT" "$SF_SCHEMA" "$RAW_TABLE" "$MV_TABLE" "$TRACK_WH" "$SF_LABEL" "$TN" <<'PY'
import os, sys, json
import snowflake.connector as sc
from cryptography.hazmat.primitives import serialization
OUT,SCHEMA,RAW,MV,WH,SFLABEL,TN = sys.argv[1:8]
def num(x):
    try: return int(x)
    except Exception: return None
pk=serialization.load_pem_private_key(open('keys/rsa_key.p8','rb').read(),password=None)
pkb=pk.private_bytes(serialization.Encoding.DER,serialization.PrivateFormat.PKCS8,serialization.NoEncryption())
con=sc.connect(account=os.environ['SF_ACCOUNT'],user=os.environ['SF_USER'],private_key=pkb,login_timeout=30)
cur=con.cursor()
for q in ("use role ACCOUNTADMIN", f"use warehouse {WH}", "use database BENCH2COST", f"use schema {SCHEMA}"):
    cur.execute(q)
def show_fallback(tbl):
    # metadata-only bytes+rows for objects missing from TABLE_STORAGE_METRICS (e.g. an
    # interactive MV). Try SHOW MATERIALIZED VIEWS then SHOW TABLES; read bytes/rows by col name.
    for stmt in (f"SHOW MATERIALIZED VIEWS LIKE '{tbl}' IN SCHEMA BENCH2COST.{SCHEMA}",
                 f"SHOW TABLES LIKE '{tbl}' IN SCHEMA BENCH2COST.{SCHEMA}"):
        try:
            cur.execute(stmt); row=cur.fetchone()
            if not row: continue
            cols={d[0].lower():i for i,d in enumerate(cur.description)}
            b=num(row[cols['bytes']]) if 'bytes' in cols else None
            r=num(row[cols['rows']])  if 'rows'  in cols else None
            if b is not None or r is not None:
                print(f"  fell back to SHOW for {tbl}: bytes={b} rows={r}", file=sys.stderr)
                return b,r
        except Exception as e: print(f"  WARN show {tbl}: {str(e)[:120]}", file=sys.stderr)
    return None,None
def sf(tbl):
    b=r=None
    try:
        cur.execute(f"""select active_bytes from BENCH2COST.information_schema.table_storage_metrics
                        where table_schema='{SCHEMA}' and table_name='{tbl}'""")
        row=cur.fetchone(); b=num(row[0]) if row else None
    except Exception as e: print(f"  WARN bytes {tbl}: {str(e)[:120]}", file=sys.stderr)
    try:
        cur.execute(f"""select row_count from BENCH2COST.information_schema.tables
                        where table_schema='{SCHEMA}' and table_name='{tbl}'""")
        row=cur.fetchone(); r=num(row[0]) if row else None
    except Exception as e: print(f"  WARN rows {tbl}: {str(e)[:120]}", file=sys.stderr)
    if b is None or r is None:   # e.g. interactive MV not in TABLE_STORAGE_METRICS
        fb,fr=show_fallback(tbl)
        if b is None: b=fb
        if r is None: r=fr
    return b,r
raw_b,raw_r=sf(RAW); mv_b,mv_r=sf(MV)
print(f"Snowflake: {RAW}={raw_b}B/{raw_r} rows  {MV}={mv_b}B/{mv_r} rows", file=sys.stderr)
# ClickHouse entries are PLACEHOLDERS (bytes/rows = null). This script is Snowflake-only; fill the CH
# numbers in by hand before charting — from CH system.parts: sum(bytes_on_disk), sum(rows) on the raw
# 'quotes' table and the 'quotes_daily' rollup. Placeholders are skipped by the chart until filled.
doc={
  "raw":[{"system":"ClickHouse (AWS)","bytes":None,"rows":None},
         {"system":SFLABEL,"bytes":raw_b,"rows":raw_r}],
  "mv": [{"system":"ClickHouse (AWS)","bytes":None,"rows":None},
         {"system":SFLABEL,"bytes":mv_b,"rows":mv_r}],
  "note": (f"active on-disk size, compressed; {TN.upper()} ({SCHEMA}). "
           "ClickHouse bytes/rows are PLACEHOLDERS (null) — fill from CH system.parts before charting. "
           "Snowflake null = TABLE_STORAGE_METRICS lagging; re-run.")
}
os.makedirs(os.path.dirname(OUT) or '.', exist_ok=True)
json.dump(doc, open(OUT,'w'), indent=2)
print(f"wrote {OUT}", file=sys.stderr)
con.close()
PY
