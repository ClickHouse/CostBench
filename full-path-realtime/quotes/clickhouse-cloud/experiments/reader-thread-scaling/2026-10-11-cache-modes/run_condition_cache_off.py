#!/usr/bin/env python3
"""Measure the second cold case: all six core points, query-condition cache explicitly disabled."""
import datetime, hashlib, json, pathlib, random
from connection import request
from run_cache_test import queries, workload_sql, CORES
ROOT=pathlib.Path(__file__).resolve().parent

def main():
    stamp=datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ')
    run_id='reader-qccoff-'+stamp
    rng=random.Random(6112027);plan=[]
    for n in range(1,13):
        cells=[(q,c) for q in ['D1','D2'] for c in CORES];rng.shuffle(cells)
        for q,c in cells:
            modes=['cold']
            for m in modes:plan.append({'round':n,'query':q,'max_threads':c,'cache_mode':m,'query_condition_cache':0})
    (ROOT/'raw'/(run_id+'-manifest.json')).write_text(json.dumps({'run_id':run_id,'phase':'qcc-off','trials':len(plan),'rounds':12,'seed':6112027,'plan':plan,'scope':'Cold data-cache comparison with query-condition cache explicitly off; no overlapping queries or retries'},indent=2)+'\n')
    q=queries();ledger=ROOT/'raw'/(run_id+'.jsonl');assert not ledger.exists()
    print('START',run_id,len(plan),'trials',flush=True)
    for i,cell in enumerate(plan,1):
        qid=f"{run_id}-{i}-{cell['query']}-t{cell['max_threads']}-{cell['cache_mode']}-qcc0"
        sql=workload_sql(q[cell['query']],cell['max_threads'],cell['cache_mode'],run_id).replace('use_query_cache=0','use_query_cache=0,use_query_condition_cache=0')
        (ROOT/'sql'/f"qccoff-{i:03}-{cell['query']}-t{cell['max_threads']}-{cell['cache_mode']}.sql").write_text(sql+'\n')
        row={**cell,'sequence':i,'phase':'qcc-off','run_id':run_id,'query_id':qid,'client_started_at_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'sql_sha256':hashlib.sha256(sql.encode()).hexdigest()}
        try:
            r=request(sql,qid);row.update({'success':True,'http_summary':r['summary'],'client_wall_s':r['client_wall_s']});assert int(r['summary']['read_bytes'])>0
        except Exception as error:row.update({'success':False,'error':str(error)})
        with ledger.open('a') as stream:stream.write(json.dumps(row)+'\n')
        print('qccoff',i,'/',len(plan),cell['query'],cell['max_threads'],cell['cache_mode'],'server_ms',round(int(row.get('http_summary',{}).get('elapsed_ns',0))/1e6,3),'ok',row['success'],flush=True)
        if not row['success']:raise SystemExit('Stopped; failed observation preserved.')
    print('FINISH',run_id,flush=True)

if __name__=='__main__':main()
