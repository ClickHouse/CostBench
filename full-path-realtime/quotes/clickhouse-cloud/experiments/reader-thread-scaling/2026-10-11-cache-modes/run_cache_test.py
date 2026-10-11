#!/usr/bin/env python3
"""Paired hot/cold drill-down scaling; per-query cache controls, no global cache purge."""
import argparse, csv, datetime, hashlib, json, pathlib, random, re
from connection import request

ROOT = pathlib.Path(__file__).resolve().parent
CORES = [2, 4, 8, 16, 30, 59]
CACHE = {
    'hot': 'enable_filesystem_cache=1,use_page_cache_with_distributed_cache=1,force_read_through_distributed_cache=1',
    'filesystem-off': 'enable_filesystem_cache=0,use_page_cache_with_distributed_cache=1,force_read_through_distributed_cache=1',
    'cold': 'enable_filesystem_cache=0,use_page_cache_with_distributed_cache=0,force_read_through_distributed_cache=0',
}

def queries():
    text = re.sub(r'--[^\n]*', '', (ROOT/'queries/queries_raw.sql').read_text())
    parsed = [re.sub(r'\s+', ' ', part).strip() for part in text.split(';') if part.strip()]
    assert len(parsed) == 2
    return dict(zip(['D1', 'D2'], parsed))

def workload_sql(query, cores, cache, comment):
    if not re.search(r'\bLIMIT\b', query, re.I): query += ' LIMIT 1000000'
    return query + f" SETTINGS max_threads={cores},{CACHE[cache]},use_uncompressed_cache=0,use_query_cache=0,enable_parallel_replicas=0,max_parallel_replicas=1,log_queries=1,log_query_settings=1,log_queries_probability=1,log_queries_min_query_duration_ms=0,max_execution_time=30,timeout_before_checking_execution_speed=0,max_rows_to_read=1000000000,max_bytes_to_read=100000000000,max_result_rows=1000000,result_overflow_mode='throw',log_comment='{comment}' FORMAT Null"

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--phase', choices=['pilot', 'warmup', 'measured'], required=True)
    parser.add_argument('--rounds', type=int, required=True)
    args = parser.parse_args()
    assert 1 <= args.rounds <= 12
    identity = list(csv.DictReader((ROOT/'results/00-identity-before.csv').open()))[0]
    assert float(identity['vCPUs']) == 59
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ')
    run_id = 'reader-cache-' + stamp + '-' + args.phase
    levels = [16,59] if args.phase == 'pilot' else CORES
    modes = ['hot', 'filesystem-off', 'cold'] if args.phase == 'pilot' else ['hot'] if args.phase == 'warmup' else ['hot', 'cold']
    rng = random.Random(6112027)
    plan = []
    for round_no in range(1, args.rounds+1):
        cells = [(q,c) for q in ['D1','D2'] for c in levels]
        rng.shuffle(cells)
        for query, cores in cells:
            order = modes[:]
            if args.phase != 'warmup': rng.shuffle(order)
            for cache in order:
                plan.append({'round':round_no,'query':query,'max_threads':cores,'cache_mode':cache})
    q = queries()
    manifest = {'run_id':run_id,'phase':args.phase,'rounds':args.rounds,'trials':len(plan),'cores':levels,'cache_modes':modes,'cache_settings':CACHE,'seed':6112027,'plan':plan,'schedule':'Sequential paired cache treatments; fixed randomized core/query cells; no overlap or automatic retries','cache_scope':'Hot: filesystem, page and distributed data caches. Cold: bypass those three data-cache layers per query. Metadata/mark/query-condition caches are held at the same settings. Query-result and uncompressed caches explicitly off in both. Pilot also isolates enable_filesystem_cache alone.','queries':{name:hashlib.sha256(sql.encode()).hexdigest() for name,sql in q.items()}}
    (ROOT/'raw'/(run_id+'-manifest.json')).write_text(json.dumps(manifest,indent=2)+'\n')
    ledger = ROOT/'raw'/(run_id+'.jsonl')
    assert not ledger.exists()
    print('START',run_id,len(plan),'trials',flush=True)
    for i, cell in enumerate(plan,1):
        query_id = f"{run_id}-{i}-{cell['query']}-t{cell['max_threads']}-{cell['cache_mode']}"
        sql = workload_sql(q[cell['query']],cell['max_threads'],cell['cache_mode'],run_id)
        target = ROOT/'sql'/f"{args.phase}-{i:03}-{cell['query']}-t{cell['max_threads']}-{cell['cache_mode']}.sql"
        target.write_text(sql+'\n')
        record = {**cell,'sequence':i,'phase':args.phase,'run_id':run_id,'query_id':query_id,'client_started_at_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'sql_sha256':hashlib.sha256(sql.encode()).hexdigest()}
        try:
            r = request(sql,query_id)
            record.update({'success':True,'http_summary':r['summary'],'client_wall_s':r['client_wall_s']})
            assert int(r['summary']['read_bytes']) > 0 and int(r['summary']['elapsed_ns']) > 0
        except Exception as error:
            record.update({'success':False,'error':str(error)})
        with ledger.open('a') as stream:stream.write(json.dumps(record)+'\n')
        print(args.phase,i,'/',len(plan),cell['query'],cell['max_threads'],cell['cache_mode'],'server_ms',round(int(record.get('http_summary',{}).get('elapsed_ns',0))/1e6,3),'ok',record['success'],flush=True)
        if not record['success']:raise SystemExit('Stopped; failed observation preserved.')
    print('FINISH',run_id,flush=True)

if __name__ == '__main__':main()
