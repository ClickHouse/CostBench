#!/usr/bin/env python3
"""Interleaved single-query, warm-cache scaling; immutable results in raw/*.jsonl."""
import argparse, datetime, hashlib, json, pathlib, random, re, time, uuid
from connection import request

ROOT=pathlib.Path(__file__).resolve().parent
QUERY_NAMES=['D1','D2','A1','A2','A3','A4']
THREADS=[1,2,4,8,16]

def queries():
    parsed=[]
    for file in ['queries_raw.sql','queries_mv.sql']:
        text=(ROOT/'queries'/file).read_text()
        text=re.sub(r'--[^\n]*','',text)
        parsed.extend(re.sub(r'\s+',' ',part).strip() for part in text.split(';') if part.strip())
    assert len(parsed)==6
    return dict(zip(QUERY_NAMES,parsed))

def workload_sql(query,threads,comment):
    # The guard exceeds every possible benchmark result cardinality; confirm no truncation.
    if not re.search(r'\bLIMIT\b',query,re.I): query+=' LIMIT 1000000'
    return query+f" SETTINGS max_threads={threads},enable_parallel_replicas=0,max_parallel_replicas=1,use_query_cache=0,log_queries=1,log_query_settings=1,log_queries_probability=1,log_queries_min_query_duration_ms=0,max_execution_time=30,timeout_before_checking_execution_speed=0,max_rows_to_read=1000000000,max_bytes_to_read=100000000000,max_result_rows=1000000,result_overflow_mode='throw',log_comment='{comment}' FORMAT Null"

def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--phase',choices=['pilot','warmup','measured'],required=True)
    parser.add_argument('--rounds',type=int,required=True)
    args=parser.parse_args()
    stamp=datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ')
    run_id='reader-scaling-'+stamp+'-'+args.phase
    plan=[]
    rng=random.Random(6112026)
    q=queries()
    for round_no in range(args.rounds):
        names=QUERY_NAMES[round_no%6:]+QUERY_NAMES[:round_no%6]
        for qi,name in enumerate(names):
            levels=THREADS[:]
            rng.shuffle(levels)
            for threads in levels:
                plan.append({'round':round_no+1,'query':name,'max_threads':threads})
    manifest={'run_id':run_id,'phase':args.phase,'started_at_utc':stamp,'dataset':'quotes_async_t4','threads':THREADS,'rounds':args.rounds,'trials':len(plan),'seed':6112026,'schedule':'sequential, interleaved randomized thread limits; rotated query order; no query overlap','cache':'result cache explicitly disabled; underlying data and query-condition caches allowed warm','changes':'max_threads only between treatments; settings and data otherwise fixed','query_source_hashes':{n:hashlib.sha256(s.encode()).hexdigest() for n,s in q.items()},'plan':plan}
    (ROOT/'raw'/(run_id+'-manifest.json')).write_text(json.dumps(manifest,indent=2)+'\n')
    output=ROOT/'raw'/(run_id+'.jsonl')
    print('START',run_id,len(plan),'trials',flush=True)
    for i,entry in enumerate(plan):
        query_id=run_id+'-'+str(i+1)+'-'+entry['query']+'-t'+str(entry['max_threads'])
        record={**entry,'phase':args.phase,'run_id':run_id,'sequence':i+1,'query_id':query_id,'client_started_at_utc':datetime.datetime.now(datetime.timezone.utc).isoformat()}
        sql=workload_sql(q[entry['query']],entry['max_threads'],run_id)
        record['sql_sha256']=hashlib.sha256(sql.encode()).hexdigest()
        sql_path=ROOT/'sql'/('trial-'+entry['query']+'-t'+str(entry['max_threads'])+'.sql')
        if not sql_path.exists(): sql_path.write_text(sql+'\n')
        try:
            response=request(sql,query_id)
            record.update({'http_summary':response['summary'],'client_wall_s':response['client_wall_s'],'success':True})
            assert int(response['summary'].get('read_bytes',0))>0,'Missing bytes read'
            assert int(response['summary'].get('elapsed_ns',0))>0,'Missing server elapsed time'
        except Exception as error:
            record.update({'success':False,'error':str(error)})
        with output.open('a') as stream: stream.write(json.dumps(record)+'\n')
        print(args.phase,i+1,'/',len(plan),entry['query'],'threads',entry['max_threads'],'server_ms',round(int(record.get('http_summary',{}).get('elapsed_ns',0))/1e6,3),'read_GB',round(int(record.get('http_summary',{}).get('read_bytes',0))/1e9,4),'ok',record['success'],flush=True)
        if not record['success']: raise SystemExit('Stopped on failure; failed observation preserved.')
    print('FINISH',run_id,flush=True)

if __name__=='__main__': main()
