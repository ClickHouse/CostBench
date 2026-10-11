#!/usr/bin/env python3
"""Offline cache validation and pooled drill-down rates, with all trials retained."""
import csv, hashlib, json, pathlib, statistics
ROOT=pathlib.Path(__file__).resolve().parent
CORES=[2,4,8,16,30,59]

def quantile(values,p):
    v=sorted(values);x=(len(v)-1)*p;i=int(x);j=min(i+1,len(v)-1)
    return v[i]+(v[j]-v[i])*(x-i)

def write(name,rows):
    with (ROOT/'derived'/name).open('w',newline='') as stream:
        w=csv.DictWriter(stream,fieldnames=list(rows[0]));w.writeheader();w.writerows(rows)

def main():
    trials=[json.loads(line) for p in sorted((ROOT/'raw').glob('*.jsonl')) for line in p.read_text().splitlines()]
    logs=list(csv.DictReader((ROOT/'results/query-logs-all.csv').open()))
    byid={r['query_id']:r for r in logs}
    assert len(byid)==len(logs)==len(trials)
    assert len({r['query_id'] for r in trials})==len(trials)
    rows=[]
    for t in trials:
        prefix='qccoff' if t['phase']=='qcc-off' else t['phase']
        sql=(ROOT/'sql'/f"{prefix}-{t['sequence']:03}-{t['query']}-t{t['max_threads']}-{t['cache_mode']}.sql").read_text().rstrip('\n')
        assert hashlib.sha256(sql.encode()).hexdigest()==t['sql_sha256']
        assert t['success'];l=byid[t['query_id']];s=json.loads(l['settings_json']);e=json.loads(l['profile_events_json'])
        assert l['type']=='QueryFinish' and int(l['exception_code'])==0
        assert l['query_cache_usage']=='None' and s['use_query_cache']=='0'
        assert int(s.get('use_query_condition_cache',1))==t.get('query_condition_cache',1)
        assert int(s['max_threads'])==t['max_threads']
        for name,v in [('enable_filesystem_cache',int(t['cache_mode']=='hot')),('use_page_cache_with_distributed_cache',int(t['cache_mode']!='cold')),('force_read_through_distributed_cache',int(t['cache_mode']!='cold'))]:
            assert s[name]==str(v),(name,s,t)
        ns=int(t['http_summary']['elapsed_ns']);elapsed=ns/1e9
        assert abs(elapsed*1000-int(l['query_duration_ms']))<5
        assert int(l['read_bytes'])==int(t['http_summary']['read_bytes'])
        assert int(l['read_rows'])==int(t['http_summary']['read_rows'])
        cpu=(int(l['user_cpu_us'])+int(l['system_cpu_us']))/1e6
        assert cpu>0
        rows.append({**{k:t[k] for k in ['phase','round','query','max_threads','cache_mode','sequence','query_id','run_id']},
            'started_at_utc':l['started_at_utc'],'finished_at_utc':l['finished_at_utc'],'hostname':l['hostname'],
            'query_condition_cache':t.get('query_condition_cache',1),'query_condition_cache_hits':e.get('QueryConditionCacheHits',0),
            'server_elapsed_s':elapsed,'client_wall_s':t['client_wall_s'],'read_rows':int(l['read_rows']),'read_bytes':int(l['read_bytes']),
            'throughput_GB_s':int(l['read_bytes'])/1e9/elapsed,'cpu_s':cpu,'average_busy_cores':cpu/elapsed,
            'MB_s_per_busy_core':int(l['read_bytes'])/1e6/cpu,'MB_s_per_selected_core':int(l['read_bytes'])/1e6/elapsed/t['max_threads'],
            'peak_memory_bytes':int(l['peak_memory_bytes']),'result_rows':int(l['result_rows']),'query_cache_usage':l['query_cache_usage'],
            'page_cache_hits':e.get('PageCacheHits',0),'page_cache_misses':e.get('PageCacheMisses',0),'page_cache_read_bytes':e.get('PageCacheReadBytes',0),
            's3_read_bytes':e.get('ReadBufferFromS3Bytes',0),'distributed_cache_read_bytes':e.get('DistrCacheServerCachedReadBufferCacheReadBytes',0),
            'distributed_cache_object_read_bytes':e.get('DistrCacheServerCachedReadBufferObjectStorageReadBytes',0),
            'cpu_wait_s':int(l['cpu_wait_us'])/1e6,'io_wait_s':int(l['io_wait_us'])/1e6})
    measured=[r for r in rows if r['phase']=='measured'];assert len(measured)==288
    second=[r for r in rows if r['phase']=='qcc-off'];assert len(second)==144
    assert all(r['query_condition_cache']==0 and r['query_condition_cache_hits']==0 for r in second)
    combined=[];individual=[]
    for mode,qcc in [('hot',1),('cold',1),('cold',0)]:
        for cores in CORES:
            group=[r for r in measured+second if r['cache_mode']==mode and r['query_condition_cache']==qcc and r['max_threads']==cores]
            assert len(group)==24
            assert all(len([r for r in group if r['round']==n])==2 for n in range(1,13))
            if mode=='cold':
                assert all(r['page_cache_hits']==r['page_cache_read_bytes']==r['distributed_cache_read_bytes']==r['distributed_cache_object_read_bytes']==0 and r['s3_read_bytes']>0 for r in group)
            else:
                assert all(r['s3_read_bytes']==0 and r['page_cache_misses']==0 and r['page_cache_hits']>0 for r in group)
            def summary(g):
                elapsed=sum(r['server_elapsed_s'] for r in g);byte_count=sum(r['read_bytes'] for r in g);cpu=sum(r['cpu_s'] for r in g)
                return {'cache_mode':mode,'query_condition_cache':qcc,'max_threads':cores,'n':len(g),'throughput_GB_s':byte_count/1e9/elapsed,
                    'MB_s_per_busy_core':byte_count/1e6/cpu,'MB_s_per_selected_core':byte_count/1e6/elapsed/cores,
                    'average_busy_cores':cpu/elapsed,'CPU_utilization_of_59_pct':cpu/elapsed/59*100,
                    'mean_query_peak_MiB':statistics.mean(r['peak_memory_bytes'] for r in g)/2**20,
                    'mean_read_rows':statistics.mean(r['read_rows'] for r in g),'mean_read_GB':statistics.mean(r['read_bytes'] for r in g)/1e9,
                    'total_server_s':elapsed,'total_CPU_s':cpu,'total_read_bytes':byte_count,'s3_read_bytes':sum(r['s3_read_bytes'] for r in g),
                    'page_cache_hits':sum(r['page_cache_hits'] for r in g),'page_cache_misses':sum(r['page_cache_misses'] for r in g),
                    'query_condition_cache_hits':sum(r['query_condition_cache_hits'] for r in g),
                    'server_latency_ms_median':statistics.median(r['server_elapsed_s']*1000 for r in g),
                    'server_latency_ms_p25':quantile([r['server_elapsed_s']*1000 for r in g],.25),'server_latency_ms_p75':quantile([r['server_elapsed_s']*1000 for r in g],.75)}
            c=summary(group)
            perround=[sum(r['read_bytes'] for r in group if r['round']==n)/1e9/sum(r['server_elapsed_s'] for r in group if r['round']==n) for n in range(1,13)]
            c.update({'throughput_GB_s_p25':quantile(perround,.25),'throughput_GB_s_median':statistics.median(perround),'throughput_GB_s_p75':quantile(perround,.75)})
            combined.append(c)
            for q in ['D1','D2']:
                g=[r for r in group if r['query']==q];assert len(g)==12
                assert len({r['result_rows'] for r in g})==1
                individual.append({'query':q,**summary(g)})
    write('all-trials.csv',rows);write('combined-workload.csv',combined);write('individual-query-summary.csv',individual)
    validation={'passed':True,'pilot_trials':sum(r['phase']=='pilot' for r in rows),'warmup_trials':sum(r['phase']=='warmup' for r in rows),'measured_trials':len(measured)+len(second),'failures':0,'missing_logs':0,'outliers_removed':0,'query_result_cache_hits':0,'cold_trials_verified_s3_and_no_page_or_distributed_reads':288,'hot_trials_verified_page_hits_and_no_s3_or_page_misses':144,'measurement_start_utc':min(r['started_at_utc'] for r in measured+second),'measurement_end_utc':max(r['finished_at_utc'] for r in measured+second),'hosts':sorted({r['hostname'] for r in measured+second})}
    (ROOT/'derived/validation.json').write_text(json.dumps(validation,indent=2)+'\n')
    for r in combined:print(r['cache_mode'],'qcc',r['query_condition_cache'],r['max_threads'],f"{r['throughput_GB_s']:.3f} GB/s",f"{r['MB_s_per_busy_core']:.1f} MB/s/busy core",flush=True)
    print(json.dumps(validation,indent=2))

if __name__=='__main__':main()
