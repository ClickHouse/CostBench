#!/usr/bin/env python3
"""Extract exact experiment query IDs after natural log flush; preserve all counters."""
import argparse,csv,datetime,json,pathlib
from connection import request,bounded

ROOT=pathlib.Path(__file__).resolve().parent

def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--phase',choices=['pilot','warmup','measured','all'],default='all')
    args=parser.parse_args()
    trials=[]
    for p in sorted((ROOT/'raw').glob('*.jsonl')):
        trials.extend(json.loads(line) for line in p.read_text().splitlines())
    if args.phase!='all': trials=[r for r in trials if r['phase']==args.phase]
    assert trials
    earliest=min(datetime.datetime.fromisoformat(r['client_started_at_utc']) for r in trials)-datetime.timedelta(minutes=1)
    latest=max(datetime.datetime.fromisoformat(r['client_started_at_utc']) for r in trials)+datetime.timedelta(minutes=1)
    date_from=earliest.strftime('%Y-%m-%d')
    date_to=latest.strftime('%Y-%m-%d')
    from_utc=earliest.strftime('%Y-%m-%d %H:%M:%S')
    ids=','.join("'"+r['query_id']+"'" for r in trials)
    sql=f"""SELECT query_id,hostname,type,query_start_time_microseconds AS started_at_utc,event_time_microseconds AS finished_at_utc,
query_duration_ms,read_rows,read_bytes,result_rows,result_bytes,memory_usage AS peak_memory_bytes,peak_threads_usage,
normalized_query_hash,query_cache_usage,exception_code,
ProfileEvents['UserTimeMicroseconds'] AS user_cpu_us,ProfileEvents['SystemTimeMicroseconds'] AS system_cpu_us,
ProfileEvents['OSCPUWaitMicroseconds'] AS cpu_wait_us,ProfileEvents['OSIOWaitMicroseconds'] AS io_wait_us,
toJSONString(ProfileEvents) AS profile_events_json,toJSONString(Settings) AS settings_json
FROM clusterAllReplicas(default,merge('system','^query_log'))
WHERE event_date BETWEEN toDate('{date_from}') AND toDate('{date_to}') AND event_time>=toDateTime('{from_utc}','UTC')
AND query_id IN ({ids}) AND is_initial_query=1 AND type IN ('QueryFinish','ExceptionWhileProcessing','ExceptionBeforeStart')
ORDER BY query_start_time_microseconds LIMIT 2000"""
    sql=bounded(sql)
    (ROOT/'sql'/('query-logs-'+args.phase+'.sql')).write_text(sql)
    response=request(sql)
    result=ROOT/'results'/('query-logs-'+args.phase+'.csv')
    result.write_text(response['body'])
    rows=list(csv.DictReader(response['body'].splitlines()))
    seen={r['query_id'] for r in rows}
    missing=[r['query_id'] for r in trials if r['query_id'] not in seen]
    print('Saved',len(rows),'log entries for',len(trials),'trials; missing',len(missing))
    if missing: print('Missing IDs:',json.dumps(missing))
    if not missing:
        index={r['query_id']:r for r in rows}
        assert len(index)==len(rows),'Duplicate initial query records'
        for r in trials:
            log=index[r['query_id']]
            assert log['query_cache_usage']=='None'
            assert int(log['read_bytes'])==int(r['http_summary']['read_bytes'])
            assert int(log['read_rows'])==int(r['http_summary']['read_rows'])
            settings=json.loads(log['settings_json'])
            assert int(settings['max_threads'])==r['max_threads']
            assert settings['use_query_cache']=='0'
        print('Verified byte/row counters and requested thread limits; no result cache reads.')
    for r in rows[:8]:
        elapsed=int(r['query_duration_ms'])/1000
        print(r['query_id'].split('-')[-2:],r['query_duration_ms'],'ms',round(int(r['read_bytes'])/1e9,3),'GB','busy_cores',round((int(r['user_cpu_us'])+int(r['system_cpu_us']))/1e6/elapsed,2) if elapsed else None,'peak_threads',r['peak_threads_usage'])

if __name__=='__main__': main()
