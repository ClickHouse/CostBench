#!/usr/bin/env python3
"""Save service/dataset context after the sequential trials have finished."""
import csv, datetime, json, pathlib
from connection import request, bounded
ROOT=pathlib.Path(__file__).resolve().parent
def save(name,sql):
    (ROOT/'sql'/(name+'.sql')).write_text(sql+'\n');r=request(sql);(ROOT/'results'/(name+'.csv')).write_text(r['body']);print(name,'saved',flush=True)
def main():
    for name,source in [('identity-after','00-identity-before'),('parts-after','05-parts-before'),('capacity-after','06-capacity'),('background-after','07-background-before')]:save(name,(ROOT/'sql'/(source+'.sql')).read_text())
    defaults="SELECT name,value,description FROM system.settings WHERE name IN ('use_query_cache','use_query_condition_cache','use_uncompressed_cache','enable_filesystem_cache','use_page_cache_with_distributed_cache','force_read_through_distributed_cache') ORDER BY name LIMIT 20"
    save('default-query-settings-after',bounded(defaults))
    before=list(csv.DictReader((ROOT/'results/05-parts-before.csv').open()));after=list(csv.DictReader((ROOT/'results/parts-after.csv').open()));assert before==after,'Benchmark active parts changed.'
    identity=list(csv.DictReader((ROOT/'results/identity-after.csv').open()))[0];initial=list(csv.DictReader((ROOT/'results/00-identity-before.csv').open()))[0]
    for key in ['hostname','version','vCPUs','memory_bytes']:assert initial[key]==identity[key]
    trials=[json.loads(line)for p in sorted((ROOT/'raw').glob('*.jsonl'))for line in p.read_text().splitlines()];start=min(datetime.datetime.fromisoformat(r['client_started_at_utc'])for r in trials).strftime('%Y-%m-%d %H:%M:%S');end=datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
    activity=f"""SELECT multiIf(startsWith(log_comment,'reader-cache-'),'hot-cold experiment',startsWith(log_comment,'reader-qccoff-'),'cold condition-cache-off experiment','other queries') AS activity,count() AS completed_queries,sum(query_duration_ms)/1000 AS summed_elapsed_s,sum(ProfileEvents['UserTimeMicroseconds']+ProfileEvents['SystemTimeMicroseconds'])/1000000 AS summed_cpu_s,max(peak_threads_usage) AS peak_threads FROM clusterAllReplicas(default,merge('system','^query_log')) WHERE event_date BETWEEN toDate('{start}') AND toDate('{end}') AND event_time BETWEEN toDateTime('{start}','UTC') AND toDateTime('{end}','UTC') AND is_initial_query=1 AND type='QueryFinish' GROUP BY activity ORDER BY activity LIMIT 20"""
    save('query-activity',bounded(activity));(ROOT/'derived/context-validation.json').write_text(json.dumps({'passed':True,'active_parts_identical':True,'service_capacity_version_host_identical':True,'before_uptime_s':int(initial['uptime_s']),'after_uptime_s':int(identity['uptime_s'])},indent=2)+'\n')
if __name__=='__main__':main()
