#!/usr/bin/env python3
"""Run after measurements, to avoid perturbing benchmark queries."""
import csv
import json
import pathlib
from connection import bounded, request

ROOT = pathlib.Path(__file__).resolve().parent


def save(name, query):
    sql = bounded(query)
    (ROOT / 'sql' / (name + '.sql')).write_text(sql + '\n')
    result = request(sql)
    (ROOT / 'results' / (name + '.csv')).write_text(result['body'])
    print(name, len(result['body']), 'bytes')
    return list(csv.DictReader(result['body'].splitlines()))


def main():
    validation = json.loads((ROOT / 'derived/validation.json').read_text())
    start = validation['measured_window_start_utc']
    end = validation['measured_window_end_utc']
    date_from, date_to = start[:10], end[:10]
    parts = save('13-parts-after', "SELECT table,name,rows,bytes_on_disk,marks,modification_time FROM system.parts WHERE database='quotes_async_t4' AND table IN ('quotes','quotes_daily') AND active ORDER BY table,name LIMIT 5000")
    before = list(csv.DictReader((ROOT / 'results/05-parts-before.csv').open()))
    assert parts == before, 'Active parts changed during the test'
    capacity = save('14-capacity-after', "SELECT metric,value FROM system.asynchronous_metrics WHERE metric IN ('CGroupMaxCPU','CGroupMemoryTotal','CGroupMemoryUsed','CGroupMemoryUsedWithoutPageCache','MemoryResident','UserspacePageCacheBytes','OSMemoryTotal') ORDER BY metric LIMIT 20")
    cap = {r['metric']: float(r['value']) for r in capacity}
    assert cap['CGroupMaxCPU'] == 16 and cap['CGroupMemoryTotal'] == 64 * 2**30
    save('15-load-after', "SELECT metric,value FROM system.metrics WHERE metric IN ('Query','QueryThread','BackgroundMergesAndMutationsPoolTask') ORDER BY metric LIMIT 20")
    overlap = f"event_date BETWEEN toDate('{date_from}') AND toDate('{date_to}') AND query_start_time_microseconds<toDateTime64('{end}',6,'UTC') AND event_time_microseconds>=toDateTime64('{start}',6,'UTC')"
    external = save('16-other-query-activity', f"""SELECT query_kind,normalized_query_hash,count() AS queries,
sum(query_duration_ms) AS total_elapsed_ms,max(query_duration_ms) AS max_elapsed_ms,
sum(ProfileEvents['UserTimeMicroseconds']+ProfileEvents['SystemTimeMicroseconds'])/1e6 AS total_cpu_s,
sum(read_bytes) AS total_read_bytes,max(memory_usage) AS max_peak_memory_bytes
FROM clusterAllReplicas(default,merge('system','^query_log'))
WHERE {overlap} AND is_initial_query=1 AND query_id NOT LIKE 'reader-scaling-%'
AND type IN ('QueryFinish','ExceptionWhileProcessing','ExceptionBeforeStart')
GROUP BY query_kind,normalized_query_hash ORDER BY total_cpu_s DESC LIMIT 1000""")
    columns = save('17-context-log-schema', "SELECT table,name,type FROM system.columns WHERE database='system' AND table IN ('asynchronous_metric_log','metric_log') AND (name IN ('event_time','event_date','metric','value') OR name ILIKE '%BackgroundMerges%') ORDER BY table,name LIMIT 100")
    async_fields = {r['name'] for r in columns if r['table'] == 'asynchronous_metric_log'}
    if {'event_time','event_date','metric','value'} <= async_fields:
        series = save('18-capacity-during-measurement', f"""SELECT hostName() AS hostname,metric,count() AS samples,min(value) AS min_value,max(value) AS max_value
FROM clusterAllReplicas(default,merge('system','^asynchronous_metric_log'))
WHERE event_date BETWEEN toDate('{date_from}') AND toDate('{date_to}')
AND event_time BETWEEN toDateTime('{start[:19]}','UTC') AND toDateTime('{end[:19]}','UTC')
AND metric IN ('CGroupMaxCPU','CGroupMemoryTotal') GROUP BY hostname,metric ORDER BY hostname,metric LIMIT 100""")
        assert series, 'No historical capacity samples'
        for r in series:
            expected = 16 if r['metric'] == 'CGroupMaxCPU' else 64 * 2**30
            assert float(r['min_value']) == expected == float(r['max_value']), 'Capacity varied'
        validation['capacity_constant_in_async_metric_log'] = True
    merge_fields = [r['name'] for r in columns if r['table']=='metric_log' and r['name'].startswith('CurrentMetric_')]
    if 'CurrentMetric_BackgroundMergesAndMutationsPoolTask' in merge_fields:
        save('19-background-tasks-during-measurement', f"""SELECT hostName() AS hostname,count() AS samples,
max(CurrentMetric_BackgroundMergesAndMutationsPoolTask) AS max_background_merge_tasks
FROM clusterAllReplicas(default,merge('system','^metric_log'))
WHERE event_date BETWEEN toDate('{date_from}') AND toDate('{date_to}')
AND event_time BETWEEN toDateTime('{start[:19]}','UTC') AND toDateTime('{end[:19]}','UTC')
GROUP BY hostname ORDER BY hostname LIMIT 100""")
    validation['active_parts_unchanged'] = True
    validation['capacity_after_vCPUs'] = cap['CGroupMaxCPU']
    validation['capacity_after_GiB'] = cap['CGroupMemoryTotal'] / 2**30
    validation['other_initial_queries'] = sum(int(r['queries']) for r in external)
    validation['other_query_cpu_s'] = sum(float(r['total_cpu_s']) for r in external)
    (ROOT / 'derived/validation.json').write_text(json.dumps(validation, indent=2) + '\n')
    print(json.dumps(validation, indent=2))


if __name__ == '__main__':
    main()
