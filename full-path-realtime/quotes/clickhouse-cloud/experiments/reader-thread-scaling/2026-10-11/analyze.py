#!/usr/bin/env python3
"""Join the complete trial ledger to server logs, validate, and derive scaling stats.

Standard library only. Nothing is removed or retried. Pilot/warmup observations
are retained in the joined ledger but excluded from measured summaries.
"""
import csv
import hashlib
import json
import pathlib
import random
import statistics

ROOT = pathlib.Path(__file__).resolve().parent
THREADS = [1, 2, 4, 8, 16]
QUERIES = ['D1', 'D2', 'A1', 'A2', 'A3', 'A4']


def quantile(values, p):
    values = sorted(values)
    pos = (len(values) - 1) * p
    lo = int(pos)
    hi = min(lo + 1, len(values) - 1)
    return values[lo] + (values[hi] - values[lo]) * (pos - lo)


def write_csv(name, rows):
    path = ROOT / 'derived' / name
    with path.open('w', newline='') as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def validate_saved_context(validation):
    """Recheck context offline when after-run evidence has been collected."""
    results = ROOT / 'results'
    def records(name):
        return list(csv.DictReader((results / name).open()))
    if (results / '13-parts-after.csv').exists():
        assert records('05-parts-before.csv') == records('13-parts-after.csv')
        validation['active_parts_unchanged'] = True
    if (results / '14-capacity-after.csv').exists():
        cap = {r['metric']: float(r['value']) for r in records('14-capacity-after.csv')}
        assert cap['CGroupMaxCPU'] == 16 and cap['CGroupMemoryTotal'] == 64 * 2**30
        validation['capacity_after_vCPUs'] = cap['CGroupMaxCPU']
        validation['capacity_after_GiB'] = cap['CGroupMemoryTotal'] / 2**30
    if (results / '18-capacity-during-measurement.csv').exists():
        series = records('18-capacity-during-measurement.csv')
        assert series
        for r in series:
            expected = 16 if r['metric'] == 'CGroupMaxCPU' else 64 * 2**30
            assert float(r['min_value']) == expected == float(r['max_value'])
        validation['capacity_constant_in_async_metric_log'] = True
    if (results / '16-other-query-activity.csv').exists():
        others = records('16-other-query-activity.csv')
        validation['other_initial_queries'] = sum(int(r['queries']) for r in others)
        validation['other_query_cpu_s'] = sum(float(r['total_cpu_s']) for r in others)
    if (results / '19-background-tasks-during-measurement.csv').exists():
        background = records('19-background-tasks-during-measurement.csv')
        validation['background_merge_tasks_max_sampled'] = max(int(r['max_background_merge_tasks']) for r in background)


def main():
    trials = [json.loads(line) for p in sorted((ROOT / 'raw').glob('*.jsonl'))
              for line in p.read_text().splitlines()]
    logs = list(csv.DictReader((ROOT / 'results/query-logs-all.csv').open()))
    by_id = {r['query_id']: r for r in logs}
    assert len(by_id) == len(logs), 'Duplicate initial query log records'
    assert len({r['query_id'] for r in trials}) == len(trials), 'Duplicate trial IDs'
    assert all(r['success'] for r in trials), 'A failed observation exists'
    missing = [r['query_id'] for r in trials if r['query_id'] not in by_id]
    assert not missing, f'Missing query logs: {missing}'
    baseline_settings = {r['name']: r['value'] for r in csv.DictReader((ROOT / 'results/02-settings.csv').open())}
    joined = []
    for trial in trials:
        log = by_id[trial['query_id']]
        settings = json.loads(log['settings_json'])
        events = json.loads(log['profile_events_json'])
        assert log['type'] == 'QueryFinish' and int(log['exception_code']) == 0
        assert log['query_cache_usage'] == 'None'
        assert settings['use_query_cache'] == '0'
        assert int(settings['max_threads']) == trial['max_threads']
        # Query-log Settings omits unchanged defaults on this server.
        assert settings.get('enable_parallel_replicas', baseline_settings['enable_parallel_replicas']) == '0'
        assert settings['max_parallel_replicas'] == '1'
        assert int(log['read_bytes']) == int(trial['http_summary']['read_bytes'])
        assert int(log['read_rows']) == int(trial['http_summary']['read_rows'])
        assert int(log['result_rows']) < 1_000_000
        elapsed = int(trial['http_summary']['elapsed_ns']) / 1e9
        cpu = (int(log['user_cpu_us']) + int(log['system_cpu_us'])) / 1e6
        # Query-log milliseconds are rounded; the HTTP summary retains ns.
        assert abs(elapsed * 1000 - int(log['query_duration_ms'])) < 5
        joined.append({
            'phase': trial['phase'], 'run_id': trial['run_id'],
            'round': trial['round'], 'query': trial['query'],
            'max_threads': trial['max_threads'], 'sequence': trial['sequence'],
            'query_id': trial['query_id'], 'hostname': log['hostname'],
            'started_at_utc': log['started_at_utc'],
            'finished_at_utc': log['finished_at_utc'],
            'server_elapsed_s': elapsed, 'query_log_duration_ms': int(log['query_duration_ms']),
            'client_wall_s': trial['client_wall_s'],
            'read_rows': int(log['read_rows']), 'read_bytes': int(log['read_bytes']),
            'throughput_GB_s': int(log['read_bytes']) / 1e9 / elapsed,
            'result_rows': int(log['result_rows']), 'result_bytes': int(log['result_bytes']),
            'logged_cpu_s': cpu, 'cpu_counters_reported': cpu > 0,
            'cpu_s': cpu if cpu > 0 else None,
            'average_busy_cores': cpu / elapsed if cpu > 0 else None,
            'GB_per_cpu_s': int(log['read_bytes']) / 1e9 / cpu if cpu > 0 else None,
            'cpu_wait_s': int(log['cpu_wait_us']) / 1e6,
            'io_wait_s': int(log['io_wait_us']) / 1e6,
            'peak_memory_bytes': int(log['peak_memory_bytes']),
            'peak_threads_usage': int(log['peak_threads_usage']),
            'page_cache_hits': events.get('PageCacheHits', 0),
            'page_cache_misses': events.get('PageCacheMisses', 0),
            'mark_cache_misses': events.get('MarkCacheMisses', 0),
            'query_condition_cache_hits': events.get('QueryConditionCacheHits', 0),
            'query_condition_cache_misses': events.get('QueryConditionCacheMisses', 0),
            'object_storage_read_bytes': events.get('DistrCacheServerCachedReadBufferObjectStorageReadBytes', 0),
            'distributed_cache_read_bytes': events.get('DistrCacheServerCachedReadBufferCacheReadBytes', 0),
            'async_remote_wait_s': events.get('AsynchronousRemoteReadWaitMicroseconds', 0) / 1e6,
            'sync_remote_wait_s': events.get('SynchronousRemoteReadWaitMicroseconds', 0) / 1e6,
            'query_cache_usage': log['query_cache_usage'],
        })
    measured = [r for r in joined if r['phase'] == 'measured']
    assert len(measured) == 360, 'Measured run incomplete or extra runs present'
    summaries = []
    for query in QUERIES:
        for cap in THREADS:
            group = [r for r in measured if r['query'] == query and r['max_threads'] == cap]
            assert len(group) == 12 and {r['round'] for r in group} == set(range(1, 13))
            assert len({r['result_rows'] for r in group}) == 1
            elapsed = sum(r['server_elapsed_s'] for r in group)
            cpu_group = [r for r in group if r['cpu_s'] is not None]
            cpu = sum(r['cpu_s'] for r in cpu_group)
            cpu_elapsed = sum(r['server_elapsed_s'] for r in cpu_group)
            cpu_bytes = sum(r['read_bytes'] for r in cpu_group)
            byte_count = sum(r['read_bytes'] for r in group)
            row = {'query': query, 'max_threads': cap, 'n': len(group),
                   'throughput_GB_s': byte_count / 1e9 / elapsed,
                   'cpu_observations': len(cpu_group),
                   'average_busy_cores': cpu / cpu_elapsed if cpu_elapsed else None,
                   'GB_per_cpu_s': cpu_bytes / 1e9 / cpu if cpu else None,
                   'cpu_utilization_of_16_pct': cpu / cpu_elapsed / 16 * 100 if cpu_elapsed else None,
                   'read_GB_mean': statistics.mean(r['read_bytes'] for r in group) / 1e9,
                   'read_rows_mean': statistics.mean(r['read_rows'] for r in group),
                   'result_rows': group[0]['result_rows'],
                   'page_cache_misses_sum': sum(r['page_cache_misses'] for r in group),
                   'object_storage_read_bytes_sum': sum(r['object_storage_read_bytes'] for r in group),
                   'cpu_wait_s_sum': sum(r['cpu_wait_s'] for r in group),
                   'io_wait_s_sum': sum(r['io_wait_s'] for r in group)}
            for label, field, factor in [('latency_ms', 'server_elapsed_s', 1000),
                                         ('trial_throughput_GB_s', 'throughput_GB_s', 1),
                                         ('cpu_s', 'cpu_s', 1),
                                         ('peak_memory_MiB', 'peak_memory_bytes', 1 / 2**20)]:
                values = [r[field] * factor for r in group if r[field] is not None]
                for suffix, p in [('p25', .25), ('median', .5), ('p75', .75), ('p95', .95)]:
                    row[f'{label}_{suffix}'] = quantile(values, p) if values else None
                row[f'{label}_min'] = min(values) if values else None
                row[f'{label}_max'] = max(values) if values else None
            summaries.append(row)
    index = {(r['query'], r['max_threads']): r for r in summaries}
    for row in summaries:
        base = index[(row['query'], 1)]
        row['speedup_vs_1_thread'] = row['throughput_GB_s'] / base['throughput_GB_s']
        row['cap_scaling_efficiency_pct'] = row['speedup_vs_1_thread'] / row['max_threads'] * 100
    comparisons = []
    rng = random.Random(6112026)
    for query in QUERIES:
        by_round = {(r['round'], r['max_threads']): r for r in measured if r['query'] == query}
        paired = [by_round[(n, 16)]['throughput_GB_s'] / by_round[(n, 8)]['throughput_GB_s']
                  for n in range(1, 13)]
        boot = [statistics.median(rng.choices(paired, k=12)) for _ in range(10000)]
        low, high = index[(query, 8)], index[(query, 16)]
        comparisons.append({
            'query': query, 'throughput_8_GB_s': low['throughput_GB_s'],
            'throughput_16_GB_s': high['throughput_GB_s'],
            'throughput_ratio_16_over_8': high['throughput_GB_s'] / low['throughput_GB_s'],
            'latency_8_median_ms': low['latency_ms_median'],
            'latency_16_median_ms': high['latency_ms_median'],
            'latency_ratio_16_over_8': high['latency_ms_median'] / low['latency_ms_median'],
            'paired_throughput_ratio_p25': quantile(paired, .25),
            'paired_throughput_ratio_median': quantile(paired, .5),
            'paired_throughput_ratio_p75': quantile(paired, .75),
            'paired_median_bootstrap_95pct_low': quantile(boot, .025),
            'paired_median_bootstrap_95pct_high': quantile(boot, .975),
            'busy_cores_8': low['average_busy_cores'], 'busy_cores_16': high['average_busy_cores'],
            'GB_per_cpu_s_8': low['GB_per_cpu_s'], 'GB_per_cpu_s_16': high['GB_per_cpu_s'],
        })
    write_csv('all-trials.csv', joined)
    write_csv('scaling-summary.csv', summaries)
    write_csv('8-vs-16.csv', comparisons)
    validation = {
        'total_trials': len(joined), 'measured_trials': len(measured),
        'pilot_trials': sum(r['phase'] == 'pilot' for r in joined),
        'warmup_trials': sum(r['phase'] == 'warmup' for r in joined),
        'measured_window_start_utc': min(r['started_at_utc'] for r in measured),
        'measured_window_end_utc': max(r['finished_at_utc'] for r in measured),
        'hosts': sorted({r['hostname'] for r in measured}),
        'failures': 0, 'missing_logs': 0, 'duplicate_logs': 0,
        'result_cache_reads': 0,
        'measured_page_cache_misses': sum(r['page_cache_misses'] for r in measured),
        'measured_object_storage_read_bytes': sum(r['object_storage_read_bytes'] for r in measured),
        'all_thread_caps_confirmed_in_query_log': True,
        'all_http_rows_and_bytes_match_query_log': True,
        'all_result_row_counts_below_limit': True,
        'outliers_removed': 0,
        'measured_trials_without_cpu_counters': sum(not r['cpu_counters_reported'] for r in measured),
        'caveat': 'Bootstrap interval measures repeat variation in this session, not portability to other hosts or datasets.',
    }
    validate_saved_context(validation)
    (ROOT / 'derived/validation.json').write_text(json.dumps(validation, indent=2) + '\n')
    for row in comparisons:
        print(row['query'], f"8→16 throughput {row['throughput_8_GB_s']:.3f}→{row['throughput_16_GB_s']:.3f} GB/s",
              f"({row['throughput_ratio_16_over_8']:.3f}×)",
              f"median latency {row['latency_8_median_ms']:.3f}→{row['latency_16_median_ms']:.3f} ms")
    print(json.dumps(validation, indent=2))


if __name__ == '__main__':
    main()
