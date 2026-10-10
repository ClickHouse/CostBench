#!/usr/bin/env python3
"""Reproduce per-query reader metrics and family curves from committed evidence."""
import argparse
import collections
import csv
import datetime as dt
import hashlib
import io
import json
import pathlib
import statistics

HERE = pathlib.Path(__file__).resolve().parent
PROVIDER = HERE.parents[3]
REPO = HERE.parents[6]
FULL_ROWS = 113217743918
HASHES = {'A1': '4268017077458496684', 'A2': '13116456435110875434',
          'A3': '16441976537628221706', 'A4': '16076059573740575684',
          'D1': '14900422428945064879', 'D2': '4120463799277501782'}
NUMERIC = ['query_duration_ms', 'user_cpu_us', 'system_cpu_us', 'has_user_cpu',
           'has_system_cpu', 'read_rows', 'read_bytes', 'query_cache_hits']
SOURCES = [
    ('drill', 'raw/drilldown_20260808T065602Z.jsonl', ['D1', 'D2']),
    ('aggregate', 'mv/dashboard_20260808T065559Z.jsonl', ['A1', 'A2', 'A3', 'A4']),
]

def stamp(value):
    return dt.datetime.fromisoformat(value.replace('Z', '+00:00')).replace(tzinfo=dt.timezone.utc)

def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()

def csv_text(rows):
    stream = io.StringIO(newline='')
    writer = csv.DictWriter(stream, fieldnames=list(rows[0]), lineterminator='\n')
    writer.writeheader()
    writer.writerows(rows)
    return stream.getvalue()

def final_charts(active):
    """Four accepted panels; rates use ratios of totals, never idle time."""
    grouped = collections.defaultdict(list)
    for row in active:
        grouped[row['family'], row['iteration'], row['rows_ingested']].append(row)
    family_rows, coverage = [], []
    for (family, iteration, rows), batch in sorted(grouped.items()):
        wall_s = sum(r['query_duration_ms'] for r in batch) / 1000
        cpu_s = sum(r['cpu_ms'] for r in batch) / 1000
        family_rows.append({
            'family': family, 'iteration': iteration, 'rows_ingested': rows,
            'query_count': len(batch),
            'total_read_bytes': sum(r['read_bytes'] for r in batch),
            'total_read_rows': sum(r['read_rows'] for r in batch),
            'total_elapsed_seconds': wall_s, 'total_cpu_seconds': cpu_s,
            'bytes_per_second': sum(r['read_bytes'] for r in batch) / wall_s,
            'bytes_per_cpu_second': sum(r['read_bytes'] for r in batch) / cpu_s,
            'average_busy_cores': cpu_s / wall_s,
        })
        full = [r for r in batch if r['benchmark_query'] in ('A3', 'A4')]
        if full:
            assert len(full) == 2
            elapsed = sum(r['query_duration_ms'] for r in full) / 1000
            coverage.append({
                'iteration': iteration, 'rows_ingested': rows, 'query_count': 2,
                'raw_quotes_represented': 2 * rows,
                'total_elapsed_seconds': elapsed,
                'quotes_per_second': 2 * rows / elapsed,
            })
    latency_raw, latency_display = [], []
    for family, rel, queries in SOURCES:
        records = [json.loads(line) for line in (PROVIDER / 'results_t2' / rel).read_text().splitlines()]
        end = next(i for i, r in enumerate(records) if r['raw_rows'] == FULL_ROWS)
        records = [r for r in records[:end + 1] if r['raw_rows'] > 0]
        window = 5 if family == 'drill' else 7
        published = [r for r in records if r['raw_rows'] <= 100_000_000_000]
        for r in records:
            for q, result in zip(queries, r['result']):
                latency_raw.append({'family': family, 'benchmark_query': q,
                                    'iteration': r['iteration'], 'rows_ingested': r['raw_rows'],
                                    'latency_seconds': result[0]})
        # Preserve the Blog 2 100B display horizon and its sampling convention.
        indices = list(range(0, len(published), 1 if family == 'drill' else 3))
        if indices[-1] != len(published) - 1:
            indices.append(len(published) - 1)
        indices.extend(range(len(published), len(records)))
        for i in indices:
            source = published if i < len(published) else records
            neighbors = source[max(0, i - window // 2):min(len(source), i + window // 2 + 1)]
            latency = statistics.mean(statistics.median(r['result'][q][0] for r in neighbors)
                                      for q in range(len(queries)))
            latency_display.append({'family': family, 'iteration': records[i]['iteration'],
                                    'rows_ingested': records[i]['raw_rows'], 'latency_seconds': latency})
    assert len(coverage) == 221 and len(latency_display) == 131
    assert len(latency_raw) == 958
    ids = [('drill', 'drilldown', 'Drill-downs', '5 4'),
           ('aggregate', 'aggregation', 'Interactive aggs', '')]
    def series(rows, field):
        return [{'id': sid, 'label': label, 'dash': dash,
                 'points': [[r['rows_ingested'] / FULL_ROWS, r[field]] for r in rows if r['family'] == family]}
                for family, sid, label, dash in ids]
    def panel(pid, unit, maximum, ticks, curves, axis=None, minimum=None, sampled=True):
        value = {'id': pid, 'unit': unit, 'yMax': maximum, 'yTicks': ticks,
                 'color': '#e9ec5b', 'sampled': sampled, 'series': curves}
        if axis:
            value['axisUnit'] = axis
        if minimum:
            value['yMin'] = minimum
        return value
    data = {'fullRows': FULL_ROWS, 'rowTicks': [0, 25, 50, 75, 100],
            'playback': {'durationMs': 15000, 'holdMs': 2000}, 'panels': [
        panel('throughput', 'bytes-rate', 4.5e9, [0, 1.5e9, 3e9, 4.5e9],
              series(family_rows, 'bytes_per_second'), 'Uncompressed GB/s'),
        panel('efficiency', 'bytes-core-rate', 450e6, [0, 150e6, 300e6, 450e6],
              series(family_rows, 'bytes_per_cpu_second'), 'Uncompressed MB/s/core'),
        panel('latency', 'seconds', 2, [.01, .1, 1],
              series(latency_display, 'latency_seconds'), minimum=.005, sampled=False),
        panel('coverage', 'coverage-rate', 8e12, [0, 2e12, 4e12, 6e12, 8e12],
              [{'id': 'full-table-aggs', 'label': 'Full-table aggs · A3/A4', 'dash': '',
                'points': [[r['rows_ingested'] / FULL_ROWS, r['quotes_per_second']] for r in coverage]}],
              'Trillion raw quotes/s'),
    ]}
    return {
        'data-processing-throughput.csv': csv_text([{k: r[k] for k in
            ['family', 'iteration', 'rows_ingested', 'query_count', 'total_read_bytes',
             'total_elapsed_seconds', 'bytes_per_second']} for r in family_rows]),
        'data-throughput-per-cpu-core.csv': csv_text([{k: r[k] for k in
            ['family', 'iteration', 'rows_ingested', 'query_count', 'total_read_bytes',
             'total_cpu_seconds', 'bytes_per_cpu_second']} for r in family_rows]),
        'effective-aggregation-throughput.csv': csv_text(coverage),
        'query-latency-per-query.csv': csv_text(latency_raw),
        'query-latency.csv': csv_text(latency_display),
        'chart-data.json': json.dumps(data, indent=2) + '\n',
    }

def derive():
    raw = list(csv.DictReader((HERE / 'query-log.csv').open(encoding='utf-8-sig')))
    assert len(raw) == 1241
    assert len({(r['benchmark_query'], r['started_at_utc']) for r in raw}) == len(raw)
    for r in raw:
        assert r['query_hash'] == HASHES[r['benchmark_query']]
        for column in NUMERIC:
            r[column] = int(r[column])
        assert r['query_duration_ms'] > 0
        assert r['user_cpu_us'] + r['system_cpu_us'] > 0
        assert r['has_user_cpu'] or r['has_system_cpu']
        assert r['query_cache_hits'] == 0
    profiles = list(csv.DictReader((HERE / 'query-log-profile-stats.csv').open()))
    index = {(r['benchmark_query'], r['started_at_utc']): r for r in profiles}
    assert len(index) == len(profiles) == len(raw)
    for r in raw:
        p = index[r['benchmark_query'], r['started_at_utc']]
        for field in ['query_duration_ms', 'user_cpu_us', 'system_cpu_us', 'read_rows', 'read_bytes']:
            assert int(p[field]) == r[field], (r['benchmark_query'], field)
        assert int(p['selected_rows']) == r['read_rows']
        assert int(p['selected_bytes']) == r['read_bytes']
    used, active, curves, inputs = set(), [], [], []
    for family, rel, queries in SOURCES:
        path = PROVIDER / 'results_t2' / rel
        observations = [json.loads(line) for line in path.read_text().splitlines()]
        inputs.append({'path': str(path.relative_to(REPO)), 'sha256': digest(path),
                       'observations': len(observations)})
        completed = False
        family_points = []
        for observation in observations:
            start = stamp(observation['iteration_started_at'])
            batch = []
            for q in queries:
                candidates = [r for r in raw if r['benchmark_query'] == q
                              and 0 <= (stamp(r['started_at_utc']) - start).total_seconds() < 20]
                assert len(candidates) == 1, (q, observation['iteration'], len(candidates))
                r = candidates[0]
                key = (q, r['started_at_utc'])
                assert key not in used
                used.add(key)
                cpu_ms = (r['user_cpu_us'] + r['system_cpu_us']) / 1000
                batch.append({
                    'family': family, 'benchmark_query': q, 'iteration': observation['iteration'],
                    'started_at_utc': r['started_at_utc'], 'rows_ingested': observation['raw_rows'],
                    'query_duration_ms': r['query_duration_ms'],
                    'user_cpu_us': r['user_cpu_us'], 'system_cpu_us': r['system_cpu_us'],
                    'cpu_ms': cpu_ms, 'average_cores': cpu_ms / r['query_duration_ms'],
                    'read_rows': r['read_rows'], 'read_bytes': r['read_bytes'],
                    'bytes_per_second': r['read_bytes'] / (r['query_duration_ms'] / 1000),
                    'bytes_per_cpu_second': r['read_bytes'] / (cpu_ms / 1000),
                })
            rows = observation['raw_rows']
            included = rows > 0 and not completed
            if rows >= FULL_ROWS:
                completed = True
            if not included:
                continue
            active.extend(batch)
            family_points.append({
                'family': family, 'iteration': observation['iteration'], 'rows_ingested': rows,
                'query_count': len(batch),
                'cpu_ms_per_query': statistics.mean(r['cpu_ms'] for r in batch),
                'average_cores_per_query': sum(r['cpu_ms'] for r in batch) / sum(r['query_duration_ms'] for r in batch),
                'rows_read_per_query': statistics.mean(r['read_rows'] for r in batch),
                'query_duration_ms': statistics.mean(r['query_duration_ms'] for r in batch),
            })
        assert len(family_points) == (37 if family == 'drill' else 221)
        assert family_points[-1]['rows_ingested'] == FULL_ROWS
        assert all(a['rows_ingested'] < b['rows_ingested'] for a, b in zip(family_points, family_points[1:]))
        curves.extend(family_points)
    assert len(used) == 1240 and len(active) == 958
    excluded = [r for r in raw if (r['benchmark_query'], r['started_at_utc']) not in used]
    assert len(excluded) == 1 and excluded[0]['started_at_utc'].startswith('2026-08-08 06:54:40')
    metadata = {
        'database': 'quotes_async_t4', 'complete_dataset_rows': FULL_ROWS,
        'source_query_log': {'path': str((HERE / 'query-log.csv').relative_to(REPO)),
                             'sha256': digest(HERE / 'query-log.csv'), 'records': len(raw)},
        'runner_sources': inputs, 'query_hashes': HASHES,
        'matched_runner_executions': len(used), 'active_executions': len(active),
        'active_counts': dict(collections.Counter(r['benchmark_query'] for r in active)),
        'match': 'One query of each expected type in [iteration_started_at, +20 seconds).',
        'active_window': 'Positive-row iterations through first full-dataset observation inclusive; later repeats excluded.',
        'cpu_ms_formula': '(UserTimeMicroseconds + SystemTimeMicroseconds) / 1000',
        'average_cores_formula': 'cpu_ms / query_duration_ms per query; family average is sum(cpu_ms) / sum(query_duration_ms), weighted by elapsed time',
        'rows_read_formula': 'Arithmetic mean of query_log.read_rows across the two or four queries in each iteration',
        'duration_precision': 'Integer milliseconds; average cores are approximate, particularly for very short aggregation queries.',
        'smoothing': 'None', 'outlier_exclusions': [], 'query_cache_hits': 0,
        'unmatched_before_runner': [{k: r[k] for k in ['benchmark_query', 'started_at_utc']} for r in excluded],
        'zero_row_baseline': 'Retained in query-log.csv, omitted from plotted metrics.',
        'endpoint': {f: [r for r in curves if r['family'] == f][-1] for f in ['drill', 'aggregate']},
    }
    metadata['accepted_panels'] = ['Data processing throughput', 'Data throughput per CPU core',
                                   'Query latency', 'Effective aggregation throughput']
    metadata['family_rate_formula'] = 'sum(read_bytes) / sum(query_duration_ms / 1000) per scheduled iteration'
    metadata['per_core_rate_formula'] = 'sum(read_bytes) / sum((user_cpu_us + system_cpu_us) / 1000000) per scheduled iteration'
    metadata['per_core_unit'] = 'Bytes per CPU-second; equivalent to bytes/s divided by time-weighted average busy cores. Not divided by 16 provisioned cores.'
    metadata['effective_aggregation_formula'] = '2 * runner.raw_rows / ((A3.query_duration_ms + A4.query_duration_ms) / 1000)'
    metadata['effective_aggregation_scope'] = 'A3 and A4 full-table aggregations only; raw quotes represented by current pre-aggregates. Logical answer coverage, not raw rows scanned or sustained query capacity.'
    metadata['latency_display'] = {'source': 'runner result[query_number][0], seconds',
        'family_mean': 'Equal mean of independently smoothed query latencies',
        'median_windows': {'drill': 5, 'aggregate': 7},
        'horizon': 'Through 100B rows, median windows use observations in that horizon; beyond 100B, use the full positive-row active window through first complete dataset.',
        'sampling': 'Drill: every observation. Aggregate: every third observation through 100B, always include the last; every observation beyond 100B.',
        'boundary': 'Centered windows shrink at either endpoint.', 'outlier_exclusions': []}
    metadata['sanitization'] = 'Removed query IDs, hostnames and repeated query text; measurements, hashes and UTC timestamps retained.'
    metadata['profile_validation'] = {
        'path': str((HERE / 'query-log-profile-stats.csv').relative_to(REPO)),
        'sha256': digest(HERE / 'query-log-profile-stats.csv'), 'records': len(profiles),
        'matched_counter_values_equal': True,
        'selected_rows_equals_read_rows': True, 'selected_bytes_equals_read_bytes': True,
        'active_max_threads': sorted({int(index[r['benchmark_query'], r['started_at_utc']]['max_threads_setting']) for r in active}),
    }
    assert metadata['profile_validation']['active_max_threads'] == [16]
    metadata['sql_sources'] = [{'path': str(p.relative_to(REPO)), 'sha256': digest(p)}
                               for p in sorted((HERE / 'queries').glob('*.sql'))]
    return {**final_charts(active), 'metrics-per-query.csv': csv_text(active), 'chart-data.csv': csv_text(curves),
            'provenance.json': json.dumps(metadata, indent=2) + '\n'}

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--check', action='store_true', help='Verify committed outputs without rewriting them')
    args = parser.parse_args()
    for name, expected in derive().items():
        path = HERE / name
        if args.check:
            assert path.read_text() == expected, f'Stale derived evidence: {path}'
        else:
            path.write_text(expected)
    print('Verified' if args.check else 'Generated', '958 per-query measurements and 258 family observations.')

if __name__ == '__main__':
    main()
