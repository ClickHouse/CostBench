#!/usr/bin/env python3
"""Reproduce Blog 6 writer metrics from the committed, sanitized source CSVs.

Python 3.9+; standard library only. --check regenerates in a temporary directory
and compares every committed derived file without changing repository files.
"""
from pathlib import Path
from collections import defaultdict
from datetime import datetime, timezone
import argparse, copy, csv, hashlib, json, math, shutil, tempfile

HERE = Path(__file__).resolve().parent
FULL_ROWS = 113217743918
START = int(datetime(2026, 8, 8, 6, 55, 56, tzinfo=timezone.utc).timestamp())
END = int(datetime(2026, 8, 9, 19, 40, 58, tzinfo=timezone.utc).timestamp())
STAGES = ['Parsing', 'Sorting', 'Pre-aggregation', 'Compression', 'Other ingestion']
MANIFEST = json.loads((HERE / 'source-manifest.json').read_text())


def read_source(name):
    key = name.removesuffix('.json')
    source = MANIFEST['sources'][key]
    blocks = []
    for table in source['tables']:
        path = HERE / table['path']
        assert hashlib.sha256(path.read_bytes()).hexdigest() == table['sha256'], path
        with path.open(newline='') as stream:
            reader = csv.reader(stream)
            assert next(reader) == table['columns'], path
            rows = []
            for row in reader:
                assert len(row) == len(table['columns']), path
                rows.append([cell if kind == 'string' else json.loads(cell)
                             for cell, kind in zip(row, table['cell_encoding'])])
        assert len(rows) == table['rows'], path
        blocks.append(rows)
    if source['shape'] == 'sections':
        return blocks
    if source['shape'] == 'single_row':
        assert len(blocks[0]) == 1
        return blocks[0][0]
    return blocks[0]


def write_csv(out, name, columns, rows):
    with (out / (name + '.csv')).open('w', newline='') as stream:
        writer = csv.writer(stream, lineterminator="\n")
        writer.writerow(columns)
        writer.writerows(rows)


def numeric_csv(out, name):
    with (out / (name + '.csv')).open(newline='') as stream:
        return [{key: float(value) for key, value in row.items() if not key.endswith('_utc')}
                for row in csv.DictReader(stream)]


def derive_rates(out):
    """Whole-service interval counters, rates and duration-weighted display."""
    EVIDENCE = out
    metric = {row[0]: row for row in read_source('active-metric-series.json')}
    parts = {row[0]: row for row in read_source('active-parts-series.json')}
    raw = [row for row in read_source('active-newpart-series.json') if row[1] == 'quotes']
    assert len(raw) == len(metric) == len(parts) == 442
    assert sum(row[3] for row in raw) == FULL_ROWS

    rows = []
    cumulative = 0
    for t, table, new_parts, inserted_rows, uncompressed, compressed in raw:
        m, a = metric[t], parts[t]
        beginning, ending = max(t, START), min(t + 300, END)
        duration = ending - beginning
        assert duration > 0 and new_parts > 0 and m[6] > 0
        cumulative += inserted_rows
        cpu_seconds = m[6] / 1e6
        rows.append({
            'bucket_start_utc': datetime.fromtimestamp(beginning, timezone.utc).isoformat(),
            'bucket_end_utc': datetime.fromtimestamp(ending, timezone.utc).isoformat(),
            'wall_seconds': duration, 'cumulative_raw_rows': cumulative,
            'progress': cumulative / FULL_ROWS, 'inserted_raw_rows': inserted_rows,
            'inserted_raw_uncompressed_bytes': uncompressed,
            'inserted_raw_compressed_bytes': compressed, 'new_raw_parts': new_parts,
            'writer_cpu_seconds': cpu_seconds, 'writer_average_cores': cpu_seconds / duration,
            'ingest_bytes_per_second': uncompressed / duration,
            'ingest_bytes_per_cpu_second': uncompressed / cpu_seconds,
            'merge_read_uncompressed_bytes': m[10],
            'merge_bytes_per_second': m[10] / duration,
            'merge_rows_per_second': m[9] / duration,
            'max_active_parts_per_partition': a[1],
            'average_rows_per_new_part': inserted_rows / new_parts,
            'raw_new_part_compression_ratio': uncompressed / compressed,
            'delayed_inserts': m[11], 'rejected_inserts': m[12],
        })
    assert cumulative == FULL_ROWS
    assert sum(row['wall_seconds'] for row in rows) == END - START
    assert all(row['delayed_inserts'] == row['rejected_inserts'] == 0 for row in rows)
    with (EVIDENCE / 'chart-metrics.csv').open('w', newline='') as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0]), lineterminator="\n"); w.writeheader(); w.writerows(rows)

    total_bytes = sum(r['inserted_raw_uncompressed_bytes'] for r in rows)
    total_cpu = sum(r['writer_cpu_seconds'] for r in rows)
    total_merge = sum(r['merge_read_uncompressed_bytes'] for r in rows)
    total_parts = sum(r['new_raw_parts'] for r in rows)
    summary = {
        'run_start_utc': rows[0]['bucket_start_utc'], 'run_end_utc': rows[-1]['bucket_end_utc'],
        'raw_rows': FULL_ROWS, 'raw_uncompressed_bytes': total_bytes,
        'writer_nodes': 2, 'cores_per_node': 2, 'query_max_threads': 2,
        'mean_ingest_gb_s': total_bytes / (END - START) / 1e9,
        'ingest_mb_per_cpu_second': total_bytes / total_cpu / 1e6,
        'mean_writer_cores': total_cpu / (END - START),
        'mean_merge_gb_s': total_merge / (END - START) / 1e9,
        'mean_total_processing_gb_s': (total_bytes + total_merge) / (END - START) / 1e9,
        'total_processing_mb_per_cpu_second': (total_bytes + total_merge) / total_cpu / 1e6,
        'mean_inserted_rows_per_second': FULL_ROWS / (END - START),
        'max_active_parts_per_partition': max(r['max_active_parts_per_partition'] for r in rows),
        'mean_rows_per_new_part': FULL_ROWS / total_parts,
        'equivalent_3000_row_batches_per_new_part': FULL_ROWS / total_parts / 3000,
        'raw_new_part_compression_ratio': total_bytes / sum(r['inserted_raw_compressed_bytes'] for r in rows),
        'delayed_inserts': sum(r['delayed_inserts'] for r in rows),
        'rejected_inserts': sum(r['rejected_inserts'] for r in rows),
    }
    (EVIDENCE / 'rate-summary.json').write_text(json.dumps(summary, indent=2) + '\n')

    # User-authorized strong display smoothing. Positive Gaussian weights retain
    # broad trends, never exclude observations, and use actual elapsed durations.
    # Rates are ratios of weighted counter totals; both stacked components share
    # the same wall-time or CPU-time denominator. Raw exports above stay untouched.
    SIGMA_SECONDS = 3600
    RADIUS_SECONDS = 3 * SIGMA_SECONDS
    times = [(datetime.fromisoformat(r['bucket_start_utc']).timestamp() +
              datetime.fromisoformat(r['bucket_end_utc']).timestamp()) / 2 for r in rows]
    display = []
    for i, r in enumerate(rows):
        neighbors = [(j, math.exp(-0.5 * ((t - times[i]) / SIGMA_SECONDS) ** 2))
                     for j, t in enumerate(times) if abs(t - times[i]) <= RADIUS_SECONDS]
        def weighted(field):
            return sum(rows[j][field] * weight for j, weight in neighbors)
        duration, cpu = weighted('wall_seconds'), weighted('writer_cpu_seconds')
        ingestion, merges = weighted('inserted_raw_uncompressed_bytes'), weighted('merge_read_uncompressed_bytes')
        display.append({
            'bucket_end_utc': r['bucket_end_utc'], 'progress': r['progress'],
            'cumulative_raw_rows': r['cumulative_raw_rows'],
            'ingest_bytes_per_second': ingestion / duration,
            'merge_bytes_per_second': merges / duration,
            'total_bytes_per_second': (ingestion + merges) / duration,
            'ingest_bytes_per_cpu_second': ingestion / cpu,
            'merge_bytes_per_cpu_second': merges / cpu,
            'total_bytes_per_cpu_second': (ingestion + merges) / cpu,
            'inserted_rows_per_second': weighted('inserted_raw_rows') / duration,
            'max_active_parts_per_partition': sum(rows[j]['max_active_parts_per_partition'] * weight * rows[j]['wall_seconds'] for j, weight in neighbors) / duration,
        })
    with (EVIDENCE / 'chart-display-metrics.csv').open('w', newline='') as f:
        w = csv.DictWriter(f, fieldnames=list(display[0]), lineterminator="\n"); w.writeheader(); w.writerows(display)
    for r in display:
        assert math.isclose(r['total_bytes_per_second'], r['ingest_bytes_per_second'] + r['merge_bytes_per_second'], rel_tol=1e-12)
        assert math.isclose(r['total_bytes_per_cpu_second'], r['ingest_bytes_per_cpu_second'] + r['merge_bytes_per_cpu_second'], rel_tol=1e-12)

    return summary


def derive_preparation(out, rate_summary):
    """Reconstruct summary rows and allocate measured insert CPU by samples."""
    OUT = out
    def export(name, columns, rows):
        write_csv(out, name, columns, rows)
    def smooth(rows, numerators, denominator):
        times=[(max(r['t'],START)+min(r['t']+r['width'],END))/2 for r in rows]
        result=[]
        for i,r in enumerate(rows):
            weights=[(j,math.exp(-.5*((t-times[i])/3600)**2)) for j,t in enumerate(times) if abs(t-times[i])<=10800]
            denom=sum(rows[j][denominator]*w for j,w in weights)
            assert denom>0
            result.append({'progress':r['progress'], **{n:sum(rows[j][n]*w for j,w in weights)/denom for n in numerators}})
        return result

    newparts=read_source('newpart-timing-series.json')
    views=read_source('view-series.json')
    consolidation=read_source('merge-consolidation-series.json')
    integrity=read_source('part-integrity.json')
    before=read_source('before-run-parts.json')
    operation_types=read_source('part-operation-types.json')
    assert not any(r[1].startswith('Mutate') for r in operation_types)
    assert all(r[3]==0 for r in operation_types)
    assert all(r[4]==r[5] for r in consolidation if r[1]=='quotes')
    assert len(newparts)==len(consolidation)==884
    assert [row[:6] for row in newparts] == read_source('active-newpart-series'), 'NewPart exports differ'
    assert all(r[2]==r[3] for r in integrity), 'Duplicate part-producing events'
    raw={r[0]:r for r in newparts if r[1]=='quotes'}
    agg={r[0]:r for r in newparts if r[1]=='quotes_daily'}
    mv={r[0]:r for r in views}
    assert sum(r[3] for r in raw.values())==FULL_ROWS
    assert sum(r[2] for r in raw.values())==256268
    assert sum(r[2] for r in agg.values())==256268
    assert sum(r[3] for r in agg.values())==sum(r[3] for r in views)==1366093459
    assert sum(r[2] for r in views)==FULL_ROWS

    merge_by_bucket=defaultdict(lambda:[0]*10)
    for r in consolidation:
        for i,v in enumerate(r[2:]): merge_by_bucket[r[0]][i]+=v

    summary_merge_by_bucket={r[0]:r for r in consolidation if r[1]=='quotes_daily'}
    reduction=[]; cum_raw=cum_summary=cum_uncompressed=cum_disk=maintained_summary=0
    for t,r in sorted(raw.items()):
        a=agg[t]; m=merge_by_bucket[t]; duration=min(t+300,END)-max(t,START)
        cum_raw+=r[3]; cum_summary+=a[3]; cum_uncompressed+=r[4]; cum_disk+=r[5]
        sm=summary_merge_by_bucket[t]
        maintained_summary+=a[3]+sm[4]-sm[5]
        assert maintained_summary>0
        reduction.append({'t':t,'width':300,'seconds':duration,'progress':cum_raw/FULL_ROWS,
            'raw_rows':r[3],'summary_rows':a[3],'uncompressed':r[4],'disk':r[5],
            'cumulative_raw_rows':cum_raw,'cumulative_summary_rows':cum_summary,
            'maintained_summary_rows':maintained_summary,
            'cumulative_uncompressed':cum_uncompressed,'cumulative_disk':cum_disk,
            'new_parts':r[2]+a[2],'merge_input_parts':m[1],'merge_output_parts':m[0],
            'net_parts_removed':m[1]-m[0], 'sorting_us':r[6]+a[6],
            'view_cpu_us':mv[t][5], 'view_elapsed_ms':mv[t][4]})
    assert reduction[-1]['progress']==1
    assert sum(r['seconds'] for r in reduction)==END-START
    red_display=smooth(reduction,['uncompressed','disk','raw_rows','summary_rows','new_parts','merge_input_parts','merge_output_parts','net_parts_removed'],'seconds')
    sort_display=smooth(reduction,['sorting_us','view_cpu_us'],'raw_rows')
    state_rows=[{**r,'state_seconds':r['maintained_summary_rows']*r['seconds']} for r in reduction]
    state_display=smooth(state_rows,['state_seconds'],'seconds')
    for r,d in zip(reduction,red_display):
        d['compression_factor']=r['cumulative_uncompressed']/r['cumulative_disk']
        d['row_reduction_factor']=r['cumulative_raw_rows']/r['cumulative_summary_rows']
    for r,d,state in zip(reduction,red_display,state_display):
        d['maintained_summary_rows']=state['state_seconds']
        d['maintained_row_reduction_factor']=r['cumulative_raw_rows']/state['state_seconds']

    trace = []
    for i in range(10):
        audited = read_source(f'cpu-stage-audit-{i:02}')
        assert audited == read_source(f'cpu-stage-chunk-{i:02}'), 'Codec audit changed sample counts'
        trace.extend(audited)
    flush = []
    for i in range(37):
        flush.extend(read_source(f'flush-cpu-hour-{i:02}'))
    assert sum(r[2] for r in flush)==256268
    assert sum(r[7] for r in flush)==FULL_ROWS+1366093459
    assert all(r[4]==r[5] for r in trace), 'Unsymbolized CPU samples'
    assert len({r[3] for r in trace})==2
    # Scope text is retained verbatim; observed stage names define ingestion scope.
    trace_counts=defaultdict(lambda:defaultdict(int))
    for t,scope,stage,host,count,symbolized in trace:
        if stage in STAGES: trace_counts[(t,host)][stage]+=count
    flush_counts=defaultdict(lambda:[0]*7)
    for t,host,*numbers in flush:
        for i,v in enumerate(numbers): flush_counts[(t,host)][i]+=v
    cpu_by_bucket=defaultdict(lambda:{'cpu_us':0,**{s:0 for s in STAGES},'executions':0})
    for key,counts in flush_counts.items():
        samples=trace_counts[key]; total_samples=sum(samples.values())
        assert total_samples>0,key
        r=cpu_by_bucket[key[0]]; r['cpu_us']+=counts[1]; r['executions']+=counts[0]
        for s in STAGES: r[s]+=counts[1]*samples[s]/total_samples
        assert math.isclose(sum(counts[1]*samples[s]/total_samples for s in STAGES),counts[1],rel_tol=1e-12)
    assert len(flush_counts) == 150, 'Expected both nodes in all 75 CPU buckets'
    raw30=defaultdict(int)
    for t,r in raw.items(): raw30[t//1800*1800]+=r[3]
    cpu=[]; cumulative=0
    for t,c in sorted(cpu_by_bucket.items()):
        n=raw30[t]; cumulative+=n
        assert n>0
        cpu.append({'t':t,'width':1800,'progress':cumulative/FULL_ROWS,'raw_rows':n,
            'seconds':min(t+1800,END)-max(t,START),**c})
    assert cumulative==FULL_ROWS
    assert len(cpu)==75
    cpu_work=smooth(cpu,STAGES+['cpu_us'],'raw_rows')
    cpu_share=smooth(cpu,STAGES,'cpu_us')
    for row in cpu_share: assert math.isclose(sum(row[s] for s in STAGES),1,rel_tol=1e-12)

    export('newpart-timing-series',['bucket_unix','table','parts','rows','uncompressed_bytes','part_size_bytes','sorting_elapsed_us','merging_elapsed_us','duration_ms'],newparts)
    export('view-series',['bucket_unix','executions','rows_in','rows_out','view_elapsed_ms','cpu_us','sorting_elapsed_us','merging_elapsed_us','real_us','peak_memory_bytes'],views)
    export('merge-consolidation-series',['bucket_unix','table','merges','input_parts','rows_out','rows_read','bytes_read','output_part_size_bytes','output_uncompressed_bytes','cpu_us','duration_ms','peak_memory_bytes'],consolidation)
    export('ingestion-cpu-query-series',['bucket_unix','hostname','executions','cpu_us','os_cpu_us','elapsed_ms','read_rows','written_rows','sorting_elapsed_us'],flush)
    export('cpu-stage-samples',['bucket_unix','scope','stage','hostname','cpu_samples','symbolized_samples'],trace)
    for name,series in [('reduction-metrics',reduction),('reduction-display',red_display),('sorting-display',sort_display),('cpu-metrics',cpu),('cpu-work-display',cpu_work),('cpu-share-display',cpu_share)]:
        export(name,list(series[0]),[[r[k] for k in series[0]] for r in series])

    total_cpu=sum(r['cpu_us'] for r in cpu)/1e6
    phase_cpu={s:sum(r[s] for r in cpu)/1e6 for s in STAGES}
    merge_cpu=sum(r[9] for r in consolidation)/1e6
    base=rate_summary
    view_run=read_source('view-run.json')[0]
    sample_totals={s:sum(r[4] for r in trace if r[2]==s) for s in STAGES}
    summary={
        'run_start_utc':'2026-08-08 06:55:56','run_end_utc':'2026-08-09 19:40:58',
        'nodes':2,'cpu_cores_per_node':2,'total_cpu_cores':4,'query_max_threads':2,
        'raw_quotes_ingested':FULL_ROWS,'wall_seconds':END-START,
        'mean_raw_quotes_per_second':FULL_ROWS/(END-START),
        'raw_new_part_uncompressed_bytes':cum_uncompressed,'raw_new_part_size_bytes':cum_disk,
        'raw_initial_part_compression_factor':cum_uncompressed/cum_disk,
        'raw_initial_part_disk_reduction_percent':100*(1-cum_disk/cum_uncompressed),
        'initial_mv_summary_rows':cum_summary,'initial_mv_row_reduction_factor':FULL_ROWS/cum_summary,
        'initial_mv_row_reduction_percent':100*(1-cum_summary/FULL_ROWS),
        'cutoff_summary_rows_from_part_counters':maintained_summary,
        'cutoff_row_reduction_factor_from_part_counters':FULL_ROWS/maintained_summary,
        'summary_rows_removed_by_active_window_merges':cum_summary-maintained_summary,
        'successful_flushes':256268,'recorded_ingestion_cpu_seconds':total_cpu,
        'recorded_ingestion_cpu_ms_per_million_quotes':total_cpu/FULL_ROWS*1e9,
        'recorded_materialized_view_cpu_seconds':view_run[8]/1e6,
        'recorded_materialized_view_cpu_ms_per_million_quotes':view_run[8]/FULL_ROWS*1000,
        'recorded_sorting_elapsed_seconds':sum(r['sorting_us'] for r in reduction)/1e6,
        'recorded_sorting_elapsed_ms_per_million_quotes':sum(r['sorting_us'] for r in reduction)/FULL_ROWS*1000,
        'ingestion_cpu_samples':sum(sample_totals.values()),'sample_counts':sample_totals,
        'estimated_stage_cpu_seconds':phase_cpu,
        'estimated_stage_share_percent':{s:100*v/total_cpu for s,v in phase_cpu.items()},
        'estimated_stage_cpu_ms_per_million_quotes':{s:v/FULL_ROWS*1e9 for s,v in phase_cpu.items()},
        'background_merges':sum(r[2] for r in consolidation),'merge_input_parts':sum(r[3] for r in consolidation),
        'replacement_parts':sum(r[2] for r in consolidation),
        'net_parts_removed_by_merges':sum(r[3]-r[2] for r in consolidation),
        'new_raw_and_summary_parts':512536,
        'merge_input_parts_per_second':sum(r[3] for r in consolidation)/(END-START),
        'replacement_parts_per_second':sum(r[2] for r in consolidation)/(END-START),
        'net_parts_removed_per_second':sum(r[3]-r[2] for r in consolidation)/(END-START),
        'new_parts_per_second':512536/(END-START),
        'mean_merge_fan_in':sum(r[3] for r in consolidation)/sum(r[2] for r in consolidation),
        'part_event_net_growth_during_window':512536-sum(r[3]-r[2] for r in consolidation),
        'recorded_benchmark_merge_cpu_seconds':merge_cpu,
        'recorded_summary_table_merge_cpu_seconds':sum(r[9] for r in consolidation if r[1]=='quotes_daily')/1e6,
        'max_recorded_raw_merge_memory_bytes':max(r[11] for r in consolidation if r[1]=='quotes'),
        'max_recorded_summary_merge_memory_bytes':max(r[11] for r in consolidation if r[1]=='quotes_daily'),
        'recorded_writer_service_cpu_seconds':base['mean_writer_cores']*(END-START),
        'mean_writer_service_cores':base['mean_writer_cores'],
        'max_active_parts_per_partition':base['max_active_parts_per_partition'],
        'mean_total_processing_mb_s':base['mean_total_processing_gb_s']*1000,
        'mean_total_processing_mb_per_cpu_second':base['total_processing_mb_per_cpu_second'],
        'delayed_insert_counter':base['delayed_inserts'],'rejected_insert_counter':base['rejected_inserts'],
        'pre_run_parts_activity':before,
    }
    assert math.isclose(sum(phase_cpu.values()),total_cpu,rel_tol=1e-12)
    summary['recorded_benchmark_cpu_accounting_remainder_seconds']=summary['recorded_writer_service_cpu_seconds']-total_cpu-merge_cpu
    summary['recorded_mv_execution_and_summary_merge_cpu_share_percent']=100*(summary['recorded_materialized_view_cpu_seconds']+summary['recorded_summary_table_merge_cpu_seconds'])/summary['recorded_writer_service_cpu_seconds']
    if 'current-part-snapshot' in MANIFEST['sources']:
        summary['current_storage_snapshot']=read_source('current-part-snapshot.json')
        current={r[0]:r for r in summary['current_storage_snapshot']}
        coverage=read_source('summary-coverage-snapshot.json')
        assert len(coverage)==2 and all(r[2]==FULL_ROWS for r in coverage)
        assert current['quotes'][2]==FULL_ROWS
        summary['current_snapshot_raw_data_compression_factor']=current['quotes'][3]/current['quotes'][4]
        summary['current_snapshot_row_reduction_factor']=FULL_ROWS/current['quotes_daily'][2]
        summary['current_snapshot_summary_coverage']=coverage
        summary['after_run_merges']=read_source('after-run-merges.json')
        summary['before_run_table_identities']=read_source('before-run-table-identities.json')
        summary['whole_service_cpu_share_percent']={
            'Benchmark ingestion':100*total_cpu/summary['recorded_writer_service_cpu_seconds'],
            'Benchmark background merges':100*merge_cpu/summary['recorded_writer_service_cpu_seconds'],
            'Other / unmatched service work':100*summary['recorded_benchmark_cpu_accounting_remainder_seconds']/summary['recorded_writer_service_cpu_seconds']}
    (OUT/'preparation-summary.json').write_text(json.dumps(summary,indent=2)+'\n')

    return summary


def derive_memory(out):
    """Sum node means after host/second aggregation, preserving partial buckets."""
    EVIDENCE = out
    def export(name, rows):
        write_csv(out, name, list(rows[0]), [list(row.values()) for row in rows])
    GIB=2**30
    node=read_source('memory-node-series.json')
    dashboard=read_source('memory-dashboard-series.json')
    capacity=read_source('memory-capacity-check.json')
    run=list(csv.DictReader((EVIDENCE/'chart-metrics.csv').open()))
    hosts=sorted({r[1] for r in node})
    assert len(hosts)==len(capacity)==2
    assert {r[0] for r in capacity}==set(hosts)
    assert all(r[4]==r[5]==8*GIB for r in capacity)
    available=sum(r[4] for r in capacity)
    buckets=defaultdict(list)
    for r in node:buckets[r[0]].append(r)
    assert len(buckets)==len(run)==len(dashboard)==442
    assert len(node)==884 and len({(r[0],r[1]) for r in node})==884

    raw=[]
    for r in run:
        start=datetime.fromisoformat(r['bucket_start_utc']).timestamp()
        end=datetime.fromisoformat(r['bucket_end_utc']).timestamp()
        t=int(start)//300*300
        group=buckets[t]
        assert len(group)==2 and {n[1] for n in group}==set(hosts)
        assert all(n[2]>0 and n[3]>=n[2] and 0<=n[5]<=n[6] for n in group)
        seconds=end-start
        assert seconds==float(r['wall_seconds'])
        mean=sum(n[4]/n[2] for n in group)
        assert 0<=mean<=available
        raw.append({'bucket_unix':t,'midpoint_unix':(start+end)/2,
                    'wall_seconds':seconds,'progress':float(r['progress']),
                    'rows_ingested':int(r['cumulative_raw_rows']),
                    'memory_usage_bytes':mean,'available_memory_bytes':available,
                    'utilization_percent':100*mean/available,
                    'node_timestamp_samples':sum(n[2] for n in group),
                    'log_samples':sum(n[3] for n in group)})

    display=[]
    for r in raw:
        neighbors=[(n,math.exp(-.5*((n['midpoint_unix']-r['midpoint_unix'])/3600)**2))
                   for n in raw if abs(n['midpoint_unix']-r['midpoint_unix'])<=10800]
        denom=sum(n['wall_seconds']*w for n,w in neighbors)
        mean=sum(n['memory_usage_bytes']*n['wall_seconds']*w for n,w in neighbors)/denom
        display.append({'progress':r['progress'],'rows_ingested':r['rows_ingested'],
                        'memory_usage_bytes':mean,'available_memory_bytes':available,
                        'memory_usage_gib':mean/GIB,'utilization_percent':100*mean/available})
    export('memory-metrics',raw)
    export('memory-display',display)
    seconds=sum(r['wall_seconds'] for r in raw)
    assert seconds==132302
    mean=sum(r['memory_usage_bytes']*r['wall_seconds'] for r in raw)/seconds
    dashboard_mean=sum(r[5] for r in dashboard)/sum(r[1] for r in dashboard)
    summary = {
        'nodes': 2, 'available_memory_bytes': available,
        'mean_memory_usage_gib': mean / GIB,
        'mean_utilization_percent': 100 * mean / available,
        'dashboard_exact_mean_gib': dashboard_mean / GIB,
        'node_aggregation_difference_percent': 100 * (mean / dashboard_mean - 1),
        'last_display': display[-1], 'buckets': len(raw),
        'source_log_samples': sum(r[3] for r in node),
        'distinct_host_seconds': sum(r[2] for r in node),
    }
    (EVIDENCE / 'memory-summary.json').write_text(json.dumps(summary, indent=2) + '\n')
    return summary


def derive_merges(out):
    """Count processed merge input rows, including repeated work."""
    OUT = out
    SIGMA = 3600
    RADIUS = 10800
    def read(name):
        with (OUT / name).open() as f:
            return list(csv.DictReader(f))

    source = MANIFEST['sources']['active-metric-series']['tables'][0]
    metrics = [dict(zip(source['columns'], row)) for row in read_source('active-metric-series')]
    reduction = read('reduction-metrics.csv')
    assert len(metrics) == len(reduction) == 442
    by_time = {int(r['t']): r for r in reduction}
    assert len(by_time) == len(metrics)
    raw = []
    for m in metrics:
        t = int(m['bucket_unix'])
        r = by_time[t]
        seconds = min(t + 300, END) - max(t, START)
        assert seconds == float(r['seconds']) and seconds > 0
        raw.append({'bucket_unix': t, 'seconds': seconds,
                    'midpoint_unix': (max(t, START) + min(t + 300, END)) / 2,
                    'progress': float(r['progress']),
                    'rows_ingested': int(float(r['cumulative_raw_rows'])),
                    'merged_rows': int(m['merged_rows']),
                    'merged_rows_per_second': int(m['merged_rows']) / seconds})
    assert sum(r['seconds'] for r in raw) == END - START
    assert raw[-1]['rows_ingested'] == FULL_ROWS and raw[-1]['progress'] == 1

    display = []
    for r in raw:
        weighted = [(other, math.exp(-.5 * ((other['midpoint_unix'] - r['midpoint_unix']) / SIGMA) ** 2))
                    for other in raw if abs(other['midpoint_unix'] - r['midpoint_unix']) <= RADIUS]
        seconds = sum(other['seconds'] * weight for other, weight in weighted)
        rate = sum(other['merged_rows'] * weight for other, weight in weighted) / seconds
        assert math.isfinite(rate) and rate >= 0
        display.append({'progress': r['progress'], 'rows_ingested': r['rows_ingested'],
                        'merged_rows_per_second': rate})

    for name, values in [('merge-throughput-metrics', raw), ('merge-throughput-display', display)]:
        with (OUT / f'{name}.csv').open('w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=list(values[0]), lineterminator="\n")
            writer.writeheader(); writer.writerows(values)

    summary = {
        'wall_seconds': END - START, 'merged_rows': sum(r['merged_rows'] for r in raw),
        'mean_merged_rows_per_second': sum(r['merged_rows'] for r in raw) / (END - START),
        'buckets': len(raw), 'final_display_rows_per_second': display[-1]['merged_rows_per_second'],
    }
    (OUT / 'merge-throughput-summary.json').write_text(json.dumps(summary, indent=2) + '\n')
    return summary


def build_payloads(out):
    config = json.loads((HERE / 'chart-config.json').read_text())
    rates = numeric_csv(out, 'chart-display-metrics')
    red = numeric_csv(out, 'reduction-metrics')
    reduced = numeric_csv(out, 'reduction-display')
    work = numeric_csv(out, 'cpu-work-display')
    shares = numeric_csv(out, 'cpu-share-display')
    merges = numeric_csv(out, 'merge-throughput-display')
    memory = numeric_csv(out, 'memory-display')
    def points(rows, key, multiplier=1):
        return [[row['progress'], row[key] * multiplier] for row in rows]
    cpu_capacity = [[row['progress'], row['total_bytes_per_second'] / row['total_bytes_per_cpu_second']]
                    for row in rates]
    write_csv(out, 'resource-utilization',
           ['progress', 'rows_ingested', 'used_cpu_cores', 'available_cpu_cores',
            'cpu_used_percent', 'cpu_unused_percent', 'memory_usage_bytes',
            'available_memory_bytes', 'memory_used_percent', 'memory_unused_percent'],
           [[progress, progress * FULL_ROWS, cores, 4, 100 * cores / 4, 100 * (1 - cores / 4),
             row['memory_usage_bytes'], row['available_memory_bytes'],
             row['utilization_percent'], 100 * (1 - row['memory_usage_bytes'] / row['available_memory_bytes'])]
            for (progress, cores), row in zip(cpu_capacity, memory)])
    for number, data in config.items():
        for panel in data['panels']:
            ident = panel['id']
            if ident in ('throughput', 'efficiency'):
                denominator = 'second' if ident == 'throughput' else 'cpu_second'
                panel['points'] = points(rates, 'total_bytes_per_' + denominator)
                for component in panel['stacked']:
                    field = 'ingest' if component['id'] == 'ingestion' else 'merge'
                    component['points'] = points(rates, field + '_bytes_per_' + denominator)
            elif ident in ('cpu-work', 'cpu-share'):
                rows = work if ident == 'cpu-work' else shares
                multiplier = 1000 if ident == 'cpu-work' else 100
                panel['points'] = points(rows, 'cpu_us', 1000) if ident == 'cpu-work' else [[r['progress'], 100] for r in rows]
                for component in panel['stacked']:
                    component['points'] = points(rows, component['label'], multiplier)
            elif ident == 'compression-volume':
                panel['points'] = points(red, 'cumulative_uncompressed')
                panel['series'][0]['points'] = points(red, 'cumulative_uncompressed')
                panel['series'][1]['points'] = points(red, 'cumulative_disk')
                panel['reductionFactor']['points'] = points(reduced, 'compression_factor')
            elif ident == 'maintained-volume':
                panel['points'] = points(red, 'cumulative_raw_rows')
                panel['series'][0]['points'] = points(red, 'cumulative_raw_rows')
                panel['series'][1]['points'] = points(reduced, 'maintained_summary_rows')
                panel['reductionFactor']['points'] = points(reduced, 'maintained_row_reduction_factor')
            elif ident == 'merge-throughput':
                panel['points'] = points(merges, 'merged_rows_per_second')
            elif ident == 'parts':
                panel['points'] = points(rates, 'max_active_parts_per_partition')
            else:
                raise ValueError(ident)
            for component in panel.get('stacked', []):
                assert [p[0] for p in component['points']] == [p[0] for p in panel['points']]
            if panel.get('stacked'):
                assert all(math.isclose(sum(c['points'][i][1] for c in panel['stacked']), value, rel_tol=1e-12)
                           for i, (_, value) in enumerate(panel['points']))
            if 'reductionFactor' in panel:
                assert all(math.isclose(a[1] / b[1], factor[1], rel_tol=1e-12)
                           for a, b, factor in zip(panel['series'][0]['points'], panel['series'][1]['points'], panel['reductionFactor']['points']))
            # One wide CSV for each panel contains every plotted series and factor.
            series = panel.get('stacked') or panel.get('series') or []
            factor = panel.get('reductionFactor')
            columns = ['progress', 'rows_ingested', 'value'] + [c.get('id', c.get('label')) for c in series]
            if factor:
                columns.append('factor')
            write_csv(out, number + '-' + ident, columns,
                   [[progress, progress * FULL_ROWS, value] + [c['points'][i][1] for c in series]
                    + ([factor['points'][i][1]] if factor else [])
                    for i, (progress, value) in enumerate(panel['points'])])
        data['cpuCapacity']['points'] = copy.deepcopy(cpu_capacity)
        data['memoryCapacity']['points'] = points(memory, 'memory_usage_bytes')
        assert [p[0] for p in cpu_capacity] == [r['progress'] for r in memory]
        assert all(0 <= used <= 4 for _, used in cpu_capacity)
        (out / ('chart-data-' + number + '.json')).write_text(json.dumps(data, indent=2) + '\n')


def derive(out):
    rate_summary = derive_rates(out)
    preparation_summary = derive_preparation(out, rate_summary)
    memory_summary = derive_memory(out)
    merge_summary = derive_merges(out)
    build_payloads(out)
    for query, item in MANIFEST['queries'].items():
        assert hashlib.sha256((HERE / query).read_bytes()).hexdigest() == item['sha256'], query
    # Load all supporting exports too: their hashes and row counts are checked.
    for key in MANIFEST['sources']:
        read_source(key)
    provenance = json.loads((HERE / 'definitions.json').read_text())
    provenance['input_sha256'] = {
        str(path.relative_to(HERE)): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in sorted([HERE / 'derive.py', HERE / 'chart-config.json', HERE / 'source-manifest.json',
                            HERE / 'definitions.json', HERE / 'export_results.py'] + list((HERE / 'results').glob('*.csv')) + list((HERE / 'queries').rglob('*.sql')))
    }
    provenance['validation'] = {
        'raw_quotes': FULL_ROWS, 'five_minute_buckets': 442, 'half_hour_cpu_buckets': 75,
        'flush_chunks': 37, 'trace_chunks': 10, 'successful_insert_flushes': 256268,
        'ingestion_cpu_samples': preparation_summary['ingestion_cpu_samples'],
        'all_ingestion_cpu_samples_symbolized': True, 'zero_empty_calibration_buckets': True,
        'writer_nodes': 2, 'raw_and_summary_new_parts': 512536,
        'part_event_errors': 0, 'part_mutations': 0,
        'delayed_inserts': rate_summary['delayed_inserts'], 'rejected_inserts': rate_summary['rejected_inserts'],
        'memory_buckets_with_both_nodes': 442,
        'dashboard_memory_mean_difference_percent': memory_summary['node_aggregation_difference_percent'],
        'merged_input_rows': merge_summary['merged_rows'],
    }
    (out / 'provenance.json').write_text(json.dumps(provenance, indent=2) + '\n')
    return sorted(p.name for p in out.iterdir())


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--check', action='store_true', help='Compare fresh outputs byte for byte; do not modify files.')
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix='writer-metrics-') as temporary:
        out = Path(temporary)
        names = derive(out)
        if args.check:
            failures = [name for name in names if not (HERE / name).exists() or (HERE / name).read_bytes() != (out / name).read_bytes()]
            if failures:
                raise SystemExit('Reproduction differs: ' + ', '.join(failures))
        else:
            for name in names:
                shutil.copyfile(out / name, HERE / name)
    print(('Verified' if args.check else 'Regenerated') + f' {len(names)} derived artifacts for 08a–08d.')


if __name__ == '__main__':
    main()
