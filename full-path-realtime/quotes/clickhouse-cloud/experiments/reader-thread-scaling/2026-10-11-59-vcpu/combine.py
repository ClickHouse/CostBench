#!/usr/bin/env python3
"""Fixed benchmark query mix: weighted bytes / weighted execution time.

Primary weighting mirrors the original frequency: each drill-down once per
hour; each of four aggregations six times per hour. Idle intervals are excluded.
Equal query weighting is also saved as an explicitly separate sensitivity check.
"""
import csv
import pathlib
from analyze import quantile, write_csv

ROOT = pathlib.Path(__file__).resolve().parent
rows = [r for r in csv.DictReader((ROOT / 'derived/all-trials.csv').open()) if r['phase']=='measured']
mixes = {'original_frequency': {'D1':1,'D2':1,'A1':6,'A2':6,'A3':6,'A4':6},
         'one_of_each': {'D1':1,'D2':1,'A1':1,'A2':1,'A3':1,'A4':1},
         'drilldowns_only': {'D1':1,'D2':1}}
combined = []
rounds = []
for name, weights in mixes.items():
    for cap in [2,4,8,16,30,59]:
        group = [r for r in rows if int(r['max_threads'])==cap and r['query'] in weights]
        assert len(group)==12*len(weights)
        byte_count = sum(int(r['read_bytes'])*weights[r['query']] for r in group)
        row_count = sum(int(r['read_rows'])*weights[r['query']] for r in group)
        elapsed = sum(float(r['server_elapsed_s'])*weights[r['query']] for r in group)
        cpu_group = [r for r in group if r['cpu_s']]
        cpu = sum(float(r['cpu_s'])*weights[r['query']] for r in cpu_group)
        rates = []
        row_rates = []
        for round_no in range(1,13):
            batch = [r for r in group if int(r['round'])==round_no]
            assert {r['query'] for r in batch}==set(weights)
            b = sum(int(r['read_bytes'])*weights[r['query']] for r in batch)
            read_rows = sum(int(r['read_rows'])*weights[r['query']] for r in batch)
            t = sum(float(r['server_elapsed_s'])*weights[r['query']] for r in batch)
            rates.append(b/1e9/t)
            row_rates.append(read_rows/1e6/t)
            rounds.append({'mix':name, 'max_threads':cap, 'round':round_no,
                           'weighted_read_bytes':b, 'weighted_server_s':t,
                           'throughput_GB_s':b/1e9/t,
                           'weighted_read_rows':read_rows,
                           'million_read_rows_s':read_rows/1e6/t})
        combined.append({'mix':name, 'max_threads':cap, 'n_rounds':12,
                         'queries_per_reference_cycle':sum(weights.values()),
                         'throughput_GB_s':byte_count/1e9/elapsed,
                         'throughput_GB_s_p25':quantile(rates,.25),
                         'throughput_GB_s_median':quantile(rates,.5),
                         'throughput_GB_s_p75':quantile(rates,.75),
                         'million_read_rows_s':row_count/1e6/elapsed,
                         'million_read_rows_s_p25':quantile(row_rates,.25),
                         'million_read_rows_s_p75':quantile(row_rates,.75),
                         'MB_s_per_thread_limit':byte_count/1e6/elapsed/cap,
                         'MB_s_per_thread_limit_p25':quantile(rates,.25)*1000/cap,
                         'MB_s_per_thread_limit_p75':quantile(rates,.75)*1000/cap,
                         'weighted_GB_per_reference_cycle':byte_count/1e9/12,
                         'weighted_server_s_per_reference_cycle':elapsed/12,
                         'observed_CPU_s_per_reference_cycle':cpu/12,
                         'trials_without_CPU_counters':len(group)-len(cpu_group),
                         'CPU_figure_complete':len(group)==len(cpu_group)})
    selected = [r for r in combined if r['mix']==name]
    base = selected[0]['throughput_GB_s']
    for row in selected:
        row['speedup_vs_2_cores'] = row['throughput_GB_s']/base
        row['cap_scaling_efficiency_pct'] = row['speedup_vs_2_cores']/(row['max_threads']/2)*100
write_csv('combined-workload.csv',combined)
write_csv('combined-rounds.csv',rounds)
for r in combined:
    print(r['mix'],r['max_threads'],f"{r['throughput_GB_s']:.3f} GB/s",
          f"{r['MB_s_per_thread_limit']:.1f} MB/s per permitted query thread",
          'CPU counters missing:',r['trials_without_CPU_counters'])
