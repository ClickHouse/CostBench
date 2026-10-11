#!/usr/bin/env python3
"""Adjacent-cap comparisons and same-cap bridge to the previous node."""
import csv
import pathlib
import random
import statistics
from analyze import quantile, write_csv

ROOT=pathlib.Path(__file__).resolve().parent
BASE=ROOT.with_name('2026-10-11')
caps=[2,4,8,16,30,59]
summary=list(csv.DictReader((ROOT/'derived/scaling-summary.csv').open()))
index={(r['query'],int(r['max_threads'])):r for r in summary}
trials=[r for r in csv.DictReader((ROOT/'derived/all-trials.csv').open()) if r['phase']=='measured']
adjacent=[]
rng=random.Random(6112026)
for query in ['D1','D2','A1','A2','A3','A4']:
    measured={(int(r['round']),int(r['max_threads'])):r for r in trials if r['query']==query}
    for low,high in zip(caps,caps[1:]):
        a,b=index[query,low],index[query,high]
        ratios=[float(measured[n,high]['throughput_GB_s'])/float(measured[n,low]['throughput_GB_s']) for n in range(1,13)]
        boot=[statistics.median(rng.choices(ratios,k=12)) for _ in range(10000)]
        ratio=float(b['throughput_GB_s'])/float(a['throughput_GB_s'])
        adjacent.append({'query':query,'lower_cores':low,'higher_cores':high,
                         'throughput_low_GB_s':float(a['throughput_GB_s']),
                         'throughput_high_GB_s':float(b['throughput_GB_s']),
                         'throughput_ratio':ratio,'ideal_core_ratio':high/low,
                         'ratio_over_ideal':ratio/(high/low),
                         'median_latency_low_ms':float(a['latency_ms_median']),
                         'median_latency_high_ms':float(b['latency_ms_median']),
                         'paired_median_ratio':quantile(ratios,.5),
                         'paired_median_bootstrap_95pct_low':quantile(boot,.025),
                         'paired_median_bootstrap_95pct_high':quantile(boot,.975)})
write_csv('adjacent-core-comparisons.csv',adjacent)
baseline={(r['query'],int(r['max_threads'])):r for r in csv.DictReader((BASE/'derived/scaling-summary.csv').open())}
bridge=[]
for query in ['D1','D2','A1','A2','A3','A4']:
    for cap in [2,4,8,16]:
        a,b=baseline[query,cap],index[query,cap]
        bridge.append({'query':query,'cores':cap,
                       'baseline_16vCPU_service_GB_s':float(a['throughput_GB_s']),
                       'new_59vCPU_service_GB_s':float(b['throughput_GB_s']),
                       'same_cap_throughput_ratio':float(b['throughput_GB_s'])/float(a['throughput_GB_s']),
                       'baseline_median_latency_ms':float(a['latency_ms_median']),
                       'new_median_latency_ms':float(b['latency_ms_median'])})
write_csv('cross-node-same-cap.csv',bridge)
combined=list(csv.DictReader((ROOT/'derived/combined-workload.csv').open()))
primary={int(r['max_threads']):r for r in combined if r['mix']=='original_frequency'}
rounds={(int(r['round']),int(r['max_threads'])):r for r in csv.DictReader((ROOT/'derived/combined-rounds.csv').open()) if r['mix']=='original_frequency'}
mixed=[]
for low,high in zip(caps,caps[1:]):
    ratio=float(primary[high]['throughput_GB_s'])/float(primary[low]['throughput_GB_s'])
    pairs=[float(rounds[n,high]['throughput_GB_s'])/float(rounds[n,low]['throughput_GB_s']) for n in range(1,13)]
    mixed.append({'lower_cores':low,'higher_cores':high,'throughput_ratio':ratio,
                  'ideal_core_ratio':high/low,'ratio_over_ideal':ratio/(high/low),
                  'paired_ratio_p25':quantile(pairs,.25),'paired_ratio_median':quantile(pairs,.5),
                  'paired_ratio_p75':quantile(pairs,.75)})
write_csv('combined-adjacent-comparisons.csv',mixed)
for r in combined:
    if r['mix']=='original_frequency':
        print(r['max_threads'],'cores:',round(float(r['throughput_GB_s']),3),'GB/s')
print('Saved adjacent comparisons and cross-node bridge.')
