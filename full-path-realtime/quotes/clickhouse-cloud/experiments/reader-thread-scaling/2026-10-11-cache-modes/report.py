#!/usr/bin/env python3
"""Rebuild the findings from the frozen combined CSV, without a service call."""
import csv, json, pathlib
ROOT=pathlib.Path(__file__).resolve().parent
def main():
    rows=list(csv.DictReader((ROOT/'derived/combined-workload.csv').open()));assert len(rows)==18
    index={(r['cache_mode'],int(r['query_condition_cache']),int(r['max_threads'])):r for r in rows}
    cases=[('hot',1,'Hot caches'),('cold',1,'Cold · condition cache on'),('cold',0,'Cold · condition cache off')]
    v=json.loads((ROOT/'derived/validation.json').read_text());context=json.loads((ROOT/'derived/context-validation.json').read_text());assert v['passed'] and context['passed']
    lines=['# Measured reader scaling with three cache cases','',f"The experiment completed **{v['measured_trials']} measured queries** with no failed queries, missing logs, result-cache reads or removed outliers. Measurement window: **{v['measurement_start_utc']}–{v['measurement_end_utc']} UTC**. The service remained on the same 59-vCPU / 236-GiB node and ClickHouse 26.6.1.2292, with identical active data parts before and after.",'','Two original drill-down queries, D1 and D2, each run 12 times per core point and cache case against a fixed 113.2B-quote dataset. Their primary-key filters read about 89.4M quotes and 3.49GB per query; these are actually processed bytes, not the entire dataset divided by latency.','', '## Combined data processing throughput','', '| CPU cores | Hot caches, GB/s | Cold, condition cache on, GB/s | Cold, condition cache off, GB/s |','| ---: | ---: | ---: | ---: |']
    for c in [2,4,8,16,30,59]:lines.append('| '+str(c)+' | '+' | '.join(f"{float(index[m,q,c]['throughput_GB_s']):.3f}"for m,q,_ in cases)+' |')
    lines+=['','## Throughput per CPU core','', 'This uses measured CPU work, matching 11a: total uncompressed MB / total CPU seconds. It is not GB/s divided by the selected-core setting. Both denominators and actual busy-core counts are saved in the CSV.','', '| CPU cores | Hot, MB/s/core | Cold, condition on, MB/s/core | Cold, condition off, MB/s/core |','| ---: | ---: | ---: | ---: |']
    for c in [2,4,8,16,30,59]:lines.append('| '+str(c)+' | '+' | '.join(f"{float(index[m,q,c]['MB_s_per_busy_core']):.1f}"for m,q,_ in cases)+' |')
    lines+=['','## Scaling and the original 3.25GB/s reference','']
    for m,q,label in cases:
        rates={c:float(index[m,q,c]['throughput_GB_s'])for c in [2,8,16,59]}
        lines.append(f"- **{label}:** {rates[16]:.3f}GB/s at 16 cores; {rates[59]:.3f}GB/s at 59. From 8→16 cores, throughput changes by **{rates[16]/rates[8]:.2f}×**; from 16→59 by **{rates[59]/rates[16]:.2f}×** for a 3.69× increase in the setting.")
    lines+=['','The evidence supports scaling, with the actual cache-dependent gains shown above. It does not justify claiming perfect linear scaling across every point. Cold object reads add waiting time; the measured CPU-normalized rate and selected-core utilization help distinguish processing work from elapsed-time throughput.','', 'The original **3.25GB/s** in 11a is a centered-Gaussian-smoothed display endpoint during continuous fresh ingestion on a 16-vCPU / 64-GiB service running 26.4.1.2029. This repeat uses a static dataset, different physical capacity and a newer version. Its cold modes do not recreate ongoing ingestion or establish the old cache state. Numerical closeness to 3.25 is not a reproduction criterion.','', '## Query-condition cache comparison','']
    for c in [16,59]:
        on=float(index['cold',1,c]['throughput_GB_s']);off=float(index['cold',0,c]['throughput_GB_s']);lines.append(f"- At **{c} cores**, cold reads yield **{on:.3f}GB/s with condition caching on** and **{off:.3f}GB/s with it off** ({(off/on-1)*100:+.1f}% off versus on).")
    lines+=['','The condition-off block ran after the paired hot/cold condition-on block. That timing difference limits causal attribution of small differences. Per-round interquartile ranges and every individual trial are retained; neither case is merged into another.','', '## Cache and service evidence','', '| Case | Measured trials | S3 bytes | Page-cache hits | Page-cache misses | Query-condition cache hits |','| --- | ---: | ---: | ---: | ---: | ---: |']
    for m,q,label in cases:
        g=[r for r in rows if r['cache_mode']==m and int(r['query_condition_cache'])==q]
        lines.append('| '+label+' | '+str(sum(int(r['n'])for r in g))+' | '+' | '.join(str(sum(int(r[k])for r in g))for k in ['s3_read_bytes','page_cache_hits','page_cache_misses','query_condition_cache_hits'])+' |')
    lines+=['','All cold observations also have zero page-cache read bytes and zero distributed-cache read bytes. Full profile counters and exact settings are in the query-log CSV; the pilot includes the filesystem-off-only case demonstrating why that one setting was insufficient on this Cloud service.','', 'The current cache settings, before/after node identity and active parts, background merges, and query activity are saved under `results/`. The raw trial ledger stores server nanosecond durations and client transport time separately.','', '## Reproduce','', 'Run `python3 analyze_cache.py` and `python3 report.py` offline. See [README.md](README.md) for live repeat instructions, cache definitions and the complete evidence contract.']
    details=list(csv.DictReader((ROOT/'derived/individual-query-summary.csv').open()))
    extra=['## Individual-query detail at 16 and 59 cores','','Latency is the median server execution time; memory is the arithmetic mean of per-query peak memory. All other points and quartiles are in the individual-query CSV.','','| Cores | Case | Query | Median latency, ms | Average busy cores | Mean peak memory, MiB |','| ---: | --- | --- | ---: | ---: | ---: |']
    for c in [16,59]:
        for m,q,label in cases:
            for query in ['D1','D2']:
                r=next(r for r in details if int(r['max_threads'])==c and r['cache_mode']==m and int(r['query_condition_cache'])==q and r['query']==query)
                extra.append(f"| {c} | {label} | {query} | {float(r['server_latency_ms_median']):.1f} | {float(r['average_busy_cores']):.2f} | {float(r['mean_query_peak_MiB']):.1f} |")
    activity=list(csv.DictReader((ROOT/'results/query-activity.csv').open()));other=next((r for r in activity if r['activity']=='other queries'),None)
    if other:extra+=['',f"The activity extraction also records {int(other['completed_queries'])} other initial completed queries totaling {float(other['summed_cpu_s']):.1f}CPU seconds over the observation window. These include activity outside the test; the service was not globally isolated. The benchmark runner itself issued one workload query at a time."]
    at=lines.index('## Reproduce');lines[at:at]=extra+['']
    (ROOT/'REPORT.md').write_text('\n'.join(lines)+'\n');print('REPORT.md regenerated from 18 measured points.')
if __name__=='__main__':main()
