#!/usr/bin/env python3
"""Write the findings report from saved, validated evidence; no network access."""
import csv
import json
import pathlib

ROOT = pathlib.Path(__file__).resolve().parent
def read(name):
    return list(csv.DictReader((ROOT / name).open()))
def f(row, key):
    return float(row[key])
caps = [2,4,8,16,30,59]
combined = {(r['mix'],int(r['max_threads'])):r for r in read('derived/combined-workload.csv')}
summary = {(r['query'],int(r['max_threads'])):r for r in read('derived/scaling-summary.csv')}
validation = json.loads((ROOT/'derived/validation.json').read_text())
before = read('results/00-identity-before.csv')[0]
dd = {n:combined['drilldowns_only',n] for n in caps}
ratio = f(dd[16],'throughput_GB_s')/f(dd[8],'throughput_GB_s')
speedup = f(dd[59],'throughput_GB_s')/f(dd[2],'throughput_GB_s')
lines = [
    '# Reader CPU scaling on a 59-vCPU service: measured findings', '',
    f"The combined drill-down workload rises from **{f(dd[2],'throughput_GB_s'):.2f} GB/s at 2 cores to {f(dd[59],'throughput_GB_s'):.2f} GB/s at 59 cores: {speedup:.2f}× throughput**. From 8 to 16 cores it rises from **{f(dd[8],'throughput_GB_s'):.2f} to {f(dd[16],'throughput_GB_s'):.2f} GB/s ({ratio:.3f}×)**. That step is essentially linear; gains taper at the larger core settings.", '',
    'The measurements support a chart showing strong parallel scaling for these drill-downs. They do not support a claim of perfect linear scaling across every query and all 2–59 core settings. All six core points were measured on one fixed **59-vCPU / 236-GiB** read service, using `max_threads` to control parallelism. They are not six physical service-size measurements.', '',
    '## Three workload options', '',
    'Each option keeps the same weights at every core setting. The original-frequency mix remains the benchmark-wide primary result. The drill-down-only subset is a separate, explicitly scoped option for illustrating scan parallelism. The one-of-each mix is a sensitivity check, not a replacement for the benchmark schedule.', '',
    '| CPU cores | Drill-downs only GB/s | Original-frequency mix GB/s | One of each query GB/s |',
    '|---:|---:|---:|---:|']
for cap in caps:
    values=[f(combined[mix,cap],'throughput_GB_s') for mix in ['drilldowns_only','original_frequency','one_of_each']]
    lines.append(f'| {cap} | {values[0]:.3f} | {values[1]:.3f} | {values[2]:.3f} |')
lines += ['',
    '- Drill-downs only: D1 and D2 once each.',
    '- Original-frequency mix: D1/D2 once each; A1–A4 six times each, matching their original hourly / ten-minute cadence.',
    '- One of each query: equal counts of D1, D2, A1, A2, A3 and A4.', '',
    'Every rate is weighted total uncompressed read bytes divided by weighted total server execution time. Idle scheduling gaps are excluded. These are rates while queries execute, not sustained ingestion rates or hourly service throughput.', '',
    '![Workload options](derived/workload-options.png)', '',
    '## Recommended drill-down view', '',
    'Use **CPU cores** on a linear x-axis, with a fixed 113.2B-quote dataset and one combined D1/D2 curve. GB/s aligns with the existing data-processing visual. A rows-per-second version is also valid when it counts rows actually processed.', '',
    '| CPU cores | Uncompressed GB/s | Million quotes processed/s | MB/s per selected core |',
    '|---:|---:|---:|---:|']
for cap in caps:
    r=dd[cap]
    lines.append(f"| {cap} | {f(r,'throughput_GB_s'):.3f} | {f(r,'million_read_rows_s'):.1f} | {f(r,'MB_s_per_thread_limit'):.1f} |")
lines += ['',
    '![Drill-down metric options](derived/drilldown-metric-options.png)', '',
    'The raw table contains **113,217,743,918 quotes**. Each drill-down selects AAPL through the `(sym, t)` primary-key prefix and reads about **89.4M quotes / 3.49GB uncompressed**. It does not scan the entire raw table. Dividing the complete 113.2B dataset by its latency would include index-skipped data in the apparent processing rate. The measured rows/s variant excludes skipped data.', '',
    'The per-core panel divides throughput by the selected core budget. The existing ingestion-progress visual normalizes by measured CPU work. Those denominators differ and should remain explicit.', '',
    '### Adjacent core settings', '',
    '| Change | Core-count factor | Drill-down throughput factor |',
    '|---|---:|---:|']
for low,high in zip(caps,caps[1:]):
    observed=f(dd[high],'throughput_GB_s')/f(dd[low],'throughput_GB_s')
    lines.append(f'| {low} → {high} | {high/low:.3f}× | {observed:.3f}× |')
lines += ['',
    'The 2→59 span uses 29.5× as many permitted processing cores and delivers about 19.4× drill-down throughput. The larger settings therefore show diminishing returns. The comparison isolates parallelism on the large service; it cannot establish the performance or cost of physically downsizing to a 2-vCPU / 8-GiB node.', '',
    '## Individual-query evidence', '',
    'Pooled GB/s and median server execution time; 12 repetitions per cell. Every measured observation is included.', '',
    '| Query | 8-core GB/s | 16-core GB/s | 30-core GB/s | 59-core GB/s | 8-core latency | 16-core latency | 59-core latency |',
    '|---|---:|---:|---:|---:|---:|---:|---:|']
names={'D1':'D1 · hourly OHLCV bars','D2':'D2 · risk / liquidity profile','A1':'A1 · single-symbol summary','A2':'A2 · watchlist summary','A3':'A3 · top movers','A4':'A4 · daily activity'}
for q in names:
    a,b,c,d=(summary[q,n] for n in [8,16,30,59])
    lines.append(f"| {names[q]} | {f(a,'throughput_GB_s'):.3f} | {f(b,'throughput_GB_s'):.3f} | {f(c,'throughput_GB_s'):.3f} | {f(d,'throughput_GB_s'):.3f} | {f(a,'latency_ms_median'):.3f} ms | {f(b,'latency_ms_median'):.3f} ms | {f(d,'latency_ms_median'):.3f} ms |")
lines += ['',
    '![Individual-query scaling](derived/thread-scaling.png)', '',
    'A1/A2 already execute in roughly 5–10ms at the smallest tested setting and gain little from extra cores. A3/A4 improve from roughly 63–65ms at 2 cores to about 12–13ms at 30/59 cores. Their timings flatten at the larger settings. This supports calling them already very fast for this workload; it does not prove they are as fast as theoretically possible.', '',
    '## CPU work', '',
    'CPU seconds are query-log `UserTimeMicroseconds + SystemTimeMicroseconds`. Dividing by server elapsed time gives average concurrent CPU use in core equivalents. Peak thread usage also includes helper threads and is not a count of busy CPU cores.', '',
    '| Query / cores | Average concurrent CPU use | Median CPU seconds | GB per CPU second |',
    '|---|---:|---:|---:|']
for q in ['D1','D2']:
    for cap in [2,8,16,30,59]:
        r=summary[q,cap]
        lines.append(f"| {q} / {cap} | {f(r,'average_busy_cores'):.2f} | {f(r,'cpu_s_median'):.3f} | {f(r,'GB_per_cpu_s'):.3f} |")
lines += ['',
    '![CPU scaling](derived/cpu-scaling.png)', '',
    'The substantial scans spread roughly similar total CPU work across more cores, with a modest rise in CPU work at the highest setting. At 59, average query-associated CPU use is about 39–41 core equivalents. The experiment does not isolate the cause of diminishing returns, so it should not attribute them to a particular hardware or scheduling bottleneck.', '',
    f"{validation['measured_trials_without_cpu_counters']} measured A1 observations lack nonzero CPU counters. Their CPU-derived fields are marked unavailable; all latency, byte, row, memory and cache observations remain included. Every D1, D2, A2, A3 and A4 observation has positive CPU counters. The drill-down-only combined CPU figures therefore have complete coverage.", '',
    '## Same-cap bridge to the 16-vCPU baseline', '',
    'The lower core settings were repeated on the larger node. The main curve uses only measurements from that node. The bridge records possible host, cache and timing differences rather than assuming exact equivalence.', '',
    '| Query | New-node / baseline throughput at 8 cores | At 16 cores |',
    '|---|---:|---:|']
bridge={(r['query'],int(r['cores'])):r for r in read('derived/cross-node-same-cap.csv')}
for q in names:
    lines.append(f"| {q} | {f(bridge[q,8],'same_cap_throughput_ratio'):.3f}× | {f(bridge[q,16],'same_cap_throughput_ratio'):.3f}× |")
lines += ['',
    'Exact comparisons for 2/4/8/16 are saved in `derived/cross-node-same-cap.csv`. Both runs used ClickHouse 26.6.1.2292 and identical active benchmark parts. The physical node and memory capacity changed between runs; those differences are preserved in their metadata.', '',
    '## Validation and environment', '',
    f"- Version **{before['version']}**, server timezone UTC. One node, unchanged identity/version with increasing uptime before/after.",
    f"- Measured window: **{validation['measured_window_start_utc']}–{validation['measured_window_end_utc']} UTC**.",
    '- 36 pilot + 36 additional warmup + **432 measured** executions; 12 measured repeats for each of six queries and six core settings.',
    '- **504/504** initial server-query records collected; no failed, missing or duplicate observations, automatic retries or removed outliers.',
    '- All core settings confirmed in query-log Settings; result cache off and no result-cache reads; parallel replicas disabled.',
    '- HTTP read-byte/row counters exactly match query logs. Nanosecond server timings agree with rounded query-log durations.',
    '- Result guards remain non-binding; A3 retains LIMIT 20. FORMAT Null excludes result-transfer time. Client transport wall time is preserved separately.',
    '- Raw and summary table parts, row counts, stored bytes, marks and modification times remain identical before/after and match the baseline.',
    '- 424 asynchronous metric samples per capacity field confirm constant **59 vCPUs / 236GiB** during measurement.',
    '- No measured page-cache misses or object-storage read bytes were recorded; underlying caches were warmed, while the query-result cache stayed disabled.',
    f"- {validation['other_initial_queries']} other initial-query records overlap the measured window, with {validation['other_query_cpu_s']:.2f} reported CPU seconds. Sampled background merge tasks reach {validation['background_merge_tasks_max_sampled']}; the benchmark data parts remain unchanged. The service is not assumed to be otherwise idle.",
    '- Latencies are server execution times on a warm-cache, fixed dataset. This is a controlled parallelism experiment separate from the original active-ingestion benchmark.',
    '- IQRs and paired/bootstrap statistics describe repeat variation in this session. Twelve repeats do not establish a production tail-latency SLA or portability across hosts, datasets and physical tiers.', '',
    '## Reproduce', '',
    'See [README.md](README.md) for offline and live commands. `analyze.py`, `combine.py`, `extend_analysis.py`, `report.py` and `plot.py` regenerate findings from saved evidence. SQL, CSV/JSONL, full profile maps, cache counters, service context and file hashes are included. No credentials are included.', '',
    'Per the ClickHouse `agent-query-safety` rule, queries use readonly access with explicit row, scan and time guards. Per `schema-pk-filter-on-orderby`, saved plans verify the selective `sym` prefix. A3/A4 intentionally scan the complete small maintained summary table within those bounds.', '',
    'Official references: [max_threads](https://clickhouse.com/docs/reference/settings/session-settings/max-threads#max_threads), [Query complexity restrictions](https://clickhouse.com/docs/operations/settings/query-complexity), [Choosing a primary key](https://clickhouse.com/docs/best-practices/choosing-a-primary-key).', '']
(ROOT/'REPORT.md').write_text('\n'.join(lines))
print('Wrote REPORT.md from validated evidence.')
