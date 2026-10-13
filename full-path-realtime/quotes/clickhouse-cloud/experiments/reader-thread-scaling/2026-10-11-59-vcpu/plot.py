#!/usr/bin/env python3
"""Evidence-native plots; no smoothing or observation removal."""
import csv
import pathlib
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.ticker import ScalarFormatter

ROOT = pathlib.Path(__file__).resolve().parent
rows = list(csv.DictReader((ROOT / 'derived/scaling-summary.csv').open()))
index = {(r['query'], int(r['max_threads'])): r for r in rows}
threads = [2, 4, 8, 16, 30, 59]
colors = {'D1': '#1479B8', 'D2': '#8A4FAD', 'A1': '#C37D22', 'A2': '#C25454',
          'A3': '#248F73', 'A4': '#1479B8'}
plt.rcParams.update({'font.family': 'DejaVu Sans', 'font.size': 10,
                     'axes.spines.top': False, 'axes.spines.right': False,
                     'svg.fonttype': 'none', 'figure.facecolor': 'white'})


def axes_style(ax):
    ax.set_xscale('linear')
    ax.set_xticks(threads)
    ax.get_xaxis().set_major_formatter(ScalarFormatter())
    ax.set_xlabel('CPU cores')
    ax.grid(axis='y', color='#DDE1E5', linewidth=.7)
    ax.set_axisbelow(True)


def draw_series(ax, query, metric, lo, hi, label):
    values = [float(index[(query, n)][metric]) for n in threads]
    lower = [float(index[(query, n)][lo]) for n in threads]
    upper = [float(index[(query, n)][hi]) for n in threads]
    ax.fill_between(threads, lower, upper, color=colors[query], alpha=.14)
    ax.plot(threads, values, color=colors[query], marker='o', linewidth=2,
            markersize=5, label=label)
    return values


fig, axes = plt.subplots(1, 2, figsize=(12, 4.8), gridspec_kw={'width_ratios': [1.1, 1]})
fig.subplots_adjust(top=.76, bottom=.22, left=.07, right=.97, wspace=.26)
fig.suptitle('More CPU cores increase drill-down throughput', x=.07, ha='left',
             fontsize=18, fontweight='bold', y=.97)
fig.text(.07, .88, 'Fixed read service: 59 vCPUs · 236 GiB | Same dataset | Warm data cache | Result cache off',
         fontsize=10, color='#4D5660')
for query, label in [('D1', 'D1 · hourly bars'), ('D2', 'D2 · risk and liquidity')]:
    values = draw_series(axes[0], query, 'throughput_GB_s',
                         'trial_throughput_GB_s_p25', 'trial_throughput_GB_s_p75', label)
    axes[0].annotate(f'{values[-1]:.2f} GB/s', (59, values[-1]), xytext=(-6, 10),
                     textcoords='offset points', ha='right', color=colors[query], fontweight='bold')
axes[0].set_title('Drill-downs: about 3.49 GB read per query', loc='left', fontsize=11)
axes[0].set_ylabel('Uncompressed data processing throughput (GB/s)')
axes[0].set_ylim(0, max(float(index[q, n]['throughput_GB_s']) for q in ['D1', 'D2'] for n in threads)*1.25)
axes[0].legend(loc='upper left', frameon=False, fontsize=9)
for query, label in [('A1', 'A1 · single-symbol summary'), ('A2', 'A2 · watchlist summary'),
                     ('A3', 'A3 · top movers'), ('A4', 'A4 · daily activity')]:
    draw_series(axes[1], query, 'latency_ms_median', 'latency_ms_p25', 'latency_ms_p75', label)
axes[1].set_title('Aggregations: smaller scans gain less', loc='left', fontsize=11)
axes[1].set_ylabel('Server execution time (ms, log scale)')
axes[1].set_yscale('log')
axes[1].set_yticks([5, 10, 20, 50, 100])
axes[1].get_yaxis().set_major_formatter(ScalarFormatter())
axes[1].set_ylim(3.5, 160)
axes[1].legend(loc='upper right', frameon=False, fontsize=8)
for ax in axes:
    axes_style(ax)
fig.text(.07, .07, '12 repetitions per query and limit. Throughput: total bytes / total server time. Latency: median. Shading: IQR.\n'
         'Queries run sequentially in shuffled order. CPU parallelism is controlled with max_threads on the fixed service.',
         fontsize=9, color='#4D5660', linespacing=1.7)
for suffix in ['svg', 'png']:
    fig.savefig(ROOT / 'derived' / ('thread-scaling.' + suffix), dpi=180)
plt.close(fig)

fig, axes = plt.subplots(1, 2, figsize=(11, 4.6))
fig.subplots_adjust(top=.77, bottom=.24, left=.075, right=.97, wspace=.28)
fig.suptitle('How CPU use changes with parallel execution',
             x=.075, ha='left', fontsize=16, fontweight='bold', y=.97)
fig.text(.075, .87, 'Measured CPU time includes query-associated worker and helper threads.',
         fontsize=10, color='#4D5660')
for query, label in [('D1', 'D1 · hourly bars'), ('D2', 'D2 · risk and liquidity')]:
    group = [index[(query, n)] for n in threads]
    axes[0].plot(threads, [float(r['average_busy_cores']) for r in group],
                 marker='o', color=colors[query], label=label, linewidth=2)
    draw_series(axes[1], query, 'cpu_s_median', 'cpu_s_p25', 'cpu_s_p75', label)
axes[0].plot(threads, threads, '--', linewidth=1, color='#A8AEB5', label='Selected core limit')
axes[0].set_ylabel('Average concurrent CPU use (core equivalents)')
axes[0].set_ylim(0, 65)
axes[0].set_title('CPU seconds ÷ server elapsed seconds', loc='left', fontsize=11)
axes[0].legend(loc='upper left', frameon=False, fontsize=9)
axes[1].set_ylabel('CPU time per query (seconds)')
axes[1].set_ylim(0, max(float(index[q, n]['cpu_s_p75']) for q in ['D1', 'D2'] for n in threads)*1.2)
axes[1].set_title('CPU work per query', loc='left', fontsize=11)
axes[1].legend(loc='lower right', frameon=False, fontsize=9)
for ax in axes:
    axes_style(ax)
fig.text(.075, .07, 'CPU time: query-log UserTimeMicroseconds + SystemTimeMicroseconds.\n'
         '12 repetitions per treatment. No smoothing, retries or removed observations.',
         fontsize=9, color='#4D5660', linespacing=1.7)
for suffix in ['svg', 'png']:
    fig.savefig(ROOT / 'derived' / ('cpu-scaling.' + suffix), dpi=180)
plt.close(fig)
print('Saved thread-scaling and cpu-scaling as SVG and PNG.')

all_combined = list(csv.DictReader((ROOT/'derived/combined-workload.csv').open()))
options = [
    ('drilldowns_only', 'Drill-downs only', 'D1 + D2, one execution each', 'drilldown-scaling', '#1479B8'),
    ('original_frequency', 'Original query-frequency mix', 'D1/D2 once each; A1–A4 six times each', 'combined-scaling', '#8A4FAD'),
    ('one_of_each', 'One execution of each query', 'D1 + D2 + A1 + A2 + A3 + A4, equal counts', 'one-of-each-scaling', '#248F73')]
fields = [
    ('throughput_GB_s', 'throughput_GB_s_p25', 'throughput_GB_s_p75', 'Data processing throughput', 'Uncompressed GB/s'),
    ('MB_s_per_thread_limit', 'MB_s_per_thread_limit_p25', 'MB_s_per_thread_limit_p75', 'Data throughput per CPU core', 'Uncompressed MB/s/core')]
limits = [max(float(r[field]) for r in all_combined)*1.22 for field, *_ in fields]

def combined_axes(ax, title, unit, limit):
    ax.set_title(title, loc='left', fontsize=12)
    ax.set_ylabel(unit)
    axes_style(ax)
    ax.set_xlim(1.7,68)
    ax.set_ylim(0,limit)

for mix, title, description, name, color in options:
    combined = [r for r in all_combined if r['mix']==mix]
    cores = [int(r['max_threads']) for r in combined]
    fig, axes = plt.subplots(1,2,figsize=(11.5,4.8))
    fig.subplots_adjust(top=.75,bottom=.31,left=.075,right=.97,wspace=.28)
    fig.suptitle(title+' · CPU scaling',x=.075,ha='left',fontsize=18,fontweight='bold',y=.97)
    fig.text(.075,.87,'Dataset fixed at 113.2B quotes · '+description,fontsize=10,color='#4D5660')
    for j, (ax, (field,low,high,panel,unit)) in enumerate(zip(axes,fields)):
        values=[float(r[field]) for r in combined]
        ax.fill_between(cores,[float(r[low]) for r in combined],[float(r[high]) for r in combined],color=color,alpha=.14)
        ax.plot(cores,values,'o-',color=color,linewidth=2,markersize=6)
        combined_axes(ax,panel,unit,limits[j])
        for x,y in zip(cores,values):
            if x < 8:
                continue
            ax.annotate(f'{y:.2f}' if j==0 else f'{y:.0f}',(x,y),xytext=(0,9),textcoords='offset points',ha='center',color=color,fontweight='bold')
    fig.text(.075,.075,'Combined rate: weighted bytes / weighted query execution time; scheduling idle gaps excluded. Warm data cache; result cache off.\n'
             'Per-core panel divides by selected cores. Parallelism set with max_threads on a fixed 59-vCPU / 236-GiB service.\n'
             '12 measured rounds; shading shows round IQR. Shared axis ranges across options. No smoothing or discarded observations.',
             fontsize=8.5,color='#4D5660',linespacing=1.65)
    for suffix in ['svg','png']:
        fig.savefig(ROOT/'derived'/(name+'.'+suffix),dpi=180)
    plt.close(fig)

fig, axes = plt.subplots(1,2,figsize=(12,5.2))
fig.subplots_adjust(top=.72,bottom=.32,left=.075,right=.97,wspace=.28)
fig.suptitle('Three workload options from the same measurements',x=.075,ha='left',fontsize=18,fontweight='bold',y=.97)
fig.text(.075,.89,'Fixed 113.2B-quote dataset · 59-vCPU / 236-GiB service · 12 repeats at each core setting',fontsize=10,color='#4D5660')
for mix,title,description,name,color in options:
    selected=[r for r in all_combined if r['mix']==mix]
    cores=[int(r['max_threads']) for r in selected]
    for j,(ax,(field,low,high,panel,unit)) in enumerate(zip(axes,fields)):
        ax.plot(cores,[float(r[field]) for r in selected],'o-',color=color,linewidth=2,markersize=5,label=title)
        combined_axes(ax,panel,unit,limits[j])
fig.legend(*axes[0].get_legend_handles_labels(),loc='lower left',bbox_to_anchor=(.068,.17),ncol=3,frameon=False,fontsize=9)
fig.text(.075,.055,'Each option keeps its weights fixed across cores. Primary benchmark mix: D1/D2 once each; A1–A4 six times each.\n'
         'Rates exclude idle gaps and use server execution time. CPU cores controls max_threads; physical service capacity stays fixed.\n'
         'Warm data cache; result cache off. No smoothing, automatic retries or removed observations.',fontsize=8.5,color='#4D5660',linespacing=1.65)
for suffix in ['svg','png']:
    fig.savefig(ROOT/'derived'/('workload-options.'+suffix),dpi=180)
plt.close(fig)
print('Saved three combined options and workload-options as SVG and PNG.')

drilldowns = [r for r in all_combined if r['mix']=='drilldowns_only']
cores = [int(r['max_threads']) for r in drilldowns]
fig, axes = plt.subplots(1,2,figsize=(11.5,4.8))
fig.subplots_adjust(top=.75,bottom=.30,left=.075,right=.97,wspace=.28)
fig.suptitle('Drill-down throughput · two ways to express the same work',x=.075,ha='left',fontsize=17,fontweight='bold',y=.97)
fig.text(.075,.87,'Fixed 113.2B-quote dataset · D1 + D2 · About 89.4M quotes read per query',fontsize=10,color='#4D5660')
for ax, field, low, high, title, unit in [
    (axes[0], 'throughput_GB_s','throughput_GB_s_p25','throughput_GB_s_p75','Data processing throughput','Uncompressed GB/s'),
    (axes[1], 'million_read_rows_s','million_read_rows_s_p25','million_read_rows_s_p75','Quotes processed per second','Million quotes/s')]:
    values=[float(r[field]) for r in drilldowns]
    ax.fill_between(cores,[float(r[low]) for r in drilldowns],[float(r[high]) for r in drilldowns],color='#1479B8',alpha=.14)
    ax.plot(cores,values,'o-',color='#1479B8',linewidth=2,markersize=6)
    combined_axes(ax,title,unit,max(values)*1.22)
    for x,y in zip(cores,values):
        if x < 8:
            continue
        ax.annotate(f'{y:.2f}' if ax==axes[0] else f'{y:.0f}',(x,y),xytext=(0,9),textcoords='offset points',ha='center',color='#1479B8',fontweight='bold')
fig.text(.075,.07,'Total measured bytes or read rows / total server execution time. Both drill-downs included equally; idle gaps excluded.\n'
         'Only rows actually processed count; data skipped by the primary index is excluded. Warm data cache; result cache off.\n'
         'Parallelism set with max_threads on a fixed 59-vCPU / 236-GiB service. 12 rounds; shading shows IQR.',fontsize=8.5,color='#4D5660',linespacing=1.65)
for suffix in ['svg','png']:
    fig.savefig(ROOT/'derived'/('drilldown-metric-options.'+suffix),dpi=180)
plt.close(fig)
print('Saved drilldown-metric-options as SVG and PNG.')
