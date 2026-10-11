#!/usr/bin/env python3
"""Reproduce the two reader visuals from pinned numeric evidence (stdlib only)."""
from pathlib import Path
from datetime import datetime,timezone
from collections import defaultdict
import csv,json,math,hashlib,argparse,tempfile
HERE=Path(__file__).resolve().parent
FULL=113217743918
START=int(datetime(2026,8,8,6,55,56,tzinfo=timezone.utc).timestamp())
END=int(datetime(2026,8,9,19,40,58,tzinfo=timezone.utc).timestamp())
SIGMA=3600; RADIUS=10800

def epoch(s):
 return datetime.fromisoformat(s.replace('Z','+00:00')).replace(tzinfo=timezone.utc).timestamp()
def read(name):
 return list(csv.DictReader((HERE/'evidence'/name).open()))
def weight(t,u):
 return math.exp(-.5*((t-u)/SIGMA)**2) if abs(t-u)<=RADIUS else 0

def derive(out):
 for source in json.loads((HERE/'source-manifest.json').read_text()):
  assert hashlib.sha256((HERE/source['path']).read_bytes()).hexdigest()==source['sha256'],source['path']
 configs=json.loads((HERE/'config.json').read_text())
 old=json.loads((HERE/'evidence/prior-chart-data.json').read_text())
 panels={p['id']:p for p in old['panels']}
 query_rows=read('metrics-per-query.csv')
 groups=defaultdict(list)
 for q in query_rows:groups[(q['family'],int(q['iteration']))].append(q)
 runners={}
 for family,file in [('drill','drilldown_20260808T065602Z.jsonl'),('aggregate','dashboard_20260808T065559Z.jsonl')]:
  for line in (HERE/'evidence'/file).read_text().splitlines():
   r=json.loads(line);runners[(family,r['iteration'])]=r
 rates={}
 for metric,file,num,den in [('throughput','data-processing-throughput.csv','total_read_bytes','total_elapsed_seconds'),('efficiency','data-throughput-per-cpu-core.csv','total_read_bytes','total_cpu_seconds'),('coverage','effective-aggregation-throughput.csv','raw_quotes_represented','total_elapsed_seconds')]:
  rates[metric]=defaultdict(list)
  for row in read(file):
   family=row.get('family','aggregate');r=runners[(family,int(row['iteration']))]
   rates[metric][family].append({'progress':int(row['rows_ingested'])/FULL,'time':epoch(r['iteration_started_at']),'num':float(row[num]),'den':float(row[den])})
 latency=defaultdict(list); active_queries=[]; query_memory=defaultdict(list)
 memory_lookup={(r['started_at_utc'],r['query_hash']):r for r in read('10-query-peak-memory.csv')}
 hashes={'A1':'4268017077458496684','A2':'13116456435110875434','A3':'16441976537628221706','A4':'16076059573740575684','D1':'14900422428945064879','D2':'4120463799277501782'}
 matched_memory=[]
 for (family,iteration),queries in groups.items():
  r=runners[(family,iteration)];p=int(r['raw_rows'])/FULL;assert 0<p<=1
  duration=sum(float(x[0]) for x in r['result'])/len(r['result'])
  latency[family].append({'progress':p,'time':epoch(r['iteration_started_at']),'num':duration,'den':1})
  for q in queries:
   active_queries.append({'time':epoch(q['started_at_utc']),'num':(float(q['user_cpu_us'])+float(q['system_cpu_us']))/1e6,'den':float(q['query_duration_ms'])/1000})
   memory=memory_lookup[(q['started_at_utc'],hashes[q['benchmark_query']])]
   assert all(memory[k]==q[k] for k in ['query_duration_ms','read_rows','read_bytes'])
   matched_memory.append([q[k] for k in ['family','benchmark_query','iteration','started_at_utc','rows_ingested']]+[int(memory['peak_memory_bytes'])])
  mean_peak=sum(int(memory_lookup[(q['started_at_utc'],hashes[q['benchmark_query']])]['peak_memory_bytes']) for q in queries)/len(queries)
  query_memory[family].append({'progress':p,'time':epoch(r['iteration_started_at']),'num':mean_peak,'den':1})
 rates['latency']=latency
 display_rows=[]
 for metric in panels:
  panel=panels[metric];panel['sampled']=True
  if metric=='latency':panel['axisUnit']='Response time'
  for line in panel['series']:
   family='drill' if line['id']=='drilldown' else 'aggregate'
   source=sorted(rates[metric][family],key=lambda x:x['progress'])
   assert len(source)==(37 if family=='drill' else 221),(metric,family,len(source))
   points=[]
   for r in source:
    neighbours=[(weight(r['time'],n['time']),n) for n in source if weight(r['time'],n['time'])]
    v=sum(w*n['num'] for w,n in neighbours)/sum(w*n['den'] for w,n in neighbours)
    points.append([r['progress'],v]);display_rows.append([metric,line['id'],r['time'],r['progress'],r['num']/r['den'],v])
   line['points']=points
  if metric=='coverage':
   panel['series'][0]['label']='Full-table aggs · A3/A4'
   panel['axisUnit']='Trillion quotes/s'
 memory_series=[];memory_display=[]
 for family,label in [('drill','Drill-downs'),('aggregate','Interactive aggs')]:
  source=sorted(query_memory[family],key=lambda r:r['progress']);points=[]
  for r in source:
   neighbours=[(weight(r['time'],n['time']),n) for n in source if weight(r['time'],n['time'])]
   v=sum(w*n['num'] for w,n in neighbours)/sum(w for w,n in neighbours)
   points.append([r['progress'],v]);memory_display.append([family,r['time'],r['progress'],r['num'],v])
  memory_series.append({'id':family,'label':label,'points':points})
 parts=[x for x in read('active-newpart-series.csv') if x['table']=='quotes']
 resources=read('03-resource-series.csv');by_time={int(x['t']):x for x in resources}
 assert len(parts)==len(resources)==442
 capacity={x['metric']:x for x in read('04-capacity.csv')}
 assert float(capacity['CGroupMaxCPU']['minimum'])==float(capacity['CGroupMaxCPU']['maximum'])==16
 assert float(capacity['CGroupMemoryTotal']['minimum'])==float(capacity['CGroupMemoryTotal']['maximum'])==64*2**30
 cumulative=0;buckets=[]
 for part in parts:
  t=int(part['bucket_unix']);r=by_time[t];begin=max(t,START);end=min(t+300,END);duration=end-begin
  assert int(r['seconds'])==duration and int(r['max_samples_per_second'])<=2
  cumulative+=int(part['rows']);buckets.append({'time':(begin+end)/2,'progress':cumulative/FULL,'seconds':duration,'memory_sum':float(r['memory_byte_seconds'])})
 assert cumulative==FULL and sum(b['seconds'] for b in buckets)==END-START
 cpu_points=[];memory_points=[];resource_rows=[]
 for b in buckets:
  q=[(weight(b['time'],r['time']),r) for r in active_queries if weight(b['time'],r['time'])]
  cores=sum(w*r['num'] for w,r in q)/sum(w*r['den'] for w,r in q)
  neighbours=[(weight(b['time'],r['time']),r) for r in buckets if weight(b['time'],r['time'])]
  memory=sum(w*r['memory_sum'] for w,r in neighbours)/sum(w*r['seconds'] for w,r in neighbours)
  assert 0<=cores<=16 and 0<=memory<=64*2**30
  cpu_points.append([b['progress'],cores]);memory_points.append([b['progress'],memory])
  resource_rows.append([b['time'],b['progress'],cores,100*cores/16,memory,100*memory/(64*2**30)])
 for c in configs:
  data={'fullRows':FULL,'rowTicks':old['rowTicks'],'playback':old['playback'],'panels':[panels[k] for k in c['metricTitles']],
   'cpuCapacity':{'availableCores':16,'scope':'during-query-execution','points':cpu_points},
   'queryPeakMemory':{'availableBytes':64*2**30,'scope':'arithmetic mean per-query peaks within each scheduled family, Gaussian display smoothing','series':memory_series}}
  (out/c['view']).mkdir(parents=True,exist_ok=True);(out/c['view']/'chart-data.json').write_text(json.dumps(data,separators=(',',':'))+'\n')
 for filename,headers,rows in [('display-metrics.csv',['metric','series','time_unix','progress','raw_value','smoothed_value'],display_rows),('resource-display.csv',['time_unix','progress','average_query_cores','cpu_used_percent','average_service_memory_bytes','service_memory_used_percent'],resource_rows),('matched-query-memory.csv',['family','benchmark_query','iteration','started_at_utc','rows_ingested','peak_memory_bytes'],matched_memory),('query-memory-display.csv',['family','time_unix','progress','raw_mean_peak_bytes','smoothed_mean_peak_bytes'],memory_display)]:
  with (out/filename).open('w',newline='') as f:
   w=csv.writer(f,lineterminator='\n');w.writerow(headers);w.writerows(rows)
 provenance={'sources':json.loads((HERE/'source-manifest.json').read_text()),'activeWindowUTC':['2026-08-08 06:55:56','2026-08-09 19:40:58'],'fullRows':FULL,
  'smoothing':{'method':'Centered Gaussian','sigmaSeconds':SIGMA,'radiusSeconds':RADIUS,'coordinates':'actual UTC observation times','edgeHandling':'renormalize available observations','exclusions':[],
   'rates':'sum(w * numerator) / sum(w * denominator); query rates use execution seconds, efficiency uses consumed CPU seconds, latency uses equal per-query means of original runner durations. No prior median smoothing or decimation.',
   'resources':'CPU: sum(w * query CPU seconds) / sum(w * query execution seconds), across both scheduled families. Displayed query memory: equal per-query peaks within each family iteration, then Gaussian-weighted iteration means. Service memory time series retained as evidence only.'},
  'cpu':{'capacity':16,'scope':'during matched benchmark query execution; excludes idle schedule gaps','formula':'sum(UserTimeMicroseconds + SystemTimeMicroseconds) / (1000 * sum(query_duration_ms)); divide by 16 for capacity share','queries':len(active_queries),'weights':'elapsed execution time and Gaussian time neighborhood','families':['2 drill-downs each hour','4 interactive aggregates every 10 minutes']},
  'memory':{'capacityBytes':64*2**30,'displayMetric':'query_log.memory_usage at QueryFinish','scope':'average per-query peak for each family; equal query weights within each iteration, Gaussian weights between iterations','queries':len(matched_memory),'semanticsSource':'https://github.com/ClickHouse/ClickHouse/blob/master/src/Interpreters/executeQuery.cpp#L443','serviceMemoryEvidence':'CurrentMetric_MemoryTracking series retained for context; not used as a query memory statistic'},
  'xMapping':{'queryMetrics':'Measured raw_rows from each matched runner iteration, through first full dataset observation','resources':'Actual timestamp join to writer NewPart cumulative raw rows in 442 five-minute buckets'},
  'priorFormulas':'../provenance.json','rawEvidencePreserved':True,'querySource':'https://github.com/ClickHouse/CostBench/pull/44'}
 (out/'source-provenance.json').write_text(json.dumps(provenance,indent=2)+'\n')
 print(json.dumps({'panels':{k:[{'label':s['label'],'points':len(s['points']),'endpoint':s['points'][-1][1]} for s in p['series']] for k,p in panels.items()},'resourceEndpoint':resource_rows[-1]}))

if __name__=='__main__':
 parser=argparse.ArgumentParser();parser.add_argument('--check',action='store_true');args=parser.parse_args()
 if args.check:
  with tempfile.TemporaryDirectory() as d:
   out=Path(d);derive(out)
   for f in out.rglob('*'):
    if f.is_file():assert f.read_bytes()==(HERE/f.relative_to(out)).read_bytes(),f.relative_to(out)
  print('Reproduction passed; source and derived files unchanged')
 else:derive(HERE)
