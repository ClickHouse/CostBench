#!/usr/bin/env python3
"""Bind 11c to the two owner-selected, validated cache cases."""
import csv,hashlib,json,pathlib
ROOT=pathlib.Path(__file__).resolve().parent
CORES=[2,4,8,16,30,59]
CASES=[('hot',1,'hot','Hot caches',''),('cold',0,'cold','Cold caches','6 4')]
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def main():
    source=ROOT
    validation=json.loads((source/'derived/validation.json').read_text());assert validation['passed'] and validation['measured_trials']==432
    evidence=ROOT/'derived'
    rows=list(csv.DictReader((evidence/'combined-workload.csv').open()));assert len(rows)==18;selected=[]
    for mode,qcc,key,label,dash in CASES:
        group=sorted([r for r in rows if r['cache_mode']==mode and int(r['query_condition_cache'])==qcc],key=lambda r:int(r['max_threads']))
        assert [int(r['max_threads'])for r in group]==CORES and all(int(r['n'])==24 for r in group)
        selected.append((key,label,dash,group))
    panels=[]
    for id,column,unit,axis,ymax,ticks,multiplier in [('throughput','throughput_GB_s','bytes-rate','Uncompressed GB/s',20e9,[0,5e9,10e9,15e9,20e9],1e9),('efficiency','MB_s_per_busy_core','bytes-core-rate','Uncompressed MB/s/core',450e6,[0,150e6,300e6,450e6],1e6)]:
        series=[]
        for key,label,dash,g in selected:
            values=[float(r[column])*multiplier for r in g];assert all(0<v<ymax for v in values)
            series.append({'id':key,'label':label,'dash':dash,'values':values})
        panels.append({'id':id,'unit':unit,'axisUnit':axis,'color':'#e9ec5b','yMax':ymax,'yTicks':ticks,'series':series})
    resources=[]
    for key,label,dash,g in selected:
        busy=[float(r['average_busy_cores'])for r in g];used=[b/n*100 for b,n in zip(busy,CORES)];assert all(0<v<=100 for v in used)
        resources.append({'id':key,'label':'Hot'if key=='hot'else'Cold','averageBusyCores':busy,'CPUUsedPercentOfSelectedCores':used,'meanQueryPeakBytes':[float(r['mean_query_peak_MiB'])*2**20 for r in g]})
    data={'id':'11c','cores':CORES,'fullRows':113217743918,'service':{'vCPUs':59,'memoryGiB':236,'version':'26.6.1.2292'},'panels':panels,'resources':resources,'playback':{'stepMs':1800},'evidence':{'file':'combined-workload.csv','sha256':sha(evidence/'combined-workload.csv')}}
    (evidence/'11c-chart-data.json').write_text(json.dumps(data,separators=(',',':'))+'\n')
    with (evidence/'11c-resource-utilization.csv').open('w',newline='') as f:
        writer=csv.writer(f,lineterminator='\n');writer.writerow(['cache_case','cores','mean_busy_cores','cpu_used_percent_of_selected_cores','headroom_percent','mean_query_peak_bytes','mean_query_peak_MiB','service_memory_GiB'])
        for r in resources:
            for i,c in enumerate(CORES):writer.writerow([r['id'],c,r['averageBusyCores'][i],r['CPUUsedPercentOfSelectedCores'][i],100-r['CPUUsedPercentOfSelectedCores'][i],r['meanQueryPeakBytes'][i],r['meanQueryPeakBytes'][i]/2**20,236])
    provenance={'id':'11c','sourcePackage':'.','ownerSelection':'Hot caches plus Cold data caches with query-condition cache off. The condition-on cold case remains in the source CSV and is not plotted.','selectedMeasuredQueries':288,'evidenceMeasuredQueries':432,'capacity':data['service'],'fixedDatasetRows':data['fullRows'],'xAxis':'CPU cores: max_threads varies on one fixed 59-vCPU node. These are not six physical service resizes.','workload':'Original D1 and D2; equal counts; 12 repeats per query, core point and case; fixed data without ongoing ingestion.','throughput':'sum(uncompressed read_bytes) / sum(server elapsed_ns); decimal GB/s.','perCore':'sum(uncompressed read_bytes) / sum(User + System CPU seconds); decimal MB/s/core, matching 11a.','CPUCapacityBand':'sum(CPU seconds) / sum(server elapsed seconds) / selected N cores. Percentage relative to N-core budget, not measured utilization of a physically resized N-core service.','memoryBand':'Arithmetic mean of per-query peaks; measured independently by cache case and core point. Memory capacity is the actual 236-GiB service.','cachePolicy':{'hot':'Filesystem, page and distributed data caches plus query-condition caching enabled.','cold':'Filesystem, page and distributed data caches bypassed; query-condition caching explicitly off.','resultCache':'Normal disabled setting, verified no result-cache reads.'},'geometry':'Monotone cubic geometry through measured values; slider, markers and readouts snap to six observations. No numerical smoothing, averaging across core points or extrapolation.','comparisonLimit':'Static-data test on a newer version and larger node; separate from original Run 2 continuous ingestion and its Gaussian display smoothing. Hot and selected Cold cases came from separate sequential measurement blocks.','files':{name:sha(evidence/name)for name in ['combined-workload.csv','individual-query-summary.csv','all-trials.csv','validation.json','context-validation.json']}}
    (evidence/'11c-source-provenance.json').write_text(json.dumps(provenance,indent=2)+'\n');print(json.dumps({'passed':True,'measuredPoints':12,'selectedTrials':288,'sourceSha256':data['evidence']['sha256']}))
if __name__=='__main__':main()
