#!/usr/bin/env python3
# /// script
# requires-python = ">=3.10"
# dependencies = ["matplotlib==3.10.9"]
# ///
"""Evidence-native September freshness, component costs and full-path charts."""
from __future__ import annotations
import argparse,csv,json,sys,statistics,hashlib
from pathlib import Path
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter
DB=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(DB.parent/"redshift-serverless/visualizations"))
from _layout import configure_figure,resolve_layout,save_figure,write_json,WHITE,MUTED,GRID
RED="#FF3621";YELLOW="#FDFF62"
RUN=DB/"results/serverless_baseline_full_20260918T170453Z"
COST=DB/"costs/out/serverless_20260918"
def read(p): return json.loads(p.read_text())
def source(p): return {"path":str(p),"sha256":hashlib.sha256(p.read_bytes()).hexdigest()}
def emit(fig,a,name,data,summary):
    name=name+("_wide" if a.wide else "");png,svg=save_figure(fig,a.output_dir,name,a.dpi,wide=a.wide);plt.close(fig)
    csvpath=a.output_dir/(name+"_data.csv")
    if data:
        with csvpath.open("w",newline="") as f:
            w=csv.DictWriter(f,fieldnames=list(data[0]));w.writeheader();w.writerows(data)
    summary.update(schema_version=1,chart=name.removesuffix("_wide"),layout={"variant":"wide" if a.wide else "standard","data_and_math_unchanged":True},outputs={"png":str(png),"svg":str(svg),"csv":str(csvpath)})
    write_json(a.output_dir/(name+"_summary.json"),summary)
def setup(a,name,panels=1):
    _,size,_=resolve_layout(name,a.wide,(12,6.6),a.dpi)
    fig,axes=plt.subplots(1,panels,figsize=size,dpi=a.dpi if a.wide else None,squeeze=False)
    bg=configure_figure(fig,wide=a.wide)
    for ax in axes[0]:
        ax.set_facecolor(bg);ax.tick_params(colors=WHITE,labelsize=13 if a.wide else 10)
        ax.spines[["top","right"]].set_visible(False);ax.spines[["left","bottom"]].set_color(MUTED)
        ax.grid(axis="y",color=GRID,alpha=.6)
    fig.subplots_adjust(left=.085,right=.95,bottom=.17,top=.69 if a.wide else .84,wspace=.22)
    return fig,list(axes[0])
def freshness(a):
    path=RUN/"freshness/mv_freshness_20260918T170453Z.jsonl";records=[json.loads(s) for s in path.read_text().splitlines()]
    final=read(RUN/"run_context.json")["expected_rows"];accepted=read(RUN/"validation/validation_report_settled_20260921T092624Z.json")["freshness_samples"]["accepted_indices"]
    data=[]
    for i in accepted:
        r=records[i]
        if r["mv_source_rows"] is None or r["provider_committed_rows"]<=0: continue
        data.append({"source_index":i,"observed_at":r["observed_at"],"collected_until":r["updated_at"],"raw_rows":r["provider_committed_rows"],"mv_source_rows":r["mv_source_rows"],"rows_behind":r["mv_rows_behind"],"refresh_age_seconds":r["mv_refresh_age_sec"]})
    fig,axes=setup(a,"databricks_freshness",2)
    for ax,key,label in zip(axes,["rows_behind","refresh_age_seconds"],["Row-watermark gap (million rows)","Age of latest completed refresh (seconds)"]):
        ax.plot([r["raw_rows"]/1e9 for r in data],[r[key]/(1e6 if key=="rows_behind" else 1) for r in data],color=RED,alpha=.8,linewidth=1.5)
        ax.set_xlabel("Provider-committed rows (billions)",color=WHITE,fontsize=14 if a.wide else 11);ax.set_ylabel(label,color=WHITE,fontsize=14 if a.wide else 11)
        ax.set_xlim(0,final/1e9);ax.set_title("Raw → MV source watermark" if key=="rows_behind" else "Completed-refresh age",color=WHITE,fontsize=17 if a.wide else 13)
    fig.text(.085,.08,"Metadata sampled non-atomically; neither metric is exact per-record latency.",color=MUTED,fontsize=13 if a.wide else 10)
    final_refresh=records[-1]["latest_successful_refresh"]
    emit(fig,a,"databricks_freshness",data,{"sources":{"freshness":source(path)},"semantics":"Row difference and observational refresh age are separate metrics. No conversion of age into raw-to-MV time lag. Parallel ingestion means row counts do not identify exact rows.","accepted_samples":len(data),"negative_row_gap_samples":sum(r["rows_behind"]<0 for r in data),"maximum_rows_behind":max(r["rows_behind"] for r in data),"maximum_refresh_age_sec":max(r["refresh_age_seconds"] for r in data),"final_completed_refresh":final_refresh,"final_rows":final})
def components(a):
    path=COST/"fresh_data_path.json";s=read(path);data=[]
    for k,v in s["components"].items(): data.append({"component":k,"cost_usd":v["total_cost_usd"],"status":v["status"]})
    fig,[ax]=setup(a,"databricks_cost_components")
    names={"zerobus":"Zerobus ingestion","clustering":"Predictive Optimization (estimated)","mv_refresh":"MV refresh" if s["components"]["mv_refresh"]["total_cost_usd"] is not None else "MV refresh: usage unavailable"}
    for i,r in enumerate(data):
        if r["cost_usd"] is not None:
            ax.barh(i,r["cost_usd"],color=RED,height=.5);ax.text(r["cost_usd"]+8,i,f"${r['cost_usd']:,.2f}",color=WHITE,va="center",fontsize=14)
        else: ax.text(8,i,"Not included in a total",color=MUTED,va="center",fontsize=14)
    ax.set_yticks(range(3),[names[r["component"]] for r in data]);ax.set_ylim(2.6, -.6);ax.set_xlim(0,max(r["cost_usd"] or 0 for r in data)*1.2)
    ax.set_xlabel("Modeled USD · AWS Ireland · public Premium tariff",color=WHITE,fontsize=14)
    fig.subplots_adjust(left=.33)
    fig.text(.08,.085,"Allocated DBUs × $0.39/DBU; Predictive Optimization uses estimated DBUs.",color=MUTED,fontsize=12 if a.wide else 10)
    note = "MV-refresh usage pending." if s["total_cost_usd"] is None else f"Total fresh-path cost: ${s['total_cost_usd']:,.2f}."
    fig.text(.08,.05,note,color=MUTED,fontsize=12 if a.wide else 10)
    emit(fig,a,"databricks_cost_components",data,{"source":source(path),"status":s["status"],"known_components_subtotal_usd":s["known_components_subtotal_usd"],"total_cost_usd":s["total_cost_usd"],"missing_components":s["missing_components"],"caveats":s["caveats"]})
def fullpath(a):
    path=COST/"full_path.json";s=read(path)
    fresh_path=COST/"fresh_data_path.json";fresh=read(fresh_path)
    pending=bool(s["missing_components"])
    rows=[]
    for row in s["rows"]:
        displayed_fresh=row["fresh_path_cost"]
        complete=displayed_fresh is not None
        if not complete:
            displayed_fresh=fresh["known_components_subtotal_usd"]
        displayed_total=displayed_fresh+row["query_cost"]
        rows.append({**row,"displayed_fresh_path_cost_usd":displayed_fresh,
                     "displayed_total_cost_usd":displayed_total,
                     "displayed_cost_runtime_product":displayed_total*row["runtime_sec"],
                     "cost_complete":complete})
    for row in rows:
        row["displayed_relative_to_clickhouse"]=row["displayed_cost_runtime_product"]/rows[0]["displayed_cost_runtime_product"]
    note="MV-refresh usage pending." if pending else "Includes ingestion, maintenance and matched query cost."
    summary={"sources":{"full_path":source(path),"fresh_path":source(fresh_path)},
             "status":"incomplete" if pending else "complete",
             "comparison_scope":s.get("comparison_scope"),
             "missing_components":s["missing_components"],"note":note,
             "caveats":fresh["caveats"],"rows":rows}
    for name,key,title,xlabel in [
        ("fresh_data_path_cost_clickhouse_vs_databricks","displayed_fresh_path_cost_usd","Fresh-data path cost","Modeled USD"),
        ("full_path_cost_performance_clickhouse_vs_databricks","displayed_relative_to_clickhouse","Full-path cost-performance","Cost × matched query runtime, relative to ClickHouse"),
    ]:
        fig,[ax]=setup(a,name)
        fig.subplots_adjust(left=.29,right=.93)
        values=[row[key] for row in rows]
        ax.barh([0,1],values,color=[YELLOW,RED],height=.45)
        ax.set_yticks([0,1],[row["label"] for row in rows]);ax.set_ylim(1.65,-.65)
        ax.set_xlim(0,max(values)*1.32)
        ax.set_title(title+(" · provisional" if pending else ""),color=WHITE,fontsize=18 if a.wide else 14,pad=15)
        ax.set_xlabel(xlabel,color=WHITE,fontsize=14 if a.wide else 11)
        for i,(row,value) in enumerate(zip(rows,values)):
            label=f"${value:,.2f}" if key.endswith("usd") else f"{value:,.1f}×"
            if not row["cost_complete"]: label+=" + MV"
            ax.text(value+max(values)*.025,i,label,color=WHITE,va="center",fontsize=15 if a.wide else 12)
        chart_note = "Includes ingestion and maintenance." if key.endswith("usd") and not pending else note
        fig.text(.08,.065,chart_note,color=MUTED,fontsize=12 if a.wide else 9)
        emit(fig,a,name,rows,{**summary,"note":chart_note})
    name="full_path_cost_vs_query_runtime_clickhouse_vs_databricks"
    fig,[ax]=setup(a,name)
    for row,color in zip(rows,[YELLOW,RED]):
        ax.scatter(row["runtime_sec"],row["displayed_total_cost_usd"],color=color,s=160,zorder=3)
        label=row["label"]+(" (MV pending)" if not row["cost_complete"] else "")
        ax.annotate(label,(row["runtime_sec"],row["displayed_total_cost_usd"]),xytext=(10,12),textcoords="offset points",color=color,fontsize=14 if a.wide else 11)
    ax.set_xscale("log");ax.set_yscale("log")
    ax.set_xlim(max(row["runtime_sec"] for row in rows)*3,min(row["runtime_sec"] for row in rows)/3)
    ax.set_ylim(max(row["displayed_total_cost_usd"] for row in rows)*2,min(row["displayed_total_cost_usd"] for row in rows)/2)
    ax.yaxis.set_major_formatter(FuncFormatter(lambda x,_:f"${x:,.0f}"))
    ax.set_xlabel("Matched query runtime (seconds) · faster →",color=WHITE,fontsize=14 if a.wide else 11)
    ax.set_ylabel("Modeled full-path cost (USD) · lower cost ↑",color=WHITE,fontsize=14 if a.wide else 11)
    ax.set_title("Full-path cost vs query runtime"+(" · provisional" if pending else ""),color=WHITE,fontsize=18 if a.wide else 14,pad=15)
    fig.text(.08,.065,note,color=MUTED,fontsize=12 if a.wide else 9)
    emit(fig,a,name,rows,dict(summary))
    (a.output_dir/"full_path_unavailable.json").unlink(missing_ok=True)
def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument("--kind",choices=["freshness","components","fullpath"],required=True);p.add_argument("--output-dir",type=Path,required=True);p.add_argument("--wide",action="store_true");p.add_argument("--dpi",type=int,default=300);a=p.parse_args();a.output_dir.mkdir(parents=True,exist_ok=True)
    {"freshness":freshness,"components":components,"fullpath":fullpath}[a.kind](a)
if __name__=="__main__":main()
