#!/usr/bin/env python3
# /// script
# requires-python = ">=3.10"
# dependencies = ["matplotlib==3.10.9"]
# ///
"""Databricks entry point for the generic two-provider latency renderer."""
from pathlib import Path
import runpy,sys,json
BASE=Path(__file__).resolve().parents[2]
# Existing generic renderer supports labels/colors/tiers as arguments.
sys.path.insert(0,str(BASE/"redshift-serverless/visualizations"))
if __name__=="__main__":
    renderer=runpy.run_path(str(BASE/"redshift-serverless/visualizations/plot_query_latency.py"))
    renderer["main"]()
    args=renderer["parse_args"]()
    name=args.basename+("_wide" if args.wide and not args.basename.endswith("_wide") else "")
    p=args.output_dir/(name+"_summary.json");s=json.loads(p.read_text())
    s["databricks_contract"]={"runtime_source":"provider Query History total duration, compilation included, fetch excluded","raw_rows_source":"producer durability progress","cost_window":"189 dashboard / 32 drilldown accepted samples through their workload-specific H","plot_window":"own original timelines through 100B; plotted window differs from accumulated cost window","result_cache":"disabled; compact source evidence validated before cost generation","warehouse":"Serverless SQL X-Small, one cluster, eu-west-1","cost_model":"normalized runtime times public list compute rate"}
    p.write_text(json.dumps(s,indent=2)+"\n")
