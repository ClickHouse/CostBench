#!/usr/bin/env python3
"""Prepare/finalize the September full-run CostBench offline integration."""
from __future__ import annotations
import argparse,csv,hashlib,json,subprocess,sys
from pathlib import Path
DB=Path(__file__).resolve().parent
ROOT=DB.parents[1]
sys.path.insert(0,str(DB/"costs"))
from _common import read,records,provenance,save
RUN=DB/"results/serverless_baseline_full_20260918T170453Z"
OUT=DB/"costs/out/serverless_20260918"
MATCH=ROOT/"quotes/clickhouse-cloud/results_t2/matched/databricks_serverless_20260918"
CHCOST=ROOT/"quotes/clickhouse-cloud/costs/out_t2/matched/databricks_serverless_20260918"
SPECS={"dashboard":("mv/dashboard_20260918T170453Z.jsonl","mv/dashboard_20260808T065559Z.jsonl",189,112848979521,4),"drilldown":("raw/drilldown_20260918T170453Z.jsonl","raw/drilldown_20260808T065602Z.jsonl",32,111648774193,2)}

def verify_package():
    manifest=read(RUN/"validation/manifest.json")
    for artifact in manifest["artifacts"]:
        p=RUN/artifact["path"]
        if p.stat().st_size!=artifact["size_bytes"] or provenance(p)["sha256"]!=artifact["sha256"]:
            raise ValueError(f"package hash/size mismatch: {p}")
    return manifest

def prepare():
    package=verify_package();validation=read(RUN/"validation/validation_report_settled_20260921T092624Z.json")
    if validation["accepted"] is not True: raise ValueError("run validation rejected")
    context=read(RUN/"run_context.json");selection={}
    for workload,(dbfile,chfile,n,h,q) in SPECS.items():
        reference=RUN/dbfile;candidate=ROOT/"quotes/clickhouse-cloud/results_t2"/chfile
        indices=validation["query_samples"][workload]["accepted_indices"]
        if indices!=list(range(n)): raise ValueError("accepted selection changed")
        accepted=records(reference)[:n]
        if max(r["raw_rows"] for r in accepted)!=h or any(r["producer_progress_finished"] for r in accepted):
            raise ValueError("accepted horizon/phase changed")
        target=MATCH/f"{workload}_active_matched_to_databricks.jsonl"
        subprocess.run([sys.executable,str(ROOT/"utils/match_progress.py"),"--reference",str(reference),"--candidate",str(candidate),"--reference-final-rows",str(h),"--candidate-final-rows",str(h),"--count",str(n),"--output",str(target)],check=True)
        (MATCH/f"{workload}_active_matched_to_databricks_iterations_{n}.txt").touch()
        matched=records(target)
        if len(matched)!=n or len({r["iteration"] for r in matched})!=n: raise ValueError("bad matching")
        if any(len(r["result"])!=q for r in matched): raise ValueError("matched query shape")
        selected=RUN/"integration"/f"{workload}_accepted.jsonl";selected.parent.mkdir(exist_ok=True)
        selected.write_text("".join(json.dumps(r,separators=(",",":"))+"\n" for r in accepted))
        selection[workload]={"reference":provenance(reference),"candidate":provenance(candidate),"accepted":provenance(selected),"matched":provenance(target),"match_report":provenance(str(target)+".match.json"),"source_indices":indices,"iterations":n,"query_jobs":n*q,"comparison_horizon_rows":h,"denominators":[h,h]}
    save(RUN/"integration/manifest.json",{"schema_version":1,"run_id":context["run_id"],"label":"Databricks Serverless SQL · X-Small","cloud":"aws","region":"eu-west-1","plan":"premium","plan_basis":"CostBench public Premium pricing convention","package_manifest":provenance(RUN/"validation/manifest.json"),"verified_artifacts":len(package["artifacts"]),"validation_attestation":provenance(RUN/"validation/validation_report_settled_20260921T092624Z.json"),"dataset_final_rows":context["expected_rows"],"clickhouse_final_rows":113217743918,"selection":selection,"selection_note":"189/32 prefixes are the supplied validator's accepted observations, through H while producer running. First F observation is an active_endpoint under generic inspection but outside this stricter accepted window. Later warm observations excluded. 100B is only a plotting cap.","row_semantics":"Databricks raw_rows is client durability progress; matching does not establish exact query-visible rows or identical MV freshness.","sql_sources":[provenance(DB/x) for x in ["create_full_serverless_baseline_r3_20260918.sql","queries_mv.sql","queries_raw.sql"]]})

def finalize():
    verify_package();manifest=read(RUN/"integration/manifest.json")
    fresh_summary=read(OUT/"fresh_data_path.json")
    if fresh_summary.get("run_id") != manifest["run_id"]:
        raise ValueError("fresh-path costs belong to a different run")
    rows=[];sources={}
    for system,folder,tier in [("ClickHouse",CHCOST,"Enterprise"),("Databricks Serverless SQL",OUT,"premium")]:
        runtime=cost=0
        for workload,(_,_,n,_,q) in SPECS.items():
            p=folder/f"{workload}.json";s=read(p)
            if (s["iterations_included"],s["queries_per_iteration"])!=(n,q): raise ValueError("cost sample count mismatch")
            runtime+=s["total_runtime_seconds"];cost+=next(x["total_compute_cost_usd"] for x in s["costs"] if x["tier"]==tier)
            sources[f"{system}_{workload}"]=provenance(p)
        fresh=read(OUT/"fresh_data_path.json") if system.startswith("Databricks") else read(ROOT/"quotes/clickhouse-cloud/costs/out_t2/ingest.json")
        sources[f"{system}_fresh_path"]=provenance(OUT/"fresh_data_path.json" if system.startswith("Databricks") else ROOT/"quotes/clickhouse-cloud/costs/out_t2/ingest.json")
        write=fresh["total_cost_usd"] if system.startswith("Databricks") else next(c["total_compute_cost_usd"] for c in fresh["costs"] if c["tier"]==tier)
        rows.append({"label":system,"runtime_sec":runtime,"query_cost":cost,"fresh_path_cost":write,"total_cost":None if write is None else write+cost,"cost_runtime_product":None if write is None else (write+cost)*runtime})
    baseline=rows[0]["cost_runtime_product"]
    for r in rows: r["relative_to_clickhouse"]=None if r["cost_runtime_product"] is None else r["cost_runtime_product"]/baseline
    save(OUT/"full_path.json",{"schema_version":2,"status":"complete" if rows[1]["total_cost"] is not None else "incomplete","missing_components":fresh_summary["missing_components"],"comparison_scope":fresh_summary.get("comparison_scope"),"scope_source":fresh_summary.get("scope_source"),"allocated_window_plus_matched_queries_usd":fresh_summary.get("allocated_window_total_cost_usd",fresh_summary["known_components_subtotal_usd"])+rows[1]["query_cost"],"rows":rows,"sources":sources,"matching_manifest":provenance(RUN/"integration/manifest.json"),"formula":"(real-time fresh-data-path USD + matched query USD) * matched query runtime seconds","cost_model":"normalized queries + allocated DBUs and estimated maintenance at public list rates","scope_exclusions":["producer VM","object storage capacity/requests","network","control SQL","query warehouse idle/minimum billing","warm-window reads"]})
    ledger=[]
    for workload,(dbfile,chfile,n,h,q) in SPECS.items():
        for system,path in [("databricks",RUN/dbfile),("clickhouse",MATCH/f"{workload}_active_matched_to_databricks.jsonl")]:
            chosen=records(path)[:n] if system=="databricks" else records(path)
            for r in chosen:
                for i,t in enumerate(r["result"],1): ledger.append({"system":system,"workload":workload,"iteration":r["iteration"],"raw_rows":r["raw_rows"],"query":i,"seconds":t[0],"source":provenance(path)["path"],"phase":"accepted_ingestion_window"})
    with (RUN/"integration/claims.csv").open("w") as f:
        writer=csv.DictWriter(f,fieldnames=list(ledger[0]));writer.writeheader();writer.writerows(ledger)
    paths=list(OUT.glob("*.json"))+list(CHCOST.glob("*.json"))+list((DB/"costs/pricings").glob("*.json"))
    save(RUN/"integration/derived_manifest.json",{"schema_version":1,"inputs":provenance(RUN/"integration/manifest.json"),"artifacts":[provenance(p) for p in sorted(paths)],"full_path_status":read(OUT/"full_path.json")["status"]})
    save(RUN/"integration/readiness.json",{"schema_version":2,"run_id":manifest["run_id"],"status":"ready" if not fresh_summary["missing_components"] else "incomplete","missing_components":fresh_summary["missing_components"],"full_path_cost_complete":not fresh_summary["missing_components"],"allocation_status":fresh_summary.get("allocation_status"),"comparison_scope":fresh_summary.get("comparison_scope"),"note":"All cost components integrated for the accepted real-time benchmark window."})
    print(json.dumps(rows,indent=2))

def refresh_inputs():
    """Refresh package provenance while proving accepted matching inputs unchanged."""
    package=verify_package();path=RUN/"integration/manifest.json";manifest=read(path)
    for selection in manifest["selection"].values():
        for key in ("reference","candidate","accepted","matched","match_report"):
            expected=selection[key];p=ROOT/expected["path"]
            if provenance(p)["sha256"]!=expected["sha256"]:
                raise ValueError(f"accepted {key} changed: {p}")
    for expected in manifest["sql_sources"]:
        if provenance(ROOT/expected["path"])["sha256"]!=expected["sha256"]:
            raise ValueError("accepted SQL changed")
    manifest.update(package_manifest=provenance(RUN/"validation/manifest.json"),verified_artifacts=len(package["artifacts"]))
    manifest["allocation_inputs"]={kind:provenance(RUN/rel) for kind,rel in {
        "zerobus":"ingest/zerobus_ingest_allocation.csv",
        "clustering":"ingest/predictive_optimization_allocation.csv",
        "mv_refresh":"freshness/mv_refresh_allocation.csv"}.items()}
    manifest["comparison_scope"]=provenance(DB/"costs/scopes/serverless_20260918.json")
    manifest["allocation_export_sql"]=provenance(DB/"export_allocation_details.sql")
    save(path,manifest)
    print(f"Verified {len(package['artifacts'])} package artifacts; accepted query selections unchanged.")

if __name__=="__main__":
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument("action",choices=["prepare","refresh-inputs","finalize"]);args=parser.parse_args()
    {"prepare":prepare,"refresh-inputs":refresh_inputs,"finalize":finalize}[args.action]()
