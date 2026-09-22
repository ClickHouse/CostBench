#!/usr/bin/env python3
"""Apply checked-in regional tariffs to September volume and maintenance evidence."""
from __future__ import annotations
import argparse,math
from collections import defaultdict
from pathlib import Path
from _common import read,records,price,provenance,save,number
from _allocations import allocated_component

def component(kind,source,pricing,cloud,region,plan,ingest_summary=None):
    if Path(source).suffix == ".csv":
        return allocated_component(kind,source,pricing,cloud,region,plan)
    data=read(source);rate=price(pricing,cloud,region,plan)
    out={"schema_version":1,"component":kind,"source":provenance(source),"pricing_source":provenance(pricing),"pricing":rate,"run_id":data.get("run_id"),"status":"complete","cost_model":"measured_quantity_times_public_list_rate"}
    if kind=="zerobus":
        summary=read(ingest_summary)
        if not data["complete"] or data["provider_errors"] or data["provider_committed_records"]!=summary["logical_raw_rows"]:
            raise ValueError("provider/producer reconciliation failed")
        volume=number(data["provider_committed_bytes"],"provider bytes")
        pergb=number(rate["usd_per_gb"],"USD/GB");divisor=number(rate["bytes_per_gb"],"bytes/GB")
        if not math.isclose(pergb,rate["dbu_per_gb"]*rate["dbu_price_per_dbu"],abs_tol=1e-12): raise ValueError("tariff conversion mismatch")
        out.update(status="modeled",quantity_bytes=int(volume),quantity_gb=volume/divisor,total_cost_usd=volume/divisor*pergb,ipc_compression=summary["config"]["ipc_compression"],ingest_summary=provenance(ingest_summary),client_arrow_buffer_bytes=summary["durable_uncompressed_arrow_buffer_bytes"],full_dataset_rows=data["provider_committed_records"],meter_status="provider ingestion volume proxy; billing byte divisor unconfirmed",binary_gb_sensitivity_usd=volume/(2**30)*pergb,displayed_rounded_tariff_sensitivity_usd=volume/divisor*rate["displayed_usd_per_gb"],scope="Zerobus service including underlying compute; storage/producer/network excluded")
    elif kind=="clustering":
        ops=data["predictive_optimization"]["operations"]
        if not ops or len({o["operation_id"] for o in ops})!=len(ops): raise ValueError("empty/duplicate maintenance operations")
        groups=defaultdict(float)
        for op in ops:
            if op["usage_unit"]!="ESTIMATED_DBU" or op["operation_status"]!="SUCCESSFUL": raise ValueError("unexpected operation evidence")
            groups[op["table_name"]+"/"+op["operation_type"]]+=number(op["usage_quantity"],"estimated DBU")
        total=math.fsum(number(o["usage_quantity"],"DBU") for o in ops)
        out.update(status="estimated",total_dbu=total,usage_unit="ESTIMATED_DBU",operations=len(ops),operation_ids=[o["operation_id"] for o in ops],breakdown=[{"table_operation":k,"dbu":v,"cost_usd":v*rate["dbu_price_per_dbu"]} for k,v in groups.items()],total_cost_usd=total*rate["dbu_price_per_dbu"],collection_window=data["collection_window"],scope="All measured run-table Predictive Optimization: ANALYZE and CLUSTERING, raw and MV; estimates preserved")
    elif kind=="mv_refresh":
        # Refresh counts cannot substitute for measured compute consumption.
        if data.get("total_dbu") is None:
            out.update(status="pending_usage",total_dbu=None,total_cost_usd=None,missing_input="Run-scoped MV-refresh DBUs for the accepted benchmark window",planning_event_count=sum(data.get("maintenance_type_counts", {}).values()),explanation="MV usage pending; refresh counts are not compute consumption")
        else:
            if data.get("usage_unit")!="DBU" or not data.get("run_id") or not data.get("scope") or not data.get("source_query") or not data.get("pipeline_id"):
                raise ValueError("MV usage export needs DBU unit, pipeline id, scope, source_query")
            total=number(data["total_dbu"],"MV DBUs");out.update(total_dbu=total,total_cost_usd=total*rate["dbu_price_per_dbu"],usage_unit="DBU",usage_scope=data["scope"])
    return out

def assemble(folder):
    folder=Path(folder);parts={k:read(folder/f"{k}.json") for k in ["zerobus","clustering","mv_refresh"]}
    if len({v["run_id"] for v in parts.values()}) != 1 or parts["zerobus"]["run_id"] is None:
        raise ValueError("component run IDs do not match")
    missing=[k for k,v in parts.items() if v["total_cost_usd"] is None]
    known=math.fsum(v["total_cost_usd"] for v in parts.values() if v["total_cost_usd"] is not None)
    allocated=all("allocation_window" in v for v in parts.values())
    windows=[v["allocation_window"] for v in parts.values()] if allocated else []
    if windows and any(w!=windows[0] for w in windows): raise ValueError("component allocation windows differ")
    caveats=["Zerobus and MV refresh use signed, prorated DBUs at checked-in public list rates; no second allocation" if allocated else "Zerobus uses provider bytes and decimal GB assumption; binary sensitivity retained","Predictive Optimization uses ESTIMATED_DBU","Normalized query costs are separate; no invoice reconstruction"]
    out={"schema_version":2,"run_id":parts["zerobus"]["run_id"],"status":"incomplete" if missing else "complete_modeled","components":parts,"missing_components":missing,"known_components_subtotal_usd":known,"total_cost_usd":None if missing else known,"scope":"Ingestion and MV/layout maintenance during the accepted real-time window; read cost separate","caveats":caveats}
    if allocated:
        scopes=[v["comparison_scope"] for v in parts.values()]
        if any(scope!=scopes[0] for scope in scopes): raise ValueError("component comparison scopes differ")
        out.update(allocation_window=windows[0],allocated_window_total_cost_usd=known,allocation_status="complete_for_real_time_window",comparison_scope=scopes[0],scope_source=parts["zerobus"]["scope_source"])
    save(folder/"fresh_data_path.json",out)
    return out

def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument("component",choices=["zerobus","clustering","mv_refresh","assemble"])
    p.add_argument("source");p.add_argument("pricing",nargs="?");p.add_argument("output",nargs="?")
    p.add_argument("--ingest-summary");p.add_argument("--cloud",default="aws");p.add_argument("--region",default="eu-west-1");p.add_argument("--plan",default="premium");a=p.parse_args()
    if a.component=="assemble": assemble(a.source);return
    if not a.pricing or not a.output: p.error("pricing and output required")
    result=component(a.component,a.source,a.pricing,a.cloud,a.region,a.plan,a.ingest_summary);save(a.output,result)
    print(f"{a.component}: {result['status']}, USD={result['total_cost_usd']}")
if __name__=="__main__": main()
