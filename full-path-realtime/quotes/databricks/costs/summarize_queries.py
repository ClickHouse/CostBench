#!/usr/bin/env python3
"""Price accepted runner runtimes using the existing CostBench normalized model."""
from __future__ import annotations
import argparse,math
from _common import records,price,provenance,save,number

def summarize(path,pricing,cloud="aws",region="us-east-1",plan="premium",iterations=None):
    all_rows=records(path)
    if not all_rows: raise ValueError("empty runner")
    if iterations is not None and (iterations<1 or iterations>len(all_rows)):
        raise ValueError("requested iteration count is unavailable")
    rows=all_rows if iterations is None else all_rows[:iterations]
    first=rows[0];size=first["cluster_size"];q=len(first["result"])
    block=price(pricing,cloud,region,plan)
    instances=[i for i in block["instances"] if i["name"]==size]
    if len(instances)!=1: raise ValueError("missing or ambiguous warehouse price")
    values=[];ids=[];previous=-1
    for row in rows:
        if row["cluster_size"]!=size or len(row["result"])!=q: raise ValueError("runner shape/resources changed")
        if row["raw_rows"]<previous: raise ValueError("row progress regressed")
        previous=row["raw_rows"]
        if row.get("schema_version",0)>=3:
            meta=row["query_warehouse"]["api_metadata"]
            if meta.get("enable_serverless_compute") is not True or meta.get("max_num_clusters")!=1:
                raise ValueError("pricing requires one Serverless SQL cluster")
            if row.get("cache_hit")!=[[False] for _ in range(q)] or row.get("query_errors")!=[[] for _ in range(q)]:
                raise ValueError("missing/cache-hit/error query evidence")
            statements=row.get("statement_ids")
            if not isinstance(statements,list) or len(statements)!=q or any(len(x)!=1 or not x[0] for x in statements):
                raise ValueError("missing statement identifiers")
            ids.extend(x[0] for x in statements)
        for trial in row["result"]:
            if len(trial)!=1: raise ValueError("expected one measured trial per query")
            values.append(number(trial[0],"runtime"))
    if len(ids)!=len(set(ids)): raise ValueError("duplicate statement ids")
    total=math.fsum(values);dbuh=number(instances[0]["dbu_per_hour"],"DBU/hour");rate=number(block["dbu_price_per_hour"],"USD/DBU")
    return {"schema_version":2,"source_file":provenance(path)["path"],"source":provenance(path),"pricing_source":provenance(pricing),
        "system":first["system"],"version":first.get("version"),"warehouse_size":size,"cloud":cloud,"region":region,"plan":plan,
        "cost_model":"normalized_runtime_list_price","runtime_source":"runner result: provider Query History total duration; includes compilation; excludes result fetch",
        "scope_note":"Accepted prefix; idle, minimum billing and other warehouse activity excluded. Public list pricing, not invoice allocation.",
        "iterations_included":len(rows),"queries_per_iteration":q,"query_jobs":len(values),"total_runtime_seconds":total,
        "source_indices":list(range(len(rows))),"last_sampled_raw_rows":rows[-1]["raw_rows"],"raw_rows_source":first.get("raw_rows_source"),
        "costs":[{"tier":plan,"warehouse_size":size,"dbu_per_hour":dbuh,"dbu_price_per_hour":rate,"total_compute_cost_usd":total/3600*dbuh*rate}]}
def main():
    p=argparse.ArgumentParser(description=__doc__)
    for k in ["input","pricing","output"]: p.add_argument(k)
    for k,d in [("cloud","aws"),("region","us-east-1"),("plan","premium")]: p.add_argument("--"+k,default=d)
    p.add_argument("--iterations",type=int);a=p.parse_args()
    result=summarize(a.input,a.pricing,a.cloud,a.region,a.plan,a.iterations);save(a.output,result)
    print(f"{a.output}: {result['query_jobs']} jobs, {result['total_runtime_seconds']:.3f}s, ${result['costs'][0]['total_compute_cost_usd']:.6f}")
if __name__=="__main__": main()
