"""Validate and price PR #42's run-scoped usage exports without reallocating them."""
from __future__ import annotations

import csv
from collections import Counter, defaultdict
from datetime import datetime
from decimal import Decimal, localcontext
from pathlib import Path

from _common import read, provenance, price


def decimal(value):
    result = Decimal(str(value))
    if not result.is_finite():
        raise ValueError("non-finite usage or price")
    return result


def instant(value):
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def micros(delta):
    return Decimal((delta.days * 86400 + delta.seconds) * 1000000 + delta.microseconds)


def allocated_component(kind, source, pricing, cloud, region, plan):
    source = Path(source)
    run = source.parent.parent
    context_path = run / "run_context.json"
    context = read(context_path)
    manifest_path = run / "validation/manifest.json"
    manifest = read(manifest_path)
    artifact = next((a for a in manifest["artifacts"] if a["path"] == source.relative_to(run).as_posix()), None)
    if artifact is None or artifact["sha256"] != provenance(source)["sha256"] or artifact["size_bytes"] != source.stat().st_size:
        raise ValueError("allocation input not verified by run package manifest")
    with source.open(newline="") as handle:
        rows = list(csv.DictReader(handle))
    if not rows:
        raise ValueError("empty allocation input")
    rate = price(pricing, cloud, region, plan)
    unit_rate = decimal(rate["dbu_price_per_dbu"])
    if unit_rate <= 0:
        raise ValueError("invalid DBU price")
    start, end = instant(context["since"]), instant(context["producer_finished_at"])
    window = {"since": context["since"], "until": context["producer_finished_at"], "end_exclusive": True}
    scope_path = Path(__file__).parent / "scopes/serverless_20260918.json"
    scope = read(scope_path)
    if scope["run_id"] != context["run_id"] or scope["preparation_window"] != window:
        raise ValueError("allocation window differs from accepted real-time scope")
    out = {
        "schema_version": 2, "component": kind, "run_id": context["run_id"],
        "source": provenance(source), "context_source": provenance(context_path),
        "package_manifest": provenance(manifest_path), "pricing_source": provenance(pricing),
        "pricing": rate, "currency": "USD", "allocation_window": window,
        "cost_model": "allocated_usage_times_checked_in_public_list_rate",
        "status": "complete_allocated_window", "full_dataset_rows": context["expected_rows"],
        "scope": "Ingestion and maintenance during the accepted real-time benchmark window",
        "comparison_scope": scope, "scope_source": provenance(scope_path),
    }
    groups = defaultdict(Decimal)
    with localcontext() as ctx:
        ctx.prec = 80
        if kind == "clustering":
            ids = [r["operation_id"] for r in rows]
            if len(set(ids)) != len(ids):
                raise ValueError("duplicate maintenance operation")
            for r in rows:
                if (r["catalog_name"], r["schema_name"]) != (context["catalog"], context["schema"]) or r["table_name"] not in (context["raw_table"], context["mv_table"]):
                    raise ValueError("maintenance table outside run")
                if r["usage_unit"] != "ESTIMATED_DBU" or r["operation_status"] != "SUCCESSFUL":
                    raise ValueError("unexpected operation evidence")
                if not start <= instant(r["start_time"]) < instant(r["end_time"]) <= end:
                    raise ValueError("operation crosses allocation boundary; explicit allocation required")
                qty = decimal(r["estimated_dbu"])
                if qty < 0:
                    raise ValueError("negative estimated DBU")
                groups[r["table_name"] + "/" + r["operation_type"]] += qty
            total = sum(groups.values(), Decimal(0))
            compact_path = run / "evidence/clustering_status.json"
            summary = read(compact_path)["predictive_optimization"]["summary"]
            if len(rows) != summary["operation_count"] or abs(total - decimal(summary["usage_by_unit"]["ESTIMATED_DBU"])) > Decimal("1e-12"):
                raise ValueError("maintenance detail does not reconcile with compact summary")
            out.update(status="estimated", usage_unit="ESTIMATED_DBU", operations=len(rows), operation_ids=ids,
                       reconciliation_source=provenance(compact_path),
                       breakdown=[{"table_operation": k, "dbu": float(v), "cost_usd": float(v * unit_rate)} for k, v in sorted(groups.items())])
        else:
            ids = [r["record_id"] for r in rows]
            if len(set(ids)) != len(ids):
                raise ValueError("duplicate billing record_id")
            total = Decimal(0)
            exported_cost = Decimal(0)
            for r in rows:
                if r["record_type"] not in ("ORIGINAL", "RETRACTION", "RESTATEMENT"):
                    raise ValueError("unknown billing record_type")
                if r["sku_name"] not in rate["sku_names"]:
                    raise ValueError("unpriced allocation SKU")
                us, ue = instant(r["usage_start_time"]), instant(r["usage_end_time"])
                a, b = instant(r["allocated_start"]), instant(r["allocated_end"])
                if ue <= us or not a == max(us, start) < b == min(ue, end):
                    raise ValueError("allocation window mismatch")
                qty = decimal(r["allocated_dbu"])
                expected = decimal(r["source_dbu"]) * micros(b - a) / micros(ue - us)
                if abs(qty - expected) > Decimal("1e-14"):
                    raise ValueError("allocated DBU does not reconcile with interval overlap")
                if (r["record_type"] == "RETRACTION" and qty > 0) or (r["record_type"] != "RETRACTION" and qty < 0):
                    raise ValueError("billing adjustment sign mismatch")
                if kind == "zerobus":
                    if r["zerobus_request_type"] != "GRPC" or r["billing_origin_product"] != "LAKEFLOW_CONNECT":
                        raise ValueError("unexpected Zerobus product")
                else:
                    if r["currency_code"] != "USD" or decimal(r["price_per_dbu"]) != unit_rate:
                        raise ValueError("exported list price does not match checked-in pricing")
                    line_cost = decimal(r["cost"])
                    if abs(line_cost - qty * unit_rate) > Decimal("1e-14"):
                        raise ValueError("exported cost mismatch")
                    exported_cost += line_cost
                total += qty
                groups[r["sku_name"]] += qty
            if total < 0:
                raise ValueError("negative net usage")
            out.update(usage_unit="DBU", usage_records=len(rows), record_type_counts=dict(Counter(r["record_type"] for r in rows)),
                       record_ids=ids, signed_adjustments_preserved=True, allocation_applied_once=True,
                       breakdown=[{"sku_name": k, "dbu": float(v), "cost_usd": float(v * unit_rate)} for k, v in sorted(groups.items())])
            if kind == "mv_refresh":
                table = ".".join((context["catalog"], context["schema"], context["mv_table"]))
                mapped = {r["pipeline_id"] for r in rows if (r["catalog_name"], r["schema_name"], r["table_name"]) == (context["catalog"], context["schema"], table)}
                if not mapped or any(r["pipeline_id"] not in mapped for r in rows):
                    raise ValueError("MV pipeline missing run-table attribution")
                if any(r["table_name"] not in ("null", "", table) for r in rows):
                    raise ValueError("unrelated MV table")
                out.update(pipeline_ids=sorted(mapped), null_table_metadata_records=sum(r["table_name"] in ("null", "") for r in rows),
                           exported_list_cost_usd=float(exported_cost), exported_list_cost_usd_decimal=str(exported_cost))
            else:
                if len({r["table_id"] for r in rows}) != 1:
                    raise ValueError("multiple Zerobus tables")
                reconciliation = read(run / "evidence/provider_reconciliation.json")
                if not reconciliation["complete"] or reconciliation["provider_errors"] or reconciliation["provider_committed_records"] != context["expected_rows"]:
                    raise ValueError("provider row reconciliation failed")
                out.update(table_ids=sorted({r["table_id"] for r in rows}),
                           reconciliation_source=provenance(run / "evidence/provider_reconciliation.json"),
                           meter_status="run-scoped allocated DBUs; byte-volume proxy superseded")
        cost = total * unit_rate
        out.update(total_dbu=float(total), total_dbu_decimal=str(total), total_cost_usd=float(cost), total_cost_usd_decimal=str(cost))
    return out
