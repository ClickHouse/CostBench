from __future__ import annotations

import json
import sys
import tempfile
import unittest
import csv
import hashlib
import shutil
from decimal import Decimal
from pathlib import Path

DB = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(DB))
sys.path.insert(0, str(DB / "costs"))
from summarize_components import component, assemble
from summarize_queries import summarize
from _allocations import allocated_component

RUN = DB / "results/serverless_baseline_full_20260918T170453Z"
MAINTENANCE = DB / "costs/pricings/serverless_maintenance.json"


class AllocationGates(unittest.TestCase):
    def price_mv(self, source):
        return allocated_component("mv_refresh", source, MAINTENANCE, "aws", "eu-west-1", "premium")

    def fixture(self, root, mutate):
        for rel in ["run_context.json", "freshness/mv_refresh_allocation.csv", "freshness/mv_freshness_20260918T170453Z.jsonl", "validation/manifest.json"]:
            p=root/rel;p.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(RUN/rel,p)
        p=root/"freshness/mv_refresh_allocation.csv"
        with p.open() as f: rows=list(csv.DictReader(f))
        mutate(rows)
        with p.open("w",newline="") as f:
            w=csv.DictWriter(f,fieldnames=list(rows[0]));w.writeheader();w.writerows(rows)
        m=root/"validation/manifest.json";data=json.loads(m.read_text())
        record=next(a for a in data["artifacts"] if a["path"]=="freshness/mv_refresh_allocation.csv")
        record.update(size_bytes=p.stat().st_size,sha256=hashlib.sha256(p.read_bytes()).hexdigest())
        m.write_text(json.dumps(data));return p

    def test_pr42_totals_keep_null_metadata_pipeline_usage(self):
        mv=self.price_mv(RUN/"freshness/mv_refresh_allocation.csv")
        self.assertEqual(mv["usage_records"],2958)
        self.assertEqual(mv["null_table_metadata_records"],1044)
        self.assertAlmostEqual(mv["total_cost_usd"],207.7976575038633,places=10)
        self.assertAlmostEqual(mv["exported_list_cost_usd"],mv["total_cost_usd"],places=10)
        self.assertFalse(mv["comparison_scope"]["post_ingestion_work_included"])
        self.assertEqual(mv["allocation_window"]["until"],"2026-09-20T00:33:42.627Z")

    def test_signed_corrections_are_not_dropped(self):
        def mutate(rows):
            retraction=dict(rows[1],record_id="correction-retraction",record_type="RETRACTION")
            for key in ["source_dbu","allocated_dbu","cost"]: retraction[key]=str(-Decimal(retraction[key]))
            rows.extend([retraction,dict(rows[1],record_id="correction-restatement",record_type="RESTATEMENT")])
        with tempfile.TemporaryDirectory() as tmp:
            value=self.price_mv(self.fixture(Path(tmp),mutate))
            self.assertAlmostEqual(value["total_cost_usd"],207.7976575038633,places=10)
            self.assertEqual(value["record_type_counts"]["RETRACTION"],1)
            self.assertEqual(value["usage_records"],2960)

    def test_duplicate_usage_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            p=self.fixture(Path(tmp),lambda rows: rows.append(dict(rows[0])))
            with self.assertRaisesRegex(ValueError,"duplicate billing"):
                self.price_mv(p)

    def test_double_allocation_or_wrong_price_rejected(self):
        cases=[("allocated_dbu","0.0001","interval overlap"),("price_per_dbu","0.91","checked-in pricing"),("allocated_end","2026-09-18T17:30:00.000Z","allocation window"),("pipeline_id","wrong-pipeline","attribution")]
        for key,value,message in cases:
            with self.subTest(key=key), tempfile.TemporaryDirectory() as tmp:
                p=self.fixture(Path(tmp),lambda rows: rows[0].update({key:value}))
                with self.assertRaisesRegex(ValueError,message): self.price_mv(p)

    def test_active_ingest_allocations_complete_the_accepted_real_time_scope(self):
        specs={"mv_refresh":("freshness/mv_refresh_allocation.csv","serverless_maintenance.json"),"zerobus":("ingest/zerobus_ingest_allocation.csv","zerobus_ingest.json"),"clustering":("ingest/predictive_optimization_allocation.csv","serverless_maintenance.json")}
        with tempfile.TemporaryDirectory() as tmp:
            for kind,(rel,pricing) in specs.items():
                value=allocated_component(kind,RUN/rel,DB/"costs/pricings"/pricing,"aws","eu-west-1","premium")
                (Path(tmp)/f"{kind}.json").write_text(json.dumps(value))
            result=assemble(tmp)
            self.assertAlmostEqual(result["allocated_window_total_cost_usd"],695.604750434355,places=9)
            self.assertEqual(result["missing_components"],[])
            self.assertAlmostEqual(result["total_cost_usd"],695.604750434355,places=9)
            self.assertEqual(result["status"],"complete_modeled")


class IntegrationGates(unittest.TestCase):
    def test_legacy_june_cost_cannot_be_used_as_september_mv_input(self):
        with self.assertRaisesRegex(ValueError, "MV usage export"):
            component("mv_refresh", DB / "costs/mv_refresh.json", DB / "costs/pricings/serverless_maintenance.json", "aws", "eu-west-1", "premium")

    def test_pending_mv_never_becomes_zero_or_a_full_total(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            for kind, value in [("zerobus", 1), ("clustering", 2), ("mv_refresh", None)]:
                (root / f"{kind}.json").write_text(json.dumps({"run_id": "september-test", "total_cost_usd": value}))
            result = assemble(root)
            self.assertIsNone(result["total_cost_usd"])
            self.assertEqual(result["known_components_subtotal_usd"], 3)
            self.assertEqual(result["missing_components"], ["mv_refresh"])
            self.assertEqual(result["status"], "incomplete")

    def test_supplied_mv_usage_replaces_pending_cost_on_rerun(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            usage = {"run_id": "september-test", "total_dbu": 100, "usage_unit": "DBU", "pipeline_id": "test-pipeline", "source_query": "test fixture", "scope": "full run including final catch-up", "covers_final_catchup": True}
            source = root / "usage.json"
            source.write_text(json.dumps(usage))
            for kind, value in [("zerobus", 1), ("clustering", 2), ("mv_refresh", None)]:
                (root / f"{kind}.json").write_text(json.dumps({"run_id": "september-test", "total_cost_usd": value}))
            self.assertIsNone(assemble(root)["total_cost_usd"])
            priced = component("mv_refresh", source, DB / "costs/pricings/serverless_maintenance.json", "aws", "eu-west-1", "premium")
            (root / "mv_refresh.json").write_text(json.dumps(priced))
            result = assemble(root)
            self.assertEqual(priced["total_cost_usd"], 39)
            self.assertEqual(result["total_cost_usd"], 42)
            self.assertEqual(result["missing_components"], [])
            self.assertEqual(result["status"], "complete_modeled")

    def test_query_count_and_cache_validation(self):
        original = DB / "results/serverless_baseline_full_20260918T170453Z/mv/dashboard_20260918T170453Z.jsonl"
        row = json.loads(original.read_text().splitlines()[0])
        pricing = DB / "costs/pricings/sql_serverless_compute.json"
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "runner.jsonl"
            path.write_text(json.dumps(row) + "\n")
            with self.assertRaisesRegex(ValueError, "iteration count"):
                summarize(path, pricing, region="eu-west-1", iterations=2)
            row["cache_hit"][0][0] = True
            path.write_text(json.dumps(row) + "\n")
            with self.assertRaisesRegex(ValueError, "cache-hit"):
                summarize(path, pricing, region="eu-west-1", iterations=1)


if __name__ == "__main__":
    unittest.main()
