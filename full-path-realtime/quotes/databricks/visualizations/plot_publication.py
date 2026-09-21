#!/usr/bin/env python3
# /// script
# requires-python = ">=3.10"
# dependencies = ["matplotlib==3.10.9"]
# ///
"""Render cost and latency charts from one accepted publication manifest."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
from pathlib import Path
from typing import Any, Mapping, Sequence

SCRIPT_DIR = Path(__file__).resolve().parent
BENCHMARK_ROOT = SCRIPT_DIR.parents[2]
REQUIRED_ARTIFACTS = (
    "accepted_dashboard.jsonl",
    "accepted_drilldown.jsonl",
    "matched_clickhouse_dashboard.jsonl",
    "matched_clickhouse_drilldown.jsonl",
    "ingest_fresh_path_cost_clickhouse_vs_databricks_summary.json",
    "full_path_cost_performance_clickhouse_vs_databricks_summary.json",
)


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _resolve_source(value: str, publication_path: Path, *, artifact: bool) -> Path:
    path = Path(value).expanduser()
    if path.is_absolute():
        return path.resolve()
    root = publication_path.parent if artifact else BENCHMARK_ROOT
    return (root / path).resolve()


def load_publication(path: Path) -> tuple[dict[str, Any], dict[str, Path]]:
    publication_path = path.expanduser().resolve()
    payload = json.loads(publication_path.read_text(encoding="utf-8"))
    if (
        not isinstance(payload, dict)
        or payload.get("schema_version") != 1
        or payload.get("status") != "accepted"
        or payload.get("accepted") is not True
    ):
        raise ValueError("publication status is not accepted")
    sources = payload.get("sources")
    if not isinstance(sources, dict) or not sources:
        raise ValueError("publication has no source provenance")
    for name, source in sources.items():
        if not isinstance(source, Mapping):
            raise ValueError(f"publication source {name!r} is invalid")
        source_path = _resolve_source(
            str(source.get("path") or ""), publication_path, artifact=False
        )
        expected = source.get("sha256")
        if not isinstance(expected, str) or sha256(source_path) != expected:
            raise ValueError(f"publication source hash mismatch: {name}")
    artifacts = payload.get("artifacts")
    if not isinstance(artifacts, dict):
        raise ValueError("publication has no artifact provenance")
    resolved: dict[str, Path] = {}
    for name, artifact in artifacts.items():
        if not isinstance(artifact, Mapping):
            raise ValueError(f"publication artifact {name!r} is invalid")
        artifact_path = _resolve_source(
            str(artifact.get("path") or ""), publication_path, artifact=True
        )
        expected = artifact.get("sha256")
        if not isinstance(expected, str) or sha256(artifact_path) != expected:
            raise ValueError(f"publication artifact hash mismatch: {name}")
        resolved[name] = artifact_path
    missing = [name for name in REQUIRED_ARTIFACTS if name not in resolved]
    if missing:
        raise ValueError(f"publication omits artifacts: {missing}")
    return payload, resolved


def _read_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"expected JSON object at {path}")
    return value


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows = [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    if not rows or not all(isinstance(row, dict) for row in rows):
        raise ValueError(f"expected nonempty JSON objects at {path}")
    return rows


def _accepted_summary(path: Path, chart: str) -> dict[str, Any]:
    value = _read_json(path)
    if (
        value.get("schema_version") != 1
        or value.get("status") != "accepted"
        or value.get("accepted") is not True
        or value.get("chart") != chart
    ):
        raise ValueError(f"unaccepted or incompatible summary at {path}")
    return value


def _summary_row(summary: Mapping[str, Any], label: str) -> Mapping[str, Any]:
    rows = summary.get("rows")
    if not isinstance(rows, list):
        raise ValueError("performance summary omits rows")
    matches = [
        row for row in rows if isinstance(row, Mapping) and row.get("label") == label
    ]
    if len(matches) != 1:
        raise ValueError(f"performance summary does not have one {label!r} row")
    return matches[0]


def _latencies(rows: Sequence[Mapping[str, Any]]) -> list[float]:
    values: list[float] = []
    for row in rows:
        result = row.get("result")
        if not isinstance(result, list):
            raise ValueError("query record omits result")
        observations: list[float] = []
        for trial in result:
            if (
                not isinstance(trial, list)
                or len(trial) != 1
                or isinstance(trial[0], bool)
            ):
                raise ValueError("query record contains a malformed trial")
            value = float(trial[0])
            if not math.isfinite(value) or value < 0:
                raise ValueError("query record contains an invalid latency")
            observations.append(value)
        values.append(sum(observations))
    return values


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--publication-manifest", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--dpi", type=int, default=200)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    _publication, artifacts = load_publication(args.publication_manifest)
    fresh = _accepted_summary(
        artifacts[
            "ingest_fresh_path_cost_clickhouse_vs_databricks_summary.json"
        ],
        "complete_ingest_fresh_data_path_cost_clickhouse_vs_databricks",
    )
    performance = _accepted_summary(
        artifacts[
            "full_path_cost_performance_clickhouse_vs_databricks_summary.json"
        ],
        "full_path_cost_performance",
    )
    output = args.output_dir.expanduser().resolve()
    output.mkdir(parents=True, exist_ok=True)

    import matplotlib.pyplot as plt

    colors = {"ClickHouse": "#FDFF62", "Databricks": "#FF3621"}
    background = "#161614"

    fresh_values = [
        float(fresh["costs"]["clickhouse"]["total_usd"]),
        float(fresh["costs"]["databricks"]["total_usd"]),
    ]
    fig, axis = plt.subplots(figsize=(8, 4.5))
    fig.patch.set_facecolor(background)
    axis.set_facecolor(background)
    labels = ["ClickHouse", "Databricks"]
    axis.bar(labels, fresh_values, color=[colors[label] for label in labels])
    axis.set_ylabel("Complete-ingest fresh-path cost (USD)", color="white")
    axis.tick_params(colors="white")
    axis.spines[:].set_color("#555555")
    fresh_path = output / "databricks_fresh_path_cost.png"
    fig.tight_layout()
    fig.savefig(fresh_path, dpi=args.dpi, facecolor=background)
    plt.close(fig)

    relatives = [
        float(_summary_row(performance, label)["relative_to_clickhouse"])
        for label in labels
    ]
    fig, axis = plt.subplots(figsize=(8, 4.5))
    fig.patch.set_facecolor(background)
    axis.set_facecolor(background)
    axis.barh(labels, relatives, color=[colors[label] for label in labels])
    axis.set_xlabel("Cost-performance score relative to ClickHouse", color="white")
    axis.tick_params(colors="white")
    axis.spines[:].set_color("#555555")
    performance_path = output / "databricks_full_path_cost_performance.png"
    fig.tight_layout()
    fig.savefig(performance_path, dpi=args.dpi, facecolor=background)
    plt.close(fig)

    fig, axes = plt.subplots(1, 2, figsize=(12, 4.5), sharey=False)
    fig.patch.set_facecolor(background)
    for axis, workload in zip(axes, ("dashboard", "drilldown"), strict=True):
        db_rows = _read_jsonl(artifacts[f"accepted_{workload}.jsonl"])
        ch_rows = _read_jsonl(artifacts[f"matched_clickhouse_{workload}.jsonl"])
        for label, rows in (("ClickHouse", ch_rows), ("Databricks", db_rows)):
            axis.plot(
                [int(row["raw_rows"]) for row in rows],
                _latencies(rows),
                color=colors[label],
                marker="o",
                label=label,
            )
        axis.set_facecolor(background)
        axis.set_title(workload.title(), color="white")
        axis.set_xlabel("Observed raw rows", color="white")
        axis.set_ylabel("Accumulated iteration latency (seconds)", color="white")
        axis.tick_params(colors="white")
        axis.grid(True, color="#444444", alpha=0.6)
        axis.legend()
    latency_path = output / "databricks_query_latency.png"
    fig.tight_layout()
    fig.savefig(latency_path, dpi=args.dpi, facecolor=background)
    plt.close(fig)

    for path in (fresh_path, performance_path, latency_path):
        print(f"Written: {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
