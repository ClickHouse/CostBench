#!/usr/bin/env python3
"""Select candidate observations that best match reference ingest progress.

By default, the full active-ingestion window is derived independently for both
inputs: all records through the first observation at the final row count are
included, and later repeated-final-row records are excluded.  The reference
active count determines the number of candidate observations selected.
"""

from __future__ import annotations

import argparse
import json
import statistics
from pathlib import Path
from typing import Any


def load_jsonl(path: Path) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    with path.open(encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, 1):
            if not line.strip():
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError as error:
                raise SystemExit(f"{path}:{line_number}: {error}") from error
            if not isinstance(record, dict):
                raise SystemExit(f"{path}:{line_number}: expected a JSON object")
            if "raw_rows" not in record:
                raise SystemExit(f"{path}:{line_number}: missing raw_rows")
            records.append(record)
    if not records:
        raise SystemExit(f"{path}: no records")
    return records


def infer_final_rows(records: list[dict[str, Any]], explicit: int | None) -> int:
    final_rows = (
        explicit
        if explicit is not None
        else max(int(record.get("raw_rows") or 0) for record in records)
    )
    if final_rows <= 0:
        raise SystemExit(f"invalid final row count: {final_rows}")
    return final_rows


def active_records(
    records: list[dict[str, Any]], final_rows: int
) -> tuple[list[dict[str, Any]], int]:
    """Return active records plus the zero-based source index of the endpoint."""
    selected: list[dict[str, Any]] = []
    for source_index, record in enumerate(records):
        selected.append(record)
        if int(record.get("raw_rows") or 0) >= final_rows:
            return selected, source_index
    raise SystemExit(f"no observation reached final row count {final_rows}")


def optimal_indices(targets: list[float], candidates: list[float]) -> list[int]:
    """Return an increasing no-replacement assignment with minimum L1 error."""
    target_count = len(targets)
    candidate_count = len(candidates)
    if target_count > candidate_count:
        raise SystemExit(
            f"cannot match {target_count} reference observations to only "
            f"{candidate_count} candidate observations"
        )

    infinity = float("inf")
    previous = [0.0] * (candidate_count + 1)
    decisions: list[list[bool]] = []
    for target in targets:
        current = [infinity] * (candidate_count + 1)
        take_row = [False] * (candidate_count + 1)
        for candidates_seen in range(1, candidate_count + 1):
            skip = current[candidates_seen - 1]
            take = previous[candidates_seen - 1] + abs(
                target - candidates[candidates_seen - 1]
            )
            if take <= skip:
                current[candidates_seen] = take
                take_row[candidates_seen] = True
            else:
                current[candidates_seen] = skip
        decisions.append(take_row)
        previous = current

    indices: list[int] = []
    candidates_seen = candidate_count
    for target_index in range(target_count, 0, -1):
        while candidates_seen > 0 and not decisions[target_index - 1][candidates_seen]:
            candidates_seen -= 1
        if candidates_seen == 0:
            raise SystemExit("failed to reconstruct a monotonic match")
        indices.append(candidates_seen - 1)
        candidates_seen -= 1
    indices.reverse()
    return indices


def endpoint_summary(
    active: list[dict[str, Any]], source_index: int, final_rows: int
) -> dict[str, Any]:
    endpoint = active[-1]
    return {
        "final_rows": final_rows,
        "active_observations_including_endpoint": len(active),
        "active_endpoint_source_line": source_index + 1,
        "active_endpoint_iteration": endpoint.get("iteration"),
        "active_endpoint_raw_rows": int(endpoint.get("raw_rows") or 0),
        "active_endpoint_started_at": (
            endpoint.get("iteration_started_at")
            or endpoint.get("scheduled_start_at")
        ),
        "zero_row_observation_included": any(
            int(record.get("raw_rows") or 0) == 0 for record in active
        ),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--reference", required=True, type=Path)
    parser.add_argument("--candidate", required=True, type=Path)
    parser.add_argument("--reference-final-rows", type=int)
    parser.add_argument("--candidate-final-rows", type=int)
    parser.add_argument(
        "--count",
        type=int,
        help=(
            "Optional reference prefix length. Omit this for the complete "
            "active-ingestion window, including its endpoint."
        ),
    )
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()

    reference_path = args.reference.expanduser().resolve()
    candidate_path = args.candidate.expanduser().resolve()
    output_path = args.output.expanduser().resolve()
    reference_all = load_jsonl(reference_path)
    candidate_all = load_jsonl(candidate_path)
    reference_final = infer_final_rows(reference_all, args.reference_final_rows)
    candidate_final = infer_final_rows(candidate_all, args.candidate_final_rows)
    reference_active, reference_endpoint_index = active_records(
        reference_all, reference_final
    )
    candidate_active, candidate_endpoint_index = active_records(
        candidate_all, candidate_final
    )

    reference = reference_active
    if args.count is not None:
        if args.count <= 0 or args.count > len(reference_active):
            raise SystemExit(
                f"--count must be between 1 and {len(reference_active)}"
            )
        reference = reference_active[: args.count]

    reference_progress = [
        int(record.get("raw_rows") or 0) / reference_final for record in reference
    ]
    candidate_progress = [
        int(record.get("raw_rows") or 0) / candidate_final
        for record in candidate_active
    ]
    indices = optimal_indices(reference_progress, candidate_progress)
    selected = [candidate_active[index] for index in indices]

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as handle:
        for record in selected:
            handle.write(json.dumps(record, separators=(",", ":")) + "\n")

    pairs: list[dict[str, Any]] = []
    gaps: list[float] = []
    for reference_record, candidate_record, candidate_index in zip(
        reference, selected, indices
    ):
        reference_value = int(reference_record.get("raw_rows") or 0) / reference_final
        candidate_value = int(candidate_record.get("raw_rows") or 0) / candidate_final
        gap = abs(reference_value - candidate_value)
        gaps.append(gap)
        pairs.append(
            {
                "reference_iteration": reference_record.get("iteration"),
                "reference_raw_rows": int(reference_record.get("raw_rows") or 0),
                "reference_progress": reference_value,
                "candidate_active_index": candidate_index,
                "candidate_iteration": candidate_record.get("iteration"),
                "candidate_raw_rows": int(candidate_record.get("raw_rows") or 0),
                "candidate_progress": candidate_value,
                "absolute_progress_gap": gap,
            }
        )

    report_path = (
        args.report.expanduser().resolve()
        if args.report
        else output_path.with_suffix(output_path.suffix + ".match.json")
    )

    full_path_root = Path(__file__).resolve().parents[1]

    def provenance_path(path: Path) -> str:
        try:
            return path.relative_to(full_path_root).as_posix()
        except ValueError:
            return str(path)

    report = {
        "schema_version": 2,
        "matching_algorithm": "monotonic_no_replacement_minimum_total_absolute_progress_gap",
        "active_window_definition": (
            "start through first observation at final_rows, inclusive; later "
            "repeated-final-row observations excluded"
        ),
        "reference_file": provenance_path(reference_path),
        "candidate_file": provenance_path(candidate_path),
        "output_file": provenance_path(output_path),
        "count_mode": "explicit_prefix" if args.count is not None else "all_reference_active",
        "requested_count": args.count,
        "reference": {
            "total_observations": len(reference_all),
            **endpoint_summary(
                reference_active, reference_endpoint_index, reference_final
            ),
            "observations_used": len(reference),
        },
        "candidate": {
            "total_observations": len(candidate_all),
            **endpoint_summary(
                candidate_active, candidate_endpoint_index, candidate_final
            ),
            "observations_selected": len(selected),
        },
        "matched_observations": len(selected),
        "mean_absolute_progress_gap": statistics.mean(gaps) if gaps else 0.0,
        "median_absolute_progress_gap": statistics.median(gaps) if gaps else 0.0,
        "maximum_absolute_progress_gap": max(gaps) if gaps else 0.0,
        "pairs": pairs,
    }
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(
        json.dumps(
            {key: value for key, value in report.items() if key != "pairs"},
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
