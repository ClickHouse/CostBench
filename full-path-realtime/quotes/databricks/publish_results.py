#!/usr/bin/env python3
"""Publish a validated Databricks benchmark run without inventing observations."""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
import math
import os
import sys
import tempfile
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

EXPECTED_ROWS = 113_219_565_734
PRESENTATION_ROW_CAP = 100_000_000_000
PROVIDER_LABEL = "Databricks"
PROVIDER_COLOR = "#FF3621"
SOURCE_OUTPUT_NAMES = {
    "validation_report": "source_inputs/validation_report.json",
    "cost_summary": "source_inputs/cost_summary.json",
    "databricks_dashboard": "source_inputs/databricks_dashboard.jsonl",
    "databricks_drilldown": "source_inputs/databricks_drilldown.jsonl",
    "databricks_freshness": "source_inputs/databricks_freshness.jsonl",
    "clickhouse_dashboard": "source_inputs/clickhouse_dashboard.jsonl",
    "clickhouse_drilldown": "source_inputs/clickhouse_drilldown.jsonl",
    "clickhouse_ingest_cost": "source_inputs/clickhouse_ingest_cost.json",
}
OUTPUT_NAMES = (
    "accepted_dashboard.jsonl",
    "accepted_drilldown.jsonl",
    "accepted_freshness.jsonl",
    "matched_clickhouse_dashboard.jsonl",
    "matched_clickhouse_drilldown.jsonl",
    "ingest_fresh_path_cost_clickhouse_vs_databricks_summary.json",
    "full_path_cost_performance_clickhouse_vs_databricks_summary.json",
    "publication_manifest.json",
    *SOURCE_OUTPUT_NAMES.values(),
)

SCRIPT_DIR = Path(__file__).resolve().parent
GIT_ROOT = SCRIPT_DIR.parents[2]
BENCHMARK_ROOT = SCRIPT_DIR.parents[1]
DEFAULT_GLOBAL_MANIFEST = (
    BENCHMARK_ROOT / "quotes" / "global" / "visualizations" / "manifest.json"
)


def _decimal(value: Any, context: str) -> Decimal:
    if value is None or isinstance(value, bool):
        raise ValueError(f"{context} is missing or not numeric")
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError(f"{context} is not numeric") from exc
    if not result.is_finite():
        raise ValueError(f"{context} is not finite")
    return result


def _decimal_text(value: Decimal) -> str:
    text = format(value, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text if text not in ("", "-0") else "0"


def _number(value: Any, context: str, *, nonnegative: bool = True) -> float:
    if value is None or isinstance(value, bool):
        raise ValueError(f"{context} is missing or not numeric")
    try:
        result = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError(f"{context} is not numeric") from exc
    if not math.isfinite(result) or (nonnegative and result < 0):
        raise ValueError(f"{context} is invalid")
    return result


def _integer(value: Any, context: str, *, nonnegative: bool = True) -> int:
    if isinstance(value, bool):
        raise ValueError(f"{context} is not an integer")
    try:
        result = int(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError(f"{context} is not an integer") from exc
    if str(result) != str(value).strip() and not isinstance(value, int):
        raise ValueError(f"{context} is not an exact integer")
    if nonnegative and result < 0:
        raise ValueError(f"{context} is negative")
    return result


def _timestamp(value: Any, context: str) -> datetime:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{context} is not an ISO-8601 timestamp")
    text = value.strip()
    if text.endswith(("Z", "z")):
        text = text[:-1] + "+00:00"
    try:
        result = datetime.fromisoformat(text)
    except ValueError as exc:
        raise ValueError(f"{context} is not an ISO-8601 timestamp") from exc
    if result.tzinfo is None or result.utcoffset() is None:
        raise ValueError(f"{context} has no timezone")
    return result.astimezone(timezone.utc)


def _parse_json(content: bytes, path: Path, context: str) -> dict[str, Any]:
    try:
        value = json.loads(content.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError, TypeError, ValueError) as exc:
        raise ValueError(f"cannot parse {context} at {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise ValueError(f"{context} at {path} is not a JSON object")
    return value


def _read_json(path: Path, context: str) -> dict[str, Any]:
    try:
        content = path.read_bytes()
    except OSError as exc:
        raise ValueError(f"cannot read {context} at {path}: {exc}") from exc
    return _parse_json(content, path, context)


def _parse_jsonl(
    content: bytes,
    path: Path,
    context: str,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    try:
        lines = content.decode("utf-8").splitlines()
    except UnicodeDecodeError as exc:
        raise ValueError(f"cannot decode {context} at {path}: {exc}") from exc
    for line_number, line in enumerate(lines, 1):
        if not line.strip():
            continue
        try:
            value = json.loads(line)
        except (json.JSONDecodeError, TypeError, ValueError) as exc:
            raise ValueError(f"invalid JSON at {path}:{line_number}: {exc}") from exc
        if not isinstance(value, dict):
            raise ValueError(f"{path}:{line_number} is not a JSON object")
        if "_publisher_source_line" in value:
            raise ValueError(
                f"{path}:{line_number} contains reserved publisher metadata"
            )
        value["_publisher_source_line"] = line_number
        rows.append(value)
    return rows


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _json_bytes(value: Mapping[str, Any]) -> bytes:
    return (
        json.dumps(value, indent=2, sort_keys=True, ensure_ascii=False) + "\n"
    ).encode("utf-8")


def _jsonl_bytes(rows: Sequence[Mapping[str, Any]]) -> bytes:
    return "".join(
        json.dumps(row, separators=(",", ":"), sort_keys=True, ensure_ascii=False)
        + "\n"
        for row in rows
    ).encode("utf-8")


def _atomic_write(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass


def _portable_path(path: Path) -> str:
    resolved = path.expanduser().resolve()
    try:
        return resolved.relative_to(BENCHMARK_ROOT).as_posix()
    except ValueError:
        return str(resolved)


def _manifest_relative_path(path: Path) -> str:
    resolved = path.expanduser().resolve()
    try:
        return resolved.relative_to(BENCHMARK_ROOT).as_posix()
    except ValueError as exc:
        raise ValueError(
            f"global-manifest activation source is outside {BENCHMARK_ROOT}: {resolved}"
        ) from exc


def _declared_path_matches(declared: str, actual: Path, report_path: Path) -> bool:
    candidate = Path(declared).expanduser()
    candidates = [candidate] if candidate.is_absolute() else [
        report_path.parent / candidate,
        BENCHMARK_ROOT / candidate,
        GIT_ROOT / candidate,
    ]
    return any(item.resolve() == actual.resolve() for item in candidates)


def _validation_input_paths(report: Mapping[str, Any]) -> Mapping[str, Any]:
    for gate in report.get("gates", []):
        if isinstance(gate, Mapping) and gate.get("id") == "input_availability":
            evidence = gate.get("evidence")
            if isinstance(evidence, Mapping):
                paths = evidence.get("paths")
                if isinstance(paths, Mapping):
                    return paths
    return {}


def _require_declared_input(
    declared: Mapping[str, Any],
    key: str,
    actual: Path,
    report_path: Path,
) -> None:
    value = declared.get(key)
    if value in (None, ""):
        return
    if not isinstance(value, str) or not _declared_path_matches(
        value, actual, report_path
    ):
        raise ValueError(
            f"{key} path does not match validation report input: "
            f"declared={value!r}, supplied={actual}"
        )


def _accepted_indices(
    report: Mapping[str, Any],
    section: str,
    row_count: int,
) -> list[int]:
    parent_name = "freshness_samples" if section == "freshness" else "query_samples"
    parent = report.get(parent_name)
    if not isinstance(parent, Mapping):
        raise ValueError(f"validation report omits {parent_name}")
    value = parent if section == "freshness" else parent.get(section)
    if not isinstance(value, Mapping):
        raise ValueError(f"validation report omits accepted {section} samples")
    indices = value.get("accepted_indices")
    if not isinstance(indices, list) or not indices:
        raise ValueError(f"validation report has no accepted {section} indices")
    result: list[int] = []
    for position, item in enumerate(indices):
        index = _integer(item, f"{section} accepted index {position}")
        if index >= row_count:
            raise ValueError(f"{section} accepted index {index} is out of range")
        result.append(index)
    if len(set(result)) != len(result) or result != sorted(result):
        raise ValueError(f"{section} accepted indices are duplicated or unordered")
    accepted_count = value.get("accepted_count")
    if accepted_count is not None and _integer(
        accepted_count, f"{section} accepted_count"
    ) != len(result):
        raise ValueError(f"{section} accepted_count disagrees with accepted_indices")
    return result


def _valid_hash(value: Any) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 64
        and all(character in "0123456789abcdefABCDEF" for character in value)
    )


def _validate_databricks_query_record(
    record: Mapping[str, Any],
    expected_queries: int,
    *,
    context: str,
) -> int:
    raw_rows = _integer(record.get("raw_rows"), f"{context}.raw_rows")
    if record.get("producer_progress_finished") is not False:
        raise ValueError(f"{context} is not an active-ingest observation")
    if raw_rows >= EXPECTED_ROWS:
        raise ValueError(f"{context} started at or after complete ingestion")
    results = record.get("result")
    evidence = record.get("query_evidence")
    errors = record.get("query_errors")
    if not isinstance(results, list) or len(results) != expected_queries:
        raise ValueError(f"{context} does not contain {expected_queries} results")
    if not isinstance(evidence, list) or len(evidence) != expected_queries:
        raise ValueError(
            f"{context} does not contain {expected_queries} query evidence records"
        )
    if not isinstance(errors, list) or len(errors) != expected_queries:
        raise ValueError(f"{context} query_errors is not aligned")
    for index, (result, observation, query_errors) in enumerate(
        zip(results, evidence, errors, strict=True), 1
    ):
        query = f"{context}.query[{index}]"
        if not isinstance(result, list) or len(result) != 1:
            raise ValueError(f"{query} result is not a one-element trial")
        latency = _number(result[0], f"{query}.result")
        if not isinstance(observation, Mapping):
            raise ValueError(f"{query} evidence is not an object")
        canonical = _number(
            observation.get("canonical_duration_sec"),
            f"{query}.canonical_duration_sec",
        )
        if not math.isclose(latency, canonical, rel_tol=0, abs_tol=1e-12):
            raise ValueError(f"{query} canonical duration disagrees with result")
        if not isinstance(observation.get("statement_id"), str) or not observation.get(
            "statement_id"
        ):
            raise ValueError(f"{query} statement ID is missing")
        metrics = observation.get("metrics")
        if not isinstance(metrics, Mapping):
            raise ValueError(f"{query} metrics are missing")
        if metrics.get("result_from_cache") is not False:
            raise ValueError(f"{query} cache evidence is not exact false")
        if metrics.get("cache_origin_statement_id") not in (None, ""):
            raise ValueError(f"{query} has a cache-origin statement")
        if observation.get("execution_succeeded") is not True:
            raise ValueError(f"{query} execution did not succeed")
        _integer(observation.get("result_row_count"), f"{query}.result_row_count")
        if not _valid_hash(observation.get("result_hash_sha256")):
            raise ValueError(f"{query} result hash is missing or invalid")
        if observation.get("errors") != [] or query_errors != []:
            raise ValueError(f"{query} contains errors")
    return raw_rows


def _source_rows(
    rows: Sequence[Mapping[str, Any]],
    indices: Sequence[int],
    expected_queries: int,
    *,
    workload: str,
    cap: int,
) -> list[dict[str, Any]]:
    selected: list[dict[str, Any]] = []
    previous = -1
    for index in indices:
        record = rows[index]
        raw_rows = _validate_databricks_query_record(
            record, expected_queries, context=f"{workload}[{index}]"
        )
        if raw_rows < previous:
            raise ValueError(f"accepted {workload} raw_rows is not monotonic")
        previous = raw_rows
        if raw_rows > cap:
            continue
        item = copy.deepcopy(record)
        source_line = item.pop("_publisher_source_line")
        item["publication"] = {
            "status": "accepted",
            "validation_accepted": True,
            "source_index": index,
            "source_line": source_line,
            "presentation_row_cap": cap,
        }
        selected.append(item)
    if not selected:
        raise ValueError(f"no accepted {workload} observation is at or below {cap:,}")
    return selected


def _clickhouse_rows(
    rows: Sequence[Mapping[str, Any]],
    expected_queries: int,
    *,
    workload: str,
    cap: int,
) -> list[dict[str, Any]]:
    selected: list[dict[str, Any]] = []
    previous = -1
    for index, record in enumerate(rows):
        raw_rows = _integer(
            record.get("raw_rows"), f"ClickHouse {workload}[{index}].raw_rows"
        )
        if raw_rows < previous:
            raise ValueError(f"ClickHouse {workload} raw_rows is not monotonic")
        previous = raw_rows
        if raw_rows > cap:
            continue
        results = record.get("result")
        if not isinstance(results, list) or len(results) != expected_queries:
            raise ValueError(
                f"ClickHouse {workload}[{index}] does not have "
                f"{expected_queries} results"
            )
        for query_index, value in enumerate(results, 1):
            if not isinstance(value, list) or len(value) != 1:
                raise ValueError(
                    f"ClickHouse {workload}[{index}] q{query_index} "
                    "is not a one-element trial"
                )
            _number(
                value[0],
                f"ClickHouse {workload}[{index}].result[{query_index}]",
            )
        item = copy.deepcopy(record)
        item["_publication_source_index"] = index
        item["_publication_source_line"] = item.pop("_publisher_source_line")
        selected.append(item)
    if not selected:
        raise ValueError(f"ClickHouse {workload} has no records under the row cap")
    return selected


def monotonic_nearest_matches(
    left_rows: Sequence[Mapping[str, Any]],
    right_rows: Sequence[Mapping[str, Any]],
    *,
    row_key: str = "raw_rows",
) -> list[dict[str, int]]:
    """Match every left row to an ordered right subsequence with minimum row delta."""
    left = [_integer(row.get(row_key), f"left[{index}].{row_key}") for index, row in enumerate(left_rows)]
    right = [_integer(row.get(row_key), f"right[{index}].{row_key}") for index, row in enumerate(right_rows)]
    if not left or not right:
        raise ValueError("both sides of monotonic matching must be nonempty")
    if len(left) > len(right):
        raise ValueError(
            "ClickHouse has fewer eligible observations than Databricks for matching"
        )
    if left != sorted(left) or right != sorted(right):
        raise ValueError("matching inputs must be monotonic by observed raw rows")

    count_left, count_right = len(left), len(right)
    infinity = sum(abs(a - b) for a in left for b in right) + 1
    costs = [[infinity] * (count_right + 1) for _ in range(count_left + 1)]
    took = [[False] * (count_right + 1) for _ in range(count_left + 1)]
    for j in range(count_right + 1):
        costs[0][j] = 0
    for i in range(1, count_left + 1):
        for j in range(1, count_right + 1):
            skip = costs[i][j - 1]
            take = costs[i - 1][j - 1] + abs(left[i - 1] - right[j - 1])
            # Keeping an equally good earlier subsequence gives a stable,
            # lexicographically earliest tie-break.
            if take < skip:
                costs[i][j] = take
                took[i][j] = True
            else:
                costs[i][j] = skip
    if costs[count_left][count_right] >= infinity:
        raise ValueError("no complete monotonic match exists")
    pairs: list[tuple[int, int]] = []
    i, j = count_left, count_right
    while i:
        if j <= 0:
            raise ValueError("no complete monotonic match exists")
        if took[i][j]:
            pairs.append((i - 1, j - 1))
            i -= 1
            j -= 1
        else:
            j -= 1
    pairs.reverse()
    return [
        {
            "left_index": left_index,
            "right_index": right_index,
            "left_raw_rows": left[left_index],
            "right_raw_rows": right[right_index],
            "row_delta": right[right_index] - left[left_index],
            "absolute_row_delta": abs(right[right_index] - left[left_index]),
        }
        for left_index, right_index in pairs
    ]


def _match_workload(
    databricks: list[dict[str, Any]],
    clickhouse: list[dict[str, Any]],
    *,
    workload: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    matches = monotonic_nearest_matches(databricks, clickhouse)
    matched_clickhouse: list[dict[str, Any]] = []
    report: list[dict[str, Any]] = []
    for match in matches:
        db = databricks[match["left_index"]]
        ch = clickhouse[match["right_index"]]
        db_source = db["publication"]
        ch_index = int(ch.pop("_publication_source_index"))
        ch_line = int(ch.pop("_publication_source_line"))
        details = {
            "method": "minimum-total-absolute-row-delta ordered subsequence DP",
            "workload": workload,
            "databricks_source_index": db_source["source_index"],
            "databricks_source_line": db_source["source_line"],
            "clickhouse_source_index": ch_index,
            "clickhouse_source_line": ch_line,
            "databricks_raw_rows": match["left_raw_rows"],
            "clickhouse_raw_rows": match["right_raw_rows"],
            "row_delta_clickhouse_minus_databricks": match["row_delta"],
            "absolute_row_delta": match["absolute_row_delta"],
        }
        db["publication"]["match"] = details
        ch["publication"] = {
            "status": "accepted",
            "matched_to_databricks": True,
            "match": details,
        }
        matched_clickhouse.append(ch)
        report.append(details)
    return databricks, matched_clickhouse, report


def _freshness_rows(
    rows: Sequence[Mapping[str, Any]],
    indices: Sequence[int],
    *,
    cap: int,
) -> list[dict[str, Any]]:
    selected: list[dict[str, Any]] = []
    previous = -1
    for index in indices:
        record = rows[index]
        producer = record.get("producer_progress")
        if not isinstance(producer, Mapping):
            raise ValueError(f"freshness[{index}] omits producer_progress")
        raw_rows = _integer(
            producer.get("logical_raw_rows"),
            f"freshness[{index}].producer_progress.logical_raw_rows",
        )
        if raw_rows < previous:
            raise ValueError("accepted freshness logical_raw_rows is not monotonic")
        previous = raw_rows
        if raw_rows > cap:
            continue
        lag = _number(
            record.get("mv_refresh_age_sec"),
            f"freshness[{index}].mv_refresh_age_sec",
        )
        if record.get("errors") != []:
            raise ValueError(f"freshness[{index}] contains errors")
        observed = record.get("observed_at")
        _timestamp(observed, f"freshness[{index}].observed_at")
        refresh = record.get("latest_successful_refresh")
        refresh_watermark = ""
        if isinstance(refresh, Mapping):
            refresh_watermark = str(
                refresh.get("finished_at")
                or refresh.get("source_timestamp")
                or refresh.get("source_version")
                or ""
            )
        item = copy.deepcopy(record)
        source_line = item.pop("_publisher_source_line")
        item.update(
            {
                "provider": "databricks",
                "system": PROVIDER_LABEL,
                "raw_rows": raw_rows,
                "raw_rows_source": "producer_progress.logical_raw_rows",
                "base_table_rows": raw_rows,
                "lag_seconds": lag,
                "refresh_watermark": refresh_watermark,
                "publication": {
                    "status": "accepted",
                    "validation_accepted": True,
                    "source_index": index,
                    "source_line": source_line,
                    "presentation_row_cap": cap,
                },
            }
        )
        selected.append(item)
    if not selected:
        raise ValueError(f"no accepted freshness observation is at or below {cap:,}")
    return selected


def _merge_intervals(
    intervals: Iterable[tuple[datetime, datetime]],
) -> list[tuple[datetime, datetime]]:
    ordered = sorted((start, end) for start, end in intervals if end > start)
    merged: list[list[datetime]] = []
    for start, end in ordered:
        if not merged or start > merged[-1][1]:
            merged.append([start, end])
        elif end > merged[-1][1]:
            merged[-1][1] = end
    return [(start, end) for start, end in merged]


def _intersect_intervals(
    left: Sequence[tuple[datetime, datetime]],
    right: Sequence[tuple[datetime, datetime]],
) -> list[tuple[datetime, datetime]]:
    left_merged, right_merged = _merge_intervals(left), _merge_intervals(right)
    result: list[tuple[datetime, datetime]] = []
    i = j = 0
    while i < len(left_merged) and j < len(right_merged):
        start = max(left_merged[i][0], right_merged[j][0])
        end = min(left_merged[i][1], right_merged[j][1])
        if end > start:
            result.append((start, end))
        if left_merged[i][1] <= right_merged[j][1]:
            i += 1
        else:
            j += 1
    return _merge_intervals(result)


def _microseconds(start: datetime, end: datetime) -> int:
    return max(0, round((end - start).total_seconds() * 1_000_000))


def _overlap_microseconds(
    start: datetime,
    end: datetime,
    intervals: Sequence[tuple[datetime, datetime]],
) -> int:
    return sum(
        _microseconds(max(start, item_start), min(end, item_end))
        for item_start, item_end in _merge_intervals(intervals)
        if min(end, item_end) > max(start, item_start)
    )


def _record_intervals(
    dashboard: Sequence[Mapping[str, Any]],
    drilldown: Sequence[Mapping[str, Any]],
) -> list[tuple[datetime, datetime]]:
    intervals: list[tuple[datetime, datetime]] = []
    for workload, rows in (("dashboard", dashboard), ("drilldown", drilldown)):
        for index, row in enumerate(rows):
            start = _timestamp(
                row.get("iteration_started_at"),
                f"{workload}[{index}].iteration_started_at",
            )
            end = _timestamp(
                row.get("iteration_finished_at"),
                f"{workload}[{index}].iteration_finished_at",
            )
            if end <= start:
                raise ValueError(f"{workload}[{index}] has an empty interval")
            intervals.append((start, end))
    return _merge_intervals(intervals)


def allocate_matched_query_cost(
    matched_lines: Sequence[Mapping[str, Any]],
    matched_intervals: Sequence[tuple[datetime, datetime]],
    *,
    successful_query_intervals: Sequence[tuple[datetime, datetime]] | None = None,
) -> tuple[Decimal, list[dict[str, Any]]]:
    """Prorate RT billing lines against an interval union exactly once."""
    matched_union = _merge_intervals(matched_intervals)
    if not matched_union:
        raise ValueError("matched query interval union is empty")
    query_union = (
        _merge_intervals(successful_query_intervals)
        if successful_query_intervals is not None
        else None
    )
    total = Decimal("0")
    allocations: list[dict[str, Any]] = []
    for line_index, line in enumerate(matched_lines):
        if line.get("category") != "primary_rt_serving_compute":
            continue
        interval = line.get("measured_interval")
        if not isinstance(interval, Mapping):
            raise ValueError(f"matched_lines[{line_index}] omits measured_interval")
        start = _timestamp(
            interval.get("start"), f"matched_lines[{line_index}].measured_interval.start"
        )
        end = _timestamp(
            interval.get("end"), f"matched_lines[{line_index}].measured_interval.end"
        )
        if end <= start:
            raise ValueError(f"matched_lines[{line_index}] has an empty interval")
        amount = _decimal(line.get("list_cost"), f"matched_lines[{line_index}].list_cost")
        if query_union is None:
            allocated_us = _microseconds(start, end)
            matched_us = _overlap_microseconds(start, end, matched_union)
        else:
            allocated_us = _overlap_microseconds(start, end, query_union)
            matched_query_union = _intersect_intervals(query_union, matched_union)
            matched_us = _overlap_microseconds(start, end, matched_query_union)
            allocation = line.get("allocation")
            if not isinstance(allocation, Mapping):
                raise ValueError(f"matched_lines[{line_index}] omits allocation")
            declared_us = _integer(
                allocation.get("numerator_microseconds"),
                f"matched_lines[{line_index}].allocation.numerator_microseconds",
            )
            if declared_us != allocated_us:
                raise ValueError(
                    f"matched_lines[{line_index}] allocation does not reconcile "
                    "to successful query interval union"
                )
        if allocated_us <= 0:
            raise ValueError(f"matched_lines[{line_index}] has no allocated duration")
        fraction = Decimal(matched_us) / Decimal(allocated_us)
        matched_amount = amount * fraction
        total += matched_amount
        allocations.append(
            {
                "matched_line_index": line_index,
                "source_usage_row_index": line.get("usage_row_index"),
                "currency": line.get("currency"),
                "source_list_cost": str(line.get("list_cost")),
                "allocated_microseconds": allocated_us,
                "matched_union_overlap_microseconds": matched_us,
                "matched_fraction": _decimal_text(fraction),
                "matched_list_cost": _decimal_text(matched_amount),
            }
        )
    if not allocations:
        raise ValueError("cost summary has no primary_rt_serving_compute matched lines")
    return total, allocations


def _canonical_costs(cost: Mapping[str, Any]) -> tuple[str, Decimal, Decimal, Decimal]:
    if cost.get("complete") is not True:
        raise ValueError("cost summary complete is not exact true")
    completeness = cost.get("completeness")
    if not isinstance(completeness, Mapping) or completeness.get("complete") is not True:
        raise ValueError("cost summary completeness.complete is not exact true")
    primary = cost.get("canonical_undiscounted_primary_total")
    fresh = cost.get("canonical_undiscounted_fresh_path_total")
    serving = cost.get("canonical_undiscounted_query_serving_total")
    if not all(isinstance(item, Mapping) for item in (primary, fresh, serving)):
        raise ValueError("canonical primary/fresh-path/query-serving subledgers are missing")
    primary_amount = _decimal(primary.get("amount"), "canonical primary total")
    fresh_amount = _decimal(fresh.get("amount"), "canonical fresh-path total")
    serving_amount = _decimal(serving.get("amount"), "canonical query-serving total")
    currency = primary.get("currency")
    if not isinstance(currency, str) or not currency:
        raise ValueError("canonical primary total has no single currency")
    if fresh.get("currency") != currency or serving.get("currency") != currency:
        raise ValueError("canonical subledger currencies do not agree")
    if min(primary_amount, fresh_amount, serving_amount) < 0:
        raise ValueError("canonical subledger amounts must be nonnegative")
    if fresh_amount + serving_amount != primary_amount:
        raise ValueError("fresh-path and query-serving subledgers do not reconcile")
    lines = cost.get("matched_lines")
    if not isinstance(lines, list):
        raise ValueError("cost summary matched_lines is missing")
    primary_lines = [
        (index, line)
        for index, line in enumerate(lines)
        if isinstance(line, Mapping)
        and line.get("category") == "primary_rt_serving_compute"
    ]
    if not primary_lines:
        raise ValueError("query-serving subledger has no primary RT matched lines")
    for index, line in primary_lines:
        if line.get("currency") != currency:
            raise ValueError(f"matched_lines[{index}] currency does not reconcile")
    line_total = sum(
        (
            _decimal(line.get("list_cost"), f"matched_lines[{index}].list_cost")
            for index, line in primary_lines
        ),
        Decimal("0"),
    )
    if line_total != serving_amount:
        raise ValueError("primary RT matched lines do not reconcile to query-serving total")
    return currency, primary_amount, fresh_amount, serving_amount


def _successful_query_intervals(
    cost: Mapping[str, Any],
) -> list[tuple[datetime, datetime]]:
    allocation = cost.get("query_serving_allocation")
    if not isinstance(allocation, Mapping) or allocation.get("method") != "union_overlap":
        raise ValueError("cost summary does not declare union-overlap query allocation")
    values = allocation.get("successful_query_union_intervals")
    if not isinstance(values, list) or not values:
        raise ValueError("cost summary omits successful query union intervals")
    intervals: list[tuple[datetime, datetime]] = []
    for index, item in enumerate(values):
        if not isinstance(item, Mapping):
            raise ValueError(f"query union interval {index} is not an object")
        start = _timestamp(item.get("start"), f"query union interval {index}.start")
        end = _timestamp(item.get("end"), f"query union interval {index}.end")
        if end <= start:
            raise ValueError(f"query union interval {index} is empty")
        intervals.append((start, end))
    return _merge_intervals(intervals)


def _clickhouse_cost(
    value: Mapping[str, Any],
    reader_mem_gib: float,
) -> tuple[Decimal, Decimal, Decimal]:
    if "clickhouse" not in str(value.get("system") or "").casefold():
        raise ValueError("ClickHouse ingest cost source has the wrong system")
    costs = value.get("costs")
    if not isinstance(costs, list):
        raise ValueError("ClickHouse ingest cost source omits costs")
    entries = [
        item
        for item in costs
        if isinstance(item, Mapping)
        and str(item.get("tier") or "").casefold() == "enterprise"
    ]
    if len(entries) != 1:
        raise ValueError("ClickHouse ingest cost must contain one Enterprise entry")
    entry = entries[0]
    fresh = _decimal(
        entry.get("total_compute_cost_usd"),
        "ClickHouse Enterprise complete-ingest cost",
    )
    rate = _decimal(
        entry.get("compute_price_per_8gib_hour"),
        "ClickHouse Enterprise per-8-GiB-hour rate",
    )
    memory = _decimal(reader_mem_gib, "ClickHouse reader memory GiB")
    if fresh < 0 or rate <= 0 or memory <= 0:
        raise ValueError("ClickHouse cost/rate/memory is invalid")
    return fresh, rate, memory


def _runtime(rows: Sequence[Mapping[str, Any]], context: str) -> Decimal:
    total = Decimal("0")
    for row_index, row in enumerate(rows):
        results = row.get("result")
        if not isinstance(results, list):
            raise ValueError(f"{context}[{row_index}] omits result")
        for query_index, value in enumerate(results):
            if not isinstance(value, list) or len(value) != 1:
                raise ValueError(
                    f"{context}[{row_index}].result[{query_index}] is malformed"
                )
            latency = _decimal(
                value[0], f"{context}[{row_index}].result[{query_index}]"
            )
            if latency < 0:
                raise ValueError(f"{context} contains negative runtime")
            total += latency
    if total <= 0:
        raise ValueError(f"{context} accumulated runtime is not positive")
    return total


def _build_summaries(
    *,
    cost: Mapping[str, Any],
    clickhouse_ingest: Mapping[str, Any],
    databricks_dashboard: Sequence[Mapping[str, Any]],
    databricks_drilldown: Sequence[Mapping[str, Any]],
    clickhouse_dashboard: Sequence[Mapping[str, Any]],
    clickhouse_drilldown: Sequence[Mapping[str, Any]],
    matched_query_cost: Decimal,
    matched_allocations: Sequence[Mapping[str, Any]],
    reader_mem_gib: float,
    sources: Mapping[str, Mapping[str, Any]],
) -> tuple[dict[str, Any], dict[str, Any]]:
    currency, primary, db_fresh, full_db_query = _canonical_costs(cost)
    if currency.upper() != "USD":
        raise ValueError(f"global pairwise summaries require USD, found {currency!r}")
    ch_fresh, ch_rate, ch_memory = _clickhouse_cost(
        clickhouse_ingest, reader_mem_gib
    )
    db_runtime = _runtime(
        [*databricks_dashboard, *databricks_drilldown], "Databricks matched queries"
    )
    ch_runtime = _runtime(
        [*clickhouse_dashboard, *clickhouse_drilldown], "ClickHouse matched queries"
    )
    ch_query = ch_runtime / Decimal("3600") * ch_memory / Decimal("8") * ch_rate
    beta_matched_query = matched_query_cost * Decimal("0.70")
    beta_primary = db_fresh + beta_matched_query
    beta = cost.get("lakehouse_rt_beta_30_percent_off_scenario")
    if (
        not isinstance(beta, Mapping)
        or beta.get("discount_rate") != "0.30"
        or beta.get("applies_only_to") != "primary_rt_serving_compute"
        or beta.get("canonical_total_unchanged") is not True
        or beta.get("canonical_undiscounted_primary_total")
        != cost.get("canonical_undiscounted_primary_total")
    ):
        raise ValueError("cost summary omits the separate Lakehouse//RT Beta scenario")

    common_sources = copy.deepcopy(dict(sorted(sources.items())))
    fresh_summary = {
        "schema_version": 1,
        "status": "accepted",
        "accepted": True,
        "chart": "complete_ingest_fresh_data_path_cost_clickhouse_vs_databricks",
        "scope": "complete ingestion; storage and read-query costs excluded",
        "expected_full_rows": EXPECTED_ROWS,
        "presentation_row_cap": PRESENTATION_ROW_CAP,
        "costs": {
            "clickhouse": {
                "bundled_write_service_usd": float(ch_fresh),
                "total_usd": float(ch_fresh),
            },
            "databricks": {
                "canonical_undiscounted_fresh_path_usd": float(db_fresh),
                "total_usd": float(db_fresh),
                "primary_category_totals": cost.get("primary_category_totals"),
            },
        },
        "beta_scenario": {
            "separate_from_canonical": True,
            "discount_rate": "0.30",
            "applies_only_to": "primary_rt_serving_compute",
            "complete_ingest_fresh_path_usd_unchanged": float(db_fresh),
        },
        "disclosure": (
            "The Databricks fresh-data-path cost charges the validated complete "
            f"{EXPECTED_ROWS:,}-row ingest. The 100B cap applies only to chart "
            "presentation observations."
        ),
        "sources": common_sources,
    }

    rows = [
        {
            "label": "ClickHouse",
            "fresh_cost": float(ch_fresh),
            "query_cost": float(ch_query),
            "runtime_sec": float(ch_runtime),
        },
        {
            "label": PROVIDER_LABEL,
            "fresh_cost": float(db_fresh),
            "query_cost": float(matched_query_cost),
            "runtime_sec": float(db_runtime),
        },
    ]
    for row in rows:
        row["total_cost"] = row["fresh_cost"] + row["query_cost"]
        row["score"] = row["total_cost"] * row["runtime_sec"]
    baseline = rows[0]["score"]
    if baseline <= 0:
        raise ValueError("ClickHouse pairwise score is not positive")
    for row in rows:
        row["relative_to_clickhouse"] = row["score"] / baseline
    performance_summary = {
        "schema_version": 1,
        "status": "accepted",
        "accepted": True,
        "chart": "full_path_cost_performance",
        "formula": (
            "(fresh_data_path_cost_usd + matched_query_cost_usd) * "
            "accumulated_query_runtime_sec"
        ),
        "lower_is_better": True,
        "expected_full_rows": EXPECTED_ROWS,
        "presentation_row_cap": PRESENTATION_ROW_CAP,
        "rows": rows,
        "databricks_query_cost_allocation": {
            "method": (
                "prorated overlap of primary_rt_serving_compute billing allocations "
                "with the union of matched Databricks iteration intervals"
            ),
            "estimate": True,
            "concurrent_query_durations_double_counted": False,
            "canonical_complete_window_query_serving_usd": float(full_db_query),
            "matched_query_serving_usd": float(matched_query_cost),
            "matched_lines": list(matched_allocations),
            "disclosure": (
                "Estimated allocation: billing usage has no exact statement ID join. "
                "Interval unions prevent dashboard/drilldown concurrency from being "
                "charged more than once."
            ),
        },
        "clickhouse_query_cost": {
            "method": (
                "matched accumulated runtime × reader GiB / 8 × Enterprise "
                "per-8-GiB-hour rate"
            ),
            "reader_memory_gib": float(ch_memory),
            "enterprise_price_per_8gib_hour_usd": float(ch_rate),
            "matched_query_cost_usd": float(ch_query),
            "source_sha256": common_sources["clickhouse_ingest_cost"]["sha256"],
        },
        "lakehouse_rt_beta_30_percent_off_scenario": {
            "separate_from_canonical": True,
            "discount_rate": "0.30",
            "applies_only_to": "matched primary_rt_serving_compute",
            "matched_query_serving_usd": float(beta_matched_query),
            "full_path_total_usd": float(beta_primary),
            "canonical_rows_unchanged": True,
        },
        "complete_ingest_cost_disclosure": (
            f"Both canonical fresh-path costs charge the supplied complete-ingest "
            f"sources; Databricks charges all {EXPECTED_ROWS:,} validated rows. "
            "Only query and freshness presentation records are capped at 100B rows."
        ),
        "sources": common_sources,
    }
    if primary != db_fresh + full_db_query:
        raise ValueError("canonical primary cost changed while summaries were built")
    return fresh_summary, performance_summary


def _validate_manifest_update(
    manifest_path: Path,
    output_dir: Path,
) -> dict[str, Any]:
    try:
        manifest_path.resolve().relative_to(GIT_ROOT)
    except ValueError as exc:
        raise ValueError("global manifest is outside the repository") from exc
    manifest = _read_json(manifest_path, "global manifest")
    providers = manifest.get("providers")
    required = manifest.get("required_labels")
    if not isinstance(providers, dict) or not isinstance(required, dict):
        raise ValueError("global manifest omits providers or required_labels")
    if "databricks" in providers:
        raise ValueError("global manifest already contains providers.databricks")
    for chart, labels in required.items():
        if not isinstance(labels, list):
            raise ValueError(f"required_labels.{chart} is not a list")
        if any(
            isinstance(label, str) and label.casefold() == PROVIDER_LABEL.casefold()
            for label in labels
        ):
            raise ValueError(f"required_labels.{chart} already contains Databricks")
    if not isinstance(manifest.get("fresh_path_cost"), dict) or not isinstance(
        manifest.get("cost_performance"), dict
    ):
        raise ValueError("global manifest omits cost source sections")
    for section in ("fresh_path_cost", "cost_performance"):
        if "databricks_pairwise_summary" in manifest[section]:
            raise ValueError(
                f"global manifest {section}.databricks_pairwise_summary conflicts"
            )

    updated = copy.deepcopy(manifest)
    provider = {
        "enabled": True,
        "label": PROVIDER_LABEL,
        "color": PROVIDER_COLOR,
        "aggregate": _manifest_relative_path(output_dir / "accepted_dashboard.jsonl"),
        "aggregate_queries": _manifest_relative_path(SCRIPT_DIR / "queries_mv.sql"),
        "drilldown": _manifest_relative_path(output_dir / "accepted_drilldown.jsonl"),
        "drilldown_queries": _manifest_relative_path(SCRIPT_DIR / "queries_raw.sql"),
        "freshness": _manifest_relative_path(output_dir / "accepted_freshness.jsonl"),
        "validation": _manifest_relative_path(
            output_dir / SOURCE_OUTPUT_NAMES["validation_report"]
        ),
        "publication": _manifest_relative_path(output_dir / "publication_manifest.json"),
    }
    updated["providers"]["databricks"] = provider
    chart_keys = (
        "aggregate_query_latency",
        "drilldown_query_latency",
        "mv_lag",
        "fresh_path_cost",
        "full_path_cost_performance",
        "full_path_cost_vs_query_runtime",
    )
    for chart in chart_keys:
        labels = updated["required_labels"].get(chart)
        if not isinstance(labels, list):
            raise ValueError(f"global manifest omits required_labels.{chart}")
        labels.append(PROVIDER_LABEL)
    updated["fresh_path_cost"]["databricks_pairwise_summary"] = (
        _manifest_relative_path(
            output_dir
            / "ingest_fresh_path_cost_clickhouse_vs_databricks_summary.json"
        )
    )
    updated["cost_performance"]["databricks_pairwise_summary"] = (
        _manifest_relative_path(
            output_dir
            / "full_path_cost_performance_clickhouse_vs_databricks_summary.json"
        )
    )
    return updated


def _default_clickhouse_paths(manifest_path: Path) -> tuple[Path, Path]:
    manifest = _read_json(manifest_path, "global manifest")
    clickhouse = manifest.get("providers", {}).get("clickhouse")
    if not isinstance(clickhouse, Mapping):
        raise ValueError("global manifest omits providers.clickhouse")
    try:
        return (
            (BENCHMARK_ROOT / str(clickhouse["aggregate"])).resolve(),
            (BENCHMARK_ROOT / str(clickhouse["drilldown"])).resolve(),
        )
    except KeyError as exc:
        raise ValueError("global manifest omits ClickHouse query sources") from exc


def publish(
    *,
    validation_report_path: Path,
    cost_summary_path: Path,
    dashboard_path: Path,
    drilldown_path: Path,
    freshness_path: Path,
    clickhouse_dashboard_path: Path,
    clickhouse_drilldown_path: Path,
    clickhouse_ingest_cost_path: Path,
    output_dir: Path,
    clickhouse_reader_mem_gib: float = 64,
    global_manifest_path: Path | None = None,
    apply_global_manifest: bool = False,
) -> dict[str, Any]:
    paths = {
        "validation_report": validation_report_path.expanduser().resolve(),
        "cost_summary": cost_summary_path.expanduser().resolve(),
        "databricks_dashboard": dashboard_path.expanduser().resolve(),
        "databricks_drilldown": drilldown_path.expanduser().resolve(),
        "databricks_freshness": freshness_path.expanduser().resolve(),
        "clickhouse_dashboard": clickhouse_dashboard_path.expanduser().resolve(),
        "clickhouse_drilldown": clickhouse_drilldown_path.expanduser().resolve(),
        "clickhouse_ingest_cost": clickhouse_ingest_cost_path.expanduser().resolve(),
    }
    destination = output_dir.expanduser().resolve()
    if destination.exists() and not destination.is_dir():
        raise FileExistsError(f"publisher output directory is a file: {destination}")
    targets = {name: destination / name for name in OUTPUT_NAMES}
    existing = [path for path in targets.values() if path.exists()]
    if existing:
        raise FileExistsError(
            "publisher never overwrites target output: "
            + ", ".join(str(path) for path in existing)
        )
    input_contents: dict[str, bytes] = {}
    for name, path in paths.items():
        try:
            input_contents[name] = path.read_bytes()
        except OSError as exc:
            raise ValueError(f"cannot snapshot {name} at {path}: {exc}") from exc
    source_descriptors = {
        name: {
            "path": _portable_path(destination / SOURCE_OUTPUT_NAMES[name]),
            "sha256": hashlib.sha256(content).hexdigest(),
        }
        for name, content in sorted(input_contents.items())
    }
    if apply_global_manifest and global_manifest_path is None:
        raise ValueError("--apply-global-manifest requires --global-manifest")
    manifest_path = (
        global_manifest_path.expanduser().resolve()
        if global_manifest_path is not None
        else None
    )

    validation = _parse_json(
        input_contents["validation_report"],
        paths["validation_report"],
        "validation report",
    )
    cost = _parse_json(
        input_contents["cost_summary"],
        paths["cost_summary"],
        "cost summary",
    )
    if validation.get("accepted") is not True:
        raise ValueError("validation report accepted is not exact true")
    configuration = validation.get("configuration")
    if not isinstance(configuration, Mapping) or _integer(
        configuration.get("expected_rows"), "validation expected_rows"
    ) != EXPECTED_ROWS:
        raise ValueError(f"validation expected_rows must be exactly {EXPECTED_ROWS}")
    _canonical_costs(cost)
    declared = _validation_input_paths(validation)
    for key, path_key in (
        ("dashboard", "databricks_dashboard"),
        ("drilldown", "databricks_drilldown"),
        ("freshness", "databricks_freshness"),
        ("cost_summary", "cost_summary"),
    ):
        _require_declared_input(
            declared, key, paths[path_key], paths["validation_report"]
        )

    dashboard = _parse_jsonl(
        input_contents["databricks_dashboard"],
        paths["databricks_dashboard"],
        "Databricks dashboard",
    )
    drilldown = _parse_jsonl(
        input_contents["databricks_drilldown"],
        paths["databricks_drilldown"],
        "Databricks drilldown",
    )
    freshness = _parse_jsonl(
        input_contents["databricks_freshness"],
        paths["databricks_freshness"],
        "Databricks freshness",
    )
    dashboard_indices = _accepted_indices(validation, "dashboard", len(dashboard))
    drilldown_indices = _accepted_indices(validation, "drilldown", len(drilldown))
    freshness_indices = _accepted_indices(validation, "freshness", len(freshness))
    accepted_dashboard = _source_rows(
        dashboard,
        dashboard_indices,
        4,
        workload="dashboard",
        cap=PRESENTATION_ROW_CAP,
    )
    accepted_drilldown = _source_rows(
        drilldown,
        drilldown_indices,
        2,
        workload="drilldown",
        cap=PRESENTATION_ROW_CAP,
    )
    accepted_freshness = _freshness_rows(
        freshness, freshness_indices, cap=PRESENTATION_ROW_CAP
    )

    clickhouse_dashboard = _clickhouse_rows(
        _parse_jsonl(
            input_contents["clickhouse_dashboard"],
            paths["clickhouse_dashboard"],
            "ClickHouse dashboard",
        ),
        4,
        workload="dashboard",
        cap=PRESENTATION_ROW_CAP,
    )
    clickhouse_drilldown = _clickhouse_rows(
        _parse_jsonl(
            input_contents["clickhouse_drilldown"],
            paths["clickhouse_drilldown"],
            "ClickHouse drilldown",
        ),
        2,
        workload="drilldown",
        cap=PRESENTATION_ROW_CAP,
    )
    accepted_dashboard, matched_ch_dashboard, dashboard_matches = _match_workload(
        accepted_dashboard, clickhouse_dashboard, workload="dashboard"
    )
    accepted_drilldown, matched_ch_drilldown, drilldown_matches = _match_workload(
        accepted_drilldown, clickhouse_drilldown, workload="drilldown"
    )

    matched_intervals = _record_intervals(accepted_dashboard, accepted_drilldown)
    query_union = _successful_query_intervals(cost)
    lines = cost.get("matched_lines")
    if not isinstance(lines, list):
        raise ValueError("cost summary matched_lines is missing")
    matched_query_cost, matched_allocations = allocate_matched_query_cost(
        lines,
        matched_intervals,
        successful_query_intervals=query_union,
    )
    clickhouse_ingest = _parse_json(
        input_contents["clickhouse_ingest_cost"],
        paths["clickhouse_ingest_cost"],
        "ClickHouse complete-ingest cost",
    )
    fresh_summary, performance_summary = _build_summaries(
        cost=cost,
        clickhouse_ingest=clickhouse_ingest,
        databricks_dashboard=accepted_dashboard,
        databricks_drilldown=accepted_drilldown,
        clickhouse_dashboard=matched_ch_dashboard,
        clickhouse_drilldown=matched_ch_drilldown,
        matched_query_cost=matched_query_cost,
        matched_allocations=matched_allocations,
        reader_mem_gib=clickhouse_reader_mem_gib,
        sources=source_descriptors,
    )

    updated_manifest = None
    if apply_global_manifest:
        if manifest_path is None:
            raise ValueError("--apply-global-manifest requires --global-manifest")
        updated_manifest = _validate_manifest_update(
            manifest_path, destination
        )
    elif manifest_path is not None:
        try:
            manifest_path.relative_to(GIT_ROOT)
        except ValueError as exc:
            raise ValueError("global manifest is outside the repository") from exc

    contents: dict[str, bytes] = {
        "accepted_dashboard.jsonl": _jsonl_bytes(accepted_dashboard),
        "accepted_drilldown.jsonl": _jsonl_bytes(accepted_drilldown),
        "accepted_freshness.jsonl": _jsonl_bytes(accepted_freshness),
        "matched_clickhouse_dashboard.jsonl": _jsonl_bytes(matched_ch_dashboard),
        "matched_clickhouse_drilldown.jsonl": _jsonl_bytes(matched_ch_drilldown),
        "ingest_fresh_path_cost_clickhouse_vs_databricks_summary.json": _json_bytes(
            fresh_summary
        ),
        "full_path_cost_performance_clickhouse_vs_databricks_summary.json": _json_bytes(
            performance_summary
        ),
        **{
            SOURCE_OUTPUT_NAMES[name]: content
            for name, content in input_contents.items()
        },
    }
    artifact_counts = {
        "accepted_dashboard.jsonl": len(accepted_dashboard),
        "accepted_drilldown.jsonl": len(accepted_drilldown),
        "accepted_freshness.jsonl": len(accepted_freshness),
        "matched_clickhouse_dashboard.jsonl": len(matched_ch_dashboard),
        "matched_clickhouse_drilldown.jsonl": len(matched_ch_drilldown),
    }
    publication = {
        "schema_version": 1,
        "status": "accepted",
        "accepted": True,
        "provider": "databricks",
        "label": PROVIDER_LABEL,
        "color": PROVIDER_COLOR,
        "expected_full_rows": EXPECTED_ROWS,
        "presentation_row_cap": PRESENTATION_ROW_CAP,
        "sources": source_descriptors,
        "artifacts": {
            name: {
                "path": name,
                "sha256": hashlib.sha256(content).hexdigest(),
                **(
                    {"records": artifact_counts[name]}
                    if name in artifact_counts
                    else {}
                ),
            }
            for name, content in sorted(contents.items())
        },
        "matching": {
            "method": "deterministic order-preserving minimum-total-absolute-row-delta DP",
            "iteration_numbers_used_for_join": False,
            "dashboard": dashboard_matches,
            "drilldown": drilldown_matches,
        },
        "cost": {
            "matched_databricks_query_serving_usd": float(matched_query_cost),
            "allocation_is_estimate": True,
            "query_duration_concurrency_double_counted": False,
            "fresh_path_uses_complete_ingest": True,
            "beta_scenario_is_separate": True,
        },
        "disclosure": (
            f"The complete validated Databricks ingest contains {EXPECTED_ROWS:,} "
            f"rows and remains fully charged. Query and freshness presentation "
            f"records are capped at {PRESENTATION_ROW_CAP:,} raw rows."
        ),
        "global_manifest": {
            "path": _portable_path(manifest_path) if manifest_path else None,
            "applied": apply_global_manifest,
        },
    }
    publication_content = _json_bytes(publication)

    for name, content in contents.items():
        _atomic_write(targets[name], content)
    _atomic_write(targets["publication_manifest.json"], publication_content)
    if updated_manifest is not None:
        if manifest_path is None:
            raise ValueError("global manifest path disappeared before activation")
        _atomic_write(manifest_path, _json_bytes(updated_manifest))
    return publication


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--validation-report", type=Path, required=True)
    parser.add_argument("--cost-summary", type=Path, required=True)
    parser.add_argument("--dashboard", "--databricks-dashboard", dest="dashboard", type=Path, required=True)
    parser.add_argument("--drilldown", "--databricks-drilldown", dest="drilldown", type=Path, required=True)
    parser.add_argument("--freshness", "--databricks-freshness", dest="freshness", type=Path, required=True)
    parser.add_argument("--clickhouse-dashboard", type=Path)
    parser.add_argument("--clickhouse-drilldown", type=Path)
    parser.add_argument("--clickhouse-ingest-cost", type=Path, required=True)
    parser.add_argument("--clickhouse-reader-mem-gib", type=float, default=64)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--global-manifest", type=Path)
    parser.add_argument("--apply-global-manifest", action="store_true")
    return parser


def run(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    manifest_for_defaults = (
        args.global_manifest.expanduser().resolve()
        if args.global_manifest is not None
        else DEFAULT_GLOBAL_MANIFEST
    )
    default_dashboard, default_drilldown = _default_clickhouse_paths(
        manifest_for_defaults
    )
    publication = publish(
        validation_report_path=args.validation_report,
        cost_summary_path=args.cost_summary,
        dashboard_path=args.dashboard,
        drilldown_path=args.drilldown,
        freshness_path=args.freshness,
        clickhouse_dashboard_path=args.clickhouse_dashboard or default_dashboard,
        clickhouse_drilldown_path=args.clickhouse_drilldown or default_drilldown,
        clickhouse_ingest_cost_path=args.clickhouse_ingest_cost,
        output_dir=args.output_dir,
        clickhouse_reader_mem_gib=args.clickhouse_reader_mem_gib,
        global_manifest_path=args.global_manifest,
        apply_global_manifest=args.apply_global_manifest,
    )
    print(
        "Published accepted Databricks result: "
        f"{len(publication['artifacts'])} hashed artifacts"
    )
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    try:
        return run(argv)
    except KeyboardInterrupt:
        return 130
    except Exception as exc:
        print(f"FATAL: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
