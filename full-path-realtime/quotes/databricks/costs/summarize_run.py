#!/usr/bin/env python3
"""Build a deterministic, offline cost ledger from Databricks evidence exports."""

from __future__ import annotations

import argparse
import json
import math
import os
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

PRIMARY_CATEGORIES = (
    "primary_zerobus_ingest",
    "primary_mv_refresh",
    "primary_predictive_optimization",
    "primary_rt_serving_compute",
)
SECONDARY_CATEGORIES = (
    "secondary_post_ingest_query_serving",
)
EXCLUDED_CATEGORIES = (
    "excluded_storage",
    "excluded_control",
    "excluded_idle_or_minimum",
    "excluded_failed",
    "excluded_setup_or_post_ingest",
)
ALL_CATEGORIES = (
    PRIMARY_CATEGORIES
    + SECONDARY_CATEGORIES
    + EXCLUDED_CATEGORIES
    + ("unknown",)
)
REQUIRED_FILES = (
    "billing_usage.jsonl",
    "billing_list_prices.jsonl",
    "query_history.jsonl",
    "mv_event_log.jsonl",
    "predictive_optimization_operations.jsonl",
)
SUCCESS_STATUSES = {"FINISHED", "SUCCEEDED", "SUCCESS", "COMPLETED"}
FAILURE_WORDS = {"FAILED", "FAILURE", "ERROR", "CANCELED", "CANCELLED"}
ZERO = Decimal("0")


def parse_utc_timestamp(value: Any) -> datetime:
    """Parse an exact UTC timestamp and reject local or non-UTC values."""
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, (int, float)) and not isinstance(value, bool):
        number = float(value)
        if not math.isfinite(number):
            raise ValueError("timestamp must be finite")
        if abs(number) >= 100_000_000_000:
            number /= 1000.0
        parsed = datetime.fromtimestamp(number, tz=timezone.utc)
    elif isinstance(value, str):
        text = value.strip()
        if not text:
            raise ValueError("timestamp must be non-empty")
        if text.endswith(("Z", "z")):
            text = text[:-1] + "+00:00"
        parsed = datetime.fromisoformat(text)
    else:
        raise ValueError("timestamp must be an ISO-8601 string or datetime")
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("timestamp must include the UTC timezone")
    if parsed.utcoffset() != timedelta(0):
        raise ValueError("timestamp must be UTC")
    return parsed.astimezone(timezone.utc)


def iso_utc(value: datetime) -> str:
    return (
        value.astimezone(timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


def decimal_value(value: Any) -> Decimal | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
    return result if result.is_finite() else None


def decimal_text(value: Decimal) -> str:
    text = format(value, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text if text not in ("", "-0") else "0"


def decode_json(value: Any) -> Any:
    decoded = value
    for _ in range(3):
        if not isinstance(decoded, str):
            return decoded
        text = decoded.strip()
        if not text or text[0] not in '[{"':
            return decoded
        try:
            decoded = json.loads(text)
        except (TypeError, ValueError, json.JSONDecodeError):
            return decoded
    return decoded


def _normalized_key(value: Any) -> str:
    return "".join(character for character in str(value).lower() if character.isalnum())


def _row_value(row: Mapping[str, Any], name: str) -> Any:
    wanted = _normalized_key(name)
    for key, value in row.items():
        if _normalized_key(key) == wanted:
            return value
    return None


def _safe_timestamp(value: Any) -> datetime | None:
    try:
        return parse_utc_timestamp(value)
    except (TypeError, ValueError, OverflowError, OSError):
        return None


def read_jsonl(path: Path) -> tuple[list[dict[str, Any]], list[str]]:
    rows: list[dict[str, Any]] = []
    errors: list[str] = []
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError as exc:
        return [], [f"{path.name}: {type(exc).__name__}: {exc}"]
    for line_number, line in enumerate(lines, start=1):
        if not line.strip():
            continue
        try:
            value = json.loads(line)
            if not isinstance(value, Mapping):
                raise ValueError("top-level value is not an object")
            rows.append(dict(value))
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            errors.append(f"{path.name}:{line_number}: {type(exc).__name__}: {exc}")
    return rows, errors


def atomic_write_json(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(value, handle, indent=2, sort_keys=True, ensure_ascii=False)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass


def _price_scalar(value: Any, source: str) -> list[tuple[Decimal, str]]:
    decoded = decode_json(value)
    direct = decimal_value(decoded)
    if direct is not None and direct >= 0:
        return [(direct, source)]
    if not isinstance(decoded, Mapping):
        return []
    lowered = {_normalized_key(key): (key, item) for key, item in decoded.items()}
    for key in ("default", "listprice", "unitprice", "price", "value"):
        if key in lowered:
            original, item = lowered[key]
            result = decimal_value(item)
            if result is not None and result >= 0:
                return [(result, f"{source}.{original}")]
    scalar_values = [
        (decimal_value(item), f"{source}.{key}")
        for key, item in decoded.items()
        if _normalized_key(key)
        not in {"promotional", "promotion", "contract", "negotiated", "discount"}
    ]
    return [
        (value, item_source)
        for value, item_source in scalar_values
        if value is not None and value >= 0
    ]


def extract_canonical_list_price(pricing_json: Any) -> dict[str, Any]:
    """Extract only effective-list/default pricing, never promotional/contract pricing."""
    decoded = decode_json(pricing_json)
    if not isinstance(decoded, Mapping):
        return {
            "price": None,
            "source": None,
            "error": "pricing_json is not a JSON object",
        }
    by_key = {_normalized_key(key): (key, value) for key, value in decoded.items()}
    selected: list[tuple[Decimal, str]] = []
    selected_field = None
    for normalized in ("effectivelist", "default"):
        if normalized not in by_key:
            continue
        original, value = by_key[normalized]
        selected = _price_scalar(value, f"pricing.{original}")
        selected_field = original
        if selected:
            break
    if not selected:
        return {
            "price": None,
            "source": None,
            "error": (
                "pricing_json has no canonical effective-list/default scalar; "
                "promotional and contract prices are intentionally ignored"
            ),
        }
    distinct = sorted({price for price, _source in selected})
    if len(distinct) != 1:
        return {
            "price": None,
            "source": f"pricing.{selected_field}",
            "error": "canonical pricing field contains ambiguous scalar values",
        }
    sources = sorted(source for price, source in selected if price == distinct[0])
    return {"price": distinct[0], "source": sources[0], "error": None}


def match_list_price(
    usage: Mapping[str, Any],
    prices: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    """Match a usage interval to exactly one key- and interval-compatible price row."""
    usage_start = _safe_timestamp(_row_value(usage, "usage_start_time"))
    usage_end = _safe_timestamp(_row_value(usage, "usage_end_time"))
    if usage_start is None or usage_end is None or usage_end <= usage_start:
        return {
            "status": "unpriced",
            "price_row_index": None,
            "price": None,
            "currency": None,
            "source": None,
            "reason": "usage row has an invalid or empty billing interval",
        }
    keys = (
        str(_row_value(usage, "sku_name") or "").casefold(),
        str(_row_value(usage, "cloud") or "").casefold(),
        str(_row_value(usage, "usage_unit") or "").casefold(),
    )
    if any(not key for key in keys):
        return {
            "status": "unpriced",
            "price_row_index": None,
            "price": None,
            "currency": None,
            "source": None,
            "reason": "usage row omits sku_name, cloud, or usage_unit",
        }
    candidates: list[tuple[int, Mapping[str, Any]]] = []
    for index, price_row in enumerate(prices):
        price_keys = (
            str(_row_value(price_row, "sku_name") or "").casefold(),
            str(_row_value(price_row, "cloud") or "").casefold(),
            str(_row_value(price_row, "usage_unit") or "").casefold(),
        )
        if price_keys != keys:
            continue
        price_start = _safe_timestamp(_row_value(price_row, "price_start_time"))
        price_end_value = _row_value(price_row, "price_end_time")
        price_end = (
            None if price_end_value in (None, "") else _safe_timestamp(price_end_value)
        )
        if price_start is None or (
            price_end_value not in (None, "") and price_end is None
        ):
            continue
        if price_start <= usage_start and (price_end is None or usage_end <= price_end):
            candidates.append((index, price_row))
    if len(candidates) != 1:
        status = "ambiguous" if len(candidates) > 1 else "unpriced"
        return {
            "status": status,
            "price_row_index": None,
            "price": None,
            "currency": None,
            "source": None,
            "reason": (
                f"{len(candidates)} effective price rows matched sku_name, cloud, "
                "usage_unit, and the complete usage interval"
            ),
            "candidate_price_row_indices": [index for index, _row in candidates],
        }
    index, price_row = candidates[0]
    extracted = extract_canonical_list_price(_row_value(price_row, "pricing_json"))
    currency = str(_row_value(price_row, "currency_code") or "").strip()
    if extracted["price"] is None:
        return {
            "status": "unpriced",
            "price_row_index": index,
            "price": None,
            "currency": currency or None,
            "source": extracted["source"],
            "reason": extracted["error"],
        }
    if not currency:
        return {
            "status": "unpriced",
            "price_row_index": index,
            "price": None,
            "currency": None,
            "source": extracted["source"],
            "reason": "matched list-price row omits currency_code",
        }
    return {
        "status": "matched",
        "price_row_index": index,
        "price": extracted["price"],
        "currency": currency,
        "source": extracted["source"],
        "reason": "exact effective-interval list-price match",
        "price_start_time": _row_value(price_row, "price_start_time"),
        "price_end_time": _row_value(price_row, "price_end_time"),
    }


def merge_intervals(
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


def _duration_microseconds(start: datetime, end: datetime) -> int:
    return max(0, round((end - start).total_seconds() * 1_000_000))


def union_overlap_microseconds(
    start: datetime,
    end: datetime,
    intervals: Iterable[tuple[datetime, datetime]],
) -> int:
    clipped = (
        (max(start, item_start), min(end, item_end))
        for item_start, item_end in intervals
    )
    return sum(
        _duration_microseconds(item_start, item_end)
        for item_start, item_end in merge_intervals(clipped)
    )


def union_overlap_seconds(
    start: Any,
    end: Any,
    intervals: Iterable[tuple[Any, Any]],
) -> float:
    parsed_start = parse_utc_timestamp(start)
    parsed_end = parse_utc_timestamp(end)
    parsed_intervals = [
        (parse_utc_timestamp(item_start), parse_utc_timestamp(item_end))
        for item_start, item_end in intervals
    ]
    return union_overlap_microseconds(parsed_start, parsed_end, parsed_intervals) / 1e6


def _recursive_identifiers(value: Any, key_names: set[str]) -> set[str]:
    decoded = decode_json(value)
    found: set[str] = set()
    if isinstance(decoded, Mapping):
        for key, item in decoded.items():
            if _normalized_key(key) in key_names and item not in (None, ""):
                if isinstance(item, (str, int)) and not isinstance(item, bool):
                    found.add(str(item))
            found.update(_recursive_identifiers(item, key_names))
    elif isinstance(decoded, list):
        for item in decoded:
            found.update(_recursive_identifiers(item, key_names))
    return found


def _run_tag_values(value: Any) -> set[str]:
    decoded = decode_json(value)
    found: set[str] = set()
    if isinstance(decoded, Mapping):
        for key, item in decoded.items():
            if _normalized_key(key) == "runid" and item not in (None, ""):
                found.add(str(item))
        if (
            _normalized_key(decoded.get("key", "")) == "runid"
            and decoded.get("value") not in (None, "")
        ):
            found.add(str(decoded["value"]))
        for item in decoded.values():
            found.update(_run_tag_values(item))
    elif isinstance(decoded, list):
        for item in decoded:
            found.update(_run_tag_values(item))
    return found


def derive_mv_pipeline_ids(event_rows: Sequence[Mapping[str, Any]]) -> list[str]:
    names = {"pipelineid", "pipelineuuid", "dltpipelineid", "flowid"}
    found: set[str] = set()
    for row in event_rows:
        for field in ("origin_json", "origin", "details_json", "details"):
            found.update(_recursive_identifiers(_row_value(row, field), names))
    return sorted(found)


def _operation_ids(rows: Sequence[Mapping[str, Any]]) -> set[str]:
    result: set[str] = set()
    for row in rows:
        for field in ("operation_id", "id"):
            value = _row_value(row, field)
            if value not in (None, ""):
                result.add(str(value))
    return result


def _has_failure_marker(value: Any) -> bool:
    decoded = decode_json(value)
    if isinstance(decoded, Mapping):
        for key, item in decoded.items():
            normalized = _normalized_key(key)
            if normalized in {"status", "state", "result", "outcome"}:
                if str(item).strip().upper() in FAILURE_WORDS:
                    return True
            if normalized in {"failed", "haserror"} and item is True:
                return True
            if _has_failure_marker(item):
                return True
    elif isinstance(decoded, list):
        return any(_has_failure_marker(item) for item in decoded)
    return False


def classify_usage(
    usage: Mapping[str, Any],
    *,
    rt_warehouse_id: str,
    control_warehouse_id: str,
    mv_pipeline_ids: Iterable[str] = (),
    predictive_operation_ids: Iterable[str] = (),
) -> tuple[str, str]:
    """Classify one billing row before temporal serving/post-ingest allocation."""
    metadata = decode_json(_row_value(usage, "usage_metadata_json"))
    features = decode_json(_row_value(usage, "product_features_json"))
    warehouse_ids = _recursive_identifiers(metadata, {"warehouseid"})
    pipeline_ids = _recursive_identifiers(
        metadata, {"pipelineid", "pipelineuuid", "dltpipelineid", "flowid"}
    )
    operation_ids = _recursive_identifiers(metadata, {"operationid"})
    zerobus_request_types = _recursive_identifiers(
        features, {"zerobusrequesttype"}
    )
    product = str(_row_value(usage, "billing_origin_product") or "").upper()
    sku = str(_row_value(usage, "sku_name") or "").upper()
    identity = f"{product} {sku}"
    if _has_failure_marker(metadata) or _has_failure_marker(features):
        return "excluded_failed", "usage metadata/product features identify failed work"
    if "STORAGE" in identity:
        return "excluded_storage", "billing origin/SKU identifies storage"
    if control_warehouse_id in warehouse_ids:
        return "excluded_control", "usage_metadata warehouse_id matches control warehouse"
    if rt_warehouse_id in warehouse_ids:
        return (
            "primary_rt_serving_compute",
            "usage_metadata warehouse_id matches Lakehouse//RT warehouse",
        )
    if (
        "PREDICTIVE" in identity
        or "OPTIMIZATION" in identity
        or operation_ids.intersection(str(value) for value in predictive_operation_ids)
    ):
        return (
            "primary_predictive_optimization",
            "billing origin/SKU or operation_id identifies predictive optimization",
        )
    if "ZEROBUS" in identity or zerobus_request_types:
        return (
            "primary_zerobus_ingest",
            "billing identity or product features identify Zerobus ingest",
        )
    target_pipelines = {str(value) for value in mv_pipeline_ids}
    if pipeline_ids.intersection(target_pipelines):
        return (
            "primary_mv_refresh",
            "usage_metadata pipeline ID matches target MV event-log pipeline ID",
        )
    if any(token in identity for token in ("PIPELINE", "LAKEFLOW", "DLT")):
        if not target_pipelines:
            return (
                "unknown",
                "pipeline usage found but target MV event log exposed no pipeline ID",
            )
        return "unknown", "pipeline usage does not match a target MV pipeline ID"
    return "unknown", "no target warehouse, pipeline, operation, or product identity matched"


def _successful_query_intervals(
    rows: Sequence[Mapping[str, Any]],
    start: datetime,
    end: datetime,
    rt_warehouse_id: str,
) -> tuple[list[tuple[datetime, datetime]], list[str]]:
    intervals: list[tuple[datetime, datetime]] = []
    errors: list[str] = []
    for index, row in enumerate(rows):
        tags = decode_json(_row_value(row, "query_tags_json"))
        run_tags = _run_tag_values(tags)
        if not run_tags:
            continue
        warehouse_id = _row_value(row, "warehouse_id")
        if warehouse_id not in (None, "") and str(warehouse_id) != rt_warehouse_id:
            continue
        status = str(
            _row_value(row, "execution_status")
            or _row_value(row, "status")
            or ""
        ).upper()
        if status not in SUCCESS_STATUSES or _row_value(row, "error_message") not in (
            None,
            "",
        ):
            continue
        query_start = _safe_timestamp(_row_value(row, "start_time"))
        query_end = _safe_timestamp(_row_value(row, "end_time"))
        if query_start is None or query_end is None or query_end <= query_start:
            errors.append(f"query_history row {index} has an invalid successful interval")
            continue
        clipped_start, clipped_end = max(start, query_start), min(end, query_end)
        if clipped_end > clipped_start:
            intervals.append((clipped_start, clipped_end))
    return merge_intervals(intervals), errors


def _total_shape(amounts: Mapping[str, Decimal]) -> dict[str, Any]:
    by_currency = {
        currency: decimal_text(value)
        for currency, value in sorted(amounts.items())
    }
    if len(by_currency) == 1:
        currency, amount = next(iter(by_currency.items()))
    else:
        currency, amount = None, None
    return {
        "currency": currency,
        "amount": amount,
        "amount_by_currency": by_currency,
    }


def _add_total(
    totals: dict[str, dict[str, Decimal]],
    category: str,
    currency: str,
    value: Decimal,
) -> None:
    totals.setdefault(category, {})
    totals[category][currency] = totals[category].get(currency, ZERO) + value


def _line(
    *,
    usage_index: int,
    usage: Mapping[str, Any],
    price_match: Mapping[str, Any],
    category: str,
    reason: str,
    source_quantity: Decimal,
    quantity: Decimal,
    allocation_numerator_us: int,
    allocation_denominator_us: int,
    measured_start: datetime,
    measured_end: datetime,
) -> dict[str, Any]:
    price = price_match.get("price")
    list_cost = quantity * price if isinstance(price, Decimal) else None
    record_type = str(_row_value(usage, "record_type") or "")
    return {
        "usage_row_index": usage_index,
        "usage_record_id": _row_value(usage, "record_id"),
        "price_row_index": price_match.get("price_row_index"),
        "price_match_status": price_match.get("status"),
        "price_match_reason": price_match.get("reason"),
        "price_candidate_row_indices": price_match.get(
            "candidate_price_row_indices", []
        ),
        "sku_name": _row_value(usage, "sku_name"),
        "cloud": _row_value(usage, "cloud"),
        "usage_unit": _row_value(usage, "usage_unit"),
        "record_type": record_type,
        "signed_quantity_source": decimal_text(source_quantity),
        "quantity": decimal_text(quantity),
        "category": category,
        "classification_reason": reason,
        "measured_interval": {
            "start": iso_utc(measured_start),
            "end": iso_utc(measured_end),
        },
        "source_usage_interval": {
            "start": _row_value(usage, "usage_start_time"),
            "end": _row_value(usage, "usage_end_time"),
        },
        "allocation": {
            "numerator_microseconds": allocation_numerator_us,
            "denominator_microseconds": allocation_denominator_us,
            "fraction": decimal_text(
                Decimal(allocation_numerator_us)
                / Decimal(allocation_denominator_us)
            ),
        },
        "canonical_list_unit_price": (
            decimal_text(price) if isinstance(price, Decimal) else None
        ),
        "canonical_price_source": price_match.get("source"),
        "price_effective_interval": {
            "start": price_match.get("price_start_time"),
            "end": price_match.get("price_end_time"),
        },
        "currency": price_match.get("currency"),
        "list_cost": decimal_text(list_cost) if list_cost is not None else None,
        "signed_quantity_policy": (
            "The exported signed usage_quantity is retained for ORIGINAL, "
            "RETRACTION, and RESTATEMENT records; record_type never changes its sign."
        ),
    }


def build_cost_summary(
    *,
    billing_usage: Sequence[Mapping[str, Any]],
    billing_list_prices: Sequence[Mapping[str, Any]],
    query_history: Sequence[Mapping[str, Any]],
    mv_event_log: Sequence[Mapping[str, Any]],
    predictive_operations: Sequence[Mapping[str, Any]],
    measured_since: Any,
    measured_until: Any,
    rt_warehouse_id: str,
    control_warehouse_id: str,
    producer_finished_at: Any | None = None,
    input_errors: Sequence[str] = (),
    input_paths: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    start = parse_utc_timestamp(measured_since)
    end = parse_utc_timestamp(measured_until)
    if end <= start:
        raise ValueError("measured_until must be later than measured_since")
    finish = (
        end
        if producer_finished_at in (None, "")
        else parse_utc_timestamp(producer_finished_at)
    )
    active_end = min(end, max(start, finish))
    query_intervals, query_errors = _successful_query_intervals(
        query_history, start, active_end, rt_warehouse_id
    )
    post_query_intervals, post_query_errors = _successful_query_intervals(
        query_history, active_end, end, rt_warehouse_id
    )
    mv_pipeline_ids = derive_mv_pipeline_ids(mv_event_log)
    predictive_ids = _operation_ids(predictive_operations)
    predictive_failures = [
        index
        for index, row in enumerate(predictive_operations)
        if str(_row_value(row, "operation_status") or "").upper() in FAILURE_WORDS
        or _has_failure_marker(row)
    ]
    totals: dict[str, dict[str, Decimal]] = {}
    lines: list[dict[str, Any]] = []
    processing_errors = (
        list(input_errors) + query_errors + post_query_errors
    )
    if not billing_usage:
        processing_errors.append("billing_usage.jsonl contains no usage rows")
    if billing_usage and not billing_list_prices:
        processing_errors.append(
            "billing_list_prices.jsonl contains no rows for observed usage"
        )
    ambiguous_rows: list[int] = []
    unpriced_rows: list[int] = []
    unknown_rows: list[int] = []
    out_of_window_rows: list[int] = []
    invalid_quantity_rows: list[int] = []
    record_type_quantities: dict[str, Decimal] = {}

    for usage_index, usage in enumerate(billing_usage):
        source_quantity = decimal_value(_row_value(usage, "usage_quantity"))
        if source_quantity is None:
            invalid_quantity_rows.append(usage_index)
            processing_errors.append(
                f"billing_usage row {usage_index} has invalid usage_quantity"
            )
            continue
        record_type = str(_row_value(usage, "record_type") or "UNKNOWN").upper()
        record_type_quantities[record_type] = (
            record_type_quantities.get(record_type, ZERO) + source_quantity
        )
        usage_start = _safe_timestamp(_row_value(usage, "usage_start_time"))
        usage_end = _safe_timestamp(_row_value(usage, "usage_end_time"))
        if usage_start is None or usage_end is None or usage_end <= usage_start:
            processing_errors.append(
                f"billing_usage row {usage_index} has invalid usage interval"
            )
            unpriced_rows.append(usage_index)
            continue
        measured_start, measured_end = max(start, usage_start), min(end, usage_end)
        if measured_end <= measured_start:
            out_of_window_rows.append(usage_index)
            continue
        duration_us = _duration_microseconds(usage_start, usage_end)
        if duration_us <= 0:
            processing_errors.append(
                f"billing_usage row {usage_index} has zero duration"
            )
            unpriced_rows.append(usage_index)
            continue
        price_match = match_list_price(usage, billing_list_prices)
        if price_match["status"] == "ambiguous":
            ambiguous_rows.append(usage_index)
        elif price_match["status"] != "matched":
            unpriced_rows.append(usage_index)
        base_category, base_reason = classify_usage(
            usage,
            rt_warehouse_id=rt_warehouse_id,
            control_warehouse_id=control_warehouse_id,
            mv_pipeline_ids=mv_pipeline_ids,
            predictive_operation_ids=predictive_ids,
        )
        allocations: list[tuple[str, str, int, datetime, datetime]] = []
        active_start = measured_start
        active_segment_end = min(measured_end, active_end)
        post_start = max(measured_start, active_end)
        if active_segment_end > active_start:
            active_us = _duration_microseconds(active_start, active_segment_end)
            if base_category == "primary_rt_serving_compute":
                serving_us = union_overlap_microseconds(
                    active_start, active_segment_end, query_intervals
                )
                if serving_us:
                    allocations.append(
                        (
                            "primary_rt_serving_compute",
                            base_reason
                            + "; allocated by union overlap with successful run-tagged "
                            "query intervals",
                            serving_us,
                            active_start,
                            active_segment_end,
                        )
                    )
                if active_us - serving_us:
                    allocations.append(
                        (
                            "excluded_idle_or_minimum",
                            "RT warehouse billed interval not overlapped by the union of "
                            "successful run-tagged query intervals",
                            active_us - serving_us,
                            active_start,
                            active_segment_end,
                        )
                    )
            else:
                allocations.append(
                    (
                        base_category,
                        base_reason,
                        active_us,
                        active_start,
                        active_segment_end,
                    )
                )
        if measured_end > post_start:
            post_us = _duration_microseconds(post_start, measured_end)
            if base_category == "primary_rt_serving_compute":
                serving_us = union_overlap_microseconds(
                    post_start, measured_end, post_query_intervals
                )
                if serving_us:
                    allocations.append(
                        (
                            "secondary_post_ingest_query_serving",
                            base_reason
                            + "; post-ingest allocation by union overlap with "
                            "successful run-tagged query intervals",
                            serving_us,
                            post_start,
                            measured_end,
                        )
                    )
                if post_us - serving_us:
                    allocations.append(
                        (
                            "excluded_idle_or_minimum",
                            "post-ingest query warehouse billing not overlapped "
                            "by successful run-tagged queries",
                            post_us - serving_us,
                            post_start,
                            measured_end,
                        )
                    )
            else:
                allocations.append(
                    (
                        "excluded_setup_or_post_ingest",
                        "billing interval portion starts after producer finish",
                        post_us,
                        post_start,
                        measured_end,
                    )
                )
        for category, reason, allocated_us, allocation_start, allocation_end in allocations:
            quantity = source_quantity * Decimal(allocated_us) / Decimal(duration_us)
            item = _line(
                usage_index=usage_index,
                usage=usage,
                price_match=price_match,
                category=category,
                reason=reason,
                source_quantity=source_quantity,
                quantity=quantity,
                allocation_numerator_us=allocated_us,
                allocation_denominator_us=duration_us,
                measured_start=allocation_start,
                measured_end=allocation_end,
            )
            lines.append(item)
            if category == "unknown":
                unknown_rows.append(usage_index)
            if item["list_cost"] is not None:
                _add_total(
                    totals,
                    category,
                    str(item["currency"] or ""),
                    Decimal(item["list_cost"]),
                )

    category_totals = {
        category: _total_shape(totals.get(category, {}))
        for category in ALL_CATEGORIES
    }
    primary_amounts: dict[str, Decimal] = {}
    fresh_path_amounts: dict[str, Decimal] = {}
    excluded_amounts: dict[str, Decimal] = {}
    for category in PRIMARY_CATEGORIES:
        for currency, amount in totals.get(category, {}).items():
            primary_amounts[currency] = primary_amounts.get(currency, ZERO) + amount
            if category != "primary_rt_serving_compute":
                fresh_path_amounts[currency] = (
                    fresh_path_amounts.get(currency, ZERO) + amount
                )
    for category in EXCLUDED_CATEGORIES:
        for currency, amount in totals.get(category, {}).items():
            excluded_amounts[currency] = excluded_amounts.get(currency, ZERO) + amount
    canonical = _total_shape(primary_amounts)
    canonical_fresh_path = _total_shape(fresh_path_amounts)
    excluded = _total_shape(excluded_amounts)
    secondary = {
        category: category_totals[category]
        for category in SECONDARY_CATEGORIES
    }
    unknown = _total_shape(totals.get("unknown", {}))
    rt_amounts = totals.get("primary_rt_serving_compute", {})
    discounted_rt = {
        currency: amount * Decimal("0.70")
        for currency, amount in rt_amounts.items()
    }
    scenario_amounts = dict(primary_amounts)
    for currency, amount in rt_amounts.items():
        scenario_amounts[currency] = (
            scenario_amounts.get(currency, ZERO) - amount + discounted_rt[currency]
        )
    pricing_complete = not ambiguous_rows and not unpriced_rows
    classification_complete = not unknown_rows
    sources_complete = not input_errors
    currencies_complete = len(primary_amounts) <= 1
    canonical_total_present = len(primary_amounts) == 1
    complete = (
        sources_complete
        and pricing_complete
        and classification_complete
        and currencies_complete
        and canonical_total_present
        and not processing_errors
        and not invalid_quantity_rows
    )
    unpriced_quantities: dict[str, Decimal] = {}
    for index in sorted(set(ambiguous_rows + unpriced_rows)):
        if 0 <= index < len(billing_usage):
            row = billing_usage[index]
            quantity = decimal_value(_row_value(row, "usage_quantity"))
            if quantity is not None:
                unit = str(_row_value(row, "usage_unit") or "UNKNOWN")
                unpriced_quantities[unit] = unpriced_quantities.get(unit, ZERO) + quantity
    return {
        "schema_version": 1,
        "measurement_window": {
            "since": iso_utc(start),
            "until": iso_utc(end),
            "duration_seconds": (end - start).total_seconds(),
            "producer_finished_at": (
                None if producer_finished_at in (None, "") else iso_utc(finish)
            ),
        },
        "inputs": dict(input_paths or {}),
        "warehouse_ids": {
            "rt": rt_warehouse_id,
            "control": control_warehouse_id,
        },
        "matched_lines": lines,
        "record_type_signed_quantities": {
            key: decimal_text(value)
            for key, value in sorted(record_type_quantities.items())
        },
        "primary_category_totals": {
            category: category_totals[category] for category in PRIMARY_CATEGORIES
        },
        "excluded_category_totals": {
            category: category_totals[category] for category in EXCLUDED_CATEGORIES
        },
        "secondary_category_totals": secondary,
        "unknown_total": unknown,
        "excluded_total": excluded,
        "unpriced_total": {
            "list_cost": None,
            "usage_quantity_by_unit": {
                unit: decimal_text(value)
                for unit, value in sorted(unpriced_quantities.items())
            },
            "usage_row_indices": sorted(set(ambiguous_rows + unpriced_rows)),
        },
        "canonical_undiscounted_primary_total": canonical,
        "canonical_undiscounted_primary_total_amount": canonical["amount"],
        "canonical_undiscounted_primary_total_currency": canonical["currency"],
        "canonical_undiscounted_fresh_path_total": canonical_fresh_path,
        "canonical_undiscounted_query_serving_total": _total_shape(rt_amounts),
        "lakehouse_rt_beta_30_percent_off_scenario": {
            "discount_rate": "0.30",
            "applies_only_to": "primary_rt_serving_compute",
            "rt_serving_undiscounted": _total_shape(rt_amounts),
            "rt_serving_discounted": _total_shape(discounted_rt),
            "scenario_primary_total": _total_shape(scenario_amounts),
            "canonical_undiscounted_primary_total": canonical,
            "canonical_total_unchanged": True,
        },
        "query_serving_allocation": {
            "method": "union_overlap",
            "successful_query_union_intervals": [
                {"start": iso_utc(item_start), "end": iso_utc(item_end)}
                for item_start, item_end in query_intervals
            ],
            "successful_post_ingest_query_union_intervals": [
                {"start": iso_utc(item_start), "end": iso_utc(item_end)}
                for item_start, item_end in post_query_intervals
            ],
            "estimate": True,
            "disclosure": (
                "Estimated from the union overlap of successful run-tagged query "
                "intervals and RT warehouse billing intervals. Databricks billing "
                "usage has no statement_id join, so this is not exact statement-level "
                "allocation. Unioning intervals prevents concurrent queries from "
                "double-counting shared compute."
            ),
        },
        "mv_pipeline_ids": mv_pipeline_ids,
        "predictive_optimization": {
            "operation_count": len(predictive_operations),
            "failed_operation_count": len(predictive_failures),
            "failed_operation_row_indices": predictive_failures,
        },
        "completeness": {
            "sources_complete": sources_complete,
            "pricing_complete": pricing_complete,
            "classification_complete": classification_complete,
            "single_primary_currency": currencies_complete,
            "canonical_primary_total_present": canonical_total_present,
            "ambiguous_price_usage_row_indices": sorted(set(ambiguous_rows)),
            "unpriced_usage_row_indices": sorted(set(unpriced_rows)),
            "unknown_usage_row_indices": sorted(set(unknown_rows)),
            "invalid_quantity_usage_row_indices": sorted(set(invalid_quantity_rows)),
            "complete": complete,
        },
        "complete": complete,
        "errors": processing_errors,
        "assumptions": [
            "Canonical primary cost uses only undiscounted effective-list/default "
            "prices exported by system.billing.list_prices.",
            "The fresh-data-path subledger includes Zerobus ingestion, MV refresh, "
            "and predictive optimization; RT serving compute is reported separately "
            "as measured-query cost so downstream scores do not double-count it.",
            "Promotional, contract, negotiated, commitment, credit, tax, and Beta "
            "prices are excluded from the canonical total.",
            "Usage quantities are prorated only when a billing interval crosses the "
            "declared measurement window, producer finish, or RT query-union boundary.",
            "ORIGINAL, RETRACTION, and RESTATEMENT quantities retain the sign exported "
            "by Databricks; no sign is inferred from record_type.",
            "The Lakehouse//RT Beta scenario is separate and discounts only RT serving "
            "list cost by 30%; it never changes the canonical undiscounted total.",
        ],
    }


def summarize(
    *,
    evidence_dir: Path,
    measured_since: Any,
    measured_until: Any,
    rt_warehouse_id: str,
    control_warehouse_id: str,
    producer_finished_at: Any | None = None,
) -> dict[str, Any]:
    rows: dict[str, list[dict[str, Any]]] = {}
    errors: list[str] = []
    paths: dict[str, str] = {}
    for filename in REQUIRED_FILES:
        path = evidence_dir / filename
        paths[filename.removesuffix(".jsonl")] = str(path)
        rows[filename], read_errors = read_jsonl(path)
        errors.extend(read_errors)
    return build_cost_summary(
        billing_usage=rows["billing_usage.jsonl"],
        billing_list_prices=rows["billing_list_prices.jsonl"],
        query_history=rows["query_history.jsonl"],
        mv_event_log=rows["mv_event_log.jsonl"],
        predictive_operations=rows["predictive_optimization_operations.jsonl"],
        measured_since=measured_since,
        measured_until=measured_until,
        rt_warehouse_id=rt_warehouse_id,
        control_warehouse_id=control_warehouse_id,
        producer_finished_at=producer_finished_at,
        input_errors=errors,
        input_paths=paths,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build an offline Databricks benchmark cost evidence ledger"
    )
    parser.add_argument("--evidence-dir", type=Path, required=True)
    parser.add_argument("--since", "--measured-since", dest="since", required=True)
    parser.add_argument("--until", "--measured-until", dest="until", required=True)
    parser.add_argument("--rt-warehouse-id", required=True)
    parser.add_argument("--control-warehouse-id", required=True)
    parser.add_argument(
        "--producer-finished-at",
        "--producer-finish-timestamp",
        dest="producer_finished_at",
    )
    parser.add_argument("--output", type=Path, required=True)
    return parser


def run(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    output = args.output.expanduser().resolve()
    if output.exists() and output.is_dir():
        output = output / "cost_summary.json"
    summary = summarize(
        evidence_dir=args.evidence_dir.expanduser().resolve(),
        measured_since=args.since,
        measured_until=args.until,
        rt_warehouse_id=args.rt_warehouse_id,
        control_warehouse_id=args.control_warehouse_id,
        producer_finished_at=args.producer_finished_at,
    )
    atomic_write_json(output, summary)
    print(f"Cost summary written to {output}")
    return 0 if summary["complete"] else 1


def main(argv: Sequence[str] | None = None) -> int:
    try:
        return run(argv)
    except KeyboardInterrupt:
        return 130
    except Exception as exc:
        print(f"FATAL: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
