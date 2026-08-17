"""Shared presentation, validation, and evidence-strip helpers."""

from __future__ import annotations

import json
import math
import os
import struct
from pathlib import Path
from typing import Any

import matplotlib
import matplotlib.font_manager as font_manager
from matplotlib import patches
from matplotlib.textpath import TextPath

BACKGROUND_COLOR = os.environ.get("CHART_BACKGROUND_COLOR", "#2B2B2B")
GRID_COLOR = "#4A4A4A"
MUTED_COLOR = "#A0A0A0"
WHITE_COLOR = "#F4F4F4"
CLICKHOUSE_COLOR = "#FDFF62"
BIGQUERY_COLOR = "#4285F4"

KEYNOTE_WIDTH_PX = 5156
KEYNOTE_HEIGHT_PX = 2900
KEYNOTE_HEADER_SAFE_AREA_PX = 560
# Leave four extra raster pixels so the stage border's antialiasing never
# enters the exact 560px transparent header-safe area.
KEYNOTE_STAGE_TOP = 1 - (KEYNOTE_HEADER_SAFE_AREA_PX + 4) / KEYNOTE_HEIGHT_PX
KEYNOTE_STAGE_LEFT = 0.0233
KEYNOTE_STAGE_RIGHT = 0.9767
KEYNOTE_STAGE_BOTTOM = 0.018
KEYNOTE_STAGE_COLOR = os.environ.get("CHART_STAGE_COLOR", "#1C1C1A")
KEYNOTE_STAGE_BORDER_COLOR = "#343431"


def resolve_layout(
    basename: str,
    wide: bool,
    default_size: tuple[float, float],
    wide_size: tuple[float, float] = (16.0, 8.5),
    dpi: int = 300,
) -> tuple[str, tuple[float, float], dict[str, Any]]:
    """Resolve presentation geometry while keeping all data and math identical."""
    del wide_size
    size = (
        (KEYNOTE_WIDTH_PX / dpi, KEYNOTE_HEIGHT_PX / dpi)
        if wide
        else default_size
    )
    resolved_basename = (
        basename if not wide or basename.endswith("_wide") else f"{basename}_wide"
    )
    metadata = {
        "variant": "wide" if wide else "default",
        "figure_inches": [size[0], size[1]],
        "background_color": BACKGROUND_COLOR,
        "data_and_math_unchanged": True,
    }
    if wide:
        metadata.update(
            {
                "pixel_dimensions": [KEYNOTE_WIDTH_PX, KEYNOTE_HEIGHT_PX],
                "transparent_canvas": True,
                "header_safe_area_pixels": KEYNOTE_HEADER_SAFE_AREA_PX,
                "chart_stage": "subtle",
                "chart_stage_color": KEYNOTE_STAGE_COLOR,
                "chart_stage_border_color": KEYNOTE_STAGE_BORDER_COLOR,
            }
        )
    return resolved_basename, size, metadata


def configure_figure(fig: Any, *, wide: bool) -> str:
    """Apply the shared Keynote-wide canvas and return the axes background."""
    if not wide:
        fig.patch.set_facecolor(BACKGROUND_COLOR)
        fig.patch.set_alpha(1)
        return BACKGROUND_COLOR

    fig.patch.set_facecolor("none")
    fig.patch.set_alpha(0)
    stage = patches.FancyBboxPatch(
        (KEYNOTE_STAGE_LEFT, KEYNOTE_STAGE_BOTTOM),
        KEYNOTE_STAGE_RIGHT - KEYNOTE_STAGE_LEFT,
        KEYNOTE_STAGE_TOP - KEYNOTE_STAGE_BOTTOM,
        transform=fig.transFigure,
        clip_on=False,
        boxstyle="round,pad=0,rounding_size=0.012",
        linewidth=1.0,
        edgecolor=KEYNOTE_STAGE_BORDER_COLOR,
        facecolor=KEYNOTE_STAGE_COLOR,
        zorder=-20,
    )
    fig.add_artist(stage)
    return KEYNOTE_STAGE_COLOR


def png_dimensions(path: Path) -> tuple[int, int]:
    with path.open("rb") as handle:
        if handle.read(8) != b"\x89PNG\r\n\x1a\n":
            raise ValueError(f"not a PNG file: {path}")
        length = struct.unpack(">I", handle.read(4))[0]
        if handle.read(4) != b"IHDR" or length < 8:
            raise ValueError(f"missing PNG IHDR: {path}")
        return struct.unpack(">II", handle.read(8))


def save_figure(
    fig: Any,
    output_dir: Path,
    basename: str,
    dpi: int,
    *,
    wide: bool = False,
) -> tuple[Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    png = output_dir / f"{basename}.png"
    svg = output_dir / f"{basename}.svg"
    options: dict[str, Any] = {
        "facecolor": "none" if wide else fig.get_facecolor(),
        "edgecolor": "none",
        "pad_inches": 0,
    }
    if not wide:
        options["bbox_inches"] = "tight"
    fig.savefig(png, dpi=dpi, **options)
    fig.savefig(svg, **options)
    if wide:
        actual = png_dimensions(png)
        expected = (KEYNOTE_WIDTH_PX, KEYNOTE_HEIGHT_PX)
        if actual != expected:
            raise ValueError(
                f"wide PNG dimensions are {actual}; expected {expected}"
            )
    return png, svg


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def full_path_root() -> Path:
    return Path(__file__).resolve().parents[3]


def portable_payload(value: Any) -> Any:
    """Replace local absolute paths with stable full-path-relative paths."""
    if isinstance(value, dict):
        return {key: portable_payload(item) for key, item in value.items()}
    if isinstance(value, list):
        return [portable_payload(item) for item in value]
    if isinstance(value, tuple):
        return [portable_payload(item) for item in value]
    if isinstance(value, str) and Path(value).is_absolute():
        try:
            return Path(value).resolve().relative_to(full_path_root()).as_posix()
        except ValueError:
            return value
    return value


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(portable_payload(payload), indent=2) + "\n",
        encoding="utf-8",
    )


def _one(items: list[dict[str, Any]], *, context: str) -> dict[str, Any]:
    if len(items) != 1:
        raise ValueError(f"expected exactly one {context}; got {len(items)}")
    return items[0]


def _close(actual: float, expected: float, *, context: str) -> None:
    if not math.isclose(actual, expected, rel_tol=1e-8, abs_tol=1e-8):
        raise ValueError(f"{context}: reported={actual}, recomputed={expected}")


def money(value: float) -> str:
    return f"${value:.3f}" if value < 1 else f"${value:.2f}"


def duration(value: float) -> str:
    if value < 60:
        return f"{value:.1f}s"
    if value < 3600:
        return f"{value / 60:.1f}m"
    return f"{value / 3600:.1f}h"


def validate_clickhouse_query_cost(
    summary: dict[str, Any], *, tier: str
) -> dict[str, Any]:
    cost = _one(
        [item for item in summary["costs"] if item.get("tier") == tier],
        context=f"ClickHouse {tier} cost",
    )
    iterations = int(summary["iterations_included"])
    queries_per_iteration = int(summary["queries_per_iteration"])
    executions = iterations * queries_per_iteration
    runtime = float(summary["total_runtime_seconds"])
    reported_cost = float(cost["total_compute_cost_usd"])
    # Matched ClickHouse summaries intentionally preserve the accepted
    # precomputed tier cost but do not repeat the service-hour rate.  Validate
    # matched counts here; the upstream cost summarizer owns rate validation.
    return {
        "iterations": iterations,
        "queries_per_iteration": queries_per_iteration,
        "query_executions": executions,
        "runtime_sec": runtime,
        "cost_usd": reported_cost,
    }


def validate_bigquery_query_cost(
    summary: dict[str, Any], *, tier: str
) -> dict[str, Any]:
    capacity = _one(
        [
            item
            for item in summary["costs"]
            if item.get("compute_model") == "capacity" and item.get("tier") == tier
        ],
        context=f"BigQuery {tier} capacity cost",
    )
    on_demand = _one(
        [
            item
            for item in summary["costs"]
            if item.get("compute_model") == "on_demand"
        ],
        context="BigQuery on-demand cost",
    )
    iterations = int(summary["iterations_included"])
    executions = int(summary["query_jobs"])
    if executions % iterations:
        raise ValueError("BigQuery query_jobs is not divisible by iterations_included")
    queries_per_iteration = executions // iterations
    runtime = float(summary["total_runtime_seconds"])
    capacity_cost = float(capacity["total_compute_cost_usd"])
    on_demand_cost = float(on_demand["total_compute_cost_usd"])
    _close(
        capacity_cost,
        float(capacity["billed_slot_sec"])
        / float(capacity["price_unit_seconds"])
        * float(capacity["price_usd"]),
        context="BigQuery capacity query cost",
    )
    _close(
        on_demand_cost,
        float(on_demand["billed_bytes"])
        / float(on_demand["price_unit_bytes"])
        * float(on_demand["price_usd"]),
        context="BigQuery on-demand query cost",
    )
    return {
        "iterations": iterations,
        "queries_per_iteration": queries_per_iteration,
        "query_executions": executions,
        "runtime_sec": runtime,
        "capacity_cost_usd": capacity_cost,
        "on_demand_cost_usd": on_demand_cost,
    }


def validate_matched_query_totals(
    clickhouse: dict[str, Any], bigquery: dict[str, Any]
) -> None:
    for field in ("iterations", "queries_per_iteration", "query_executions"):
        if clickhouse[field] != bigquery[field]:
            raise ValueError(
                f"matched query evidence mismatch for {field}: "
                f"ClickHouse={clickhouse[field]}, BigQuery={bigquery[field]}"
            )


def draw_matched_query_totals_strip(
    fig: Any,
    clickhouse: dict[str, Any],
    bigquery: dict[str, Any],
    *,
    wide: bool,
) -> dict[str, float]:
    """Draw a compact, left-centered evidence strip below a pairwise chart."""
    runtime_ratio = bigquery["runtime_sec"] / clickhouse["runtime_sec"]
    capacity_ratio = bigquery["capacity_cost_usd"] / clickhouse["cost_usd"]
    on_demand_ratio = bigquery["on_demand_cost_usd"] / clickhouse["cost_usd"]
    # BigQuery needs slightly more width than the single-price Snowflake strip
    # because both capacity and on-demand alternatives are disclosed. Keep it
    # compact and centered rather than stretching it edge-to-edge.
    left, bottom, width, height = (
        (.100, .018, .80, .115) if wide else (.050, .014, .90, .118)
    )
    box = patches.FancyBboxPatch(
        (left, bottom), width, height,
        boxstyle="round,pad=0.006,rounding_size=0.008",
        transform=fig.transFigure,
        facecolor="#202020",
        edgecolor=GRID_COLOR,
        linewidth=1,
        clip_on=False,
        zorder=2,
    )
    fig.add_artist(box)
    header_y = bottom + height - .026
    first_y = bottom + .053
    second_y = bottom + .020
    header_size = 9.4 if wide else 8.3
    value_size = 11 if wide else 9.6

    clickhouse_runtime = duration(clickhouse["runtime_sec"])
    bigquery_runtime = duration(bigquery["runtime_sec"])
    clickhouse_cost = money(clickhouse["cost_usd"])
    bigquery_cost = (
        f"{money(bigquery['capacity_cost_usd'])} Capacity · "
        f"{money(bigquery['on_demand_cost_usd'])} On-demand"
    )
    bigquery_detail = (
        f"{runtime_ratio:,.0f}× slower · {capacity_ratio:,.0f}× Cap / "
        f"{on_demand_ratio:,.0f}× OD costlier"
    )

    # Use actual rendered text widths and three equal visual gaps. This keeps
    # the alternative BigQuery prices fully disclosed without the uneven
    # whitespace produced by fixed percentage anchors.
    font_family = matplotlib.rcParams["font.family"]
    font_properties: dict[float, font_manager.FontProperties] = {}

    def text_width(text: str, fontsize: float) -> float:
        properties = font_properties.setdefault(
            fontsize,
            font_manager.FontProperties(
                family=font_family,
                weight="bold",
                size=fontsize,
            ),
        )
        extent = TextPath((0, 0), text, prop=properties).get_extents()
        return extent.width / (72.0 * fig.get_figwidth())

    column_text = (
        (("SYSTEM", header_size), ("ClickHouse", value_size), ("BigQuery", value_size)),
        (
            ("ACCUMULATED RUNTIME", header_size),
            (clickhouse_runtime, value_size),
            (bigquery_runtime, value_size),
        ),
        (
            ("ACCUMULATED QUERY COST", header_size),
            (clickhouse_cost, value_size),
            (bigquery_cost, value_size),
        ),
        (
            ("COMPARISON", header_size),
            ("baseline", value_size),
            (bigquery_detail, value_size),
        ),
    )
    column_widths = [
        max(text_width(text, fontsize) for text, fontsize in values)
        for values in column_text
    ]
    inner_left = left + width * .025
    inner_right = left + width * .975
    remaining = inner_right - inner_left - sum(column_widths)
    if remaining <= 0:
        raise ValueError("summary strip is too narrow for its four measured columns")
    equal_gap = remaining / 3
    column_x = [inner_left]
    for column_width in column_widths[:-1]:
        column_x.append(column_x[-1] + column_width + equal_gap)

    columns = tuple(
        zip(
            column_x,
            ("SYSTEM", "ACCUMULATED RUNTIME", "ACCUMULATED QUERY COST", "COMPARISON"),
        )
    )
    for x, label in columns:
        fig.text(x, header_y, label, color=MUTED_COLOR, fontsize=header_size,
                 fontweight="bold", ha="left", va="center", zorder=3,
                 parse_math=False)
    rows = (
        (
            first_y,
            CLICKHOUSE_COLOR,
            "ClickHouse",
            clickhouse_runtime,
            clickhouse_cost,
            "baseline",
        ),
        (
            second_y,
            BIGQUERY_COLOR,
            "BigQuery",
            bigquery_runtime,
            bigquery_cost,
            bigquery_detail,
        ),
    )
    xs = [item[0] for item in columns]
    for y, color, system, runtime_text, cost_text, comparison in rows:
        values = (system, runtime_text, cost_text, comparison)
        for x, value in zip(xs, values):
            fig.text(x, y, value, color=color, fontsize=value_size,
                     fontweight="bold", ha="left", va="center", zorder=3,
                     parse_math=False)
    return {
        "runtime_ratio": runtime_ratio,
        "capacity_cost_ratio": capacity_ratio,
        "on_demand_cost_ratio": on_demand_ratio,
    }
