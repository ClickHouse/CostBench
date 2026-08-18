"""Shared, evidence-preserving helpers for Snowflake comparison charts."""

from __future__ import annotations

import csv
import hashlib
import json
import math
import os
import struct
from pathlib import Path
from typing import Any, Iterable

import matplotlib
import matplotlib.font_manager as font_manager
import matplotlib.patches as patches
from matplotlib.textpath import TextPath


BACKGROUND = os.environ.get("CHART_BACKGROUND_COLOR", "#2B2B2B")
GRID = "#4A4A4A"
MUTED = "#A0A0A0"
WHITE = "#FFFFFF"
CLICKHOUSE = "#FDFF62"
SNOWFLAKE = "#29B5E8"
SNOWFLAKE_DARK = "#147CA3"

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

matplotlib.rcParams["font.family"] = (
    "Inter"
    if any(font.name == "Inter" for font in font_manager.fontManager.ttflist)
    else "DejaVu Sans"
)
matplotlib.rcParams["font.sans-serif"] = ["Inter", "DejaVu Sans"]
matplotlib.rcParams["axes.titleweight"] = "bold"


def read_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"expected JSON object at {path}")
    return value


def iter_jsonl(path: Path) -> Iterable[tuple[int, dict[str, Any]]]:
    with path.open(encoding="utf-8") as handle:
        for line_number, raw_line in enumerate(handle, start=1):
            if raw_line.strip():
                value = json.loads(raw_line)
                if not isinstance(value, dict):
                    raise ValueError(f"expected JSON object at {path}:{line_number}")
                yield line_number, value


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


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


def write_json(path: Path, value: Any) -> None:
    path.write_text(
        json.dumps(portable_payload(value), indent=2) + "\n",
        encoding="utf-8",
    )


def scalar_trial(value: Any, *, context: str) -> float:
    if not isinstance(value, list) or len(value) != 1:
        raise ValueError(f"expected exactly one trial at {context}; got {value!r}")
    scalar = value[0]
    if scalar is None or isinstance(scalar, bool) or not isinstance(scalar, (int, float)):
        raise ValueError(f"missing/non-numeric value at {context}: {scalar!r}")
    result = float(scalar)
    if result < 0:
        raise ValueError(f"negative value at {context}: {result}")
    return result


def one(items: list[dict[str, Any]], *, field: str, value: str, context: str) -> dict[str, Any]:
    matches = [item for item in items if str(item.get(field, "")).lower() == value.lower()]
    if len(matches) != 1:
        raise ValueError(f"expected one {field}={value!r} in {context}; found {len(matches)}")
    return matches[0]


def money(value: float) -> str:
    if value < 0.1:
        return f"${value:.3f}"
    return f"${value:,.2f}"


def human_seconds(value: float, _position: float | None = None) -> str:
    if math.isclose(value, 0.0, abs_tol=1e-12):
        return "0s"
    if value < 1:
        return f"{value * 1000:g}ms"
    return f"{value:g}s"


def duration(value: float) -> str:
    if value < 60:
        return f"{value:.1f}s"
    if value < 3600:
        return f"{value / 60:.1f}m"
    return f"{value / 3600:.1f}h"


def comparison_ratio(value: float) -> str:
    """Keep meaningful precision for close comparisons without cluttering large ratios."""
    if value < 2:
        return f"{value:,.2f}"
    if value < 10:
        return f"{value:,.1f}"
    return f"{value:,.0f}"


def human_rows(value: float, _position: float | None = None) -> str:
    if value <= 0:
        return "0"
    for divisor, suffix in ((1e12, "T"), (1e9, "B"), (1e6, "M"), (1e3, "K")):
        if value >= divisor:
            return f"{value / divisor:g}{suffix}"
    return f"{value:g}"


def close(actual: float, expected: float, *, context: str, absolute: float = 0.01) -> None:
    if not math.isclose(actual, expected, rel_tol=1e-7, abs_tol=absolute):
        raise ValueError(f"{context}: actual={actual}, expected={expected}")


def rounded_bar(axis: Any, x: float, y: float, width: float, height: float, color: str) -> Any:
    patch = patches.FancyBboxPatch(
        (x, y), width, height,
        boxstyle=f"round,pad=0,rounding_size={height * 0.16}",
        linewidth=0,
        facecolor=color,
    )
    axis.add_patch(patch)
    return patch


def annotate_outlier_stats(
    axis: Any,
    report: dict[str, Any],
    observed_count: int,
    *,
    label: str = "Snowflake outliers",
    fontsize: float = 7.3,
) -> None:
    """Add an outlier summary only when the filter excluded observations."""
    if not report.get("applied"):
        raise ValueError("cannot annotate an outlier report that was not applied")
    excluded = int(report.get("excluded_observations") or 0)
    if excluded == 0:
        return
    percentage = 100 * excluded / observed_count if observed_count else 0.0
    fence = float(report["upper_fence_sec"])
    maximum = report.get("excluded_max_latency_sec")
    maximum_text = f"{float(maximum):,.1f}s" if maximum is not None else "none"
    annotation = (
        f"{label}\n"
        f"Excluded {excluded}/{observed_count} ({percentage:.1f}%)\n"
        f"Fence > {fence:,.1f}s · max {maximum_text}"
    )
    axis.text(
        0.018,
        0.975,
        annotation,
        transform=axis.transAxes,
        ha="left",
        va="top",
        color=WHITE,
        fontsize=fontsize,
        linespacing=1.25,
        zorder=20,
        bbox={
            "boxstyle": "round,pad=0.38",
            "facecolor": "#222222",
            "edgecolor": GRID,
            "linewidth": 0.75,
            "alpha": 0.91,
        },
    )


def save_figure(
    fig: Any,
    output_dir: Path,
    basename: str,
    dpi: int,
    *,
    exact_canvas: bool = False,
    transparent_canvas: bool = False,
    wide: bool = False,
) -> tuple[Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    png = output_dir / f"{basename}.png"
    svg = output_dir / f"{basename}.svg"
    exact_canvas = exact_canvas or wide
    transparent_canvas = transparent_canvas or wide
    save_options: dict[str, Any] = {
        "facecolor": "none" if transparent_canvas else fig.get_facecolor(),
        "edgecolor": "none",
        "pad_inches": 0,
    }
    if not exact_canvas:
        save_options["bbox_inches"] = "tight"
    fig.savefig(png, dpi=dpi, **save_options)
    fig.savefig(svg, **save_options)
    if wide:
        actual = png_dimensions(png)
        expected = (KEYNOTE_WIDTH_PX, KEYNOTE_HEIGHT_PX)
        if actual != expected:
            raise ValueError(
                f"wide PNG dimensions are {actual}; expected {expected}"
            )
    return png, svg


def png_dimensions(path: Path) -> tuple[int, int]:
    """Read PNG dimensions directly from IHDR without an image dependency."""
    with path.open("rb") as handle:
        if handle.read(8) != b"\x89PNG\r\n\x1a\n":
            raise ValueError(f"not a PNG file: {path}")
        length = struct.unpack(">I", handle.read(4))[0]
        if handle.read(4) != b"IHDR" or length < 8:
            raise ValueError(f"missing PNG IHDR: {path}")
        return struct.unpack(">II", handle.read(8))


def resolve_layout(
    basename: str,
    wide: bool,
    default_size: tuple[float, float],
    wide_size: tuple[float, float] = (16.0, 8.5),
    dpi: int = 300,
) -> tuple[str, tuple[float, float], dict[str, Any]]:
    """Resolve a non-overwriting presentation layout without changing chart data."""
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
        "background_color": BACKGROUND,
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
        fig.patch.set_facecolor(BACKGROUND)
        fig.patch.set_alpha(1)
        return BACKGROUND

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


def write_csv(path: Path, fieldnames: list[str], rows: Iterable[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def tier_cost(summary: dict[str, Any], tier: str, *, context: str) -> dict[str, Any]:
    return one(summary["costs"], field="tier", value=tier, context=context)


def validate_snowflake_credit_cost(summary: dict[str, Any], tier: str, *, context: str) -> float:
    entry = tier_cost(summary, tier, context=context)
    total_credits = float(summary.get("total_credits", entry.get("total_credits", 0)))
    price = float(entry["credit_price_per_credit"])
    cost = float(entry["total_cost_usd"])
    close(cost, total_credits * price, context=f"{context} credit total")
    return cost


def validate_snowflake_query_cost(
    summary: dict[str, Any],
    pricing: dict[str, Any],
    tier: str,
    *,
    context: str,
    fallback_pricing: dict[str, Any] | None = None,
) -> dict[str, Any]:
    entry = tier_cost(summary, tier, context=context)
    runtime = float(summary["total_runtime_seconds"])
    cost = float(entry["total_compute_cost_usd"])

    def find_plan(document: dict[str, Any], *, label: str) -> dict[str, Any]:
        plans = [
            plan
            for plan in document["pricing"]
            if str(plan.get("plan", "")).lower() == tier.lower()
            and str(plan.get("cloud", "")) == str(summary.get("cloud", ""))
            and str(plan.get("region", "")) == str(summary.get("region", ""))
        ]
        if len(plans) != 1:
            raise ValueError(
                f"expected one {tier} {label} pricing plan in {context}; "
                f"found {len(plans)}"
            )
        return plans[0]

    def validate_component(
        component: dict[str, Any],
        document: dict[str, Any],
        *,
        label: str,
    ) -> None:
        plan = find_plan(document, label=label)
        warehouse_name = str(component["warehouse_size"])
        warehouses = [
            item for item in plan["warehouses"]
            if str(item.get("name")) == warehouse_name
        ]
        if len(warehouses) != 1:
            raise ValueError(
                f"expected warehouse {warehouse_name!r} in {context} {label} pricing"
            )
        credits_per_hour = float(warehouses[0]["credits_per_hour"])
        credit_price = float(plan["credit_price_per_hour"])
        component_runtime = float(component["runtime_seconds"])
        component_cost = float(component["total_compute_cost_usd"])
        close(
            float(component["credits_per_hour"]),
            credits_per_hour,
            context=f"{context} {label} warehouse rate",
            absolute=1e-9,
        )
        close(
            float(component["credit_price_per_hour"]),
            credit_price,
            context=f"{context} {label} credit price",
            absolute=1e-9,
        )
        close(
            component_cost,
            component_runtime / 3600 * credits_per_hour * credit_price,
            context=f"{context} {label} component cost",
            absolute=0.00002,
        )

    components = entry.get("components")
    if isinstance(components, list) and components:
        primary_components = [item for item in components if item.get("role") == "primary"]
        fallback_components = [item for item in components if item.get("role") == "fallback"]
        if len(primary_components) != 1 or len(fallback_components) > 1:
            raise ValueError(f"invalid normalized-cost components in {context}")
        validate_component(primary_components[0], pricing, label="primary")
        if fallback_components:
            if fallback_pricing is None:
                raise ValueError(
                    f"{context} uses fallback attribution but no fallback pricing was supplied"
                )
            validate_component(
                fallback_components[0], fallback_pricing, label="fallback"
            )
        component_runtime = sum(float(item["runtime_seconds"]) for item in components)
        component_cost = sum(float(item["total_compute_cost_usd"]) for item in components)
        close(component_runtime, runtime, context=f"{context} component runtime", absolute=0.001)
        close(component_cost, cost, context=f"{context} component total", absolute=0.00002)
    else:
        # Backward-compatible validation for historical single-rate summaries.
        warehouse = str(summary["machine"])
        plan = find_plan(pricing, label="recorded warehouse")
        warehouses = [item for item in plan["warehouses"] if item.get("name") == warehouse]
        if len(warehouses) != 1:
            raise ValueError(f"expected warehouse {warehouse!r} in {context}")
        credits_per_hour = float(warehouses[0]["credits_per_hour"])
        credit_price = float(plan["credit_price_per_hour"])
        close(float(entry["credits_per_hour"]), credits_per_hour, context=f"{context} warehouse rate")
        close(float(entry["credit_price_per_hour"]), credit_price, context=f"{context} credit price")
        close(cost, runtime / 3600 * credits_per_hour * credit_price, context=f"{context} total")

    iterations = int(summary["iterations_included"])
    queries_per_iteration = int(summary["queries_per_iteration"])
    query_jobs = iterations * queries_per_iteration
    attribution = summary.get("runtime_attribution") or {}
    attributed_jobs = int(attribution.get("query_jobs", query_jobs))
    if attributed_jobs != query_jobs:
        raise ValueError(
            f"{context} runtime attribution query jobs: "
            f"actual={attributed_jobs}, expected={query_jobs}"
        )
    primary_jobs = int(attribution.get("primary_priced_query_jobs", query_jobs))
    fallback_jobs = int(attribution.get("fallback_priced_query_jobs", 0))
    if primary_jobs + fallback_jobs != query_jobs:
        raise ValueError(f"{context} primary/fallback query jobs do not sum to {query_jobs}")
    return {
        "iterations": iterations,
        "queries_per_iteration": queries_per_iteration,
        "query_jobs": query_jobs,
        "runtime_sec": runtime,
        "cost_usd": cost,
        "primary_priced_query_jobs": primary_jobs,
        "primary_priced_runtime_sec": float(
            attribution.get("primary_priced_runtime_seconds", runtime)
        ),
        "fallback_priced_query_jobs": fallback_jobs,
        "fallback_priced_runtime_sec": float(
            attribution.get("fallback_priced_runtime_seconds", 0)
        ),
        "fallback_priced_job_share": float(
            attribution.get("fallback_priced_job_share", 0)
        ),
        "query_cost_model": summary.get("query_cost_model"),
    }


def validate_clickhouse_query_cost(
    summary: dict[str, Any],
    tier: str,
    *,
    context: str,
) -> dict[str, float | int]:
    entry = tier_cost(summary, tier, context=context)
    iterations = int(summary["iterations_included"])
    queries_per_iteration = int(summary["queries_per_iteration"])
    return {
        "iterations": iterations,
        "queries_per_iteration": queries_per_iteration,
        "query_jobs": iterations * queries_per_iteration,
        "runtime_sec": float(summary["total_runtime_seconds"]),
        "cost_usd": float(entry["total_compute_cost_usd"]),
    }


def validate_matched_query_totals(
    clickhouse: dict[str, float | int],
    comparison: dict[str, float | int],
    *,
    context: str,
) -> None:
    for field in ("iterations", "queries_per_iteration", "query_jobs"):
        if int(clickhouse[field]) != int(comparison[field]):
            raise ValueError(
                f"{context} matched {field} mismatch: "
                f"ClickHouse={clickhouse[field]}, comparison={comparison[field]}"
            )


def draw_matched_query_totals_strip(
    fig: Any,
    clickhouse: dict[str, float | int],
    comparison: dict[str, float | int],
    *,
    comparison_name: str = "Snowflake",
    comparison_color: str = SNOWFLAKE,
    query_cost_display: str = "total",
    wide: bool = False,
) -> dict[str, float]:
    """Draw a compact four-column workload summary below a latency plot."""
    if query_cost_display not in {"total", "attribution"}:
        raise ValueError(
            "query_cost_display must be either 'total' or 'attribution'"
        )
    runtime_ratio = float(comparison["runtime_sec"]) / float(clickhouse["runtime_sec"])
    cost_ratio = float(comparison["cost_usd"]) / float(clickhouse["cost_usd"])
    fallback_jobs = int(comparison.get("fallback_priced_query_jobs") or 0)
    query_jobs = int(comparison["query_jobs"])
    fallback_note = (
        f"Normalized-cost proxy: {fallback_jobs}/{query_jobs} Snowflake jobs >5s "
        "priced at Gen2 Small for full elapsed time"
        if query_cost_display == "attribution" and fallback_jobs
        else None
    )

    # Keep the table compact and centered instead of stretching it across the
    # slide. Values and ratios remain evidence-backed; matched iteration/query
    # counts stay in the chart summary JSON.
    if wide:
        left, bottom, width, height = (0.210, 0.018, 0.58, 0.138 if fallback_note else 0.115)
    else:
        left, bottom, width, height = (0.160, 0.014, 0.68, 0.141 if fallback_note else 0.118)
    border = patches.FancyBboxPatch(
        (left, bottom),
        width,
        height,
        transform=fig.transFigure,
        clip_on=False,
        boxstyle="round,pad=0,rounding_size=0.008",
        linewidth=0.85,
        edgecolor=GRID,
        facecolor="#202020",
        alpha=0.97,
        zorder=30,
    )
    fig.add_artist(border)

    header_y = bottom + height - 0.026
    first_y = bottom + (0.074 if fallback_note else 0.053)
    second_y = bottom + (0.041 if fallback_note else 0.020)
    header_size = 9.4 if wide else 8.3
    value_size = 11.0 if wide else 9.6

    clickhouse_runtime = duration(float(clickhouse["runtime_sec"]))
    comparison_runtime = duration(float(comparison["runtime_sec"]))
    clickhouse_cost = money(float(clickhouse["cost_usd"]))
    comparison_cost = money(float(comparison["cost_usd"]))
    comparison_detail = (
        f"{comparison_ratio(runtime_ratio)}× slower · "
        f"{comparison_ratio(cost_ratio)}× costlier"
    )

    # Measure every column's widest rendered string, then distribute the
    # remaining horizontal space as three identical visual gaps. Equal anchor
    # spacing is misleading here because the labels have very different widths.
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
        (("SYSTEM", header_size), ("ClickHouse", value_size), (comparison_name, value_size)),
        (
            ("ACCUMULATED RUNTIME", header_size),
            (clickhouse_runtime, value_size),
            (comparison_runtime, value_size),
        ),
        (
            ("ACCUMULATED QUERY COST", header_size),
            (clickhouse_cost, value_size),
            (comparison_cost, value_size),
        ),
        (
            ("COMPARISON", header_size),
            ("baseline", value_size),
            (comparison_detail, value_size),
        ),
    )
    column_widths = [
        max(text_width(text, fontsize) for text, fontsize in values)
        for values in column_text
    ]
    inner_left = left + width * 0.025
    inner_right = left + width * 0.975
    remaining = inner_right - inner_left - sum(column_widths)
    if remaining <= 0:
        raise ValueError("summary strip is too narrow for its four measured columns")
    equal_gap = remaining / 3
    column_x = [inner_left]
    for column_width in column_widths[:-1]:
        column_x.append(column_x[-1] + column_width + equal_gap)
    system_x, runtime_x, cost_x, comparison_x = column_x

    for x, label in (
        (system_x, "SYSTEM"),
        (runtime_x, "ACCUMULATED RUNTIME"),
        (cost_x, "ACCUMULATED QUERY COST"),
        (comparison_x, "COMPARISON"),
    ):
        fig.text(
            x,
            header_y,
            label,
            color=MUTED,
            fontsize=header_size,
            fontweight="bold",
            va="center",
            zorder=31,
        )

    rows = (
        (
            "ClickHouse",
            CLICKHOUSE,
            clickhouse_runtime,
            clickhouse_cost,
            first_y,
            "baseline",
        ),
        (
            comparison_name,
            comparison_color,
            comparison_runtime,
            comparison_cost,
            second_y,
            comparison_detail,
        ),
    )
    for name, color, runtime_text, cost_text, y, comparison_text in rows:
        fig.text(
            system_x,
            y,
            name,
            color=color,
            fontsize=value_size,
            fontweight="bold",
            va="center",
            zorder=31,
        )
        fig.text(
            runtime_x,
            y,
            runtime_text,
            color=color,
            fontsize=value_size,
            fontweight="bold",
            va="center",
            zorder=31,
        )
        fig.text(
            cost_x,
            y,
            cost_text,
            color=color,
            fontsize=value_size,
            fontweight="bold",
            va="center",
            zorder=31,
        )
        fig.text(
            comparison_x,
            y,
            comparison_text,
            color=color,
            fontsize=value_size,
            fontweight="bold",
            va="center",
            zorder=31,
        )

    if fallback_note:
        fig.text(
            inner_left,
            bottom + 0.012,
            fallback_note,
            color=MUTED,
            fontsize=7.8 if wide else 6.9,
            va="center",
            zorder=31,
        )

    return {"runtime_ratio": runtime_ratio, "cost_ratio": cost_ratio}
