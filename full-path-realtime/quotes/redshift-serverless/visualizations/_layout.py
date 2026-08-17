"""Shared Redshift chart layout, colors, validation, and save helpers."""

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


BACKGROUND = os.environ.get("CHART_BACKGROUND_COLOR", "#161614")
GRID = "#4A4A4A"
MUTED = "#A0A0A0"
WHITE = "#F4F4F4"
CLICKHOUSE = "#FDFF62"
REDSHIFT = "#FF9900"
REDSHIFT_TYPED = "#E76F00"

KEYNOTE_WIDTH_PX = 5156
KEYNOTE_HEIGHT_PX = 2900
KEYNOTE_HEADER_SAFE_AREA_PX = 560
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


def resolve_layout(
    basename: str,
    wide: bool,
    default_size: tuple[float, float],
    dpi: int,
) -> tuple[str, tuple[float, float], dict[str, Any]]:
    size = (
        (KEYNOTE_WIDTH_PX / dpi, KEYNOTE_HEIGHT_PX / dpi)
        if wide
        else default_size
    )
    resolved = basename if not wide or basename.endswith("_wide") else f"{basename}_wide"
    metadata: dict[str, Any] = {
        "variant": "wide" if wide else "default",
        "figure_inches": list(size),
        "data_and_math_unchanged": True,
    }
    if wide:
        metadata.update(
            pixel_dimensions=[KEYNOTE_WIDTH_PX, KEYNOTE_HEIGHT_PX],
            transparent_canvas=True,
            header_safe_area_pixels=KEYNOTE_HEADER_SAFE_AREA_PX,
            chart_stage="subtle",
            chart_stage_color=KEYNOTE_STAGE_COLOR,
            chart_stage_border_color=KEYNOTE_STAGE_BORDER_COLOR,
        )
    return resolved, size, metadata


def configure_figure(figure: Any, *, wide: bool) -> str:
    if not wide:
        figure.patch.set_facecolor(BACKGROUND)
        return BACKGROUND
    figure.patch.set_facecolor("none")
    figure.patch.set_alpha(0)
    figure.add_artist(
        patches.FancyBboxPatch(
            (KEYNOTE_STAGE_LEFT, KEYNOTE_STAGE_BOTTOM),
            KEYNOTE_STAGE_RIGHT - KEYNOTE_STAGE_LEFT,
            KEYNOTE_STAGE_TOP - KEYNOTE_STAGE_BOTTOM,
            transform=figure.transFigure,
            clip_on=False,
            boxstyle="round,pad=0,rounding_size=0.012",
            linewidth=1,
            edgecolor=KEYNOTE_STAGE_BORDER_COLOR,
            facecolor=KEYNOTE_STAGE_COLOR,
            zorder=-20,
        )
    )
    return KEYNOTE_STAGE_COLOR


def png_dimensions(path: Path) -> tuple[int, int]:
    with path.open("rb") as handle:
        if handle.read(8) != b"\x89PNG\r\n\x1a\n":
            raise ValueError(f"not a PNG: {path}")
        length = struct.unpack(">I", handle.read(4))[0]
        if handle.read(4) != b"IHDR" or length < 8:
            raise ValueError(f"missing PNG IHDR: {path}")
        return struct.unpack(">II", handle.read(8))


def save_figure(
    figure: Any,
    output_dir: Path,
    basename: str,
    dpi: int,
    *,
    wide: bool,
) -> tuple[Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    png = output_dir / f"{basename}.png"
    svg = output_dir / f"{basename}.svg"
    options: dict[str, Any] = {
        "facecolor": "none" if wide else figure.get_facecolor(),
        "edgecolor": "none",
        "pad_inches": 0,
    }
    if not wide:
        options["bbox_inches"] = "tight"
    figure.savefig(png, dpi=dpi, **options)
    figure.savefig(svg, **options)
    if wide and png_dimensions(png) != (KEYNOTE_WIDTH_PX, KEYNOTE_HEIGHT_PX):
        raise ValueError(f"unexpected wide PNG size for {png}")
    return png, svg


def load_json(path: Path) -> dict[str, Any]:
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


def tier_cost(summary: dict[str, Any], tier: str) -> float:
    matches = [item for item in summary["costs"] if item.get("tier") == tier]
    if len(matches) != 1:
        raise ValueError(f"expected one {tier} cost; got {len(matches)}")
    return float(matches[0]["total_compute_cost_usd"])


def duration(seconds: float) -> str:
    if seconds < 60:
        return f"{seconds:.1f}s"
    if seconds < 3600:
        return f"{seconds / 60:.1f}m"
    return f"{seconds / 3600:.1f}h"


def money(value: float) -> str:
    return f"${value:.3f}" if value < 1 else f"${value:,.2f}"


def ratio_text(value: float, noun: str) -> str:
    if math.isclose(value, 1.0, rel_tol=0.005):
        return f"same {noun}"
    magnitude = value if value > 1 else 1 / value
    formatted = f"{magnitude:.1f}×" if magnitude < 10 else f"{magnitude:,.0f}×"
    direction = "higher" if noun == "cost" else "slower"
    if value < 1:
        direction = "lower" if noun == "cost" else "faster"
    return f"{formatted} {direction}"
