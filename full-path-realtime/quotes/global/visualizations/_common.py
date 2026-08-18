"""Shared primitives for the provider-neutral real-time benchmark charts."""

from __future__ import annotations

import csv
import hashlib
import json
import math
import os
import statistics
import struct
from bisect import bisect_left, bisect_right
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

import matplotlib
import matplotlib.font_manager as font_manager
import matplotlib.patches as patches

BACKGROUND = os.environ.get("CHART_BACKGROUND_COLOR", "#2B2B2B")
GRID = "#4A4A4A"
MUTED = "#A0A0A0"
WHITE = "#FFFFFF"

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


def repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def load_manifest(path: Path) -> tuple[dict[str, Any], Path]:
    resolved = path.expanduser().resolve()
    payload = json.loads(resolved.read_text(encoding="utf-8"))
    if payload.get("schema_version") != 1:
        raise ValueError(f"unsupported manifest schema at {resolved}")
    return payload, resolved


def resolve_source(value: str) -> Path:
    path = (repo_root() / value).resolve()
    if not path.exists():
        raise FileNotFoundError(path)
    return path


def validate_required_labels(
    manifest: dict[str, Any], chart_key: str, actual_labels: Iterable[str]
) -> list[str]:
    required_by_chart = manifest.get("required_labels")
    if not isinstance(required_by_chart, dict):
        raise ValueError("manifest must declare required_labels")
    required = required_by_chart.get(chart_key)
    if not isinstance(required, list) or not required or not all(
        isinstance(label, str) and label.strip() for label in required
    ):
        raise ValueError(f"manifest required_labels.{chart_key} must be a non-empty string list")

    actual = list(actual_labels)
    required_normalized = [label.casefold() for label in required]
    actual_normalized = [label.casefold() for label in actual]
    if len(set(required_normalized)) != len(required_normalized):
        raise ValueError(f"manifest required_labels.{chart_key} contains duplicates")
    if len(set(actual_normalized)) != len(actual_normalized):
        raise ValueError(f"rendered labels for {chart_key} contain duplicates: {actual}")
    if set(actual_normalized) != set(required_normalized):
        missing = [label for label in required if label.casefold() not in actual_normalized]
        unexpected = [label for label in actual if label.casefold() not in required_normalized]
        raise ValueError(
            f"incomplete {chart_key} label set; missing={missing}, unexpected={unexpected}"
        )
    return required


def iter_jsonl(path: Path) -> Iterable[tuple[int, dict[str, Any]]]:
    with path.open(encoding="utf-8") as handle:
        for line_number, raw in enumerate(handle, 1):
            if raw.strip():
                yield line_number, json.loads(raw)


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


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
            return Path(value).resolve().relative_to(repo_root()).as_posix()
        except ValueError:
            return value
    return value


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(portable_payload(payload), indent=2) + "\n",
        encoding="utf-8",
    )


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def save_figure(
    figure: Any,
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
        "facecolor": "none" if wide else figure.get_facecolor(),
        "edgecolor": "none",
        "pad_inches": 0,
    }
    if not wide:
        options["bbox_inches"] = "tight"
    figure.savefig(png, dpi=dpi, **options)
    figure.savefig(svg, **options)
    if wide:
        actual = png_dimensions(png)
        expected = (KEYNOTE_WIDTH_PX, KEYNOTE_HEIGHT_PX)
        if actual != expected:
            raise ValueError(
                f"wide PNG dimensions are {actual}; expected {expected}"
            )
    return png, svg


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


def configure_figure(figure: Any, *, wide: bool) -> str:
    """Apply the shared Keynote-wide canvas and return the axes background."""
    if not wide:
        figure.patch.set_facecolor(BACKGROUND)
        figure.patch.set_alpha(1)
        return BACKGROUND

    figure.patch.set_facecolor("none")
    figure.patch.set_alpha(0)
    stage = patches.FancyBboxPatch(
        (KEYNOTE_STAGE_LEFT, KEYNOTE_STAGE_BOTTOM),
        KEYNOTE_STAGE_RIGHT - KEYNOTE_STAGE_LEFT,
        KEYNOTE_STAGE_TOP - KEYNOTE_STAGE_BOTTOM,
        transform=figure.transFigure,
        clip_on=False,
        boxstyle="round,pad=0,rounding_size=0.012",
        linewidth=1.0,
        edgecolor=KEYNOTE_STAGE_BORDER_COLOR,
        facecolor=KEYNOTE_STAGE_COLOR,
        zorder=-20,
    )
    figure.add_artist(stage)
    return KEYNOTE_STAGE_COLOR


def png_dimensions(path: Path) -> tuple[int, int]:
    with path.open("rb") as handle:
        if handle.read(8) != b"\x89PNG\r\n\x1a\n":
            raise ValueError(f"not a PNG file: {path}")
        length = struct.unpack(">I", handle.read(4))[0]
        if handle.read(4) != b"IHDR" or length < 8:
            raise ValueError(f"missing PNG IHDR: {path}")
        return struct.unpack(">II", handle.read(8))


def scalar_trial(value: Any, context: str) -> float:
    if not isinstance(value, list) or len(value) != 1:
        raise ValueError(f"expected one trial at {context}; got {value!r}")
    scalar = value[0]
    if scalar is None or isinstance(scalar, bool) or not isinstance(scalar, (int, float)):
        raise ValueError(f"missing/non-numeric latency at {context}: {scalar!r}")
    result = float(scalar)
    if result < 0:
        raise ValueError(f"negative latency at {context}: {result}")
    return result


def timestamp(value: str) -> float:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()


def human_rows(value: float, _position: float | None = None) -> str:
    if value <= 0:
        return "0"
    for divisor, suffix in ((1e12, "T"), (1e9, "B"), (1e6, "M"), (1e3, "K")):
        if value >= divisor:
            return f"{value / divisor:g}{suffix}"
    return f"{value:g}"


def human_seconds(value: float, _position: float | None = None) -> str:
    if math.isclose(value, 0.0, abs_tol=1e-12):
        return "0s"
    if value < 1:
        return f"{value * 1000:g}ms"
    return f"{value:g}s"


def rolling_median(values: list[float], window: int) -> list[float]:
    if window < 1 or window % 2 == 0:
        raise ValueError("rolling-median window must be a positive odd integer")
    half = window // 2
    return [
        statistics.median(values[max(0, index - half):min(len(values), index + half + 1)])
        for index in range(len(values))
    ]


def time_rolling_mean(times: list[float], values: list[float], window_seconds: float) -> list[float]:
    if len(times) != len(values) or not times:
        raise ValueError("time-series arrays must be non-empty and equal length")
    half = window_seconds / 2
    prefix = [0.0]
    for value in values:
        prefix.append(prefix[-1] + value)
    output: list[float] = []
    for current in times:
        left = bisect_left(times, current - half)
        right = bisect_right(times, current + half)
        output.append((prefix[right] - prefix[left]) / (right - left))
    return output


def unique_mean_series(xs: list[int], ys: list[float]) -> tuple[list[float], list[float]]:
    pairs = sorted(zip(xs, ys, strict=True))
    grouped: list[tuple[int, list[float]]] = []
    for x, y in pairs:
        if grouped and grouped[-1][0] == x:
            grouped[-1][1].append(y)
        else:
            grouped.append((x, [y]))
    return [float(x) for x, _ in grouped], [statistics.fmean(values) for _, values in grouped]


def _pchip_slopes(xs: list[float], ys: list[float]) -> list[float]:
    count = len(xs)
    if count < 2:
        raise ValueError("PCHIP needs at least two unique x coordinates")
    widths = [xs[i + 1] - xs[i] for i in range(count - 1)]
    if any(width <= 0 for width in widths):
        raise ValueError("PCHIP x coordinates must be strictly increasing")
    deltas = [(ys[i + 1] - ys[i]) / widths[i] for i in range(count - 1)]
    if count == 2:
        return [deltas[0], deltas[0]]
    slopes = [0.0] * count
    for i in range(1, count - 1):
        left, right = deltas[i - 1], deltas[i]
        if left == 0 or right == 0 or left * right < 0:
            continue
        w1 = 2 * widths[i] + widths[i - 1]
        w2 = widths[i] + 2 * widths[i - 1]
        slopes[i] = (w1 + w2) / (w1 / left + w2 / right)

    def endpoint(h0: float, h1: float, d0: float, d1: float) -> float:
        slope = ((2 * h0 + h1) * d0 - h0 * d1) / (h0 + h1)
        if slope * d0 <= 0:
            return 0.0
        if d0 * d1 < 0 and abs(slope) > abs(3 * d0):
            return 3 * d0
        return slope

    slopes[0] = endpoint(widths[0], widths[1], deltas[0], deltas[1])
    slopes[-1] = endpoint(widths[-1], widths[-2], deltas[-1], deltas[-2])
    return slopes


def pchip_curve(xs: list[float], ys: list[float], points: int) -> tuple[list[float], list[float]]:
    slopes = _pchip_slopes(xs, ys)
    render_xs = [xs[0] + (xs[-1] - xs[0]) * i / (points - 1) for i in range(points)]
    render_ys: list[float] = []
    segment = 0
    for x in render_xs:
        while segment < len(xs) - 2 and x > xs[segment + 1]:
            segment += 1
        width = xs[segment + 1] - xs[segment]
        t = (x - xs[segment]) / width
        h00 = 2 * t**3 - 3 * t**2 + 1
        h10 = t**3 - 2 * t**2 + t
        h01 = -2 * t**3 + 3 * t**2
        h11 = t**3 - t**2
        render_ys.append(
            h00 * ys[segment] + h10 * width * slopes[segment]
            + h01 * ys[segment + 1] + h11 * width * slopes[segment + 1]
        )
    return render_xs, render_ys
