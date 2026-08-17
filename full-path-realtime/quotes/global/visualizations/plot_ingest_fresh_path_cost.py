#!/usr/bin/env python3
# /// script
# requires-python = ">=3.10"
# dependencies = ["matplotlib==3.10.9"]
# ///
"""Render accepted complete-ingest fresh-data-path costs for all systems."""

from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
from typing import Any

import matplotlib.pyplot as plt
from _common import (
    MUTED,
    WHITE,
    configure_figure,
    load_manifest,
    resolve_layout,
    resolve_source,
    save_figure,
    sha256,
    validate_required_labels,
    write_csv,
    write_json,
)
from matplotlib.patches import FancyBboxPatch, Rectangle

SNOWFLAKE_MV = "#147CA3"
BIGQUERY_MV = "#1557A6"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--manifest",
        type=Path,
        default=Path(__file__).with_name("manifest.json"),
    )
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--basename", default="ingest_fresh_path_cost_all_systems")
    parser.add_argument(
        "--wide",
        action="store_true",
        help="Render the Keynote-native 5156x2900 staged variant and append _wide to the basename.",
    )
    parser.add_argument("--dpi", type=int, default=300)
    return parser.parse_args()


def load_summary(path: Path, expected_chart: str) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict) or payload.get("schema_version") != 1:
        raise ValueError(f"unsupported summary schema at {path}")
    if payload.get("chart") != expected_chart:
        raise ValueError(
            f"expected chart={expected_chart!r} in {path}; "
            f"found {payload.get('chart')!r}"
        )
    return payload


def close(actual: float, expected: float, *, context: str) -> None:
    if not math.isclose(actual, expected, rel_tol=1e-9, abs_tol=1e-6):
        raise ValueError(f"cost mismatch for {context}: {actual} != {expected}")


def nonnegative(value: Any, *, context: str) -> float:
    result = float(value)
    if not math.isfinite(result) or result < 0:
        raise ValueError(f"invalid nonnegative cost for {context}: {value!r}")
    return result


def money(value: float) -> str:
    return f"\\${value:,.2f}"


def rounded_bar(
    axis: Any,
    x: float,
    y: float,
    width: float,
    height: float,
    color: str,
) -> FancyBboxPatch:
    patch = FancyBboxPatch(
        (x, y),
        width,
        height,
        boxstyle=f"round,pad=0,rounding_size={height * 0.12}",
        linewidth=0,
        facecolor=color,
    )
    axis.add_patch(patch)
    return patch


def main() -> int:
    args = parse_args()
    manifest, manifest_path = load_manifest(args.manifest)
    config = manifest["fresh_path_cost"]
    sf_path = resolve_source(config["snowflake_pairwise_summary"])
    bq_path = resolve_source(config["bigquery_pairwise_summary"])
    rs_path = resolve_source(config["redshift_pairwise_summary"])
    sf = load_summary(sf_path, "complete_ingest_fresh_data_path_cost")
    bq = load_summary(bq_path, "complete_ingest_and_background_fresh_path_cost")
    rs = load_summary(rs_path, "complete_ingest_fresh_data_path_cost_clickhouse_vs_redshift")

    sf_ch = sf["costs"]["clickhouse"]
    sf_cost = sf["costs"]["snowflake"]
    bq_ch = bq["systems"]["clickhouse"]
    bq_cost = bq["systems"]["bigquery"]

    ch_total = nonnegative(sf_ch["total_usd"], context="ClickHouse total")
    bq_ch_total = nonnegative(
        bq_ch["total_cost_usd"],
        context="BigQuery-pair ClickHouse total",
    )
    close(ch_total, bq_ch_total, context="pairwise ClickHouse fresh-path total")
    sf_ch_hash = sf["sources"]["clickhouse"]["sha256"]
    bq_ch_hash = bq["sources"]["clickhouse_cost"]["sha256"]
    if sf_ch_hash != bq_ch_hash:
        raise ValueError(
            "pairwise fresh-path summaries do not use the same ClickHouse "
            "complete-ingest source"
        )
    rs_ch_total = nonnegative(rs["costs"]["clickhouse"]["total_usd"], context="Redshift-pair ClickHouse total")
    close(ch_total, rs_ch_total, context="Redshift-pair ClickHouse fresh-path total")
    rs_ch_hash = rs["sources"]["clickhouse"]["sha256"]
    if sf_ch_hash != rs_ch_hash:
        raise ValueError("Redshift fresh-path summary does not use the accepted ClickHouse complete-ingest source")
    rs_writer = nonnegative(rs["costs"]["redshift"]["writer_usd"], context="Redshift writer")
    rs_msk = nonnegative(rs["costs"]["redshift"]["msk_usd"], context="Redshift MSK")
    rs_total = nonnegative(rs["costs"]["redshift"]["total_usd"], context="Redshift total")
    close(rs_total, rs_writer + rs_msk, context="Redshift fresh-path total")
    if rs["contract"].get("client_cross_az_included") is not False:
        raise ValueError("Redshift client cross-AZ must be excluded")

    sf_snowpipe = nonnegative(
        sf_cost["snowpipe_streaming_usd"],
        context="Snowflake Snowpipe Streaming",
    )
    sf_mv = nonnegative(
        sf_cost["serverless_mv_refresh_usd"],
        context="Snowflake serverless MV refresh",
    )
    sf_total = nonnegative(sf_cost["total_usd"], context="Snowflake total")
    close(sf_total, sf_snowpipe + sf_mv, context="Snowflake fresh-path total")
    if sf_cost.get("ingest_warehouse_used") is not False:
        raise ValueError(
            "Snowflake summary must explicitly exclude an ingest warehouse"
        )

    bq_write = nonnegative(
        bq_cost["storage_write_api_cost_usd"],
        context="BigQuery Storage Write API",
    )
    bq_mv_capacity = nonnegative(
        bq_cost["enterprise_mv_refresh_cost_usd"],
        context="BigQuery Capacity MV refresh",
    )
    bq_mv_on_demand = nonnegative(
        bq_cost["on_demand_mv_refresh_cost_usd"],
        context="BigQuery On-demand MV refresh",
    )
    bq_capacity_total = nonnegative(
        bq_cost["capacity_total_cost_usd"],
        context="BigQuery Capacity total",
    )
    bq_on_demand_total = nonnegative(
        bq_cost["on_demand_total_cost_usd"],
        context="BigQuery On-demand total",
    )
    close(
        bq_capacity_total,
        bq_write + bq_mv_capacity,
        context="BigQuery Capacity fresh-path total",
    )
    close(
        bq_on_demand_total,
        bq_write + bq_mv_on_demand,
        context="BigQuery On-demand fresh-path total",
    )
    reclustering = nonnegative(
        bq_cost["automatic_reclustering_separate_cost_usd"],
        context="BigQuery automatic reclustering",
    )
    close(reclustering, 0.0, context="BigQuery automatic reclustering")

    colors = {
        provider: manifest["providers"][provider]["color"]
        for provider in ("clickhouse", "snowflake", "bigquery", "redshift")
    }
    maximum = max(ch_total, sf_total, bq_capacity_total, bq_on_demand_total, rs_total)
    basename, figure_size, layout = resolve_layout(
        args.basename,
        args.wide,
        (12.5, 7.2),
        dpi=args.dpi,
    )
    fig, axis = plt.subplots(
        figsize=figure_size, dpi=args.dpi if args.wide else None
    )
    plot_background = configure_figure(fig, wide=args.wide)
    axis.set_facecolor(plot_background)
    axis.set_xlim(0, 1)
    axis.set_ylim(0, 1)
    axis.axis("off")

    left = 0.025
    full_width = 0.78 if args.wide else 0.74
    value_gap = 0.018
    bar_height = 0.085 if args.wide else 0.08
    system_fontsize = 27 if args.wide else 21
    total_fontsize = 20 if args.wide else 15
    detail_fontsize = 14.5 if args.wide else 11
    rows = {
        "clickhouse": 0.79,
        "snowflake": 0.54,
        "redshift": 0.29,
        "bigquery": 0.04,
    }
    label_y_offset = 0.105

    for provider, label, y_offset in (
        ("clickhouse", "ClickHouse", label_y_offset),
        ("snowflake", "Snowflake", label_y_offset),
        ("redshift", "Redshift Serverless", label_y_offset),
        ("bigquery", "BigQuery", label_y_offset),
    ):
        axis.text(
            left,
            rows[provider] + y_offset,
            label,
            color=colors[provider],
            fontsize=system_fontsize,
            fontweight="bold",
            va="center",
        )

    ch_width = full_width * ch_total / maximum
    rounded_bar(
        axis,
        left,
        rows["clickhouse"],
        ch_width,
        bar_height,
        colors["clickhouse"],
    )
    axis.text(
        left + ch_width + value_gap,
        rows["clickhouse"] + bar_height / 2,
        money(ch_total),
        color=WHITE,
        fontsize=total_fontsize,
        fontweight="bold",
        ha="left",
        va="center",
    )
    axis.text(
        left,
        rows["clickhouse"] - 0.065,
        (
            "Bundled write service · ingest, sorting, merges & incremental MV · "
            f"{money(ch_total)}"
        ),
        color=MUTED,
        fontsize=detail_fontsize,
        va="center",
    )

    sf_width = full_width * sf_total / maximum
    sf_bar = rounded_bar(
        axis,
        left,
        rows["snowflake"],
        sf_width,
        bar_height,
        SNOWFLAKE_MV,
    )
    sf_snowpipe_width = full_width * sf_snowpipe / maximum
    sf_segment = Rectangle(
        (left, rows["snowflake"]),
        sf_snowpipe_width,
        bar_height,
        linewidth=0,
        facecolor=colors["snowflake"],
    )
    sf_segment.set_clip_path(sf_bar)
    axis.add_patch(sf_segment)
    axis.text(
        left + sf_width + value_gap,
        rows["snowflake"] + bar_height / 2,
        money(sf_total),
        color=WHITE,
        fontsize=total_fontsize,
        fontweight="bold",
        ha="left",
        va="center",
    )
    axis.text(
        left,
        rows["snowflake"] - 0.065,
        (
            f"Snowpipe Streaming {money(sf_snowpipe)} · "
            f"serverless MV refresh {money(sf_mv)} · no ingest warehouse"
        ),
        color=MUTED,
        fontsize=detail_fontsize,
        va="center",
    )

    rs_width = full_width * rs_total / maximum
    rounded_bar(axis, left, rows["redshift"], rs_width, bar_height, colors["redshift"])
    axis.text(
        left + rs_width + value_gap,
        rows["redshift"] + bar_height / 2,
        money(rs_total),
        color=WHITE,
        fontsize=total_fontsize,
        fontweight="bold",
        ha="left",
        va="center",
    )
    axis.text(
        left,
        rows["redshift"] - 0.055,
        f"Writer workgroup {money(rs_writer)} · MSK broker-hours + storage {money(rs_msk)}",
        color=MUTED,
        fontsize=detail_fontsize,
        va="center",
    )

    bq_on_demand_width = full_width * bq_on_demand_total / maximum
    bq_capacity_width = full_width * bq_capacity_total / maximum
    bq_bar = rounded_bar(
        axis,
        left,
        rows["bigquery"],
        bq_on_demand_width,
        bar_height,
        BIGQUERY_MV,
    )
    bq_capacity_segment = Rectangle(
        (left, rows["bigquery"]),
        bq_capacity_width,
        bar_height,
        linewidth=0,
        facecolor=colors["bigquery"],
    )
    bq_capacity_segment.set_clip_path(bq_bar)
    axis.add_patch(bq_capacity_segment)
    axis.plot(
        [left + bq_capacity_width, left + bq_capacity_width],
        [rows["bigquery"], rows["bigquery"] + bar_height],
        color=plot_background,
        linewidth=1.1,
        alpha=0.65,
        solid_capstyle="butt",
    )
    axis.text(
        left + bq_on_demand_width + value_gap,
        rows["bigquery"] + bar_height / 2,
        f"{money(bq_capacity_total)} Capacity · {money(bq_on_demand_total)} On-demand",
        color=WHITE,
        fontsize=total_fontsize - 1,
        fontweight="bold",
        ha="left",
        va="center",
    )
    axis.text(
        left,
        rows["bigquery"] - 0.055,
        (
            f"Storage Write API {money(bq_write)} · MV refresh "
            f"{money(bq_mv_capacity)} Capacity / "
            f"{money(bq_mv_on_demand)} On-demand · reclustering \\$0"
        ),
        color=MUTED,
        fontsize=detail_fontsize,
        va="center",
    )

    if args.wide:
        fig.subplots_adjust(left=0.055, right=0.945, bottom=0.060, top=0.755)
    else:
        fig.subplots_adjust(left=0.008, right=0.997, bottom=0.02, top=0.995)
    output = args.output_dir.expanduser().resolve()
    png, svg = save_figure(fig, output, basename, args.dpi, wide=args.wide)
    plt.close(fig)

    csv_path = output / f"{basename}_data.csv"
    summary_path = output / f"{basename}_summary.json"
    data_rows = [
        {
            "label": "ClickHouse",
            "pricing_model": "bundled write service",
            "fresh_path_cost_usd": ch_total,
            "ingestion_cost_usd": "",
            "mv_maintenance_cost_usd": "",
            "layout_maintenance_cost_usd": "",
            "bundled_write_service_cost_usd": ch_total,
            "source_pair": "ClickHouse vs Snowflake and ClickHouse vs BigQuery",
        },
        {
            "label": "Snowflake",
            "pricing_model": "serverless components",
            "fresh_path_cost_usd": sf_total,
            "ingestion_cost_usd": sf_snowpipe,
            "mv_maintenance_cost_usd": sf_mv,
            "layout_maintenance_cost_usd": "",
            "bundled_write_service_cost_usd": "",
            "source_pair": "ClickHouse vs Snowflake",
        },
        {
            "label": "BigQuery · Capacity",
            "pricing_model": "Capacity",
            "fresh_path_cost_usd": bq_capacity_total,
            "ingestion_cost_usd": bq_write,
            "mv_maintenance_cost_usd": bq_mv_capacity,
            "layout_maintenance_cost_usd": reclustering,
            "bundled_write_service_cost_usd": "",
            "source_pair": "ClickHouse vs BigQuery",
        },
        {
            "label": "BigQuery · On-demand",
            "pricing_model": "On-demand",
            "fresh_path_cost_usd": bq_on_demand_total,
            "ingestion_cost_usd": bq_write,
            "mv_maintenance_cost_usd": bq_mv_on_demand,
            "layout_maintenance_cost_usd": reclustering,
            "bundled_write_service_cost_usd": "",
            "source_pair": "ClickHouse vs BigQuery",
        },
        {
            "label": "Redshift Serverless",
            "pricing_model": "writer uptime + MSK uptime",
            "fresh_path_cost_usd": rs_total,
            "ingestion_cost_usd": rs_msk,
            "mv_maintenance_cost_usd": rs_writer,
            "layout_maintenance_cost_usd": "",
            "bundled_write_service_cost_usd": "",
            "source_pair": "ClickHouse vs Redshift; shared by SUPER and typed read alternatives",
        },
    ]
    required_labels = validate_required_labels(
        manifest, "fresh_path_cost", (row["label"] for row in data_rows)
    )
    write_csv(csv_path, data_rows)
    write_json(
        summary_path,
        {
            "schema_version": 1,
            "chart": "global_complete_ingest_fresh_data_path_cost",
            "layout": layout,
            "contract": {
                "scope": "Complete ingestion; storage and read-query costs excluded",
                "source_operation": "Accepted pairwise fresh-path summary values are reused without query-window normalization",
                "pricing_alternatives": "BigQuery Capacity and On-demand MV refresh costs are counterfactual totals for the same work and are never added together",
                "redshift_shared_path": "One writer+MSK path is reused by both Redshift read alternatives; it is never split or doubled; client cross-AZ and RMS are excluded",
                "clickhouse_reconciliation": "Both pairwise summaries contain the same ClickHouse total and complete-ingest source hash",
                "required_labels": required_labels,
            },
            "presentation": {
                "total_label_placement": "directly after each bar; BigQuery alternatives share one compact endpoint label",
                "maximum_bar_width_axis_fraction": full_width,
            },
            "systems": {
                "clickhouse": {
                    "bundled_write_service_cost_usd": ch_total,
                    "total_cost_usd": ch_total,
                },
                "snowflake": {
                    "snowpipe_streaming_cost_usd": sf_snowpipe,
                    "serverless_mv_refresh_cost_usd": sf_mv,
                    "ingest_warehouse_used": False,
                    "total_cost_usd": sf_total,
                },
                "bigquery": {
                    "storage_write_api_cost_usd": bq_write,
                    "capacity_mv_refresh_cost_usd": bq_mv_capacity,
                    "on_demand_mv_refresh_cost_usd": bq_mv_on_demand,
                    "automatic_reclustering_separate_cost_usd": reclustering,
                    "capacity_total_cost_usd": bq_capacity_total,
                    "on_demand_total_cost_usd": bq_on_demand_total,
                },
                "redshift": {
                    "writer_workgroup_cost_usd": rs_writer,
                    "msk_cost_usd": rs_msk,
                    "client_cross_az_included": False,
                    "managed_storage_included": False,
                    "total_cost_usd": rs_total,
                },
            },
            "manifest": {
                "path": str(manifest_path),
                "sha256": sha256(manifest_path),
            },
            "sources": {
                "snowflake_pairwise_summary": {
                    "path": str(sf_path),
                    "sha256": sha256(sf_path),
                    "embedded_sources": sf["sources"],
                },
                "bigquery_pairwise_summary": {
                    "path": str(bq_path),
                    "sha256": sha256(bq_path),
                    "embedded_sources": bq["sources"],
                },
                "redshift_pairwise_summary": {
                    "path": str(rs_path),
                    "sha256": sha256(rs_path),
                    "embedded_sources": rs["sources"],
                },
            },
            "rows": data_rows,
            "outputs": {
                "png": str(png),
                "svg": str(svg),
                "csv": str(csv_path),
            },
        },
    )
    for path in (png, svg, csv_path, summary_path):
        print(f"Written: {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
