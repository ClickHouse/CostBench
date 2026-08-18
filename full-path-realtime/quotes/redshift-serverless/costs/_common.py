"""Shared path helpers for portable Redshift cost provenance."""

from __future__ import annotations

from pathlib import Path

FULL_PATH_ROOT = Path(__file__).resolve().parents[3]


def portable_path(path: Path) -> str:
    """Return repository-local paths relative to full-path-realtime."""
    resolved = path.expanduser().resolve()
    try:
        return str(resolved.relative_to(FULL_PATH_ROOT))
    except ValueError:
        return str(resolved)
