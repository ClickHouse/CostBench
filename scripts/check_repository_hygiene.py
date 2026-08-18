#!/usr/bin/env python3
"""Fail when tracked repository content contains credentials or obvious secrets."""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
CREDENTIAL_SUFFIX = "_credentials.txt"
MAX_SCANNED_BYTES = 8 * 1024 * 1024

# These intentionally target high-confidence token formats. Generic words such
# as "password" are not findings because benchmark scripts legitimately read
# credentials from environment variables and configuration objects.
SECRET_PATTERNS = (
    ("AWS access-key ID", re.compile(rb"(?:AKIA|ASIA)[0-9A-Z]{16}")),
    ("GitHub token", re.compile(rb"gh(?:p|o|u|s|r)_[A-Za-z0-9]{36,}")),
    ("Databricks personal-access token", re.compile(rb"dapi[0-9a-f]{32}")),
    ("Google API key", re.compile(rb"AIza[0-9A-Za-z_-]{35}")),
    (
        "private-key block",
        re.compile(rb"-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----"),
    ),
)


def tracked_paths() -> list[Path]:
    result = subprocess.run(
        ["git", "ls-files", "-z"],
        cwd=ROOT,
        check=True,
        stdout=subprocess.PIPE,
    )
    return [ROOT / item.decode() for item in result.stdout.split(b"\0") if item]


def main() -> int:
    findings: list[str] = []

    for path in tracked_paths():
        relative = path.relative_to(ROOT)
        # A locally deleted tracked file remains in the index until staged.
        # CI sees the committed tree, so skipping an absent worktree path keeps
        # the local check useful while a cleanup commit is being assembled.
        if not path.exists() and not path.is_symlink():
            continue
        if relative.name.lower().endswith(CREDENTIAL_SUFFIX):
            findings.append(f"forbidden credential filename: {relative}")
            continue

        try:
            if path.is_symlink() or path.stat().st_size > MAX_SCANNED_BYTES:
                continue
            payload = path.read_bytes()
        except OSError as exc:
            findings.append(f"could not inspect {relative}: {exc}")
            continue

        if b"\0" in payload:
            continue

        for label, pattern in SECRET_PATTERNS:
            if pattern.search(payload):
                findings.append(f"possible {label}: {relative}")

    if findings:
        print("Repository hygiene check failed:", file=sys.stderr)
        for finding in findings:
            print(f"- {finding}", file=sys.stderr)
        return 1

    print("Repository hygiene check passed: no tracked credential files or high-confidence secrets.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
