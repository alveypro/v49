"""Canonical path resolution helpers for OpenClaw tools.

This module provides a stable entry point for scripts that need
project-root-relative paths (especially the default SQLite DB path).
"""

from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path


_FALLBACK_ROOTS = [
    "/opt/openclaw/app",
    "/opt/openclaw",
    "/opt/airivo/app",
    "/opt/airivo",
]

_FALLBACK_DBS = [
    "/opt/openclaw/permanent_stock_database.db",
    "/opt/openclaw/permanent_stock_database.backup.db",
    "/opt/airivo/data/permanent_stock_database.db",
    "/opt/airivo/permanent_stock_database.db",
    "/opt/airivo/app/permanent_stock_database.db",
]


@lru_cache(maxsize=1)
def project_root() -> Path:
    env_root = os.getenv("OPENCLAW_ROOT", "").strip()
    if env_root and Path(env_root).is_dir():
        return Path(env_root).resolve()

    # Auto-detect from <root>/openclaw/paths.py.
    detected = Path(__file__).resolve().parents[1]
    if (detected / "openclaw").is_dir():
        return detected

    for fallback in _FALLBACK_ROOTS:
        candidate = Path(fallback)
        if candidate.is_dir():
            return candidate.resolve()
    return detected


def db_path(preferred: str | None = None) -> Path:
    candidates = [
        preferred or "",
        os.getenv("PERMANENT_DB_PATH", "").strip(),
        os.getenv("OPENCLAW_DB_PATH", "").strip(),
        os.getenv("AIRIVO_DB_PATH", "").strip(),
        str(project_root() / "permanent_stock_database.db"),
        str(project_root() / "permanent_stock_database.backup.db"),
    ] + _FALLBACK_DBS

    for candidate in candidates:
        if candidate and Path(candidate).exists():
            return Path(candidate).resolve()

    raise FileNotFoundError(
        "No DB found. Set PERMANENT_DB_PATH or OPENCLAW_ROOT. "
        f"Searched with project_root={project_root()}."
    )
