"""Canonical path resolution for the entire OpenClaw stack.

Every script should import from here instead of hardcoding absolute paths.
Resolution order:
  1. Explicit env-var override
  2. Relative to OPENCLAW_ROOT (auto-detected or env)
  3. Well-known absolute fallbacks (server / legacy)
"""

from __future__ import annotations

import os
from pathlib import Path
from functools import lru_cache

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
    env = os.getenv("OPENCLAW_ROOT", "").strip()
    if env and Path(env).is_dir():
        return Path(env).resolve()
    # Auto-detect: this file lives at <root>/openclaw/paths.py
    detected = Path(__file__).resolve().parents[1]
    if (detected / "openclaw").is_dir():
        return detected
    for fb in _FALLBACK_ROOTS:
        if Path(fb).is_dir():
            return Path(fb).resolve()
    return detected


def db_path(preferred: str | None = None) -> Path:
    """Resolve the primary stock database file."""
    candidates = [
        preferred or "",
        os.getenv("PERMANENT_DB_PATH", "").strip(),
        os.getenv("OPENCLAW_DB_PATH", "").strip(),
        os.getenv("AIRIVO_DB_PATH", "").strip(),
        str(project_root() / "permanent_stock_database.db"),
        str(project_root() / "permanent_stock_database.backup.db"),
    ] + _FALLBACK_DBS
    for p in candidates:
        if p and Path(p).exists():
            return Path(p).resolve()
    raise FileNotFoundError(
        "No DB found. Set PERMANENT_DB_PATH or OPENCLAW_ROOT. "
        f"Searched: project_root={project_root()}"
    )


def backup_db_path() -> Path:
    return project_root() / "permanent_stock_database.backup.db"


def cache_dir() -> Path:
    env = os.getenv("AIRIVO_CACHE_DIR", "").strip()
    if env:
        return Path(env)
    return project_root() / "cache_v9"


def log_dir() -> Path:
    d = project_root() / "logs" / "openclaw"
    d.mkdir(parents=True, exist_ok=True)
    return d


def config_dir() -> Path:
    return project_root() / "openclaw" / "config"


def v49_module_path() -> Path:
    return project_root() / "终极量价暴涨系统_v49.0_长期稳健版.py"


def venv_python() -> Path:
    candidates = [
        project_root() / ".venv" / "bin" / "python",
        project_root() / "venv311" / "bin" / "python",
        Path("/opt/openclaw/venv311/bin/python"),
    ]
    for c in candidates:
        if c.exists():
            return c
    return Path("python3")


def env_file() -> Path:
    return project_root() / ".env"


def evolution_dir() -> Path:
    return project_root() / "evolution"


def doctor_log_dir() -> Path:
    d = project_root() / "logs" / "doctor"
    d.mkdir(parents=True, exist_ok=True)
    return d
