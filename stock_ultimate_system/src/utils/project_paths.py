from __future__ import annotations

import os
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
ARTIFACTS_DIR_ENV = "STOCK_ULTIMATE_ARTIFACTS_DIR"
EXPERIMENTS_DIR_ENV = "STOCK_ULTIMATE_EXPERIMENTS_DIR"
REPORTS_DIR_ENV = "STOCK_ULTIMATE_REPORTS_DIR"


def _resolve_base_dir(*, env_var: str, default_relative_path: str) -> Path:
    configured = str(os.environ.get(env_var, "") or "").strip()
    if configured:
        return Path(configured)
    return PROJECT_ROOT / default_relative_path


def _resolve_under_base(base_dir: Path, path: str | Path | None = None, *, base_markers: tuple[str, ...] = ()) -> Path:
    if path is None:
        return base_dir
    candidate = Path(path)
    if candidate.is_absolute():
        return candidate
    if str(candidate) in {"", "."}:
        return base_dir
    if str(candidate) in base_markers:
        return base_dir
    return base_dir / candidate


def resolve_project_path(path: str | Path) -> Path:
    candidate = Path(path)
    if candidate.is_absolute():
        return candidate
    return PROJECT_ROOT / candidate


def resolve_artifacts_path(path: str | Path | None = None) -> Path:
    return _resolve_under_base(
        _resolve_base_dir(env_var=ARTIFACTS_DIR_ENV, default_relative_path="artifacts"),
        path,
        base_markers=("artifacts",),
    )


def resolve_experiments_path(path: str | Path | None = None) -> Path:
    return _resolve_under_base(
        _resolve_base_dir(env_var=EXPERIMENTS_DIR_ENV, default_relative_path="data/experiments"),
        path,
        base_markers=("data/experiments",),
    )


def resolve_reports_path(path: str | Path | None = None) -> Path:
    return _resolve_under_base(
        _resolve_base_dir(env_var=REPORTS_DIR_ENV, default_relative_path="data/reports"),
        path,
        base_markers=("data/reports",),
    )
