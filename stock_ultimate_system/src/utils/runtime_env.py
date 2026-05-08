from __future__ import annotations

import os
import tempfile
from pathlib import Path
from typing import Any


def _safe_cpu_count() -> int:
    return max(1, int(os.cpu_count() or 1))


def default_model_threads() -> int:
    raw = os.getenv('STOCK_SYSTEM_MODEL_THREADS', '').strip()
    if raw:
        try:
            return max(1, int(raw))
        except ValueError:
            pass
    return min(4, _safe_cpu_count())


def configure_runtime_environment() -> None:
    os.environ.setdefault('LOKY_MAX_CPU_COUNT', str(_safe_cpu_count()))
    mpl_config = Path(os.getenv('MPLCONFIGDIR', '')).expanduser() if os.getenv('MPLCONFIGDIR') else None
    if not mpl_config:
        mpl_config = Path(tempfile.gettempdir()) / 'stock_ultimate_system_mpl'
        os.environ.setdefault('MPLCONFIGDIR', str(mpl_config))
    mpl_config.mkdir(parents=True, exist_ok=True)


def with_model_threads(params: dict[str, Any] | None, *keys: str) -> dict[str, Any]:
    merged = dict(params or {})
    if any(key in merged for key in keys):
        return merged
    threads = default_model_threads()
    for key in keys:
        merged[key] = threads
    return merged


def ensure_parent_dir(path: str | Path) -> Path:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    return target
