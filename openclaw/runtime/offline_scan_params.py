from __future__ import annotations

from typing import Any, Dict
import os


def _env_bool(name: str, default: bool) -> bool:
    fallback = "1" if default else "0"
    return os.getenv(name, fallback) == "1"


def load_common_scan_params(
    *,
    prefix: str,
    score_threshold_default: float,
    top_percent_default: int,
    select_mode_default: str,
    cap_min_default: float,
    cap_max_default: float,
    include_scan_all: bool,
) -> Dict[str, Any]:
    params: Dict[str, Any] = {
        "score_threshold": float(os.getenv(f"{prefix}_SCORE_THRESHOLD", str(score_threshold_default))),
        "top_percent": int(os.getenv(f"{prefix}_TOP_PERCENT", str(top_percent_default))),
        "select_mode": os.getenv(f"{prefix}_SELECT_MODE", select_mode_default),
        "cap_min": float(os.getenv(f"{prefix}_CAP_MIN", str(cap_min_default))),
        "cap_max": float(os.getenv(f"{prefix}_CAP_MAX", str(cap_max_default))),
        "enable_consistency": _env_bool(f"{prefix}_ENABLE_CONSISTENCY", True),
        "min_align": int(os.getenv(f"{prefix}_MIN_ALIGN", "2")),
    }
    if include_scan_all:
        params["scan_all"] = _env_bool(f"{prefix}_SCAN_ALL", True)
    return params


def load_v8_scan_params() -> Dict[str, Any]:
    return {
        "score_min": float(os.getenv("V8_SCORE_MIN", "55")),
        "score_max": float(os.getenv("V8_SCORE_MAX", "70")),
        "top_percent": int(os.getenv("V8_TOP_PERCENT", "2")),
        "select_mode": os.getenv("V8_SELECT_MODE", "双重筛选(阈值+Top%)"),
        "scan_all": _env_bool("V8_SCAN_ALL", True),
        "cap_min": float(os.getenv("V8_CAP_MIN", "0")),
        "cap_max": float(os.getenv("V8_CAP_MAX", "0")),
        "enable_consistency": _env_bool("V8_ENABLE_CONSISTENCY", True),
        "min_align": int(os.getenv("V8_MIN_ALIGN", "2")),
    }
