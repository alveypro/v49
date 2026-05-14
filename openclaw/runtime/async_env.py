from __future__ import annotations

import os
from contextlib import contextmanager
from typing import Any, Dict


@contextmanager
def temp_environ(overrides: Dict[str, Any], env_lock: Any = None):
    snapshot: Dict[str, str | None] = {}

    def _apply():
        for key, value in (overrides or {}).items():
            if value is None:
                continue
            snapshot[key] = os.environ.get(key)
            os.environ[key] = str(value)

    def _restore():
        for key, old in snapshot.items():
            if old is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = old

    if env_lock is not None:
        with env_lock:
            try:
                _apply()
                yield
            finally:
                _restore()
        return

    try:
        _apply()
        yield
    finally:
        _restore()


def build_async_scan_env(strategy: str, params: Dict[str, Any]) -> Dict[str, Any]:
    payload = params or {}
    env: Dict[str, Any] = {}
    if strategy == "v4":
        env = {
            "V4_SCORE_THRESHOLD": payload.get("score_threshold"),
            "V4_TOP_PERCENT": payload.get("top_percent"),
            "V4_SELECT_MODE": payload.get("select_mode"),
            "V4_SCAN_ALL": "1" if payload.get("scan_all", True) else "0",
            "V4_CAP_MIN": payload.get("cap_min"),
            "V4_CAP_MAX": payload.get("cap_max"),
            "V4_ENABLE_CONSISTENCY": "1" if payload.get("enable_consistency", True) else "0",
            "V4_MIN_ALIGN": payload.get("min_align", 2),
        }
    elif strategy == "v5":
        env = {
            "V5_SCORE_THRESHOLD": payload.get("score_threshold"),
            "V5_TOP_PERCENT": payload.get("top_percent"),
            "V5_SELECT_MODE": payload.get("select_mode"),
            "V5_CAP_MIN": payload.get("cap_min"),
            "V5_CAP_MAX": payload.get("cap_max"),
            "V5_ENABLE_CONSISTENCY": "1" if payload.get("enable_consistency", True) else "0",
            "V5_MIN_ALIGN": payload.get("min_align", 2),
        }
    elif strategy == "v8":
        score_range = payload.get("score_threshold", [55, 70])
        if not isinstance(score_range, (list, tuple)) or len(score_range) != 2:
            score_range = [55, 70]
        env = {
            "V8_SCORE_MIN": score_range[0],
            "V8_SCORE_MAX": score_range[1],
            "V8_TOP_PERCENT": payload.get("top_percent"),
            "V8_SELECT_MODE": payload.get("select_mode"),
            "V8_SCAN_ALL": "1" if payload.get("scan_all", True) else "0",
            "V8_CAP_MIN": payload.get("cap_min"),
            "V8_CAP_MAX": payload.get("cap_max"),
            "V8_ENABLE_CONSISTENCY": "1" if payload.get("enable_consistency", True) else "0",
            "V8_MIN_ALIGN": payload.get("min_align", 2),
        }
    elif strategy == "v9":
        env = {
            "V9_SCORE_THRESHOLD": payload.get("score_threshold"),
            "V9_TOP_PERCENT": payload.get("top_percent"),
            "V9_SELECT_MODE": payload.get("select_mode"),
            "V9_SCAN_ALL": "1" if payload.get("scan_all", True) else "0",
            "V9_CAP_MIN": payload.get("cap_min"),
            "V9_CAP_MAX": payload.get("cap_max"),
            "V9_ENABLE_CONSISTENCY": "1" if payload.get("enable_consistency", True) else "0",
            "V9_MIN_ALIGN": payload.get("min_align", 2),
            "V9_HOLDING_DAYS": payload.get("holding_days", 20),
            "V9_LOOKBACK_DAYS": payload.get("lookback_days", 120),
            "V9_MIN_TURNOVER": payload.get("min_turnover", 5.0),
            "V9_CANDIDATE_COUNT": payload.get("candidate_count", 800),
        }
    elif strategy == "combo":
        env = {
            "COMBO_CANDIDATE_COUNT": payload.get("candidate_count", 800),
            "COMBO_MIN_TURNOVER": payload.get("min_turnover", 5.0),
            "COMBO_MIN_AGREE": payload.get("min_agree", 3),
            "COMBO_CAP_MIN": payload.get("cap_min", 0),
            "COMBO_CAP_MAX": payload.get("cap_max", 0),
            "COMBO_SELECT_MODE": payload.get("select_mode", "双重筛选(阈值+Top%)"),
            "COMBO_THRESHOLD": payload.get("combo_threshold", 68),
            "COMBO_TOP_PERCENT": payload.get("top_percent", 1),
            "COMBO_LOOKBACK_DAYS": payload.get("lookback_days", 90),
            "COMBO_DISAGREE_STD_WEIGHT": payload.get("disagree_std_weight", 0.35),
            "COMBO_DISAGREE_COUNT_WEIGHT": payload.get("disagree_count_weight", 1.0),
            "COMBO_MARKET_ADJUST_STRENGTH": payload.get("market_adjust_strength", 0.5),
            "COMBO_ENABLE_CONSISTENCY": "1" if payload.get("enable_consistency", True) else "0",
            "COMBO_MIN_ALIGN": payload.get("min_align", 2),
            "COMBO_AUTO_WEIGHTS": "1" if payload.get("auto_weights", True) else "0",
            "COMBO_LIGHTWEIGHT": "1" if payload.get("lightweight_mode", True) else "0",
            "COMBO_W_V4": payload.get("w_v4", 0.15),
            "COMBO_W_V5": payload.get("w_v5", 0.15),
            "COMBO_W_V7": payload.get("w_v7", 0.30),
            "COMBO_W_V8": payload.get("w_v8", 0.25),
            "COMBO_W_V9": payload.get("w_v9", 0.15),
            "COMBO_THR_V4": payload.get("thr_v4", 60),
            "COMBO_THR_V5": payload.get("thr_v5", 60),
            "COMBO_THR_V7": payload.get("thr_v7", 65),
            "COMBO_THR_V8": payload.get("thr_v8", 65),
            "COMBO_THR_V9": payload.get("thr_v9", 60),
        }
    elif strategy == "v6":
        env = {
            "V6_SCORE_THRESHOLD": payload.get("score_threshold", 85),
            "V6_TOP_PERCENT": payload.get("top_percent", 2),
            "V6_SELECT_MODE": payload.get("select_mode", "双重筛选(阈值+Top%)"),
            "V6_CAP_MIN": payload.get("cap_min", 0),
            "V6_CAP_MAX": payload.get("cap_max", 0),
            "V6_ENABLE_CONSISTENCY": "1" if payload.get("enable_consistency", True) else "0",
            "V6_MIN_ALIGN": payload.get("min_align", 2),
        }
    elif strategy == "v7":
        env = {
            "V7_SCORE_THRESHOLD": payload.get("score_threshold", 60),
            "V7_TOP_PERCENT": payload.get("top_percent", 2),
            "V7_SELECT_MODE": payload.get("select_mode", "双重筛选(阈值+Top%)"),
            "V7_SCAN_ALL": "1" if payload.get("scan_all", True) else "0",
            "V7_CAP_MIN": payload.get("cap_min", 0),
            "V7_CAP_MAX": payload.get("cap_max", 0),
            "V7_ENABLE_CONSISTENCY": "1" if payload.get("enable_consistency", True) else "0",
            "V7_MIN_ALIGN": payload.get("min_align", 2),
        }
    return {key: value for key, value in env.items() if value is not None}
