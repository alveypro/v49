#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import os
import sqlite3
from functools import lru_cache
from pathlib import Path
from typing import Dict, Optional


DEFAULT_STRATEGY_RISK: Dict[str, Dict[str, float]] = {
    "v4": {"stop_loss_pct": 0.06, "take_profit_pct": 0.08, "min_stop_loss_pct": 0.03, "max_stop_loss_pct": 0.10, "tp_sl_ratio": 1.4},
    "v5": {"stop_loss_pct": 0.055, "take_profit_pct": 0.09, "min_stop_loss_pct": 0.03, "max_stop_loss_pct": 0.10, "tp_sl_ratio": 1.5},
    "v6": {"stop_loss_pct": 0.05, "take_profit_pct": 0.10, "min_stop_loss_pct": 0.03, "max_stop_loss_pct": 0.10, "tp_sl_ratio": 1.6},
    "v7": {"stop_loss_pct": 0.07, "take_profit_pct": 0.15, "min_stop_loss_pct": 0.04, "max_stop_loss_pct": 0.14, "tp_sl_ratio": 1.8},
    "v8": {"stop_loss_pct": 0.06, "take_profit_pct": 0.12, "min_stop_loss_pct": 0.04, "max_stop_loss_pct": 0.12, "tp_sl_ratio": 1.8},
    "v9": {"stop_loss_pct": 0.06, "take_profit_pct": 0.12, "min_stop_loss_pct": 0.04, "max_stop_loss_pct": 0.12, "tp_sl_ratio": 1.8},
    "stable": {"stop_loss_pct": 0.06, "take_profit_pct": 0.14, "min_stop_loss_pct": 0.04, "max_stop_loss_pct": 0.12, "tp_sl_ratio": 2.0},
    "combo": {"stop_loss_pct": 0.06, "take_profit_pct": 0.12, "min_stop_loss_pct": 0.04, "max_stop_loss_pct": 0.12, "tp_sl_ratio": 1.8},
    "ai": {"stop_loss_pct": 0.06, "take_profit_pct": 0.12, "min_stop_loss_pct": 0.04, "max_stop_loss_pct": 0.12, "tp_sl_ratio": 1.8},
}


def normalize_strategy_name(strategy: str) -> str:
    s = str(strategy or "").strip().lower()
    if not s:
        return "v4"
    if s == "v4.0":
        return "v4"
    if s == "v5.0":
        return "v5"
    if s.startswith("v6"):
        return "v6"
    if s.startswith("v7"):
        return "v7"
    if s.startswith("v8"):
        return "v8"
    if s.startswith("v9"):
        return "v9"
    if s in {"stable_uptrend", "stable_uptrend_strategy"}:
        return "stable"
    return s


def _candidate_assistant_db_paths() -> list[str]:
    out = []
    env_path = os.getenv("TRADING_ASSISTANT_DB_PATH")
    if env_path:
        out.append(env_path)
    out.extend(
        [
            str(Path("/opt/openclaw/data/trading_assistant.db")),
            str(Path("/opt/airivo/data/trading_assistant.db")),
            str(Path(__file__).with_name("trading_assistant.db")),
        ]
    )
    # de-dup while preserving order
    seen = set()
    uniq = []
    for p in out:
        if p not in seen:
            uniq.append(p)
            seen.add(p)
    return uniq


def resolve_assistant_db_path(explicit: Optional[str] = None) -> Optional[str]:
    paths = [explicit] if explicit else _candidate_assistant_db_paths()
    for p in paths:
        if not p:
            continue
        if Path(p).exists():
            return p
    return None


@lru_cache(maxsize=16)
def _read_strategy_config_from_db(db_path: str, mtime: float) -> Dict[str, str]:
    del mtime
    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute(
            "SELECT key, value FROM config WHERE key LIKE 'risk\\_%' ESCAPE '\\'"
        ).fetchall()
        return {str(k): str(v) for k, v in rows}
    except Exception:
        return {}
    finally:
        conn.close()


def _safe_float(v: object, default: float) -> float:
    try:
        x = float(v)
        if x != x:  # NaN
            return default
        return x
    except Exception:
        return default


def get_strategy_risk_params(strategy: str, assistant_db_path: Optional[str] = None) -> Dict[str, float]:
    s = normalize_strategy_name(strategy)
    base = dict(DEFAULT_STRATEGY_RISK.get(s, DEFAULT_STRATEGY_RISK["v4"]))
    db_path = resolve_assistant_db_path(assistant_db_path)
    if not db_path:
        return base
    try:
        mtime = Path(db_path).stat().st_mtime
    except Exception:
        return base
    cfg = _read_strategy_config_from_db(db_path, mtime)

    def _get(name: str, default: float) -> float:
        return _safe_float(cfg.get(f"risk_{s}_{name}"), default)

    out = {
        "stop_loss_pct": _get("stop_loss_pct", base["stop_loss_pct"]),
        "take_profit_pct": _get("take_profit_pct", base["take_profit_pct"]),
        "min_stop_loss_pct": _get("min_stop_loss_pct", base["min_stop_loss_pct"]),
        "max_stop_loss_pct": _get("max_stop_loss_pct", base["max_stop_loss_pct"]),
        "tp_sl_ratio": _get("tp_sl_ratio", base["tp_sl_ratio"]),
    }
    out["min_stop_loss_pct"] = min(max(out["min_stop_loss_pct"], 0.02), 0.10)
    out["max_stop_loss_pct"] = min(max(out["max_stop_loss_pct"], out["min_stop_loss_pct"] + 0.01), 0.20)
    out["stop_loss_pct"] = min(max(out["stop_loss_pct"], out["min_stop_loss_pct"]), out["max_stop_loss_pct"])
    min_tp = max(0.04, out["stop_loss_pct"] * 1.2)
    out["take_profit_pct"] = min(max(out["take_profit_pct"], min_tp), 0.35)
    out["tp_sl_ratio"] = min(max(out["tp_sl_ratio"], 1.1), 3.5)
    return out


def list_all_strategy_risk_params(assistant_db_path: Optional[str] = None) -> Dict[str, Dict[str, float]]:
    out: Dict[str, Dict[str, float]] = {}
    for s in DEFAULT_STRATEGY_RISK.keys():
        out[s] = get_strategy_risk_params(s, assistant_db_path=assistant_db_path)
    return out
