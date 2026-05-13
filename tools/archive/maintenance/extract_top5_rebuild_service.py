#!/usr/bin/env python3
"""One-off extractor: build openclaw/services/top5_trader_brief_rebuild_service.py from v49_app slices."""
from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
V49 = ROOT / "v49_app.py"
OUT = ROOT / "openclaw" / "services" / "top5_trader_brief_rebuild_service.py"

HEADER = '''# -*- coding: utf-8 -*-
"""Top5 trader brief rebuild pipeline — no Streamlit.

Moved from v49_app for headless scheduler/CLI/testing. Single source of truth
for: execution sync → accuracy payload → A/B degrade → md/csv/manifest export.
"""
from __future__ import annotations

import hashlib
import json
import math
import os
import re as _re
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Tuple

import numpy as np
import pandas as pd

from openclaw.paths import db_path as openclaw_db_path
from openclaw.paths import project_root as openclaw_project_root
from openclaw.services.top5_forward_return_evaluation_service import compact_trade_date

'''

FOOTER = '''

# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def make_top5_repo_context(repo_root: Path | None = None) -> Top5RepoContext:
    root = repo_root or openclaw_project_root()
    root = Path(root).resolve()
    perm = _resolve_permanent_db_path(root)
    return Top5RepoContext(
        repo_root=root,
        permanent_db_path=perm,
        sim_db_path=os.getenv("SIM_TRADING_DB_PATH", "").strip() or str(root / "sim_trading.db"),
        assistant_db_path=os.getenv("TRADING_ASSISTANT_DB_PATH", "").strip() or str(root / "trading_assistant.db"),
        bulk_history_chunk=int(os.getenv("BULK_HISTORY_CHUNK", "200") or 200),
    )


def audit_output_dir_for_context(ctx: Top5RepoContext) -> Path:
    env = os.getenv("STRATEGY_COMPETITION_AUDIT_OUTPUT_DIR", "").strip()
    if env:
        return Path(env).expanduser().resolve()
    return (ctx.repo_root / "logs" / "openclaw" / "strategy_competition_audit").resolve()


def advice_state_path(ctx: Top5RepoContext) -> Path:
    return ctx.repo_root / "logs" / "openclaw" / "top5_advice_version_state.json"


def load_top5_advice_version_state(*, repo_root: Path | None = None) -> Dict[str, Any]:
    ctx = make_top5_repo_context(repo_root)
    path = advice_state_path(ctx)
    default_state: Dict[str, Any] = {
        "active_version": "A",
        "previous_version": "",
        "min_samples_for_switch": 30,
        "auto_degrade_enabled": True,
        "frozen_at": "",
        "updated_at": "",
    }
    try:
        if not path.exists():
            return default_state
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            return default_state
        merged = dict(default_state)
        merged.update(payload)
        merged["active_version"] = str(merged.get("active_version") or "A").upper()
        merged["previous_version"] = str(merged.get("previous_version") or "").upper()
        merged["min_samples_for_switch"] = max(1, int(safe_float_any(merged.get("min_samples_for_switch"), 30.0)))
        merged["auto_degrade_enabled"] = bool(merged.get("auto_degrade_enabled", True))
        return merged
    except Exception:
        return default_state


def save_top5_advice_version_state(state: Dict[str, Any], *, repo_root: Path | None = None) -> None:
    ctx = make_top5_repo_context(repo_root)
    merged = load_top5_advice_version_state(repo_root=ctx.repo_root)
    merged.update(dict(state or {}))
    merged["active_version"] = str(merged.get("active_version") or "A").upper()
    merged["previous_version"] = str(merged.get("previous_version") or "").upper()
    merged["min_samples_for_switch"] = max(1, int(safe_float_any(merged.get("min_samples_for_switch"), 30.0)))
    merged["auto_degrade_enabled"] = bool(merged.get("auto_degrade_enabled", True))
    merged["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    path = advice_state_path(ctx)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(merged, ensure_ascii=False, indent=2), encoding="utf-8")


def compute_top5_advice_accuracy_payload(repo_root: Path | None = None) -> Dict[str, Any]:
    ctx = make_top5_repo_context(repo_root)
    return _compute_top5_advice_accuracy_payload(ctx)


def rebuild_top5_trader_brief_exports(repo_root: Path | None = None) -> Tuple[bool, str]:
    ctx = make_top5_repo_context(repo_root)
    audit_dir = audit_output_dir_for_context(ctx)
    artifacts = sorted(
        audit_dir.glob("strategy_competition_portfolio_audit_*.json"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    if not artifacts:
        return False, "未找到 top5 审计产物"
    accuracy_payload = _compute_top5_advice_accuracy_payload(ctx)
    version_state = load_top5_advice_version_state(repo_root=ctx.repo_root)
    active_version = str(version_state.get("active_version") or "A").upper()
    min_samples = max(1, int(safe_float_any(version_state.get("min_samples_for_switch"), 30.0)))
    auto_degrade_enabled = bool(version_state.get("auto_degrade_enabled", True))
    degrade_note = ""
    if auto_degrade_enabled and active_version in {"A", "B"}:
        ab_df = accuracy_payload.get("ab_compare", pd.DataFrame())
        joint_eval = _evaluate_top5_joint_switch(
            ab_df,
            target_version=active_version,
            min_samples_for_switch=min_samples,
        )
        has_ab_samples = isinstance(ab_df, pd.DataFrame) and not ab_df.empty
        if active_version == "B" and has_ab_samples and not bool(joint_eval.get("allow_switch")):
            fallback_version = "A"
            save_top5_advice_version_state(
                {
                    "previous_version": active_version,
                    "active_version": fallback_version,
                    "frozen_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                },
                repo_root=ctx.repo_root,
            )
            active_version = fallback_version
            degrade_note = f"；自动降级触发：B未通过联合判定，已回退到{fallback_version}"
    built = _build_top5_trader_brief_from_artifact(artifacts[0], advice_version=active_version, ctx=ctx)
    audit_report = _write_top5_advice_version_audit_report(accuracy_payload, ctx)
    execution_sync_df = accuracy_payload.get("execution_sync", pd.DataFrame())
    sync_suffix = ""
    if isinstance(execution_sync_df, pd.DataFrame) and not execution_sync_df.empty:
        sync_row = execution_sync_df.iloc[0]
        sync_suffix = (
            f"；成交回报增量入库={int(safe_float_any(sync_row.get('本次增量入库'), 0.0))}"
            f"（累计{int(safe_float_any(sync_row.get('统一表累计记录'), 0.0))}）"
        )
    suffix = f"；版本审计={audit_report}" if audit_report else ""
    return True, (
        f"已刷新并重建清单：表格文件={built.get('csv','')}；文档文件={built.get('markdown','')}；"
        f"一致性清单={built.get('manifest','')}；参数版本={active_version}{degrade_note}{sync_suffix}{suffix}"
    )


__all__ = [
    "Top5RepoContext",
    "make_top5_repo_context",
    "load_top5_advice_version_state",
    "save_top5_advice_version_state",
    "compute_top5_advice_accuracy_payload",
    "rebuild_top5_trader_brief_exports",
    "audit_output_dir_for_context",
]
'''


@dataclass
class Top5RepoContext:
    repo_root: Path
    permanent_db_path: str
    sim_db_path: str
    assistant_db_path: str
    bulk_history_chunk: int = 200


def _resolve_permanent_db_path(root: Path) -> str:
    cand = os.getenv("PERMANENT_DB_PATH", "").strip()
    if cand:
        return cand
    try:
        return str(openclaw_db_path(preferred=str(root / "permanent_stock_database.db")))
    except FileNotFoundError:
        return str(root / "permanent_stock_database.db")


def main() -> None:
    lines = V49.read_text(encoding="utf-8").splitlines()
    # 1-based line slices from v49 (inclusive)
    segments = [
        (10536, 10726),  # ensure tables + sync + load/save state (v49 uses TOP5_ADVICE_STATE_PATH)
        (10793, 10808),  # collect csv + signal date
        (10812, 11694),  # numerics through compute payload end
        (11697, 11737),  # write audit report
        (11795, 12103),  # resolve maps + derive + build (uses Path(__file__) parent)
    ]
    body: List[str] = []
    for lo, hi in segments:
        body.extend(lines[lo - 1 : hi])

    text = "\n".join(body) + "\n"

    # Strip old advice state + collect (we re-define public wrappers in FOOTER); first segment included load/save — remove duplicate defs later
    # Rename private helpers to module level: prefix _
    repl = [
        ("def _ensure_top5_execution_sync_tables", "def _ensure_top5_execution_sync_tables"),
        ("TOP5_EXECUTION_UNIFIED_TABLE", "TOP5_EXECUTION_UNIFIED_TABLE"),
        ("def _load_top5_advice_version_state()", "def _v49_load_top5_advice_version_state_REMOVED()"),
    ]

    for old, new in repl:
        text = text.replace(old, new)

    # Simpler: do targeted replacements on raw extract
    text = text.replace("TOP5_ADVICE_STATE_PATH", "advice_state_path(ctx)")
    text = text.replace("def _v49_load_top5_advice_version_state_REMOVED", "def _load_top5_advice_version_state")
    text = re.sub(
        r"def _load_top5_advice_version_state\(\) -> Dict\[str, Any\]:",
        "def _load_top5_advice_version_state(ctx: Top5RepoContext) -> Dict[str, Any]:",
        text,
        count=1,
    )
    text = re.sub(
        r"def _save_top5_advice_version_state\(state: Dict\[str, Any\]\) -> None:",
        "def _save_top5_advice_version_state(ctx: Top5RepoContext, state: Dict[str, Any]) -> None:",
        text,
        count=1,
    )
    text = text.replace("merged = _load_top5_advice_version_state()", "merged = _load_top5_advice_version_state(ctx)")
    text = text.replace(
        "if not TOP5_ADVICE_STATE_PATH.exists():",
        "path = advice_state_path(ctx)\n    if not path.exists():",
    )
    # Fix duplicate path line if broken
    text = text.replace(
        "path = advice_state_path(ctx)\n    if not path.exists():",
        "path = advice_state_path(ctx)\n    try:\n        if not path.exists():",
        1,
    )

    # The load_top5 function body became mangled - **Don't use automatic extract for load/save**

    raise SystemExit("manual pipeline only — generator abandoned")


if __name__ == "__main__":
    main()
