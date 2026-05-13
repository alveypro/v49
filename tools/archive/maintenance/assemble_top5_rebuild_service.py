#!/usr/bin/env python3
from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
MID = ROOT / "openclaw" / "services" / "_top5_rebuild_body.transformed.py"
OUT = ROOT / "openclaw" / "services" / "top5_trader_brief_rebuild_service.py"

HEADER = r'''# -*- coding: utf-8 -*-
"""Top5 trader brief rebuild pipeline — no Streamlit dependency.

Generated/assembled from v49_app logic (see tools/assemble_top5_rebuild_service.py).
"""
from __future__ import annotations

import contextvars
import hashlib
import json
import math
import os
import re
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

TOP5_EXECUTION_UNIFIED_TABLE = "top5_execution_feedback_unified"
TOP5_EXECUTION_SYNC_STATE_TABLE = "top5_execution_feedback_sync_state"

_REBUILD_CTX: contextvars.ContextVar[Optional["Top5RepoContext"]] = contextvars.ContextVar(
    "top5_repo_ctx", default=None
)


@dataclass(frozen=True)
class Top5RepoContext:
    repo_root: Path
    permanent_db_path: str
    sim_db_path: str
    assistant_db_path: str
    bulk_history_chunk: int = 200


def _ctx() -> Top5RepoContext:
    c = _REBUILD_CTX.get()
    if c is None:
        raise RuntimeError("Top5 rebuild context not set")
    return c


def _resolve_permanent_db_path(root: Path) -> str:
    cand = os.getenv("PERMANENT_DB_PATH", "").strip()
    if cand:
        return cand
    try:
        return str(openclaw_db_path(preferred=str(root / "permanent_stock_database.db")))
    except FileNotFoundError:
        return str(root / "permanent_stock_database.db")


def make_top5_repo_context(repo_root: Path | None = None) -> Top5RepoContext:
    root = Path(repo_root or openclaw_project_root()).resolve()
    return Top5RepoContext(
        repo_root=root,
        permanent_db_path=_resolve_permanent_db_path(root),
        sim_db_path=os.getenv("SIM_TRADING_DB_PATH", "").strip() or str(root / "sim_trading.db"),
        assistant_db_path=os.getenv("TRADING_ASSISTANT_DB_PATH", "").strip() or str(root / "trading_assistant.db"),
        bulk_history_chunk=int(os.getenv("BULK_HISTORY_CHUNK", "200") or 200),
    )


def advice_state_path(ctx: Top5RepoContext) -> Path:
    return ctx.repo_root / "logs" / "openclaw" / "top5_advice_version_state.json"


def audit_output_dir_for_context(ctx: Top5RepoContext) -> Path:
    env = os.getenv("STRATEGY_COMPETITION_AUDIT_OUTPUT_DIR", "").strip()
    if env:
        return Path(env).expanduser().resolve()
    return (ctx.repo_root / "logs" / "openclaw" / "strategy_competition_audit").resolve()


@contextmanager
def connect_db(db_path: str) -> Iterator[sqlite3.Connection]:
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    try:
        yield conn
    finally:
        conn.close()


def _canonical_ts_code(code: Any) -> str:
    return str(code or "").strip()


def _expand_ts_code_keys(code: Any) -> List[str]:
    c = _canonical_ts_code(code)
    if not c:
        return []
    keys = [c]
    if "." in c:
        digits = c.split(".")[0]
        if digits and digits not in keys:
            keys.append(digits)
    return keys


def _iter_sqlite_in_chunks(items: List[str], chunk_size: int) -> List[List[str]]:
    out: List[List[str]] = []
    step = max(1, int(chunk_size))
    for i in range(0, len(items), step):
        out.append(items[i : i + step])
    return out


def _safe_daily_table_name(name: str, fallback: str = "daily_trading_data") -> str:
    text = str(name or "").strip()
    if not text or not text.replace("_", "").isalnum():
        return fallback
    return text


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _ensure_price_aliases(df: pd.DataFrame) -> pd.DataFrame:
    if df is None:
        return df
    out = df.copy()
    alias_pairs = [
        ("close_price", "close"),
        ("open_price", "open"),
        ("high_price", "high"),
        ("low_price", "low"),
        ("vol", "volume"),
    ]
    for left, right in alias_pairs:
        if left in out.columns and right not in out.columns:
            out[right] = out[left]
        elif right in out.columns and left not in out.columns:
            out[left] = out[right]
    return out


def _normalize_stock_df(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    out = _ensure_price_aliases(df)
    if "trade_date" in out.columns:
        out = out.sort_values("trade_date").reset_index(drop=True)
    for col in (
        "close_price",
        "close",
        "open_price",
        "open",
        "high_price",
        "high",
        "low_price",
        "low",
        "vol",
        "volume",
        "pct_chg",
        "amount",
        "turnover_rate",
    ):
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")
    out = out.ffill().bfill()
    out = _ensure_price_aliases(out)
    return out


def _load_history_range_bulk(
    conn: sqlite3.Connection,
    ts_codes: List[str],
    start_date: str,
    end_date: str,
    columns: str,
    table: str = "daily_trading_data",
) -> Dict[str, pd.DataFrame]:
    if not ts_codes:
        return {}
    chunk_size = max(1, min(int(_ctx().bulk_history_chunk), 900 - 2))
    out: Dict[str, pd.DataFrame] = {}
    for i in range(0, len(ts_codes), chunk_size):
        chunk = ts_codes[i : i + chunk_size]
        placeholders = ",".join(["?"] * len(chunk))
        query = f"""
            SELECT {columns}
            FROM {table}
            WHERE ts_code IN ({placeholders})
              AND trade_date >= ?
              AND trade_date <= ?
            ORDER BY ts_code, trade_date
        """
        df = pd.read_sql_query(query, conn, params=chunk + [start_date, end_date])
        if df is None or df.empty:
            continue
        for ts_code, grp in df.groupby("ts_code"):
            out[str(ts_code)] = _normalize_stock_df(grp)
    return out


def _get_latest_prices(ts_codes: List[str]) -> Dict[str, Dict[str, Any]]:
    """Latest close by ts_code; mirrors v49 market_context_service fallback path."""
    if not ts_codes:
        return {}
    db_path = _ctx().permanent_db_path
    query_codes: List[str] = []
    for code in ts_codes:
        canonical = _canonical_ts_code(code)
        if canonical and canonical not in query_codes:
            query_codes.append(canonical)
    if not query_codes:
        return {}
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    try:
        table = "daily_trading_data"
        try:
            from data.dao import DataAccessError, detect_daily_table  # type: ignore

            try:
                table = detect_daily_table(conn)
            except DataAccessError:
                table = "daily_trading_data"
        except Exception:
            table = "daily_trading_data"
        table = _safe_daily_table_name(table, "daily_trading_data")
        cursor = conn.cursor()
        rows: List[Tuple[str, float, str]] = []
        for chunk in _iter_sqlite_in_chunks(query_codes, 900):
            placeholders = ",".join(["?"] * len(chunk))
            query = f"""
                WITH ranked AS (
                    SELECT ts_code, close_price, trade_date,
                           ROW_NUMBER() OVER (PARTITION BY ts_code ORDER BY trade_date DESC) AS rn
                    FROM {table}
                    WHERE ts_code IN ({placeholders})
                )
                SELECT ts_code, close_price, trade_date
                FROM ranked
                WHERE rn = 1
            """
            cursor.execute(query, chunk)
            rows.extend(cursor.fetchall())
    finally:
        conn.close()
    latest_by_code: Dict[str, Dict[str, Any]] = {}
    for ts_code, close_price, trade_date in rows:
        latest_by_code[str(ts_code)] = {"price": _safe_float(close_price), "trade_date": trade_date}
    latest: Dict[str, Dict[str, Any]] = {}
    for code in ts_codes:
        keys = _expand_ts_code_keys(code)
        hit: Optional[Dict[str, Any]] = None
        for key in keys:
            info = latest_by_code.get(key)
            if info:
                hit = info
                break
        if hit:
            for key in keys:
                latest[key] = hit
    return latest


'''

FOOTER = r'''


def _run_with_ctx(ctx: Top5RepoContext, fn, *args, **kwargs):
    tok = _REBUILD_CTX.set(ctx)
    try:
        return fn(*args, **kwargs)
    finally:
        _REBUILD_CTX.reset(tok)


def load_top5_advice_version_state(*, repo_root: Path | None = None) -> Dict[str, Any]:
    ctx = make_top5_repo_context(repo_root)
    return _run_with_ctx(ctx, _load_top5_advice_version_state)


def save_top5_advice_version_state(state: Dict[str, Any], *, repo_root: Path | None = None) -> None:
    ctx = make_top5_repo_context(repo_root)
    _run_with_ctx(ctx, _save_top5_advice_version_state, state)


def compute_top5_advice_accuracy_payload(repo_root: Path | None = None) -> Dict[str, Any]:
    ctx = make_top5_repo_context(repo_root)
    return _run_with_ctx(ctx, _compute_top5_advice_accuracy_payload)


def rebuild_top5_trader_brief_exports(repo_root: Path | None = None) -> Tuple[bool, str]:
    ctx = make_top5_repo_context(repo_root)
    tok = _REBUILD_CTX.set(ctx)
    try:
        audit_dir = audit_output_dir_for_context(ctx)
        artifacts = sorted(
            audit_dir.glob("strategy_competition_portfolio_audit_*.json"),
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )
        if not artifacts:
            return False, "未找到 top5 审计产物"
        accuracy_payload = _compute_top5_advice_accuracy_payload()
        version_state = _load_top5_advice_version_state()
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
                _save_top5_advice_version_state(
                    {
                        "previous_version": active_version,
                        "active_version": fallback_version,
                        "frozen_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    }
                )
                active_version = fallback_version
                degrade_note = f"；自动降级触发：B未通过联合判定，已回退到{fallback_version}"
        built = _build_top5_trader_brief_from_artifact(artifacts[0], advice_version=active_version)
        audit_report = _write_top5_advice_version_audit_report(accuracy_payload)
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
    finally:
        _REBUILD_CTX.reset(tok)


__all__ = [
    "Top5RepoContext",
    "make_top5_repo_context",
    "advice_state_path",
    "audit_output_dir_for_context",
    "load_top5_advice_version_state",
    "save_top5_advice_version_state",
    "compute_top5_advice_accuracy_payload",
    "rebuild_top5_trader_brief_exports",
]
'''


def main() -> None:
    mid = MID.read_text(encoding="utf-8")
    # Body defines _safe_float_any; footer uses safe_float_any — alias
    if "def safe_float_any" not in mid:
        mid = mid.replace("def _safe_float_any", "def safe_float_any", 1)
        mid = mid.replace("_safe_float_any(", "safe_float_any(")
    OUT.write_text(HEADER + mid + FOOTER, encoding="utf-8")
    print("wrote", OUT, "lines", len(OUT.read_text().splitlines()))


if __name__ == "__main__":
    main()
