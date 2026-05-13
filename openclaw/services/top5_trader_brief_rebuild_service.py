# -*- coding: utf-8 -*-
"""Top5 trader brief rebuild pipeline — no Streamlit dependency.

Generated/assembled from v49_app logic
(see tools/archive/maintenance/assemble_top5_rebuild_service.py).
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
from openclaw.services.strategy_evidence_gate_service import evaluate_top5_artifact_canary_gate
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


def _ensure_top5_execution_sync_tables(conn: sqlite3.Connection) -> None:
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {TOP5_EXECUTION_UNIFIED_TABLE} (
            order_id TEXT PRIMARY KEY,
            decision_id TEXT,
            ts_code TEXT,
            side TEXT,
            status TEXT,
            source_type TEXT,
            submitted_at TEXT,
            created_at TEXT,
            updated_at TEXT,
            last_fill_time TEXT,
            filled_qty REAL,
            avg_fill_price REAL,
            total_fill_fee REAL,
            fill_slippage_bp REAL,
            fill_ratio REAL,
            decision_price REAL,
            submit_price REAL,
            close_price REAL,
            miss_reason_code TEXT,
            event_time TEXT,
            synced_at TEXT
        )
        """
    )
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {TOP5_EXECUTION_SYNC_STATE_TABLE} (
            key TEXT PRIMARY KEY,
            value TEXT,
            updated_at TEXT
        )
        """
    )
    conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{TOP5_EXECUTION_UNIFIED_TABLE}_code_time ON {TOP5_EXECUTION_UNIFIED_TABLE}(ts_code, event_time)")
    conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{TOP5_EXECUTION_UNIFIED_TABLE}_status ON {TOP5_EXECUTION_UNIFIED_TABLE}(status)")


def _sync_top5_execution_feedback_unified() -> Dict[str, Any]:
    now_text = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    stats: Dict[str, Any] = {
        "synced_rows": 0,
        "cursor": "",
        "unified_rows": 0,
        "updated_at": now_text,
        "ok": True,
        "error": "",
    }
    try:
        with connect_db(_ctx().permanent_db_path) as conn:
            _ensure_top5_execution_sync_tables(conn)
            row = conn.execute(
                f"SELECT value FROM {TOP5_EXECUTION_SYNC_STATE_TABLE} WHERE key='last_event_time' LIMIT 1"
            ).fetchone()
            cursor = str((row[0] if row else "") or "")
            sql = (
                "WITH fill_agg AS ("
                "  SELECT order_id, "
                "         SUM(COALESCE(fill_qty, 0.0)) AS filled_qty, "
                "         CASE WHEN SUM(COALESCE(fill_qty, 0.0)) > 0 "
                "              THEN SUM(COALESCE(fill_price, 0.0) * COALESCE(fill_qty, 0.0)) / SUM(COALESCE(fill_qty, 0.0)) "
                "              ELSE 0.0 END AS avg_fill_price, "
                "         SUM(COALESCE(fill_fee, 0.0)) AS total_fill_fee, "
                "         MAX(COALESCE(fill_slippage_bp, 0.0)) AS fill_slippage_bp, "
                "         MAX(COALESCE(fill_time, '')) AS last_fill_time "
                "  FROM execution_fills "
                "  GROUP BY order_id"
                ") "
                "SELECT "
                "  o.order_id, o.decision_id, o.ts_code, o.side, o.status, COALESCE(o.source_type, ''), "
                "  COALESCE(o.submitted_at, ''), COALESCE(o.created_at, ''), COALESCE(o.updated_at, ''), "
                "  COALESCE(f.last_fill_time, ''), COALESCE(f.filled_qty, 0.0), COALESCE(f.avg_fill_price, 0.0), "
                "  COALESCE(f.total_fill_fee, 0.0), COALESCE(f.fill_slippage_bp, 0.0), "
                "  COALESCE(a.fill_ratio, 0.0), COALESCE(a.decision_price, o.decision_price, 0.0), "
                "  COALESCE(a.submit_price, o.submitted_price, 0.0), COALESCE(a.close_price, 0.0), "
                "  COALESCE(a.miss_reason_code, ''), "
                "  MAX(COALESCE(o.updated_at, o.created_at, ''), COALESCE(a.updated_at, ''), COALESCE(f.last_fill_time, '')) AS event_time "
                "FROM execution_orders o "
                "LEFT JOIN fill_agg f ON f.order_id = o.order_id "
                "LEFT JOIN execution_attribution a ON a.order_id = o.order_id "
            )
            params: Tuple[Any, ...] = ()
            if cursor:
                sql += (
                    "WHERE MAX(COALESCE(o.updated_at, o.created_at, ''), COALESCE(a.updated_at, ''), COALESCE(f.last_fill_time, '')) > ? "
                )
                params = (cursor,)
            sql += "ORDER BY event_time ASC, o.order_id ASC"
            rows = conn.execute(sql, params).fetchall()
            max_event_time = cursor
            upsert_sql = (
                f"INSERT INTO {TOP5_EXECUTION_UNIFIED_TABLE} ("
                "order_id, decision_id, ts_code, side, status, source_type, submitted_at, created_at, updated_at, "
                "last_fill_time, filled_qty, avg_fill_price, total_fill_fee, fill_slippage_bp, fill_ratio, decision_price, "
                "submit_price, close_price, miss_reason_code, event_time, synced_at"
                ") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) "
                "ON CONFLICT(order_id) DO UPDATE SET "
                "decision_id=excluded.decision_id, ts_code=excluded.ts_code, side=excluded.side, status=excluded.status, "
                "source_type=excluded.source_type, submitted_at=excluded.submitted_at, created_at=excluded.created_at, "
                "updated_at=excluded.updated_at, last_fill_time=excluded.last_fill_time, filled_qty=excluded.filled_qty, "
                "avg_fill_price=excluded.avg_fill_price, total_fill_fee=excluded.total_fill_fee, "
                "fill_slippage_bp=excluded.fill_slippage_bp, fill_ratio=excluded.fill_ratio, decision_price=excluded.decision_price, "
                "submit_price=excluded.submit_price, close_price=excluded.close_price, miss_reason_code=excluded.miss_reason_code, "
                "event_time=excluded.event_time, synced_at=excluded.synced_at"
            )
            for item in rows:
                record = (
                    str(item[0] or ""),
                    str(item[1] or ""),
                    str(item[2] or ""),
                    str(item[3] or ""),
                    str(item[4] or ""),
                    str(item[5] or ""),
                    str(item[6] or ""),
                    str(item[7] or ""),
                    str(item[8] or ""),
                    str(item[9] or ""),
                    float(safe_float_any(item[10], 0.0)),
                    float(safe_float_any(item[11], 0.0)),
                    float(safe_float_any(item[12], 0.0)),
                    float(safe_float_any(item[13], 0.0)),
                    float(safe_float_any(item[14], 0.0)),
                    float(safe_float_any(item[15], 0.0)),
                    float(safe_float_any(item[16], 0.0)),
                    float(safe_float_any(item[17], 0.0)),
                    str(item[18] or ""),
                    str(item[19] or ""),
                    now_text,
                )
                conn.execute(upsert_sql, record)
                et = str(item[19] or "")
                if et and et > max_event_time:
                    max_event_time = et
            if max_event_time and max_event_time != cursor:
                conn.execute(
                    (
                        f"INSERT INTO {TOP5_EXECUTION_SYNC_STATE_TABLE} (key, value, updated_at) VALUES ('last_event_time', ?, ?) "
                        "ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at"
                    ),
                    (max_event_time, now_text),
                )
            conn.commit()
            unified_cnt = conn.execute(f"SELECT COUNT(*) FROM {TOP5_EXECUTION_UNIFIED_TABLE}").fetchone()
            stats["synced_rows"] = int(len(rows))
            stats["cursor"] = str(max_event_time or cursor or "")
            stats["unified_rows"] = int(unified_cnt[0] if unified_cnt else 0)
    except Exception as exc:
        stats["ok"] = False
        stats["error"] = str(exc)
    return stats


def _load_top5_advice_version_state() -> Dict[str, Any]:
    default_state: Dict[str, Any] = {
        "active_version": "A",
        "previous_version": "",
        "min_samples_for_switch": 30,
        "auto_degrade_enabled": True,
        "frozen_at": "",
        "updated_at": "",
    }
    try:
        if not advice_state_path(_ctx()).exists():
            return default_state
        payload = json.loads(advice_state_path(_ctx()).read_text(encoding="utf-8"))
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


def _save_top5_advice_version_state(state: Dict[str, Any]) -> None:
    merged = _load_top5_advice_version_state()
    merged.update(dict(state or {}))
    merged["active_version"] = str(merged.get("active_version") or "A").upper()
    merged["previous_version"] = str(merged.get("previous_version") or "").upper()
    merged["min_samples_for_switch"] = max(1, int(safe_float_any(merged.get("min_samples_for_switch"), 30.0)))
    merged["auto_degrade_enabled"] = bool(merged.get("auto_degrade_enabled", True))
    merged["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    advice_state_path(_ctx()).parent.mkdir(parents=True, exist_ok=True)
    advice_state_path(_ctx()).write_text(json.dumps(merged, ensure_ascii=False, indent=2), encoding="utf-8")
def _collect_top5_trader_brief_csvs() -> List[Path]:
    exports_dir = _ctx().repo_root / "exports"
    if not exports_dir.exists():
        return []
    return sorted(
        exports_dir.glob("top5_trader_brief_*.csv"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )


def _top5_brief_signal_date(path: Path) -> str:
    m = re.search(r"top5_trader_brief_(\d{8})", str(path.name))
    if not m:
        return ""
    raw = str(m.group(1))
def safe_float_any(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _normal_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(float(x) / math.sqrt(2.0)))


def _mean_diff_pvalue(sample_a: List[float], sample_b: List[float]) -> Tuple[float, float]:
    arr_a = np.asarray([float(x) for x in sample_a if np.isfinite(float(x))], dtype=float)
    arr_b = np.asarray([float(x) for x in sample_b if np.isfinite(float(x))], dtype=float)
    if len(arr_a) < 2 or len(arr_b) < 2:
        return 1.0, 0.0
    mean_a = float(np.mean(arr_a))
    mean_b = float(np.mean(arr_b))
    var_a = float(np.var(arr_a, ddof=1))
    var_b = float(np.var(arr_b, ddof=1))
    se = math.sqrt(max(var_a / max(len(arr_a), 1) + var_b / max(len(arr_b), 1), 1e-12))
    z = (mean_b - mean_a) / se if se > 0 else 0.0
    p = 2.0 * (1.0 - _normal_cdf(abs(z)))
    return max(min(float(p), 1.0), 0.0), float(z)


def _pct_text_to_decimal(value: Any) -> float:
    text = str(value or "").strip()
    if not text:
        return 0.0
    if text.endswith("%"):
        return safe_float_any(text[:-1], 0.0) / 100.0
    return safe_float_any(text, 0.0)


def _normalize_trade_day_text(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    try:
        dt = pd.to_datetime(text, errors="coerce")
        if pd.isna(dt):
            return text[:10]
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return text[:10]


def _load_sim_realized_return_rows() -> pd.DataFrame:
    path = Path(_ctx().sim_db_path)
    if not path.exists():
        return pd.DataFrame()
    try:
        conn = sqlite3.connect(str(path))
        trade_df = pd.read_sql_query(
            "SELECT trade_date, ts_code, side, price, shares FROM sim_trades ORDER BY trade_date ASC, id ASC",
            conn,
        )
        conn.close()
    except Exception:
        return pd.DataFrame()
    if trade_df.empty:
        return pd.DataFrame()
    rows: List[Dict[str, Any]] = []
    for ts_code, g in trade_df.groupby("ts_code"):
        lots: List[Dict[str, Any]] = []
        for _, row in g.iterrows():
            side = str(row.get("side") or "").lower().strip()
            price = safe_float_any(row.get("price"), 0.0)
            shares = max(0, int(safe_float_any(row.get("shares"), 0.0)))
            trade_day = _normalize_trade_day_text(row.get("trade_date"))
            if not ts_code or not trade_day or price <= 0 or shares <= 0:
                continue
            if side == "buy":
                lots.append({"entry_day": trade_day, "price": price, "shares": shares})
                continue
            if side != "sell":
                continue
            remain = shares
            matched_shares = 0
            buy_amount = 0.0
            entry_day = ""
            while remain > 0 and lots:
                lot = lots[0]
                lot_shares = int(lot.get("shares", 0))
                if lot_shares <= 0:
                    lots.pop(0)
                    continue
                used = min(remain, lot_shares)
                matched_shares += used
                buy_amount += float(lot.get("price", 0.0)) * float(used)
                if not entry_day:
                    entry_day = str(lot.get("entry_day") or "")
                lot["shares"] = lot_shares - used
                remain -= used
                if int(lot.get("shares", 0)) <= 0:
                    lots.pop(0)
            if matched_shares <= 0 or buy_amount <= 0:
                continue
            sell_amount = float(price) * float(matched_shares)
            rows.append(
                {
                    "ts_code": str(ts_code),
                    "entry_date": entry_day,
                    "exit_date": trade_day,
                    "realized_return": (sell_amount - buy_amount) / max(buy_amount, 1e-9),
                    "source": "sim_trades",
                }
            )
    return pd.DataFrame(rows)


def _load_assistant_realized_return_rows() -> pd.DataFrame:
    path = Path(_ctx().assistant_db_path)
    if not path.exists():
        return pd.DataFrame()
    try:
        conn = sqlite3.connect(str(path))
        trade_df = pd.read_sql_query(
            (
                "SELECT trade_date, ts_code, action, price, quantity, amount, profit_loss, profit_loss_pct "
                "FROM trade_history ORDER BY trade_date ASC, id ASC"
            ),
            conn,
        )
        conn.close()
    except Exception:
        return pd.DataFrame()
    if trade_df.empty:
        return pd.DataFrame()
    rows: List[Dict[str, Any]] = []
    for ts_code, g in trade_df.groupby("ts_code"):
        lots: List[Dict[str, Any]] = []
        for _, row in g.iterrows():
            action = str(row.get("action") or "").lower().strip()
            price = safe_float_any(row.get("price"), 0.0)
            shares = max(0, int(safe_float_any(row.get("quantity"), 0.0)))
            trade_day = _normalize_trade_day_text(row.get("trade_date"))
            if not ts_code or not trade_day or shares <= 0:
                continue
            if action == "buy":
                amount = safe_float_any(row.get("amount"), 0.0)
                if amount <= 0 and price > 0:
                    amount = price * float(shares)
                if amount <= 0:
                    continue
                lots.append({"entry_day": trade_day, "amount": amount, "shares": shares})
                continue
            if action != "sell":
                continue
            remain = shares
            matched_shares = 0
            buy_amount = 0.0
            entry_day = ""
            while remain > 0 and lots:
                lot = lots[0]
                lot_shares = int(lot.get("shares", 0))
                if lot_shares <= 0:
                    lots.pop(0)
                    continue
                used = min(remain, lot_shares)
                lot_unit_amount = float(lot.get("amount", 0.0)) / max(float(lot_shares), 1e-9)
                buy_amount += lot_unit_amount * float(used)
                matched_shares += used
                if not entry_day:
                    entry_day = str(lot.get("entry_day") or "")
                lot["shares"] = lot_shares - used
                lot["amount"] = float(lot.get("amount", 0.0)) - (lot_unit_amount * float(used))
                remain -= used
                if int(lot.get("shares", 0)) <= 0:
                    lots.pop(0)
            if matched_shares <= 0:
                continue
            ret = safe_float_any(row.get("profit_loss_pct"), np.nan)
            if not np.isfinite(ret):
                pl = safe_float_any(row.get("profit_loss"), 0.0)
                if buy_amount > 0:
                    ret = pl / max(buy_amount, 1e-9)
                else:
                    ret = np.nan
            if not np.isfinite(ret):
                continue
            rows.append(
                {
                    "ts_code": str(ts_code),
                    "entry_date": entry_day,
                    "exit_date": trade_day,
                    "realized_return": float(ret),
                    "source": "trade_history",
                }
            )
    return pd.DataFrame(rows)


def _load_actual_execution_return_rows() -> pd.DataFrame:
    frames: List[pd.DataFrame] = []
    sim_df = _load_sim_realized_return_rows()
    if isinstance(sim_df, pd.DataFrame) and not sim_df.empty:
        frames.append(sim_df)
    assistant_df = _load_assistant_realized_return_rows()
    if isinstance(assistant_df, pd.DataFrame) and not assistant_df.empty:
        frames.append(assistant_df)
    if not frames:
        return pd.DataFrame()
    out = pd.concat(frames, ignore_index=True)
    out["entry_date"] = out["entry_date"].astype(str).map(_normalize_trade_day_text)
    out["exit_date"] = out["exit_date"].astype(str).map(_normalize_trade_day_text)
    out["realized_return"] = pd.to_numeric(out["realized_return"], errors="coerce")
    out = out.dropna(subset=["realized_return"])
    if out.empty:
        return pd.DataFrame()
    out = out.sort_values(["ts_code", "entry_date", "exit_date"]).reset_index(drop=True)
    return out


def _load_unified_execution_fill_rows() -> pd.DataFrame:
    try:
        with connect_db(_ctx().permanent_db_path) as conn:
            _ensure_top5_execution_sync_tables(conn)
            df = pd.read_sql_query(
                (
                    f"SELECT ts_code, side, status, filled_qty, avg_fill_price, close_price, "
                    f"COALESCE(last_fill_time, submitted_at, created_at, event_time, '') AS fill_time, event_time "
                    f"FROM {TOP5_EXECUTION_UNIFIED_TABLE} "
                    "WHERE side='buy' AND COALESCE(filled_qty,0) > 0 AND COALESCE(avg_fill_price,0) > 0 "
                    "ORDER BY event_time ASC"
                ),
                conn,
            )
    except Exception:
        return pd.DataFrame()
    if df.empty:
        return pd.DataFrame()
    df["fill_date"] = df["fill_time"].astype(str).map(_normalize_trade_day_text)
    df["avg_fill_price"] = pd.to_numeric(df["avg_fill_price"], errors="coerce")
    df["filled_qty"] = pd.to_numeric(df["filled_qty"], errors="coerce").fillna(0.0)
    df["close_price"] = pd.to_numeric(df.get("close_price"), errors="coerce")
    df = df.dropna(subset=["avg_fill_price"])
    if df.empty:
        return pd.DataFrame()
    return df.reset_index(drop=True)


def _lookup_actual_execution_return(
    actual_df: pd.DataFrame,
    *,
    ts_code: str,
    signal_date: str,
    hold_days: int,
) -> Optional[float]:
    if actual_df is None or actual_df.empty or not ts_code or not signal_date:
        return None
    sdf = actual_df[actual_df["ts_code"] == str(ts_code)]
    if sdf.empty:
        return None
    signal_dt = pd.to_datetime(signal_date, errors="coerce")
    if pd.isna(signal_dt):
        return None
    entry_min = signal_dt - timedelta(days=1)
    entry_max = signal_dt + timedelta(days=5)
    exit_max = signal_dt + timedelta(days=max(hold_days, 1) + 20)
    sdf = sdf.copy()
    sdf["entry_dt"] = pd.to_datetime(sdf["entry_date"], errors="coerce")
    sdf["exit_dt"] = pd.to_datetime(sdf["exit_date"], errors="coerce")
    sdf = sdf.dropna(subset=["entry_dt", "exit_dt"])
    if sdf.empty:
        return None
    sdf = sdf[
        (sdf["entry_dt"] >= entry_min)
        & (sdf["entry_dt"] <= entry_max)
        & (sdf["exit_dt"] >= signal_dt)
        & (sdf["exit_dt"] <= exit_max)
    ]
    if sdf.empty:
        return None
    sdf["delta_days"] = (sdf["entry_dt"] - signal_dt).abs().dt.days
    sdf = sdf.sort_values(["delta_days", "exit_dt"])
    ret = safe_float_any(sdf.iloc[0].get("realized_return"), np.nan)
    if not np.isfinite(ret):
        return None
    return float(ret)


def _lookup_actual_execution_return_from_fills(
    fill_df: pd.DataFrame,
    *,
    ts_code: str,
    signal_date: str,
    hold_days: int,
    hdf: pd.DataFrame,
) -> Optional[float]:
    if fill_df is None or fill_df.empty or not ts_code or not signal_date:
        return None
    if hdf is None or hdf.empty:
        return None
    signal_dt = pd.to_datetime(signal_date, errors="coerce")
    if pd.isna(signal_dt):
        return None
    sdf = fill_df[fill_df["ts_code"] == str(ts_code)].copy()
    if sdf.empty:
        return None
    sdf["fill_dt"] = pd.to_datetime(sdf["fill_date"], errors="coerce")
    sdf = sdf.dropna(subset=["fill_dt"])
    if sdf.empty:
        return None
    sdf = sdf[
        (sdf["fill_dt"] >= (signal_dt - timedelta(days=1)))
        & (sdf["fill_dt"] <= (signal_dt + timedelta(days=5)))
    ]
    if sdf.empty:
        return None
    sdf["delta_days"] = (sdf["fill_dt"] - signal_dt).abs().dt.days
    sdf = sdf.sort_values(["delta_days", "fill_dt"])
    chosen = sdf.iloc[0]
    entry_price = safe_float_any(chosen.get("avg_fill_price"), 0.0)
    fill_day = str(chosen.get("fill_date") or "")
    if entry_price <= 0 or not fill_day:
        return None
    hdf_local = hdf.copy()
    hdf_local["trade_date"] = hdf_local["trade_date"].astype(str)
    after_fill = hdf_local[hdf_local["trade_date"] >= fill_day].head(max(1, int(hold_days)))
    if not after_fill.empty and "close" in after_fill.columns:
        exit_close = safe_float_any(after_fill.iloc[-1]["close"], 0.0)
        if exit_close > 0:
            return float((exit_close - entry_price) / max(entry_price, 1e-9))
    close_price = safe_float_any(chosen.get("close_price"), 0.0)
    if close_price > 0:
        return float((close_price - entry_price) / max(entry_price, 1e-9))
    return None


def _evaluate_top5_joint_switch(
    ab_compare_df: pd.DataFrame,
    *,
    target_version: str,
    min_samples_for_switch: int,
) -> Dict[str, Any]:
    target = str(target_version or "A").upper()
    if ab_compare_df is None or ab_compare_df.empty:
        return {"allow_switch": False, "score": 0.0, "reasons": ["缺少A/B窗口样本"], "detail": pd.DataFrame()}
    if "统计窗口" not in ab_compare_df.columns:
        return {"allow_switch": False, "score": 0.0, "reasons": ["A/B对比缺少统计窗口字段"], "detail": pd.DataFrame()}
    weights = {"近20日": 0.2, "近60日": 0.5, "近120日": 0.3}
    sample_thresholds = {
        "近20日": max(5, int(math.ceil(min_samples_for_switch * 0.6))),
        "近60日": max(1, int(min_samples_for_switch)),
        "近120日": max(min_samples_for_switch, int(math.ceil(min_samples_for_switch * 1.2))),
    }
    detail_rows: List[Dict[str, Any]] = []
    reasons: List[str] = []
    score = 0.0
    pass_count = 0
    for window, weight in weights.items():
        row = ab_compare_df[ab_compare_df["统计窗口"] == window]
        if row.empty:
            reasons.append(f"{window} 缺少对比结果")
            detail_rows.append({"统计窗口": window, "窗口通过": "否", "说明": "缺少样本"})
            continue
        item = row.iloc[0]
        b_samples = int(safe_float_any(item.get("B样本触达数"), 0.0))
        prefer = str(item.get("当前推荐版本") or "观察")
        p_val = safe_float_any(item.get("显著性p值"), 1.0)
        delta = _pct_text_to_decimal(item.get("收益差(B-A)"))
        sample_ok = b_samples >= int(sample_thresholds.get(window, min_samples_for_switch))
        prefer_ok = prefer == target
        p_ok = p_val < 0.10
        delta_ok = (delta > 0.0) if target == "B" else ((delta < 0.0) if target == "A" else False)
        window_pass = bool(sample_ok and prefer_ok and p_ok and delta_ok)
        if window_pass:
            score += weight
            pass_count += 1
        detail_rows.append(
            {
                "统计窗口": window,
                "B样本触达数": b_samples,
                "样本门槛": int(sample_thresholds.get(window, min_samples_for_switch)),
                "推荐版本": prefer,
                "显著性p值": f"{p_val:.4f}",
                "收益差(B-A)": f"{delta * 100:.2f}%",
                "窗口通过": "是" if window_pass else "否",
            }
        )
        if not window_pass:
            fail_reasons: List[str] = []
            if not sample_ok:
                fail_reasons.append("样本不足")
            if not prefer_ok:
                fail_reasons.append("推荐版本不一致")
            if not p_ok:
                fail_reasons.append("显著性不足")
            if not delta_ok:
                fail_reasons.append("收益方向不一致")
            reasons.append(f"{window} 未通过：{'/'.join(fail_reasons)}")
    mandatory_60 = any(
        str(item.get("统计窗口")) == "近60日" and str(item.get("窗口通过")) == "是"
        for item in detail_rows
    )
    allow = bool(mandatory_60 and score >= 0.70 and pass_count >= 2)
    if not allow and not reasons:
        reasons.append("联合判定得分不足")
    return {
        "allow_switch": allow,
        "score": score,
        "pass_windows": pass_count,
        "reasons": reasons,
        "detail": pd.DataFrame(detail_rows),
    }


def _build_segment_accuracy_table(df: pd.DataFrame, segment_col: str, segment_name: str) -> pd.DataFrame:
    if df.empty or segment_col not in df.columns:
        return pd.DataFrame()
    rows: List[Dict[str, Any]] = []
    for seg_value, g in df.groupby(segment_col):
        sample_cnt = int(len(g))
        touch_cnt = int(g["触达"].sum()) if "触达" in g.columns else 0
        touch_base = max(touch_cnt, 1)
        tp_cnt = int(g["止盈命中"].sum()) if "止盈命中" in g.columns else 0
        sl_cnt = int(g["止损触发"].sum()) if "止损触发" in g.columns else 0
        pass_cnt = int(g["预案达标"].sum()) if "预案达标" in g.columns else 0
        ret_series = pd.to_numeric(g.get("到期收益"), errors="coerce")
        mean_ret = float(ret_series.mean()) if ret_series.notna().any() else 0.0
        rows.append(
            {
                segment_name: str(seg_value or "未知"),
                "样本数": sample_cnt,
                "买入触达率": f"{(touch_cnt / float(max(sample_cnt, 1))) * 100:.1f}%",
                "止盈命中率": f"{(tp_cnt / float(touch_base)) * 100:.1f}%",
                "止损触发率": f"{(sl_cnt / float(touch_base)) * 100:.1f}%",
                "预案达标率": f"{(pass_cnt / float(touch_base)) * 100:.1f}%",
                "到期收益均值": f"{mean_ret * 100:.2f}%",
            }
        )
    out = pd.DataFrame(rows)
    if not out.empty and "样本数" in out.columns:
        out = out.sort_values("样本数", ascending=False).reset_index(drop=True)
    return out


def _compute_top5_advice_accuracy_payload() -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "dashboard": pd.DataFrame(),
        "missing_reasons": pd.DataFrame(),
        "summary": pd.DataFrame(),
        "industry_segments": pd.DataFrame(),
        "liquidity_segments": pd.DataFrame(),
        "volatility_segments": pd.DataFrame(),
        "ab_compare": pd.DataFrame(),
        "cost_consistency": pd.DataFrame(),
        "execution_sync": pd.DataFrame(),
        "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "export_csv": "",
    }
    csv_files = _collect_top5_trader_brief_csvs()
    if not csv_files:
        return payload
    rows: List[Dict[str, Any]] = []
    for path in csv_files:
        signal_date = _top5_brief_signal_date(path)
        if not signal_date:
            continue
        try:
            df = pd.read_csv(path)
        except Exception:
            continue
        if df.empty:
            continue
        if "股票代码" not in df.columns and "ts_code" in df.columns:
            df = df.rename(columns={"ts_code": "股票代码"})
        if "参考买入价" not in df.columns or "参考卖出价" not in df.columns or "止损价" not in df.columns:
            continue
        hold_col = "建议持有天数" if "建议持有天数" in df.columns else ""
        for _, r in df.iterrows():
            ts_code = str(r.get("股票代码") or "").strip()
            if not ts_code:
                continue
            rows.append(
                {
                    "signal_date": signal_date,
                    "ts_code": ts_code,
                    "buy_price": safe_float_any(r.get("参考买入价"), 0.0),
                    "sell_price": safe_float_any(r.get("参考卖出价"), 0.0),
                    "stop_price": safe_float_any(r.get("止损价"), 0.0),
                    "hold_days": max(1, int(safe_float_any(r.get(hold_col), 4.0))) if hold_col else 4,
                    "anchor_price": safe_float_any(r.get("基准价格", r.get("anchor_price", 0.0)), 0.0),
                    "estimated_cost_bps": safe_float_any(r.get("预估成本(bp)", r.get("estimated_cost_bps", 0.0)), 0.0),
                    "industry": str(r.get("行业") or r.get("industry") or "未知"),
                    "liquidity_amount": safe_float_any(r.get("流动性金额", r.get("liquidity_amount", 0.0)), 0.0),
                    "pct_chg": safe_float_any(r.get("涨跌幅", r.get("pct_chg", 0.0)), 0.0),
                }
            )
    if not rows:
        return payload
    signal_df = pd.DataFrame(rows)
    sync_stats = _sync_top5_execution_feedback_unified()
    payload["execution_sync"] = pd.DataFrame(
        [
            {
                "状态": "成功" if sync_stats.get("ok") else "失败",
                "本次增量入库": int(safe_float_any(sync_stats.get("synced_rows"), 0.0)),
                "统一表累计记录": int(safe_float_any(sync_stats.get("unified_rows"), 0.0)),
                "同步游标": str(sync_stats.get("cursor") or ""),
                "更新时间": str(sync_stats.get("updated_at") or ""),
                "错误": str(sync_stats.get("error") or ""),
            }
        ]
    )
    actual_exec_df = _load_actual_execution_return_rows()
    unified_fill_df = _load_unified_execution_fill_rows()
    missing_reason_global: Dict[str, int] = {}
    total_samples_all = int(len(signal_df))
    evaluated_samples_all = 0
    actual_matched_all = 0

    def _bump_reason(reason: str) -> None:
        missing_reason_global[reason] = int(missing_reason_global.get(reason, 0) + 1)

    with connect_db(_ctx().permanent_db_path) as conn:
        table = _safe_daily_table_name("daily_trading_data")
        try:
            trade_days = conn.execute(
                f"SELECT DISTINCT trade_date FROM {table} ORDER BY trade_date DESC LIMIT 130"
            ).fetchall()
        except Exception:
            _bump_reason("行情表读取失败")
            return payload
        if not trade_days:
            _bump_reason("行情交易日为空")
            return payload
        trade_day_list = [str(item[0] or "") for item in trade_days if str(item[0] or "").strip()]
        if not trade_day_list:
            _bump_reason("行情交易日为空字符串")
            return payload
        latest_trade_day = trade_day_list[0]
        window_specs = [20, 60, 120]
        out_rows: List[Dict[str, Any]] = []
        ab_rows: List[Dict[str, Any]] = []
        seg_eval_rows: List[Dict[str, Any]] = []
        for n in window_specs:
            win_days = set(trade_day_list[: min(n, len(trade_day_list))])
            win_signals = signal_df[signal_df["signal_date"].isin(win_days)].copy()
            signal_days = sorted({str(item) for item in win_signals["signal_date"].tolist() if str(item).strip()})
            if win_signals.empty:
                out_rows.append(
                    {
                        "统计窗口": f"近{n}日",
                        "信号日数": 0,
                        "建议样本数": 0,
                        "平均预估成本(bp)": 0.0,
                        "执行后收益均值(估算)": "0.00%",
                        "实盘回报覆盖率": "0.0%",
                        "实际成交收益均值": "无",
                        "Top5平均延续率": "0.0%",
                        "买入触达率": "0.0%",
                        "止盈命中率": "0.0%",
                        "止损触发率": "0.0%",
                        "到期收益均值": "0.00%",
                        "信号日达标率": "0.0%",
                        "预案达标率": "0.0%",
                    }
                )
                continue
            signal_sets: Dict[str, set] = {}
            for signal_day in signal_days:
                day_codes = set(
                    str(item)
                    for item in win_signals[win_signals["signal_date"] == signal_day]["ts_code"].tolist()
                    if str(item).strip()
                )
                if day_codes:
                    signal_sets[signal_day] = day_codes
            stability_vals: List[float] = []
            sorted_days = sorted(signal_sets.keys())
            for idx in range(1, len(sorted_days)):
                prev_codes = signal_sets.get(sorted_days[idx - 1], set())
                curr_codes = signal_sets.get(sorted_days[idx], set())
                base = max(len(curr_codes), 1)
                overlap = len(prev_codes & curr_codes) / float(base)
                stability_vals.append(overlap)
            mean_stability = float(np.mean(stability_vals)) if stability_vals else 0.0
            ts_codes = sorted({str(item) for item in win_signals["ts_code"].tolist() if str(item).strip()})
            start_date = str(win_signals["signal_date"].min())
            history = _load_history_range_bulk(
                conn,
                ts_codes=ts_codes,
                start_date=start_date,
                end_date=latest_trade_day,
                columns="trade_date, close_price, high_price, low_price",
                table=table,
            )
            sample_cnt = int(len(win_signals))
            avg_cost_bps = float(
                pd.to_numeric(win_signals.get("estimated_cost_bps"), errors="coerce").fillna(0.0).mean()
            )
            buy_touch_cnt = 0
            tp_cnt = 0
            sl_cnt = 0
            pass_cnt = 0
            evaluable_cnt = 0
            expiry_returns: List[float] = []
            b_buy_touch_cnt = 0
            b_tp_cnt = 0
            b_sl_cnt = 0
            b_pass_cnt = 0
            b_expiry_returns: List[float] = []
            actual_returns: List[float] = []
            actual_match_cnt = 0
            day_touch_map: Dict[str, int] = {}
            day_pass_map: Dict[str, int] = {}
            for _, s in win_signals.iterrows():
                ts_code = str(s.get("ts_code") or "")
                signal_day = str(s.get("signal_date") or "")
                buy_price = safe_float_any(s.get("buy_price"), 0.0)
                sell_price = safe_float_any(s.get("sell_price"), 0.0)
                stop_price = safe_float_any(s.get("stop_price"), 0.0)
                hold_days = max(1, int(safe_float_any(s.get("hold_days"), 4.0)))
                anchor_price = safe_float_any(s.get("anchor_price"), 0.0)
                estimated_cost_bps = safe_float_any(s.get("estimated_cost_bps"), 0.0)
                industry = str(s.get("industry") or "未知")
                liquidity_amount = safe_float_any(s.get("liquidity_amount"), 0.0)
                pct_chg = safe_float_any(s.get("pct_chg"), 0.0)
                if buy_price <= 0 or sell_price <= 0 or stop_price <= 0:
                    _bump_reason("建议参数缺失或无效")
                    continue
                hdf = history.get(ts_code)
                if hdf is None or hdf.empty:
                    _bump_reason("缺少历史行情")
                    continue
                hdf = _ensure_price_aliases(hdf.copy())
                if "trade_date" not in hdf.columns:
                    _bump_reason("行情缺少trade_date字段")
                    continue
                hdf["trade_date"] = hdf["trade_date"].astype(str)
                hdf = hdf[hdf["trade_date"] >= str(s.get("signal_date") or "")]
                if hdf.empty:
                    _bump_reason("信号日后无行情")
                    continue
                hdf = hdf.sort_values("trade_date")
                entry_window = hdf.head(3)
                if "low" not in entry_window.columns:
                    _bump_reason("行情缺少最低价字段")
                    continue
                evaluable_cnt += 1
                touched = entry_window[entry_window["low"] <= buy_price]
                if touched.empty:
                    if n == 60:
                        seg_eval_rows.append(
                            {
                                "industry": industry,
                                "liquidity_bucket": "低流动性" if liquidity_amount < 600000 else ("中流动性" if liquidity_amount < 2000000 else "高流动性"),
                                "volatility_bucket": "低波动" if abs(pct_chg) < 1.0 else ("中波动" if abs(pct_chg) < 2.0 else "高波动"),
                                "触达": 0,
                                "止盈命中": 0,
                                "止损触发": 0,
                                "预案达标": 0,
                                "到期收益": np.nan,
                            }
                        )
                    continue
                buy_touch_cnt += 1
                day_touch_map[signal_day] = int(day_touch_map.get(signal_day, 0) + 1)
                entry_date = str(touched.iloc[0]["trade_date"])
                after_entry = hdf[hdf["trade_date"] >= entry_date].head(hold_days)
                if after_entry.empty:
                    _bump_reason("入场后样本为空")
                    continue
                high_max = float(after_entry["high"].max()) if "high" in after_entry.columns else buy_price
                low_min = float(after_entry["low"].min()) if "low" in after_entry.columns else buy_price
                if high_max >= sell_price:
                    tp_cnt += 1
                if low_min <= stop_price:
                    sl_cnt += 1
                close_col = "close" if "close" in after_entry.columns else ""
                tp_hit = 1 if high_max >= sell_price else 0
                sl_hit = 1 if low_min <= stop_price else 0
                pass_hit = 0
                ret = np.nan
                if close_col:
                    exit_close = safe_float_any(after_entry.iloc[-1][close_col], buy_price)
                    ret = (exit_close - buy_price) / max(buy_price, 1e-9)
                    expiry_returns.append(ret)
                    if low_min > stop_price and ret >= 0:
                        pass_cnt += 1
                        day_pass_map[signal_day] = int(day_pass_map.get(signal_day, 0) + 1)
                        pass_hit = 1
                else:
                    _bump_reason("行情缺少收盘价字段")
                actual_ret = _lookup_actual_execution_return(
                    actual_exec_df,
                    ts_code=ts_code,
                    signal_date=signal_day,
                    hold_days=hold_days,
                )
                if actual_ret is None:
                    actual_ret = _lookup_actual_execution_return_from_fills(
                        unified_fill_df,
                        ts_code=ts_code,
                        signal_date=signal_day,
                        hold_days=hold_days,
                        hdf=hdf,
                    )
                if actual_ret is not None and np.isfinite(actual_ret):
                    actual_returns.append(float(actual_ret))
                    actual_match_cnt += 1
                else:
                    _bump_reason("缺少实盘成交回报")

                plan_b = _derive_trade_plan(
                    anchor_price=anchor_price if anchor_price > 0 else buy_price,
                    pct_chg=pct_chg,
                    liquidity_amount=liquidity_amount,
                    estimated_cost_bps=estimated_cost_bps,
                    version="B",
                )
                b_buy = safe_float_any(plan_b.get("参考买入价"), 0.0)
                b_sell = safe_float_any(plan_b.get("参考卖出价"), 0.0)
                b_stop = safe_float_any(plan_b.get("止损价"), 0.0)
                b_hold_days = max(1, int(safe_float_any(plan_b.get("建议持有天数"), hold_days)))
                if b_buy > 0 and b_sell > 0 and b_stop > 0:
                    b_touched = entry_window[entry_window["low"] <= b_buy]
                    if not b_touched.empty:
                        b_buy_touch_cnt += 1
                        b_entry_date = str(b_touched.iloc[0]["trade_date"])
                        b_after_entry = hdf[hdf["trade_date"] >= b_entry_date].head(b_hold_days)
                        if not b_after_entry.empty:
                            b_high_max = float(b_after_entry["high"].max()) if "high" in b_after_entry.columns else b_buy
                            b_low_min = float(b_after_entry["low"].min()) if "low" in b_after_entry.columns else b_buy
                            if b_high_max >= b_sell:
                                b_tp_cnt += 1
                            if b_low_min <= b_stop:
                                b_sl_cnt += 1
                            b_close_col = "close" if "close" in b_after_entry.columns else ""
                            if b_close_col:
                                b_exit_close = safe_float_any(b_after_entry.iloc[-1][b_close_col], b_buy)
                                b_ret = (b_exit_close - b_buy) / max(b_buy, 1e-9)
                                b_expiry_returns.append(b_ret)
                                if b_low_min > b_stop and b_ret >= 0:
                                    b_pass_cnt += 1
                if n == 60:
                    seg_eval_rows.append(
                        {
                            "industry": industry,
                            "liquidity_bucket": "低流动性" if liquidity_amount < 600000 else ("中流动性" if liquidity_amount < 2000000 else "高流动性"),
                            "volatility_bucket": "低波动" if abs(pct_chg) < 1.0 else ("中波动" if abs(pct_chg) < 2.0 else "高波动"),
                            "触达": 1,
                            "止盈命中": tp_hit,
                            "止损触发": sl_hit,
                            "预案达标": pass_hit,
                            "到期收益": ret,
                        }
                    )
            evaluated_samples_all += int(evaluable_cnt)
            actual_matched_all += int(actual_match_cnt)
            touch_base = max(buy_touch_cnt, 1)
            mean_ret = float(np.mean(expiry_returns)) if expiry_returns else 0.0
            net_mean_ret = mean_ret - (avg_cost_bps / 10000.0)
            actual_mean_ret = float(np.mean(actual_returns)) if actual_returns else np.nan
            b_touch_base = max(b_buy_touch_cnt, 1)
            b_mean_ret = float(np.mean(b_expiry_returns)) if b_expiry_returns else 0.0
            active_signal_days = [day for day in signal_days if int(day_touch_map.get(day, 0)) > 0]
            qualified_days = 0
            for day in active_signal_days:
                t_cnt = int(day_touch_map.get(day, 0))
                p_cnt = int(day_pass_map.get(day, 0))
                if t_cnt > 0 and (p_cnt / float(t_cnt)) >= 0.5:
                    qualified_days += 1
            sample_integrity = (evaluable_cnt / float(max(sample_cnt, 1))) * 100.0
            out_rows.append(
                {
                    "统计窗口": f"近{n}日",
                    "信号日数": len(signal_days),
                    "建议样本数": sample_cnt,
                    "样本完整度": f"{sample_integrity:.1f}%",
                    "平均预估成本(bp)": round(avg_cost_bps, 2),
                    "执行后收益均值(估算)": f"{net_mean_ret * 100:.2f}%",
                    "实盘回报覆盖率": f"{(actual_match_cnt / float(max(evaluable_cnt, 1))) * 100:.1f}%",
                    "实际成交收益均值": f"{actual_mean_ret * 100:.2f}%" if np.isfinite(actual_mean_ret) else "无",
                    "Top5平均延续率": f"{mean_stability * 100:.1f}%",
                    "买入触达率": f"{(buy_touch_cnt / max(sample_cnt, 1)) * 100:.1f}%",
                    "止盈命中率": f"{(tp_cnt / touch_base) * 100:.1f}%",
                    "止损触发率": f"{(sl_cnt / touch_base) * 100:.1f}%",
                    "到期收益均值": f"{mean_ret * 100:.2f}%",
                    "信号日达标率": f"{(qualified_days / max(len(active_signal_days), 1)) * 100:.1f}%",
                    "预案达标率": f"{(pass_cnt / touch_base) * 100:.1f}%",
                }
            )
            ab_row = {
                "统计窗口": f"近{n}日",
                "A样本触达数": int(buy_touch_cnt),
                "B样本触达数": int(b_buy_touch_cnt),
                "A到期收益均值": f"{mean_ret * 100:.2f}%",
                "B到期收益均值": f"{b_mean_ret * 100:.2f}%",
                "A预案达标率": f"{(pass_cnt / touch_base) * 100:.1f}%",
                "B预案达标率": f"{(b_pass_cnt / b_touch_base) * 100:.1f}%",
                "收益差(B-A)": f"{(b_mean_ret - mean_ret) * 100:.2f}%",
            }
            p_val, z_score = _mean_diff_pvalue(expiry_returns, b_expiry_returns)
            ab_row["显著性p值"] = f"{p_val:.4f}"
            ab_row["统计量Z"] = f"{z_score:.3f}"
            ab_row["显著性结论"] = "显著" if p_val < 0.10 else "不显著"
            if b_mean_ret > mean_ret and (b_pass_cnt / b_touch_base) >= (pass_cnt / touch_base):
                ab_row["当前推荐版本"] = "B"
            elif b_mean_ret < mean_ret and (b_pass_cnt / b_touch_base) <= (pass_cnt / touch_base):
                ab_row["当前推荐版本"] = "A"
            else:
                ab_row["当前推荐版本"] = "观察"
            ab_rows.append(ab_row)
    dashboard_df = pd.DataFrame(out_rows)
    payload["dashboard"] = dashboard_df
    payload["ab_compare"] = pd.DataFrame(ab_rows)
    if not dashboard_df.empty:
        cost_rows: List[Dict[str, Any]] = []
        for _, r in dashboard_df.iterrows():
            signal_ret = _pct_text_to_decimal(r.get("到期收益均值"))
            est_cost_bps = safe_float_any(r.get("平均预估成本(bp)"), 0.0)
            est_exec_ret = _pct_text_to_decimal(r.get("执行后收益均值(估算)"))
            actual_text = str(r.get("实际成交收益均值") or "无")
            actual_ret = _pct_text_to_decimal(actual_text) if actual_text not in {"无", "", "nan"} else np.nan
            consistency_note = "信号收益、执行后收益方向一致"
            if signal_ret > 0 and est_exec_ret <= 0:
                consistency_note = "执行成本侵蚀后转负，需收紧执行"
            elif signal_ret < 0 and est_exec_ret < signal_ret:
                consistency_note = "负收益被成本放大，建议降级版本"
            if np.isfinite(actual_ret):
                if est_exec_ret > 0 and actual_ret <= 0:
                    consistency_note = "实盘成交偏离估算，执行质量需复盘"
                elif abs(actual_ret - est_exec_ret) >= 0.02:
                    consistency_note = "实盘与估算偏差较大，需校准成本模型"
            else:
                consistency_note = "缺少实盘闭环成交回报，暂按估算口径"
            cost_rows.append(
                {
                    "统计窗口": r.get("统计窗口", ""),
                    "信号收益均值": r.get("到期收益均值", "0.00%"),
                    "预估执行成本": f"{est_cost_bps:.2f}bp",
                    "执行后收益均值(估算)": f"{est_exec_ret * 100:.2f}%",
                    "实际成交收益均值": actual_text,
                    "一致性结论": consistency_note,
                }
            )
        payload["cost_consistency"] = pd.DataFrame(cost_rows)
    total_missing = max(total_samples_all - evaluated_samples_all, 0)
    payload["summary"] = pd.DataFrame(
        [
            {
                "总建议样本数": int(total_samples_all),
                "可评估样本数": int(evaluated_samples_all),
                "缺失样本数": int(total_missing),
                "样本完整度": f"{(evaluated_samples_all / float(max(total_samples_all, 1))) * 100:.1f}%",
                "实盘回报覆盖率": f"{(actual_matched_all / float(max(evaluated_samples_all, 1))) * 100:.1f}%",
                "最近更新时间": payload["updated_at"],
            }
        ]
    )
    if missing_reason_global:
        total_reasons = float(sum(missing_reason_global.values()))
        miss_rows = [
            {
                "缺失原因": reason,
                "样本数量": int(cnt),
                "占比": f"{(cnt / max(total_reasons, 1.0)) * 100:.1f}%",
            }
            for reason, cnt in sorted(missing_reason_global.items(), key=lambda kv: kv[1], reverse=True)
        ]
        payload["missing_reasons"] = pd.DataFrame(miss_rows)
    if not dashboard_df.empty:
        try:
            export_dir = _ctx().repo_root / "exports"
            export_dir.mkdir(parents=True, exist_ok=True)
            date_key = datetime.now().strftime("%Y%m%d")
            export_csv = export_dir / f"top5_advice_accuracy_{date_key}.csv"
            dashboard_df.to_csv(export_csv, index=False)
            payload["export_csv"] = str(export_csv)
        except Exception:
            pass
    if seg_eval_rows:
        seg_df = pd.DataFrame(seg_eval_rows)
        ind_df = _build_segment_accuracy_table(seg_df, "industry", "行业")
        if not ind_df.empty:
            payload["industry_segments"] = ind_df.head(10)
        payload["liquidity_segments"] = _build_segment_accuracy_table(seg_df, "liquidity_bucket", "流动性分层")
        payload["volatility_segments"] = _build_segment_accuracy_table(seg_df, "volatility_bucket", "波动分层")
    return payload
def _write_top5_advice_version_audit_report(accuracy_payload: Dict[str, Any]) -> str:
    try:
        export_dir = _ctx().repo_root / "exports"
        export_dir.mkdir(parents=True, exist_ok=True)
        date_key = datetime.now().strftime("%Y%m%d")
        report_path = export_dir / f"top5_advice_version_audit_{date_key}.md"
        ab_df = accuracy_payload.get("ab_compare", pd.DataFrame())
        summary_df = accuracy_payload.get("summary", pd.DataFrame())
        lines: List[str] = [
            f"# Top5 建议参数版本审计（{datetime.now().strftime('%Y-%m-%d')}）",
            "",
            f"- 更新时间：{accuracy_payload.get('updated_at', '')}",
            "",
        ]
        if isinstance(summary_df, pd.DataFrame) and not summary_df.empty:
            row = summary_df.iloc[0]
            lines.extend(
                [
                    "## 样本概况",
                    "",
                    f"- 总建议样本数：{row.get('总建议样本数', 0)}",
                    f"- 可评估样本数：{row.get('可评估样本数', 0)}",
                    f"- 缺失样本数：{row.get('缺失样本数', 0)}",
                    f"- 样本完整度：{row.get('样本完整度', '0.0%')}",
                    "",
                ]
            )
        if isinstance(ab_df, pd.DataFrame) and not ab_df.empty:
            lines.extend(["## 参数版本 A/B 对比", ""])
            for _, r in ab_df.iterrows():
                lines.append(
                    f"- {r.get('统计窗口','')}: A={r.get('A到期收益均值','')} / B={r.get('B到期收益均值','')} "
                    f"| 推荐={r.get('当前推荐版本','观察')} | 收益差={r.get('收益差(B-A)','0.00%')}"
                )
            csv_path = export_dir / f"top5_advice_ab_compare_{date_key}.csv"
            ab_df.to_csv(csv_path, index=False)
            lines.extend(["", f"- A/B 对比CSV：`{csv_path}`", ""])
        report_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        return str(report_path)
    except Exception:
        return ""
def _resolve_stock_name_map(ts_codes: List[str]) -> Dict[str, str]:
    symbols = sorted({str(code or "").strip() for code in ts_codes if str(code or "").strip()})
    if not symbols:
        return {}
    name_map: Dict[str, str] = {}
    try:
        conn = sqlite3.connect(_ctx().permanent_db_path)
        placeholders = ",".join(["?"] * len(symbols))
        sql = f"SELECT ts_code, name FROM stock_basic WHERE ts_code IN ({placeholders})"
        rows = conn.execute(sql, tuple(symbols)).fetchall()
        conn.close()
        for row in rows:
            ts_code = str(row[0] or "").strip()
            stock_name = str(row[1] or "").strip()
            if ts_code:
                name_map[ts_code] = stock_name
    except Exception:
        return {}
    return name_map


def _resolve_latest_price_map(ts_codes: List[str]) -> Dict[str, float]:
    symbols = [str(code or "").strip() for code in ts_codes if str(code or "").strip()]
    if not symbols:
        return {}
    price_map: Dict[str, float] = {}
    try:
        latest_prices = _get_latest_prices(symbols)
        for ts_code in symbols:
            price = float((latest_prices.get(ts_code) or {}).get("price") or 0.0)
            if price > 0:
                price_map[ts_code] = price
    except Exception:
        return {}
    return price_map


def _top5_execution_priority(
    *,
    target_weight: float,
    liquidity_amount: float,
    estimated_cost_bps: float,
    risk_share: float,
) -> str:
    if liquidity_amount < 600000 or estimated_cost_bps > 25:
        return "P3"
    if target_weight >= 0.20 or risk_share > 0.30:
        return "P1"
    return "P2"


def _top5_execution_controls(
    *,
    priority: str,
    risk_tag: str,
    estimated_cost_bps: float,
    liquidity_amount: float,
) -> Dict[str, str]:
    tags = str(risk_tag or "")
    if "流动性敏感" in tags or liquidity_amount < 600000:
        route = "被动限价为主；盘口恢复前不主动扫单"
        first_wave = "目标名义10%-15%"
    elif "交易成本偏高" in tags or estimated_cost_bps > 25:
        route = "被动排队优先；主动成交需交易员复核"
        first_wave = "目标名义15%-20%"
    elif priority == "P1":
        route = "分批限价；可小比例主动补量"
        first_wave = "目标名义20%-25%"
    else:
        route = "分批限价；等待价差稳定后推进"
        first_wave = "目标名义15%-20%"
    return {
        "委托方式": route,
        "首波上限": first_wave,
        "交易台硬门禁": "竞价流动性低于快照30%、实时滑点>35bp、价格跌破止损且15分钟未收复，任一触发即冻结",
    }


def _resolve_top5_trade_compact(payload: Dict[str, Any], competition_run_id: str, artifact_path: Path) -> str:
    candidates = [
        str(payload.get("trade_date") or ""),
        str(competition_run_id or ""),
        str(payload.get("created_at") or ""),
        artifact_path.name,
    ]
    for value in candidates:
        compact = compact_trade_date(value)
        if compact:
            return compact
        match = re.search(r"(20\d{6})", value)
        if match:
            return str(match.group(1))
    return ""


def _derive_trade_plan(
    *,
    anchor_price: float,
    pct_chg: float,
    liquidity_amount: float,
    estimated_cost_bps: float,
    version: str = "A",
) -> Dict[str, Any]:
    price_base = float(anchor_price or 0.0)
    if price_base <= 0:
        price_base = 1.0

    buy_discount = 0.006
    hold_days = 4
    take_profit = 0.050
    stop_loss = 0.022
    if pct_chg >= 2.0:
        buy_discount = 0.012
        hold_days = 3
        take_profit = 0.042
        stop_loss = 0.020
    elif pct_chg <= -1.0:
        buy_discount = 0.003
        hold_days = 5
        take_profit = 0.058
        stop_loss = 0.025
    if liquidity_amount < 600000:
        hold_days += 1
        buy_discount += 0.004
    if estimated_cost_bps > 25:
        buy_discount += 0.003
        take_profit += 0.005
    if str(version).upper() == "B":
        # 版本B：分层校准（更保守入场，减少噪声触发）
        vol = abs(float(pct_chg or 0.0))
        if vol >= 2.0:
            buy_discount += 0.002
            hold_days = max(2, hold_days - 1)
            stop_loss = min(0.030, stop_loss + 0.002)
        elif vol < 1.0:
            hold_days += 1
            take_profit += 0.003
        if liquidity_amount < 600000:
            stop_loss = max(0.016, stop_loss - 0.002)
        if estimated_cost_bps >= 30:
            take_profit += 0.004

    buy_price = round(price_base * (1.0 - buy_discount), 2)
    sell_price = round(buy_price * (1.0 + take_profit), 2)
    stop_price = round(buy_price * (1.0 - stop_loss), 2)
    response = (
        f"若开盘高开>2%：先观望，回踩{buy_price:.2f}附近再分批；"
        f"若跌破{stop_price:.2f}且15分钟未收复：减仓至0；"
        "若成交显著萎缩：仅保留被动挂单并下调单笔委托。"
    )
    return {
        "参考买入价": buy_price,
        "建议持有天数": int(hold_days),
        "参考卖出价": sell_price,
        "止损价": stop_price,
        "触发应对": response,
    }


def _build_top5_trader_brief_from_artifact(artifact_path: Path, *, advice_version: str = "A") -> Dict[str, str]:
    raw_bytes = artifact_path.read_bytes()
    payload = json.loads(raw_bytes.decode("utf-8"))
    canary_gate = evaluate_top5_artifact_canary_gate(payload)
    if canary_gate.get("passed") is not True and os.getenv("OPENCLAW_ALLOW_LEGACY_TOP5_ARTIFACT", "0") != "1":
        reasons = ",".join(canary_gate.get("blocking_reasons") or [])
        raise ValueError(f"Top5审计产物未通过canary/pass硬门禁:{reasons}")
    top5_rows = [dict(item) for item in (payload.get("top5_portfolio_audit") or []) if isinstance(item, dict)]
    if not top5_rows:
        raise ValueError("最新 top5 审计产物未包含候选股票")

    competition_run_id = str(payload.get("competition_run_id") or "unknown_run").strip()
    trade_compact = _resolve_top5_trade_compact(payload, competition_run_id, artifact_path)
    artifact_digest = hashlib.sha256(raw_bytes).hexdigest()
    artifact_resolved = str(artifact_path.expanduser().resolve())
    lineage_payload = payload.get("unified_signal_lineage") or []
    lineage_rows = [dict(x) for x in lineage_payload if isinstance(x, dict)]
    artifact_stat = artifact_path.stat()
    artifact_mtime_utc = datetime.utcfromtimestamp(float(artifact_stat.st_mtime)).strftime("%Y-%m-%dT%H:%M:%SZ")

    export_dir = _ctx().repo_root / "exports"
    export_dir.mkdir(parents=True, exist_ok=True)
    stem = f"top5_trader_brief_{trade_compact or 'unknown'}_{competition_run_id}"
    md_path = export_dir / f"{stem}.md"
    csv_path = export_dir / f"{stem}.csv"

    ts_codes = [str(item.get("ts_code") or "").strip() for item in top5_rows]
    name_map = _resolve_stock_name_map(ts_codes)
    latest_price_map = _resolve_latest_price_map(ts_codes)
    rows: List[Dict[str, Any]] = []
    for idx, item in enumerate(top5_rows, start=1):
        risk = item.get("risk") if isinstance(item.get("risk"), dict) else {}
        src = item.get("source") if isinstance(item.get("source"), dict) else {}
        cost = item.get("cost") if isinstance(item.get("cost"), dict) else {}
        ts_code = str(item.get("ts_code") or "").strip()
        stock_name = (
            str(item.get("name") or "").strip()
            or str(src.get("name") or "").strip()
            or str(risk.get("name") or "").strip()
            or name_map.get(ts_code, "")
        )
        liquidity_amount = float(risk.get("liquidity_amount") or 0.0)
        risk_share = float(risk.get("risk_contribution_share") or 0.0)
        estimated_cost_bps = float(cost.get("estimated_cost_bps") or 0.0)
        pct_chg = float(risk.get("pct_chg") or 0.0)
        anchor_price = float(
            risk.get("latest_price")
            or src.get("latest_price")
            or item.get("latest_price")
            or latest_price_map.get(ts_code, 0.0)
            or 0.0
        )
        risk_tags: List[str] = []
        if liquidity_amount < 600000:
            risk_tags.append("流动性敏感")
        if risk_share > 0.30:
            risk_tags.append("风险贡献偏高")
        if estimated_cost_bps > 25:
            risk_tags.append("交易成本偏高")
        risk_tag = "、".join(risk_tags) if risk_tags else "正常"
        target_weight = float(item.get("weight") or 0.0)
        execution_priority = _top5_execution_priority(
            target_weight=target_weight,
            liquidity_amount=liquidity_amount,
            estimated_cost_bps=estimated_cost_bps,
            risk_share=risk_share,
        )
        action_note = "按基准节奏分批执行"
        if "流动性敏感" in risk_tags:
            action_note = "先被动挂单，降低参与率"
        elif "风险贡献偏高" in risk_tags:
            action_note = "缩小子单上限并提高风控复核频率"
        elif "交易成本偏高" in risk_tags:
            action_note = "优先被动排队，避免追价"
        forecast_note = "盘前预判：预计窄幅震荡，可按计划推进。"
        if pct_chg <= -1.0:
            forecast_note = "盘前预判：短线承压后可能出现修复反弹，早盘波动偏大。"
        elif pct_chg >= 2.0:
            forecast_note = "盘前预判：高开后冲高回落风险上升，不宜追价。"
        if liquidity_amount < 600000:
            forecast_note = "盘前预判：流动性偏紧，成交分层明显，需控制参与率。"
        trade_plan = _derive_trade_plan(
            anchor_price=anchor_price,
            pct_chg=pct_chg,
            liquidity_amount=liquidity_amount,
            estimated_cost_bps=estimated_cost_bps,
            version=str(advice_version or "A"),
        )
        execution_controls = _top5_execution_controls(
            priority=execution_priority,
            risk_tag=risk_tag,
            estimated_cost_bps=estimated_cost_bps,
            liquidity_amount=liquidity_amount,
        )
        operation_advice = (
            f"{forecast_note} 执行建议：{action_note} "
            f"参考买入≈{trade_plan['参考买入价']:.2f}，"
            f"持有{trade_plan['建议持有天数']}天，"
            f"目标卖出≈{trade_plan['参考卖出价']:.2f}，"
            f"止损≈{trade_plan['止损价']:.2f}。"
        )

        rows.append(
            {
                "序号": idx,
                "股票代码": ts_code,
                "股票名称": stock_name,
                "行业": str(risk.get("industry") or ""),
                "目标权重": target_weight,
                "综合得分": float(src.get("final_stock_score") or 0.0),
                "流动性金额": liquidity_amount,
                "涨跌幅": pct_chg,
                "基准价格": round(anchor_price, 2) if anchor_price > 0 else np.nan,
                "参考买入价": trade_plan["参考买入价"],
                "建议持有天数": trade_plan["建议持有天数"],
                "参考卖出价": trade_plan["参考卖出价"],
                "止损价": trade_plan["止损价"],
                "参数版本": str(advice_version or "A").upper(),
                "预估成本(bp)": estimated_cost_bps,
                "风险贡献占比": risk_share,
                "执行优先级": execution_priority,
                "风险标签": risk_tag,
                "委托方式": execution_controls["委托方式"],
                "首波上限": execution_controls["首波上限"],
                "交易台硬门禁": execution_controls["交易台硬门禁"],
                "清单状态": "盘前复核后可执行",
                "操作建议": operation_advice,
                "触发应对": trade_plan["触发应对"],
                "审计竞赛运行ID": competition_run_id,
                "审计trade_date": trade_compact,
                "审计文件SHA256": artifact_digest,
                "来源审计路径": artifact_resolved,
            }
        )

    df = pd.DataFrame(rows)
    df.to_csv(csv_path, index=False)

    title_disp = (
        f"{trade_compact[0:4]}-{trade_compact[4:6]}-{trade_compact[6:8]}" if len(trade_compact) == 8 else datetime.now().strftime("%Y-%m-%d")
    )
    md_lines = [
        f"# Top5 交易员执行清单（审计锚定 {title_disp}）",
        "",
        f"- 来源审计文件：`{artifact_resolved}`",
        f"- 竞赛运行 ID：`{competition_run_id}`",
        f"- 审计锚定 trade_date：`{trade_compact or 'unknown'}`",
        f"- 审计文件 SHA256：`{artifact_digest}`",
        f"- 审计文件更新时间（UTC）：`{artifact_mtime_utc}`",
        f"- 审计模式：`{payload.get('audit_mode', 'unknown')}`",
        f"- 建议参数版本：`{str(advice_version or 'A').upper()}`",
        f"- 股票数量：`{len(top5_rows)}`",
        "",
        "## Top3 扫描血缘（可与 signal_runs 对账）",
        "",
    ]
    if lineage_rows:
        for row in lineage_rows:
            strat = str(row.get("strategy") or "")
            rid = str(row.get("run_id") or "")
            td = str(row.get("trade_date") or "")
            ct = str(row.get("created_at") or "")
            md_lines.append(f"- `{strat}` | run_id=`{rid}` | trade_date=`{td}` | created_at=`{ct}`")
    else:
        md_lines.append(
            "- （该审计产物无 `unified_signal_lineage` 字段——由旧版 `build_strategy_competition_portfolio_audit` 生成；请以 `竞赛运行 ID` / SHA256 对账或重新跑一次审计）"
        )
    md_lines.extend(
        [
            "",
            "## Top5 列表",
            "",
        ]
    )
    for row in rows:
        md_lines.append(
            f"{row['序号']}. `{row['股票代码']} {row['股票名称']}` | 行业={row['行业']} | 权重={row['目标权重']:.6f} | "
            f"得分={row['综合得分']:.3f} | 基准价={row['基准价格']} | 买入={row['参考买入价']} | "
            f"止盈={row['参考卖出价']} | 止损={row['止损价']} | 预估成本bp={row['预估成本(bp)']:.3f}"
        )
    md_lines.extend(
        [
            "",
            "## 开盘前检查",
            "",
            "- 清单状态必须为“盘前复核后可执行”；旧版/缺字段清单只能用于人工复核。",
            "- 确认竞价流动性未明显低于快照基线。",
            "- 若开盘跳空超过 2%，重新评估滑点预估。",
            "- 若实时滑点预估超过 35bp，立即冻结该标的执行。",
            "- 未完成成交回报与盘后归因前，不得把 Top5 表现用于生产晋级。",
            "",
            "## 执行优先级与动作",
            "",
        ]
    )
    for row in rows:
        md_lines.append(
            f"- `{row['执行优先级']}` `{row['股票代码']} {row['股票名称']}`：风险标签={row['风险标签']} | "
            f"委托方式={row['委托方式']} | 首波上限={row['首波上限']} | 建议={row['操作建议']} | "
            f"硬门禁={row['交易台硬门禁']}"
        )
    md_path.write_text("\n".join(md_lines) + "\n", encoding="utf-8")
    manifest_path = export_dir / "top5_trader_brief_latest_manifest.json"
    manifest_path.write_text(
        json.dumps(
            {
                "artifact_version": str(payload.get("artifact_version") or ""),
                "audit_artifact_created_at_payload": str(payload.get("created_at") or ""),
                "audit_artifact_mtime_epoch": float(artifact_stat.st_mtime),
                "audit_artifact_mtime_utc": artifact_mtime_utc,
                "audit_mode": str(payload.get("audit_mode") or "unknown"),
                "artifact_path": artifact_resolved,
                "artifact_sha256": artifact_digest,
                "competition_run_id": competition_run_id,
                "csv": str(csv_path.resolve()),
                "markdown": str(md_path.resolve()),
                "trade_date_compact": trade_compact,
                "top5_canary_gate": canary_gate,
                "unified_signal_lineage": lineage_rows,
                "written_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            },
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    return {"markdown": str(md_path), "csv": str(csv_path), "manifest": str(manifest_path)}


def _latest_valid_top5_audit_artifact(audit_dir: Path) -> Optional[Path]:
    artifacts = sorted(
        audit_dir.glob("strategy_competition_portfolio_audit_*.json"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    for artifact in artifacts:
        try:
            payload = json.loads(artifact.read_text(encoding="utf-8"))
        except Exception:
            continue
        rows = payload.get("top5_portfolio_audit") if isinstance(payload, dict) else None
        if isinstance(rows, list) and rows:
            return artifact
    return None



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
        artifact = _latest_valid_top5_audit_artifact(audit_dir)
        if artifact is None:
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
        built = _build_top5_trader_brief_from_artifact(artifact, advice_version=active_version)
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
