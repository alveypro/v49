from __future__ import annotations

import hashlib
import json
import sqlite3
import subprocess
from pathlib import Path
from typing import Any, Dict, Iterable, Optional

from openclaw.paths import project_root


DEFAULT_DATA_TABLES = (
    ("daily_trading_data", "trade_date"),
    ("moneyflow_daily", "trade_date"),
    ("moneyflow_ind_ths", "trade_date"),
    ("top_list", "trade_date"),
    ("margin_detail", "trade_date"),
    ("stk_factor_pro_daily", "trade_date"),
    ("cyq_perf_daily", "trade_date"),
    ("hk_hold_daily", "trade_date"),
)


def canonical_json(value: Any) -> str:
    return json.dumps(value if value is not None else {}, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def build_param_version(params: Optional[Dict[str, Any]]) -> str:
    body = canonical_json(params or {})
    return f"param:sha256:{sha256_text(body)}"


def _compact_date(value: Any) -> str:
    return str(value or "").strip().replace("-", "")


def _table_max(conn: sqlite3.Connection, table: str, column: str, *, as_of_date: str = "") -> str:
    exists = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (table,),
    ).fetchone()
    if not exists:
        return ""
    if as_of_date:
        row = conn.execute(
            f"SELECT MAX({column}) FROM {table} WHERE REPLACE({column}, '-', '') <= ?",
            (_compact_date(as_of_date),),
        ).fetchone()
    else:
        row = conn.execute(f"SELECT MAX({column}) FROM {table}").fetchone()
    return str((row or [""])[0] or "")


def build_data_version(
    conn: sqlite3.Connection,
    *,
    tables: Iterable[tuple[str, str]] = DEFAULT_DATA_TABLES,
    as_of_date: str = "",
) -> str:
    parts: list[str] = []
    latest_trade_date = ""
    normalized_as_of = _compact_date(as_of_date)
    for table, column in tables:
        latest = _table_max(conn, table, column, as_of_date=normalized_as_of)
        if table == "daily_trading_data":
            latest_trade_date = latest
        parts.append(f"max_{table}={latest}")
    if normalized_as_of:
        parts.append(f"as_of_date={normalized_as_of}")
    digest_src = "|".join(parts)
    digest = sha256_text(digest_src)[:12]
    return f"trade_date:{latest_trade_date}|{'|'.join(parts)}|db_hash={digest}"


def build_code_version(*, root: Optional[Path] = None) -> str:
    repo_root = root or project_root()
    try:
        sha = subprocess.check_output(
            ["git", "-C", str(repo_root), "rev-parse", "HEAD"],
            text=True,
        ).strip()
        dirty = subprocess.run(
            ["git", "-C", str(repo_root), "diff", "--quiet"],
            check=False,
        ).returncode
        return f"git:{sha[:12]}:dirty{1 if dirty else 0}"
    except Exception:
        return "git:unknown:dirty1"
