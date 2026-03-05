from __future__ import annotations

import os
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Dict, Generator, Optional, Tuple


class DataAccessError(RuntimeError):
    pass


def resolve_db_path(preferred: Optional[str] = None) -> Path:
    try:
        from openclaw.paths import db_path as _canonical_db_path
        return _canonical_db_path(preferred)
    except Exception:
        pass

    env_candidates = [
        os.getenv("PERMANENT_DB_PATH", "").strip(),
        os.getenv("OPENCLAW_DB_PATH", "").strip(),
        os.getenv("AIRIVO_DB_PATH", "").strip(),
    ]

    cwd_candidates = [
        str(Path("permanent_stock_database.db").resolve()),
        str(Path("permanent_stock_database.backup.db").resolve()),
        str(Path("trading_assistant.db").resolve()),
    ]

    abs_candidates = [
        "/opt/openclaw/permanent_stock_database.db",
        "/opt/openclaw/permanent_stock_database.backup.db",
        "/opt/airivo/data/permanent_stock_database.db",
        "/opt/airivo/permanent_stock_database.db",
        "/opt/airivo/app/permanent_stock_database.db",
        "/opt/airivo/app/trading_assistant.db",
    ]

    candidates = [preferred or ""] + env_candidates + cwd_candidates + abs_candidates
    for p in candidates:
        if p and Path(p).exists():
            return Path(p).resolve()
    raise DataAccessError(
        "No available DB path. Set PERMANENT_DB_PATH/OPENCLAW_DB_PATH/AIRIVO_DB_PATH."
    )


@contextmanager
def db_conn(preferred: Optional[str] = None) -> Generator[sqlite3.Connection, None, None]:
    path = resolve_db_path(preferred)
    conn = sqlite3.connect(str(path))
    try:
        yield conn
    finally:
        conn.close()


def detect_daily_table(conn: sqlite3.Connection) -> str:
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = {r[0] for r in cur.fetchall()}
    if "daily_trading_data" in tables:
        return "daily_trading_data"
    if "daily_data" in tables:
        return "daily_data"
    raise DataAccessError("daily table not found (daily_trading_data/daily_data)")


def stock_filter_sql(conn: sqlite3.Connection) -> str:
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(stock_basic)")
    cols = {r[1] for r in cur.fetchall()}
    if "list_status" in cols:
        return "list_status='L'"
    if "is_active" in cols:
        return "is_active=1"
    return "1=1"


def latest_trade_date(conn: sqlite3.Connection) -> Optional[str]:
    table = detect_daily_table(conn)
    cur = conn.cursor()
    cur.execute(f"SELECT MAX(trade_date) FROM {table}")
    row = cur.fetchone()
    return str(row[0]) if row and row[0] is not None else None


def recent_trade_profile(
    conn: sqlite3.Connection,
    date_limit: int = 10,
    recent_window: int = 3,
) -> Dict[str, object]:
    table = detect_daily_table(conn)
    cur = conn.cursor()

    cur.execute(f"SELECT MAX(trade_date) FROM {table}")
    row = cur.fetchone()
    last_trade = str(row[0]) if row and row[0] is not None else ""

    distinct_dates = []
    if date_limit > 0:
        cur.execute(f"SELECT DISTINCT trade_date FROM {table} ORDER BY trade_date DESC LIMIT ?", (int(date_limit),))
        distinct_dates = [str(r[0]) for r in cur.fetchall() if r and r[0] is not None]

    records_last_trade_date = 0
    if last_trade:
        cur.execute(f"SELECT COUNT(*) FROM {table} WHERE trade_date = ?", (last_trade,))
        r = cur.fetchone()
        records_last_trade_date = int(r[0]) if r and r[0] is not None else 0

    recent_counts: Dict[str, int] = {}
    for d in distinct_dates[: max(0, int(recent_window))]:
        cur.execute(f"SELECT COUNT(*) FROM {table} WHERE trade_date = ?", (d,))
        r = cur.fetchone()
        recent_counts[d] = int(r[0]) if r and r[0] is not None else 0

    return {
        "daily_table": table,
        "last_trade_date": last_trade,
        "records_last_trade_date": records_last_trade_date,
        "recent_trade_dates": distinct_dates,
        "recent_counts": recent_counts,
    }


def table_exists(conn: sqlite3.Connection, name: str) -> bool:
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (name,))
    return cur.fetchone() is not None


def table_has_column(conn: sqlite3.Connection, table: str, col: str) -> bool:
    try:
        cur = conn.cursor()
        cur.execute(f"PRAGMA table_info({table})")
        cols = [row[1] for row in cur.fetchall()]
        return col in cols
    except Exception:
        return False


def table_max_value(conn: sqlite3.Connection, table: str, col: str) -> Optional[str]:
    cur = conn.cursor()
    cur.execute(f"SELECT MAX({col}) FROM {table}")
    row = cur.fetchone()
    if not row or row[0] is None:
        return None
    return str(row[0])


def read_schema_version(conn: sqlite3.Connection) -> Tuple[int, str]:
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS schema_version (
            version INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            applied_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    cur.execute("SELECT version, name FROM schema_version ORDER BY version DESC LIMIT 1")
    row = cur.fetchone()
    if not row:
        return 0, "base"
    return int(row[0]), str(row[1])
