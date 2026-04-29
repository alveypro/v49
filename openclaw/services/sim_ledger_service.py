from __future__ import annotations

import sqlite3
from typing import Any, Callable, Dict, List, Optional

import pandas as pd


def init_sim_db(*, db_path: str) -> None:
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS sim_account (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            initial_cash REAL,
            cash REAL,
            per_buy_amount REAL,
            auto_buy_top_n INTEGER,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS sim_positions (
            ts_code TEXT PRIMARY KEY,
            name TEXT,
            shares INTEGER,
            avg_cost REAL,
            buy_date TEXT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS sim_trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            trade_date TEXT,
            ts_code TEXT,
            name TEXT,
            side TEXT,
            price REAL,
            shares INTEGER,
            amount REAL,
            pnl REAL,
            batch_id TEXT,
            source TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS sim_auto_buy_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_time TEXT,
            signature TEXT,
            status TEXT,
            buy_count INTEGER,
            message TEXT,
            top_n INTEGER,
            per_buy_amount REAL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS sim_meta (
            key TEXT PRIMARY KEY,
            value TEXT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    cursor.execute(
        """
        INSERT OR IGNORE INTO sim_account (id, initial_cash, cash, per_buy_amount, auto_buy_top_n)
        VALUES (1, 1000000.0, 1000000.0, 100000.0, 10)
        """
    )
    cursor.execute(
        """
        INSERT OR IGNORE INTO sim_meta (key, value)
        VALUES ('auto_buy_enabled', '1')
        """
    )
    conn.commit()
    conn.close()


def get_sim_account(*, db_path: str, safe_float: Callable[[Any, float], float]) -> Dict[str, Any]:
    init_sim_db(db_path=db_path)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT initial_cash, cash, per_buy_amount, auto_buy_top_n
        FROM sim_account WHERE id = 1
        """
    )
    row = cursor.fetchone()
    conn.close()
    if not row:
        return {"initial_cash": 1000000.0, "cash": 1000000.0, "per_buy_amount": 100000.0, "auto_buy_top_n": 10}
    return {
        "initial_cash": safe_float(row[0], 1000000.0),
        "cash": safe_float(row[1], 1000000.0),
        "per_buy_amount": safe_float(row[2], 100000.0),
        "auto_buy_top_n": int(row[3]) if row[3] is not None else 10,
    }


def update_sim_account(
    *,
    db_path: str,
    initial_cash: Optional[float] = None,
    cash: Optional[float] = None,
    per_buy_amount: Optional[float] = None,
    auto_buy_top_n: Optional[int] = None,
) -> None:
    fields = []
    params: List[Any] = []
    if initial_cash is not None:
        fields.append("initial_cash = ?")
        params.append(float(initial_cash))
    if cash is not None:
        fields.append("cash = ?")
        params.append(float(cash))
    if per_buy_amount is not None:
        fields.append("per_buy_amount = ?")
        params.append(float(per_buy_amount))
    if auto_buy_top_n is not None:
        fields.append("auto_buy_top_n = ?")
        params.append(int(auto_buy_top_n))
    if not fields:
        return
    params.append(1)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(
        f"UPDATE sim_account SET {', '.join(fields)}, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
        params,
    )
    conn.commit()
    conn.close()


def reset_sim_account(*, db_path: str, initial_cash: float, per_buy_amount: float, auto_buy_top_n: int) -> None:
    init_sim_db(db_path=db_path)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("DELETE FROM sim_positions")
    cursor.execute("DELETE FROM sim_trades")
    cursor.execute("DELETE FROM sim_auto_buy_log")
    cursor.execute(
        """
        UPDATE sim_account
        SET initial_cash = ?, cash = ?, per_buy_amount = ?, auto_buy_top_n = ?, updated_at = CURRENT_TIMESTAMP
        WHERE id = 1
        """,
        (float(initial_cash), float(initial_cash), float(per_buy_amount), int(auto_buy_top_n)),
    )
    cursor.execute("DELETE FROM sim_meta WHERE key IN ('last_ai_signature', 'last_ai_buy_time')")
    conn.commit()
    conn.close()


def get_sim_positions(*, db_path: str, safe_float: Callable[[Any, float], float]) -> Dict[str, Dict[str, Any]]:
    init_sim_db(db_path=db_path)
    conn = sqlite3.connect(db_path)
    df = pd.read_sql_query("SELECT * FROM sim_positions ORDER BY buy_date DESC", conn)
    conn.close()
    positions: Dict[str, Dict[str, Any]] = {}
    for _, row in df.iterrows():
        positions[row["ts_code"]] = {
            "name": row.get("name"),
            "shares": int(row.get("shares") or 0),
            "avg_cost": safe_float(row.get("avg_cost"), 0.0),
            "buy_date": row.get("buy_date") or "",
        }
    return positions


def upsert_sim_position(*, db_path: str, ts_code: str, name: str, shares: int, avg_cost: float, buy_date: str) -> None:
    init_sim_db(db_path=db_path)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO sim_positions (ts_code, name, shares, avg_cost, buy_date)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(ts_code) DO UPDATE SET
            name = excluded.name,
            shares = excluded.shares,
            avg_cost = excluded.avg_cost,
            buy_date = CASE
                WHEN sim_positions.buy_date IS NULL OR sim_positions.buy_date = '' THEN excluded.buy_date
                ELSE sim_positions.buy_date
            END,
            updated_at = CURRENT_TIMESTAMP
        """,
        (ts_code, name, int(shares), float(avg_cost), buy_date),
    )
    conn.commit()
    conn.close()


def delete_sim_position(*, db_path: str, ts_code: str) -> None:
    init_sim_db(db_path=db_path)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("DELETE FROM sim_positions WHERE ts_code = ?", (ts_code,))
    conn.commit()
    conn.close()


def add_sim_trade(
    *,
    db_path: str,
    trade_date: str,
    ts_code: str,
    name: str,
    side: str,
    price: float,
    shares: int,
    amount: float,
    pnl: float,
    batch_id: str = "",
    source: str = "",
) -> None:
    init_sim_db(db_path=db_path)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO sim_trades (trade_date, ts_code, name, side, price, shares, amount, pnl, batch_id, source)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (trade_date, ts_code, name, side, float(price), int(shares), float(amount), float(pnl), batch_id, source),
    )
    conn.commit()
    conn.close()


def get_sim_trades(*, db_path: str, limit: int = 500) -> pd.DataFrame:
    init_sim_db(db_path=db_path)
    conn = sqlite3.connect(db_path)
    trades = pd.read_sql_query(
        f"SELECT * FROM sim_trades ORDER BY trade_date DESC, created_at DESC LIMIT {int(limit)}",
        conn,
    )
    conn.close()
    return trades


def get_sim_meta(*, db_path: str, key: str) -> str:
    init_sim_db(db_path=db_path)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT value FROM sim_meta WHERE key = ?", (key,))
    row = cursor.fetchone()
    conn.close()
    return row[0] if row else ""


def set_sim_meta(*, db_path: str, key: str, value: str) -> None:
    init_sim_db(db_path=db_path)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO sim_meta (key, value, updated_at)
        VALUES (?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = CURRENT_TIMESTAMP
        """,
        (key, value),
    )
    conn.commit()
    conn.close()


def get_sim_auto_buy_max_total_amount(*, db_path: str, safe_float: Callable[[Any, float], float]) -> float:
    value = get_sim_meta(db_path=db_path, key="auto_buy_max_total_amount")
    return safe_float(value, 0.0)


def get_sim_auto_buy_enabled(*, db_path: str) -> bool:
    value = get_sim_meta(db_path=db_path, key="auto_buy_enabled")
    return value != "0"


def set_sim_auto_buy_enabled(*, db_path: str, enabled: bool) -> None:
    set_sim_meta(db_path=db_path, key="auto_buy_enabled", value="1" if enabled else "0")


def add_sim_auto_buy_log(
    *,
    db_path: str,
    run_time: str,
    signature: str,
    status: str,
    buy_count: int,
    message: str,
    top_n: int,
    per_buy_amount: float,
) -> None:
    init_sim_db(db_path=db_path)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO sim_auto_buy_log (run_time, signature, status, buy_count, message, top_n, per_buy_amount)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (run_time, signature, status, int(buy_count), message, int(top_n), float(per_buy_amount)),
    )
    conn.commit()
    conn.close()


def get_sim_auto_buy_logs(*, db_path: str, limit: int = 50) -> pd.DataFrame:
    init_sim_db(db_path=db_path)
    conn = sqlite3.connect(db_path)
    logs = pd.read_sql_query(
        f"SELECT * FROM sim_auto_buy_log ORDER BY run_time DESC, created_at DESC LIMIT {int(limit)}",
        conn,
    )
    conn.close()
    return logs
