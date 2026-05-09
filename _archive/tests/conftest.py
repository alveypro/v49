"""Shared fixtures for openclaw test suite."""
from __future__ import annotations

import sqlite3
import tempfile
from pathlib import Path
from typing import Generator

import pytest


@pytest.fixture()
def tmp_db(tmp_path: Path) -> Generator[Path, None, None]:
    """Create a temporary SQLite DB with stock_basic and daily_trading_data."""
    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute(
        """
        CREATE TABLE stock_basic (
            ts_code TEXT PRIMARY KEY,
            name TEXT,
            industry TEXT,
            list_status TEXT DEFAULT 'L',
            circ_mv REAL DEFAULT 0,
            total_mv REAL DEFAULT 0
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE daily_trading_data (
            ts_code TEXT,
            trade_date TEXT,
            open_price REAL,
            high_price REAL,
            low_price REAL,
            close_price REAL,
            pre_close REAL DEFAULT 0,
            vol REAL DEFAULT 0,
            amount REAL DEFAULT 0,
            pct_chg REAL DEFAULT 0,
            turnover_rate REAL DEFAULT 0,
            PRIMARY KEY (ts_code, trade_date)
        )
        """
    )
    conn.execute(
        "INSERT INTO stock_basic (ts_code, name, industry) VALUES (?, ?, ?)",
        ("000001.SZ", "平安银行", "银行"),
    )
    conn.execute(
        "INSERT INTO stock_basic (ts_code, name, industry) VALUES (?, ?, ?)",
        ("600000.SH", "浦发银行", "银行"),
    )
    for i in range(60):
        date = f"2025{(i // 28 + 1):02d}{(i % 28 + 1):02d}"
        for code in ("000001.SZ", "600000.SH"):
            conn.execute(
                """
                INSERT INTO daily_trading_data
                (ts_code, trade_date, open_price, high_price, low_price, close_price, vol, amount, pct_chg)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (code, date, 10.0 + i * 0.1, 10.5 + i * 0.1, 9.8 + i * 0.1, 10.2 + i * 0.1, 100000, 1000000, 0.5),
            )
    conn.commit()
    conn.close()
    yield db_path
