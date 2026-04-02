"""Tests for data.dao — database access layer."""
from __future__ import annotations

import os
import sqlite3
from pathlib import Path

import pytest

from data.dao import (
    DataAccessError,
    db_conn,
    detect_daily_table,
    latest_trade_date,
    read_schema_version,
    resolve_db_path,
    stock_filter_sql,
    table_exists,
    table_has_column,
    table_max_value,
)


class TestResolveDbPath:
    def test_finds_existing_db(self, tmp_db: Path):
        result = resolve_db_path(str(tmp_db))
        assert result == tmp_db.resolve()

    def test_raises_when_nothing_found(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
        bogus = str(tmp_path / "nonexistent.db")
        monkeypatch.chdir(tmp_path)
        monkeypatch.delenv("PERMANENT_DB_PATH", raising=False)
        monkeypatch.delenv("OPENCLAW_DB_PATH", raising=False)
        monkeypatch.delenv("AIRIVO_DB_PATH", raising=False)
        with pytest.raises(DataAccessError, match="No available DB path"):
            resolve_db_path(bogus)

    def test_env_override(self, tmp_db: Path, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("PERMANENT_DB_PATH", str(tmp_db))
        result = resolve_db_path()
        assert result == tmp_db.resolve()


class TestDbConn:
    def test_context_manager_yields_connection(self, tmp_db: Path):
        with db_conn(str(tmp_db)) as conn:
            assert isinstance(conn, sqlite3.Connection)
            cur = conn.execute("SELECT COUNT(*) FROM stock_basic")
            assert cur.fetchone()[0] == 2

    def test_connection_closed_after_exit(self, tmp_db: Path):
        with db_conn(str(tmp_db)) as conn:
            pass
        with pytest.raises(Exception):
            conn.execute("SELECT 1")


class TestDetectDailyTable:
    def test_detects_daily_trading_data(self, tmp_db: Path):
        conn = sqlite3.connect(str(tmp_db))
        assert detect_daily_table(conn) == "daily_trading_data"
        conn.close()

    def test_raises_when_no_table(self, tmp_path: Path):
        empty_db = tmp_path / "empty.db"
        conn = sqlite3.connect(str(empty_db))
        conn.execute("CREATE TABLE dummy (id INTEGER)")
        with pytest.raises(DataAccessError, match="daily table not found"):
            detect_daily_table(conn)
        conn.close()


class TestLatestTradeDate:
    def test_returns_max_date(self, tmp_db: Path):
        conn = sqlite3.connect(str(tmp_db))
        result = latest_trade_date(conn)
        assert result is not None
        assert len(result) == 8
        conn.close()


class TestStockFilterSql:
    def test_with_list_status(self, tmp_db: Path):
        conn = sqlite3.connect(str(tmp_db))
        sql = stock_filter_sql(conn)
        assert "list_status" in sql
        conn.close()


class TestTableHelpers:
    def test_table_exists(self, tmp_db: Path):
        conn = sqlite3.connect(str(tmp_db))
        assert table_exists(conn, "stock_basic") is True
        assert table_exists(conn, "nonexistent_table") is False
        conn.close()

    def test_table_has_column(self, tmp_db: Path):
        conn = sqlite3.connect(str(tmp_db))
        assert table_has_column(conn, "stock_basic", "ts_code") is True
        assert table_has_column(conn, "stock_basic", "nonexistent_col") is False
        conn.close()

    def test_table_max_value(self, tmp_db: Path):
        conn = sqlite3.connect(str(tmp_db))
        result = table_max_value(conn, "daily_trading_data", "trade_date")
        assert result is not None
        conn.close()


class TestSchemaVersion:
    def test_read_schema_version_creates_table(self, tmp_db: Path):
        conn = sqlite3.connect(str(tmp_db))
        version, name = read_schema_version(conn)
        assert version == 0
        assert name == "base"
        conn.close()
