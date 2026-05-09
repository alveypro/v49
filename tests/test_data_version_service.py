from __future__ import annotations

import sqlite3

from openclaw.services.data_version_service import build_data_version


def test_build_data_version_can_pin_historical_as_of_date():
    conn = sqlite3.connect(":memory:")
    conn.executescript(
        """
        CREATE TABLE daily_trading_data(trade_date TEXT);
        CREATE TABLE moneyflow_daily(trade_date TEXT);
        INSERT INTO daily_trading_data(trade_date) VALUES ('20260320'), ('20260402');
        INSERT INTO moneyflow_daily(trade_date) VALUES ('20260319'), ('20260402');
        """
    )

    version = build_data_version(conn, as_of_date="2026-03-20")

    assert version.startswith("trade_date:20260320|")
    assert "max_daily_trading_data=20260320" in version
    assert "max_moneyflow_daily=20260319" in version
    assert "as_of_date=20260320" in version


def test_build_data_version_defaults_to_latest_database_dates():
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE daily_trading_data(trade_date TEXT)")
    conn.executemany("INSERT INTO daily_trading_data(trade_date) VALUES (?)", [("20260320",), ("20260402",)])

    version = build_data_version(conn)

    assert version.startswith("trade_date:20260402|")
    assert "max_daily_trading_data=20260402" in version
