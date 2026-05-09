from __future__ import annotations

import sqlite3

from openclaw.services.formal_pool_benchmark_service import build_formal_pool_benchmark_return_series


def _init_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE signal_runs (
            run_id TEXT PRIMARY KEY,
            run_type TEXT NOT NULL,
            strategy TEXT NOT NULL,
            trade_date TEXT NOT NULL DEFAULT '',
            status TEXT NOT NULL DEFAULT 'created',
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE signal_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT NOT NULL,
            ts_code TEXT NOT NULL,
            score REAL NOT NULL DEFAULT 0,
            rank_idx INTEGER NOT NULL DEFAULT 0
        );
        CREATE TABLE daily_trading_data (
            ts_code TEXT,
            trade_date TEXT,
            close_price REAL
        );
        """
    )


def test_formal_pool_benchmark_replays_forward_returns_from_scan_items():
    conn = sqlite3.connect(":memory:")
    _init_schema(conn)
    conn.execute(
        "INSERT INTO signal_runs(run_id, run_type, strategy, trade_date, status) VALUES (?, ?, ?, ?, ?)",
        ("scan_v5", "scan", "v5", "20260102", "success"),
    )
    conn.execute(
        "INSERT INTO signal_items(run_id, ts_code, score, rank_idx) VALUES (?, ?, ?, ?)",
        ("scan_v5", "000001.SZ", 80.0, 1),
    )
    conn.executemany(
        "INSERT INTO daily_trading_data(ts_code, trade_date, close_price) VALUES (?, ?, ?)",
        [
            ("000001.SZ", "20260102", 10.0),
            ("000001.SZ", "20260103", 10.5),
            ("000001.SZ", "20260104", 11.0),
        ],
    )

    review = build_formal_pool_benchmark_return_series(
        conn,
        strategies=["v5"],
        as_of_date="2026-01-02",
        holding_days=2,
        top_n_per_strategy=3,
    )

    assert review["available"] is True
    assert review["run_count"] == 1
    assert review["signal_count"] == 1
    assert review["return_series_pct"] == [10.000000000000009]
    assert review["contract"]["no_proxy_metrics"] is True


def test_formal_pool_benchmark_blocks_when_forward_prices_are_missing():
    conn = sqlite3.connect(":memory:")
    _init_schema(conn)
    conn.execute(
        "INSERT INTO signal_runs(run_id, run_type, strategy, trade_date, status) VALUES (?, ?, ?, ?, ?)",
        ("scan_v5", "scan", "v5", "20260102", "success"),
    )
    conn.execute(
        "INSERT INTO signal_items(run_id, ts_code, score, rank_idx) VALUES (?, ?, ?, ?)",
        ("scan_v5", "000001.SZ", 80.0, 1),
    )

    review = build_formal_pool_benchmark_return_series(
        conn,
        strategies=["v5"],
        as_of_date="2026-01-02",
        holding_days=2,
    )

    assert review["available"] is False
    assert "no_replayable_formal_pool_forward_returns" in review["blocking_reasons"]
    assert any(reason.startswith("return_replay_blocking_counts:") for reason in review["blocking_reasons"])


def test_formal_pool_benchmark_blocks_when_lineage_tables_are_missing():
    conn = sqlite3.connect(":memory:")

    review = build_formal_pool_benchmark_return_series(
        conn,
        strategies=["v5"],
        as_of_date="2026-01-02",
        holding_days=2,
    )

    assert review["available"] is False
    assert "missing_signal_runs_table" in review["blocking_reasons"]
    assert "missing_signal_items_table" in review["blocking_reasons"]
    assert "missing_daily_trading_data_table" in review["blocking_reasons"]


def test_formal_pool_benchmark_matches_scan_runs_with_dashed_trade_dates():
    conn = sqlite3.connect(":memory:")
    _init_schema(conn)
    conn.execute(
        "INSERT INTO signal_runs(run_id, run_type, strategy, trade_date, status) VALUES (?, ?, ?, ?, ?)",
        ("scan_v5", "scan", "v5", "2026-01-02", "success"),
    )
    conn.execute(
        "INSERT INTO signal_items(run_id, ts_code, score, rank_idx) VALUES (?, ?, ?, ?)",
        ("scan_v5", "000001.SZ", 80.0, 1),
    )
    conn.executemany(
        "INSERT INTO daily_trading_data(ts_code, trade_date, close_price) VALUES (?, ?, ?)",
        [
            ("000001.SZ", "20260102", 10.0),
            ("000001.SZ", "20260103", 11.0),
        ],
    )

    review = build_formal_pool_benchmark_return_series(
        conn,
        strategies=["v5"],
        as_of_date="20260102",
        holding_days=1,
    )

    assert review["available"] is True
    assert review["run_count"] == 1
