import json
import sqlite3
import subprocess
import sys
from pathlib import Path

from src.primary_result_market_data_readiness import build_primary_result_market_data_readiness


def _write_db(path: Path, *, missing_benchmark: bool = False) -> Path:
    conn = sqlite3.connect(path)
    conn.execute(
        """
        CREATE TABLE daily_trading_data (
            ts_code TEXT,
            trade_date TEXT,
            open_price REAL,
            high_price REAL,
            low_price REAL,
            close_price REAL,
            pre_close REAL,
            pct_chg REAL,
            vol REAL,
            amount REAL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE data_update_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            update_type TEXT,
            start_date TEXT,
            end_date TEXT,
            stocks_count INTEGER,
            success_count INTEGER,
            error_count INTEGER,
            status TEXT,
            error_message TEXT,
            created_at TEXT
        )
        """
    )
    rows = [
        ("300383.SZ", "20260415", 17.70, 18.10, 17.60, 17.85, 17.70, 0.8475, 1000000.0, 900000.0),
        ("300383.SZ", "20260416", 18.00, 18.90, 17.95, 18.78, 17.85, 5.2101, 1200000.0, 950000.0),
        ("000001.SH", "20260415", 4020.0, 4030.0, 4010.0, 4027.2095, 4010.0, 0.4292, 1000000.0, 2000000.0),
    ]
    if not missing_benchmark:
        rows.append(("000001.SH", "20260416", 4030.0, 4060.0, 4025.0, 4055.5468, 4027.2095, 0.7036, 1100000.0, 2200000.0))
    conn.executemany("INSERT INTO daily_trading_data VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", rows)
    conn.execute(
        """
        INSERT INTO data_update_log
        (update_type, start_date, end_date, stocks_count, success_count, error_count, status, error_message, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        ("daily_trading_update", "20260416", "20260416", 2, 1, 0, "completed", "", "2026-04-16 17:35:00"),
    )
    conn.commit()
    conn.close()
    return path


def test_primary_result_market_data_readiness_ready_when_target_and_benchmark_cover_window(tmp_path):
    db_path = _write_db(tmp_path / "stock.db")
    output = tmp_path / "market_data_readiness.json"

    exit_code, payload = build_primary_result_market_data_readiness(
        sqlite_db_path=db_path,
        ts_code="300383.SZ",
        benchmark_ts_code="000001.SH",
        window_start="2026-04-15",
        window_end="2026-04-16",
        output_path=output,
    )

    assert exit_code == 0
    assert payload["readiness_version"] == "primary_result_market_data_readiness.v1"
    assert payload["status"] == "ready"
    assert payload["target_coverage"]["window_row_count"] == 2
    assert payload["benchmark_coverage"]["window_row_count"] == 2
    assert payload["target_quality"]["latest_amount"] == 950000.0
    assert payload["latest_update_log"]["status"] == "completed"
    assert output.exists()


def test_primary_result_market_data_readiness_blocks_missing_benchmark_rows(tmp_path):
    db_path = _write_db(tmp_path / "stock.db", missing_benchmark=True)

    exit_code, payload = build_primary_result_market_data_readiness(
        sqlite_db_path=db_path,
        ts_code="300383.SZ",
        benchmark_ts_code="000001.SH",
        window_start="2026-04-15",
        window_end="2026-04-16",
    )

    assert exit_code == 1
    assert payload["status"] == "blocked"
    assert "benchmark ts_code must have enough rows in the observation window" in payload["blocking_reasons"]
    assert "benchmark latest trade date must cover window_end" in payload["blocking_reasons"]


def test_primary_result_market_data_readiness_rejects_unapproved_table(tmp_path):
    db_path = _write_db(tmp_path / "stock.db")

    exit_code, payload = build_primary_result_market_data_readiness(
        sqlite_db_path=db_path,
        sqlite_table="unsafe_table",
        ts_code="300383.SZ",
        benchmark_ts_code="000001.SH",
        window_start="2026-04-15",
        window_end="2026-04-16",
    )

    assert exit_code == 1
    assert payload["status"] == "blocked"
    assert "SQLite table must be explicitly allowed" in payload["blocking_reasons"]


def test_primary_result_market_data_readiness_blocks_future_window_without_reading_db(tmp_path):
    db_path = _write_db(tmp_path / "stock.db")

    exit_code, payload = build_primary_result_market_data_readiness(
        sqlite_db_path=db_path,
        ts_code="300383.SZ",
        benchmark_ts_code="000001.SH",
        window_start="2026-04-20T01:30:00Z",
        window_end="2026-04-18",
    )

    assert exit_code == 1
    assert payload["status"] == "blocked"
    assert "window_end must be greater than or equal to window_start" in payload["blocking_reasons"]
    assert "SQLite table must exist" not in payload["blocking_reasons"]
    assert payload["error"] is None


def test_primary_result_market_data_readiness_blocks_zero_amount(tmp_path):
    db_path = _write_db(tmp_path / "stock.db")
    conn = sqlite3.connect(db_path)
    conn.execute("UPDATE daily_trading_data SET amount=0 WHERE ts_code='300383.SZ' AND trade_date='20260416'")
    conn.commit()
    conn.close()

    exit_code, payload = build_primary_result_market_data_readiness(
        sqlite_db_path=db_path,
        ts_code="300383.SZ",
        benchmark_ts_code="000001.SH",
        window_start="2026-04-15",
        window_end="2026-04-16",
    )

    assert exit_code == 1
    assert payload["status"] == "blocked"
    assert "target must have positive volume and amount in the observation window" in payload["blocking_reasons"]
    assert "target latest amount must meet the liquidity capacity floor" in payload["blocking_reasons"]


def test_primary_result_market_data_readiness_blocks_target_limit_state(tmp_path):
    db_path = _write_db(tmp_path / "stock.db")
    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        UPDATE daily_trading_data
        SET close_price=21.42, high_price=21.42, low_price=18.70, pct_chg=20.0
        WHERE ts_code='300383.SZ' AND trade_date='20260416'
        """
    )
    conn.commit()
    conn.close()

    exit_code, payload = build_primary_result_market_data_readiness(
        sqlite_db_path=db_path,
        ts_code="300383.SZ",
        benchmark_ts_code="000001.SH",
        window_start="2026-04-15",
        window_end="2026-04-16",
    )

    assert exit_code == 1
    assert payload["status"] == "blocked"
    assert "target observation window must not include limit-up or limit-down states" in payload["blocking_reasons"]


def test_inspect_primary_result_market_data_readiness_cli_outputs_json(tmp_path):
    project_root = Path(__file__).resolve().parents[1]
    script_path = project_root / "scripts" / "inspect_primary_result_market_data_readiness.py"
    db_path = _write_db(tmp_path / "stock.db")
    output = tmp_path / "market_data_readiness.json"

    completed = subprocess.run(
        [
            sys.executable,
            str(script_path),
            "--sqlite-db",
            str(db_path),
            "--ts-code",
            "300383.SZ",
            "--benchmark-ts-code",
            "000001.SH",
            "--window-start",
            "2026-04-15",
            "--window-end",
            "2026-04-16",
            "--output",
            str(output),
            "--json",
        ],
        cwd=project_root,
        capture_output=True,
        text=True,
        check=True,
    )

    payload = json.loads(completed.stdout)
    assert payload["status"] == "ready"
    assert json.loads(output.read_text(encoding="utf-8"))["status"] == "ready"
