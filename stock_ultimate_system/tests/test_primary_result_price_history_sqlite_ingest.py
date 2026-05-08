import csv
import json
import sqlite3
import subprocess
import sys
from pathlib import Path

from src.primary_result_price_history_sqlite_ingest import import_primary_result_price_history_from_sqlite


def _write_sqlite_db(path: Path, *, benchmark_latest: bool = True) -> Path:
    conn = sqlite3.connect(path)
    conn.execute(
        """
        CREATE TABLE daily_trading_data (
            ts_code TEXT,
            trade_date TEXT,
            close_price REAL
        )
        """
    )
    rows = [
        ("300383.SZ", "20260415", 17.85),
        ("300383.SZ", "20260416", 18.78),
        ("000001.SZ", "20260415", 10.0),
    ]
    if benchmark_latest:
        rows.append(("000001.SZ", "20260416", 10.2))
    conn.executemany("INSERT INTO daily_trading_data VALUES (?, ?, ?)", rows)
    conn.execute("CREATE TABLE unrelated_table (ts_code TEXT, trade_date TEXT, close_price REAL)")
    conn.commit()
    conn.close()
    return path


def test_primary_result_price_history_sqlite_ingest_writes_canonical_csv_and_manifest(tmp_path):
    db_path = _write_sqlite_db(tmp_path / "stock.db")
    output_csv = tmp_path / "primary_result_price_history_latest.csv"
    manifest = tmp_path / "primary_result_price_history_sqlite_ingest_latest.json"

    exit_code, payload = import_primary_result_price_history_from_sqlite(
        sqlite_db_path=db_path,
        output_csv_path=output_csv,
        manifest_output_path=manifest,
        ts_code="300383.SZ",
        benchmark_ts_code="000001.SZ",
        window_start="2026-04-15T09:30:00Z",
        window_end="2026-04-16T15:00:00Z",
    )

    assert exit_code == 0
    assert payload["ingest_version"] == "primary_result_price_history_sqlite_ingest.v1"
    assert payload["status"] == "imported"
    assert payload["row_counts"] == {"output_total": 4, "observed": 2, "benchmark": 2}
    assert payload["sqlite_db"]["size_bytes"] > 0
    assert payload["output_csv_hash"]
    assert manifest.exists()
    with output_csv.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    assert rows == [
        {"ts_code": "000001.SZ", "trade_date": "2026-04-15", "close": "10.0"},
        {"ts_code": "000001.SZ", "trade_date": "2026-04-16", "close": "10.2"},
        {"ts_code": "300383.SZ", "trade_date": "2026-04-15", "close": "17.85"},
        {"ts_code": "300383.SZ", "trade_date": "2026-04-16", "close": "18.78"},
    ]


def test_primary_result_price_history_sqlite_ingest_blocks_missing_benchmark_window(tmp_path):
    db_path = _write_sqlite_db(tmp_path / "stock.db", benchmark_latest=False)
    output_csv = tmp_path / "primary_result_price_history_latest.csv"

    exit_code, payload = import_primary_result_price_history_from_sqlite(
        sqlite_db_path=db_path,
        output_csv_path=output_csv,
        ts_code="300383.SZ",
        benchmark_ts_code="000001.SZ",
        window_start="2026-04-15",
        window_end="2026-04-16",
    )

    assert exit_code == 1
    assert payload["status"] == "blocked"
    assert "benchmark must have at least two rows in window" in payload["blocking_reasons"]
    assert not output_csv.exists()


def test_primary_result_price_history_sqlite_ingest_rejects_unapproved_table(tmp_path):
    db_path = _write_sqlite_db(tmp_path / "stock.db")

    exit_code, payload = import_primary_result_price_history_from_sqlite(
        sqlite_db_path=db_path,
        sqlite_table="unrelated_table",
        ts_code="300383.SZ",
        benchmark_ts_code="000001.SZ",
        window_start="2026-04-15",
        window_end="2026-04-16",
    )

    assert exit_code == 1
    assert payload["status"] == "blocked"
    assert "SQLite table must be explicitly allowed" in payload["blocking_reasons"]


def test_import_primary_result_price_history_from_sqlite_cli(tmp_path):
    project_root = Path(__file__).resolve().parents[1]
    script_path = project_root / "scripts" / "import_primary_result_price_history_from_sqlite.py"
    db_path = _write_sqlite_db(tmp_path / "stock.db")
    output_csv = tmp_path / "primary_result_price_history_latest.csv"
    manifest = tmp_path / "primary_result_price_history_sqlite_ingest_latest.json"

    completed = subprocess.run(
        [
            sys.executable,
            str(script_path),
            "--sqlite-db",
            str(db_path),
            "--output-csv",
            str(output_csv),
            "--manifest-output",
            str(manifest),
            "--ts-code",
            "300383.SZ",
            "--benchmark-ts-code",
            "000001.SZ",
            "--window-start",
            "2026-04-15",
            "--window-end",
            "2026-04-16",
            "--json",
        ],
        cwd=project_root,
        capture_output=True,
        text=True,
        check=True,
    )

    payload = json.loads(completed.stdout)
    assert payload["status"] == "imported"
    assert json.loads(manifest.read_text(encoding="utf-8"))["status"] == "imported"
