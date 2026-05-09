import json
import sqlite3
import subprocess
import sys
from pathlib import Path

import pandas as pd

from src.candidate_quality.data_quality import (
    build_candidate_data_quality_gate,
    build_data_quality_report,
    write_data_quality_artifacts,
)


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _build_quality_db(db_path: Path) -> None:
    conn = sqlite3.connect(str(db_path))
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
            change_amount REAL,
            pct_chg REAL,
            vol REAL,
            amount REAL,
            turnover_rate REAL,
            created_at TEXT
        )
        """
    )
    rows = []
    for idx in range(1, 6):
        trade_date = f"2026050{idx}"
        rows.append(
            (
                "000001.SZ",
                trade_date,
                10.0 + idx,
                10.4 + idx,
                9.8 + idx,
                10.2 + idx,
                10.0 + idx,
                0.2,
                1.5,
                1_000_000 + idx,
                20_000_000 + idx,
                1.0,
                "2026-05-07T00:00:00",
            )
        )
    rows.extend(
        [
            (
                "000002.SZ",
                "20260501",
                0.0,
                10.0,
                9.0,
                9.5,
                9.3,
                0.2,
                2.0,
                0.0,
                0.0,
                0.2,
                "2026-05-07T00:00:00",
            ),
            (
                "000002.SZ",
                "20260502",
                9.5,
                60.0,
                9.4,
                55.0,
                9.5,
                45.5,
                45.0,
                1_000,
                5_000,
                0.2,
                "2026-05-07T00:00:00",
            ),
        ]
    )
    conn.executemany(
        """
        INSERT INTO daily_trading_data (
            ts_code, trade_date, open_price, high_price, low_price, close_price,
            pre_close, change_amount, pct_chg, vol, amount, turnover_rate, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    conn.commit()
    conn.close()


def test_data_quality_report_scores_and_blocks_bad_rows(tmp_path):
    db_path = tmp_path / "quality.db"
    _build_quality_db(db_path)

    report = build_data_quality_report(db_path=db_path, lookback_trade_days=5)
    by_code = {item["ts_code"]: item for item in report["stocks"]}

    assert report["status"] == "passed"
    assert by_code["000001.SZ"]["quality_level"] == "pass"
    assert by_code["000002.SZ"]["quality_level"] == "blocked"
    assert "insufficient_trade_date_coverage" in by_code["000002.SZ"]["blocking_reasons"]
    assert "nonpositive_price" in by_code["000002.SZ"]["blocking_reasons"]


def test_data_quality_report_missing_database_fails(tmp_path):
    report = build_data_quality_report(db_path=tmp_path / "missing.db")

    assert report["status"] == "failed"
    assert report["blocking_reasons"] == ["database_missing"]


def test_candidate_data_quality_gate_blocks_unproven_candidates(tmp_path):
    db_path = tmp_path / "quality.db"
    _build_quality_db(db_path)
    report = build_data_quality_report(db_path=db_path, lookback_trade_days=5)

    gate = build_candidate_data_quality_gate(
        report=report,
        candidate_codes=["000001.SZ", "000002.SZ", "300750.SZ"],
    )

    assert gate["status"] == "failed"
    assert gate["blocked_codes"] == ["000002.SZ", "300750.SZ"]
    missing = [item for item in gate["candidates"] if item["ts_code"] == "300750.SZ"][0]
    assert missing["blocking_reasons"] == ["missing_data_quality_record"]


def test_write_data_quality_artifacts_persists_json_and_csv(tmp_path):
    db_path = tmp_path / "quality.db"
    _build_quality_db(db_path)
    report = build_data_quality_report(db_path=db_path, lookback_trade_days=5)

    paths = write_data_quality_artifacts(report, output_dir=tmp_path)

    assert Path(paths["report_path"]).exists()
    assert Path(paths["stock_csv_path"]).read_text(encoding="utf-8").startswith("ts_code,quality_score")


def test_candidate_data_quality_report_cli_writes_report_and_gate(tmp_path):
    db_path = tmp_path / "quality.db"
    _build_quality_db(db_path)
    candidate_csv = tmp_path / "candidates.csv"
    pd.DataFrame({"ts_code": ["000001.SZ", "000002.SZ"]}).to_csv(candidate_csv, index=False)

    result = subprocess.run(
        [
            sys.executable,
            str(PROJECT_ROOT / "scripts" / "build_candidate_data_quality_report.py"),
            "--db-path",
            str(db_path),
            "--output-dir",
            str(tmp_path),
            "--candidate-csv",
            str(candidate_csv),
            "--lookback-trade-days",
            "5",
            "--json",
        ],
        cwd=str(PROJECT_ROOT),
        check=True,
        capture_output=True,
        text=True,
    )
    payload = json.loads(result.stdout)

    assert payload["status"] == "passed"
    assert payload["gate_status"] == "failed"
    assert (tmp_path / "data_quality_report_latest.json").exists()
    assert (tmp_path / "candidate_data_quality_gate_latest.json").exists()
