import json
import sqlite3
import subprocess
import sys
from pathlib import Path

from src.candidate_quality.realistic_backtest import (
    RealisticBacktestConfig,
    build_capacity_constraint_report,
    build_realistic_backtest,
    build_transaction_cost_breakdown,
)


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _build_db(db_path: Path, rows: list[tuple]) -> None:
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
            pct_chg REAL,
            vol REAL,
            amount REAL
        )
        """
    )
    conn.executemany("INSERT INTO daily_trading_data VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", rows)
    conn.commit()
    conn.close()


def _lineage(*codes: str, data_as_of: str = "20260506") -> dict:
    return {
        "schema_version": "candidate_lineage.v1",
        "status": "passed",
        "run_id": "candidate-test",
        "data_as_of": data_as_of,
        "candidates": [{"ts_code": code} for code in codes],
    }


def test_realistic_backtest_uses_trade_calendar_and_computes_return(tmp_path):
    db_path = tmp_path / "bt.db"
    _build_db(
        db_path,
        [
            ("000001.SZ", "20260507", 10.0, 10.4, 9.9, 10.2, 10.0, 2.0, 1_000_000, 10_000_000),
            ("000001.SZ", "20260508", 10.2, 10.5, 10.1, 10.4, 10.2, 2.0, 1_000_000, 10_000_000),
            ("000001.SZ", "20260511", 10.4, 10.7, 10.3, 10.6, 10.4, 2.0, 1_000_000, 10_000_000),
        ],
    )

    payload = build_realistic_backtest(
        db_path=db_path,
        lineage=_lineage("000001.SZ"),
        config=RealisticBacktestConfig(hold_trade_days=2, portfolio_notional=100_000),
    )

    assert payload["status"] == "passed"
    row = payload["candidates"][0]
    assert row["buy_trade_date"] == "20260507"
    assert row["sell_trade_date"] == "20260511"
    assert row["gross_realistic_return"] == 0.06
    assert row["realistic_return"] < row["gross_realistic_return"]
    assert row["transaction_costs"]["total_transaction_cost"] > 0


def test_transaction_cost_and_capacity_reports_are_generated(tmp_path):
    db_path = tmp_path / "bt.db"
    _build_db(
        db_path,
        [
            ("000001.SZ", "20260507", 10.0, 10.4, 9.9, 10.2, 10.0, 2.0, 1_000_000, 10_000_000),
            ("000001.SZ", "20260508", 10.2, 10.5, 10.1, 10.4, 10.2, 2.0, 1_000_000, 10_000_000),
        ],
    )

    payload = build_realistic_backtest(
        db_path=db_path,
        lineage=_lineage("000001.SZ"),
        config=RealisticBacktestConfig(hold_trade_days=1, portfolio_notional=100_000),
    )
    cost = build_transaction_cost_breakdown(payload)
    capacity = build_capacity_constraint_report(payload)

    assert cost["status"] == "passed"
    assert cost["summary"]["total_transaction_cost"] > 0
    assert capacity["status"] == "passed"
    assert capacity["summary"]["worst_participation_rate"] > 0


def test_capacity_constraint_can_review_or_block(tmp_path):
    db_path = tmp_path / "bt.db"
    _build_db(
        db_path,
        [
            ("000001.SZ", "20260507", 10.0, 10.4, 9.9, 10.2, 10.0, 2.0, 1_000_000, 1_000_000),
            ("000001.SZ", "20260508", 10.2, 10.5, 10.1, 10.4, 10.2, 2.0, 1_000_000, 1_000_000),
        ],
    )

    payload = build_realistic_backtest(
        db_path=db_path,
        lineage=_lineage("000001.SZ"),
        config=RealisticBacktestConfig(
            hold_trade_days=1,
            portfolio_notional=200_000,
            capacity_warn_participation_rate=0.05,
            capacity_block_participation_rate=0.10,
        ),
    )

    assert payload["status"] == "failed"
    assert payload["candidates"][0]["status"] == "blocked"
    assert payload["candidates"][0]["blocking_reasons"] == ["capacity_participation_exceeds_block_limit"]


def test_realistic_backtest_blocks_no_volume_and_limit_rules(tmp_path):
    db_path = tmp_path / "bt.db"
    _build_db(
        db_path,
        [
            ("000001.SZ", "20260507", 10.0, 10.0, 10.0, 11.0, 10.0, 10.0, 1_000_000, 10_000_000),
            ("000001.SZ", "20260508", 11.0, 11.0, 11.0, 11.0, 11.0, 0.0, 1_000_000, 10_000_000),
            ("000002.SZ", "20260507", 10.0, 10.2, 9.9, 10.0, 10.0, 0.0, 0, 0),
            ("000002.SZ", "20260508", 10.0, 10.2, 9.9, 10.1, 10.0, 1.0, 1_000_000, 10_000_000),
            ("000003.SZ", "20260507", 10.0, 10.2, 9.9, 10.0, 10.0, 0.0, 1_000_000, 10_000_000),
            ("000003.SZ", "20260508", 9.0, 9.0, 9.0, 9.0, 10.0, -10.0, 1_000_000, 10_000_000),
        ],
    )

    payload = build_realistic_backtest(
        db_path=db_path,
        lineage=_lineage("000001.SZ", "000002.SZ", "000003.SZ"),
        config=RealisticBacktestConfig(hold_trade_days=1),
    )
    by_code = {item["ts_code"]: item for item in payload["candidates"]}

    assert payload["status"] == "failed"
    assert by_code["000001.SZ"]["blocking_reasons"] == ["buy_limit_up"]
    assert by_code["000002.SZ"]["blocking_reasons"] == ["buy_suspended_or_no_trade"]
    assert by_code["000003.SZ"]["blocking_reasons"] == ["sell_limit_down"]


def test_realistic_backtest_cli_writes_latest_artifact(tmp_path):
    db_path = tmp_path / "bt.db"
    _build_db(
        db_path,
        [
            ("000001.SZ", "20260507", 10.0, 10.4, 9.9, 10.2, 10.0, 2.0, 1_000_000, 10_000_000),
            ("000001.SZ", "20260508", 10.2, 10.5, 10.1, 10.4, 10.2, 2.0, 1_000_000, 10_000_000),
        ],
    )
    lineage = tmp_path / "candidate_lineage_latest.json"
    lineage.write_text(json.dumps(_lineage("000001.SZ"), ensure_ascii=False), encoding="utf-8")

    result = subprocess.run(
        [
            sys.executable,
            str(PROJECT_ROOT / "scripts" / "build_candidate_realistic_backtest.py"),
            "--db-path",
            str(db_path),
            "--lineage",
            str(lineage),
            "--output-dir",
            str(tmp_path),
            "--hold-trade-days",
            "1",
            "--portfolio-notional",
            "100000",
            "--json",
        ],
        cwd=str(PROJECT_ROOT),
        check=True,
        capture_output=True,
        text=True,
    )
    payload = json.loads(result.stdout)

    assert payload["status"] == "passed"
    assert (tmp_path / "realistic_backtest_latest.json").exists()
    assert (tmp_path / "transaction_cost_breakdown_latest.json").exists()
    assert (tmp_path / "capacity_constraint_report_latest.json").exists()
