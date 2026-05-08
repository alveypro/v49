import json
import sqlite3
import subprocess
import sys
from pathlib import Path

from src.candidate_quality.observation import (
    build_candidate_failure_attribution,
    build_candidate_observation_result,
    build_candidate_quality_20_sample_report,
)


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _build_db(db_path: Path, rows: list[tuple]) -> None:
    conn = sqlite3.connect(str(db_path))
    conn.execute("CREATE TABLE daily_trading_data (ts_code TEXT, trade_date TEXT, close_price REAL)")
    conn.executemany("INSERT INTO daily_trading_data VALUES (?, ?, ?)", rows)
    conn.commit()
    conn.close()


def _write_snapshot_and_ledger(tmp_path: Path, codes: list[str] | None = None) -> tuple[Path, Path]:
    codes = codes or ["000001.SZ"]
    items = []
    lines = []
    for idx, code in enumerate(codes, start=1):
        observation_id = f"20260506:{code}"
        items.append(
            {
                "observation_id": observation_id,
                "ts_code": code,
                "rank": idx,
                "selected_at": "20260506",
                "observation_horizons": [5, 20, 60],
                "selection_reason": "test",
            }
        )
        lines.append(json.dumps({"observation_id": observation_id, "ts_code": code}, ensure_ascii=False))
    snapshot = tmp_path / "candidate_observation_snapshot_latest.json"
    snapshot.write_text(
        json.dumps(
            {
                "schema_version": "candidate_observation_snapshot.v1",
                "status": "frozen",
                "snapshot_date": "20260506",
                "lineage_run_id": "candidate-test",
                "items": items,
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    ledger = tmp_path / "candidate_observation_ledger.jsonl"
    ledger.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return snapshot, ledger


def test_candidate_observation_result_computes_returns_and_drawdown(tmp_path):
    db_path = tmp_path / "prices.db"
    dates = ["20260506", "20260507", "20260508", "20260511", "20260512", "20260513", "20260514"]
    rows = [("000001.SZ", date, 10.0 + idx) for idx, date in enumerate(dates)]
    rows += [("000001.SH", date, 100.0 + idx) for idx, date in enumerate(dates)]
    _build_db(db_path, rows)
    snapshot, ledger = _write_snapshot_and_ledger(tmp_path)

    result = build_candidate_observation_result(
        snapshot_path=snapshot,
        ledger_path=ledger,
        sqlite_db_path=db_path,
        benchmark_ts_code="000001.SH",
        horizons=[5],
    )

    row = result["candidates"][0]
    assert result["status"] == "passed"
    assert row["status"] == "completed"
    assert row["returns"]["5d"] == 0.5
    assert row["hit_status"]["5d"] == "hit"


def test_candidate_observation_result_pending_when_future_window_not_ready(tmp_path):
    db_path = tmp_path / "prices.db"
    _build_db(db_path, [("000001.SZ", "20260506", 10.0), ("000001.SH", "20260506", 100.0)])
    snapshot, ledger = _write_snapshot_and_ledger(tmp_path)

    result = build_candidate_observation_result(
        snapshot_path=snapshot,
        ledger_path=ledger,
        sqlite_db_path=db_path,
        benchmark_ts_code="000001.SH",
        horizons=[5],
    )

    assert result["status"] == "pending"
    assert result["candidates"][0]["status"] == "pending"
    assert "insufficient_5d_trade_dates" in result["candidates"][0]["blocking_reasons"]


def test_candidate_observation_failure_attribution_and_20_sample_block(tmp_path):
    result = {
        "schema_version": "candidate_observation_result.v1",
        "status": "blocked",
        "candidates": [
            {
                "observation_id": "20260506:000001.SZ",
                "ts_code": "000001.SZ",
                "status": "blocked",
                "blocking_reasons": ["missing_5d_price"],
                "returns": {"5d": None},
                "excess_returns": {"5d": None},
                "max_drawdown": None,
            }
        ],
    }

    attribution = build_candidate_failure_attribution(result)
    report = build_candidate_quality_20_sample_report(result)

    assert attribution["items"][0]["primary_failure_category"] == "data_or_calendar_insufficient"
    assert report["status"] == "blocked"
    assert report["blocking_reasons"] == ["insufficient_completed_samples"]


def test_candidate_observation_closure_cli_writes_three_outputs(tmp_path):
    db_path = tmp_path / "prices.db"
    dates = ["20260506", "20260507", "20260508", "20260511", "20260512", "20260513", "20260514"]
    rows = [("000001.SZ", date, 10.0 + idx) for idx, date in enumerate(dates)]
    rows += [("000001.SH", date, 100.0 + idx) for idx, date in enumerate(dates)]
    _build_db(db_path, rows)
    _write_snapshot_and_ledger(tmp_path)

    result = subprocess.run(
        [
            sys.executable,
            str(PROJECT_ROOT / "scripts" / "build_candidate_observation_closure.py"),
            "--exp-dir",
            str(tmp_path),
            "--db-path",
            str(db_path),
            "--benchmark",
            "000001.SH",
            "--json",
        ],
        cwd=str(PROJECT_ROOT),
        check=True,
        capture_output=True,
        text=True,
    )
    payload = json.loads(result.stdout)

    assert payload["status"] == "pending"
    assert (tmp_path / "candidate_observation_result_latest.json").exists()
    assert (tmp_path / "candidate_failure_attribution_latest.json").exists()
    assert (tmp_path / "candidate_quality_20_sample_report.json").exists()
