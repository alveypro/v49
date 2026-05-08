import json
import sqlite3
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

from src.primary_result_audit import build_primary_result_audit
from src.primary_result_daily_closure_orchestrator import run_primary_result_daily_closure_orchestrator
from src.primary_result_execution import build_primary_result_execution
from src.primary_result_observation import build_primary_result_observation
from src.primary_result_rollback import build_primary_result_rollback
from src.unified_result_builder import build_primary_result_api_payload


def _write_primary_result_artifacts(tmp_path: Path) -> Path:
    exp_dir = tmp_path / "data" / "experiments"
    exp_dir.mkdir(parents=True)
    (exp_dir / "candidates_top_latest.csv").write_text(
        "ts_code,stock_name,industry,signal,risk_level,final_score\n"
        "000001.SZ,平安银行,银行,strong_buy,medium,155\n",
        encoding="utf-8",
    )
    (exp_dir / "daily_research_status_latest.json").write_text('{"state":"completed"}', encoding="utf-8")
    (exp_dir / "buylist_latest.json").write_text('{"items":[{"ts_code":"000001.SZ"}]}', encoding="utf-8")
    (exp_dir / "governance_audit_latest.json").write_text("{}", encoding="utf-8")
    (exp_dir / "t1_execution_checklist_latest.json").write_text("{}", encoding="utf-8")
    (exp_dir / "t12_rollback_drill_latest.json").write_text('{"triggered": false}', encoding="utf-8")
    return exp_dir


def _promote_to_observing(exp_dir: Path) -> None:
    l2_payload = build_primary_result_api_payload(exp_dir)
    audit = build_primary_result_audit(
        l2_payload,
        max_source_age_hours=100000,
        now=datetime(2026, 4, 15, 7, 30, 20, tzinfo=timezone.utc),
    )
    (exp_dir / "primary_result_audit_latest.json").write_text(json.dumps(audit, ensure_ascii=False), encoding="utf-8")
    l3_payload = build_primary_result_api_payload(exp_dir)
    execution = build_primary_result_execution(l3_payload, now=datetime(2026, 4, 15, 7, 35, 20, tzinfo=timezone.utc))
    (exp_dir / "primary_result_execution_latest.json").write_text(json.dumps(execution, ensure_ascii=False), encoding="utf-8")
    l4_payload = build_primary_result_api_payload(exp_dir)
    rollback = build_primary_result_rollback(l4_payload, now=datetime(2026, 4, 15, 7, 40, 20, tzinfo=timezone.utc))
    (exp_dir / "primary_result_rollback_latest.json").write_text(json.dumps(rollback, ensure_ascii=False), encoding="utf-8")
    rollback_payload = build_primary_result_api_payload(exp_dir)
    observation = build_primary_result_observation(
        rollback_payload,
        observation_status="observing",
        reason="local observation window opened",
        window_start="2026-04-15T09:30:00Z",
        now=datetime(2026, 4, 15, 7, 42, 20, tzinfo=timezone.utc),
    )
    (exp_dir / "primary_result_observation_latest.json").write_text(json.dumps(observation, ensure_ascii=False), encoding="utf-8")


def _write_sqlite(path: Path, *, insufficient: bool = False, failed: bool = False) -> Path:
    conn = sqlite3.connect(path)
    conn.execute(
        """
        CREATE TABLE daily_trading_data (
            ts_code TEXT,
            trade_date TEXT,
            open_price REAL,
            high_price REAL,
            low_price REAL,
            close_price REAL
            ,
            pre_close REAL,
            pct_chg REAL,
            vol REAL,
            amount REAL
        )
        """
    )
    end_close = 9.8 if failed else 10.8
    end_pct_chg = round((end_close / 10.0 - 1.0) * 100.0, 4)
    rows = [
        ("000001.SZ", "20260415", 9.9, 10.2, 9.8, 10.0, 9.9, 1.0101, 1000000.0, 1000000.0),
        ("BENCHMARK", "20260415", 99.0, 101.0, 98.0, 100.0, 99.0, 1.0101, 1000000.0, 1000000.0),
    ]
    if not insufficient:
        rows.extend(
            [
                ("000001.SZ", "20260420", 10.0, max(10.1, end_close), min(9.7, end_close), end_close, 10.0, end_pct_chg, 1200000.0, 1200000.0),
                ("BENCHMARK", "20260420", 100.0, 103.0, 99.5, 102.0, 100.0, 2.0, 1100000.0, 1100000.0),
            ]
        )
    conn.executemany("INSERT INTO daily_trading_data VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", rows)
    conn.commit()
    conn.close()
    return path


def test_primary_result_daily_closure_orchestrator_closes_success_and_registers_ledger(tmp_path):
    exp_dir = _write_primary_result_artifacts(tmp_path)
    _promote_to_observing(exp_dir)
    db_path = _write_sqlite(tmp_path / "stock.db")

    exit_code, payload = run_primary_result_daily_closure_orchestrator(
        sqlite_db_path=db_path,
        exp_dir=exp_dir,
        ts_code="000001.SZ",
        benchmark_ts_code="BENCHMARK",
        window_start="2026-04-15T09:30:00Z",
        window_end="2026-04-20T15:00:00Z",
        price_history_csv=tmp_path / "price_history.csv",
        price_history_manifest_json=tmp_path / "price_history_manifest.json",
        sqlite_ingest_manifest_json=tmp_path / "sqlite_ingest_manifest.json",
        market_data_readiness_json=tmp_path / "market_data_readiness.json",
        closure_preflight_json=tmp_path / "closure_preflight.json",
        metrics_output_json=tmp_path / "metrics.json",
        observation_output_json=exp_dir / "primary_result_observation_latest.json",
        terminal_output_json=exp_dir / "primary_result_terminal_latest.json",
        performance_ledger_jsonl=tmp_path / "ledger.jsonl",
        performance_summary_json=tmp_path / "summary.json",
        output_path=tmp_path / "daily_closure.json",
    )

    assert exit_code == 0
    assert payload["status"] == "closed_success"
    assert payload["terminal_outcome"] == "success"
    assert [stage["name"] for stage in payload["stages"]] == [
        "market_data_readiness",
        "sqlite_price_history_ingest",
        "price_history_manifest",
        "observation_closure_preflight",
        "observation_metrics",
        "terminal_outcome",
        "performance_ledger",
    ]
    assert json.loads((exp_dir / "primary_result_terminal_latest.json").read_text(encoding="utf-8"))["terminal_outcome"] == "success"
    assert json.loads((tmp_path / "summary.json").read_text(encoding="utf-8"))["success_total"] == 1


def test_primary_result_daily_closure_orchestrator_blocks_before_import_when_market_data_not_ready(tmp_path):
    exp_dir = _write_primary_result_artifacts(tmp_path)
    _promote_to_observing(exp_dir)
    db_path = _write_sqlite(tmp_path / "stock.db", insufficient=True)
    price_history = tmp_path / "price_history.csv"

    exit_code, payload = run_primary_result_daily_closure_orchestrator(
        sqlite_db_path=db_path,
        exp_dir=exp_dir,
        ts_code="000001.SZ",
        benchmark_ts_code="BENCHMARK",
        window_start="2026-04-15T09:30:00Z",
        window_end="2026-04-20T15:00:00Z",
        price_history_csv=price_history,
    )

    assert exit_code == 1
    assert payload["status"] == "blocked"
    assert [stage["name"] for stage in payload["stages"]] == ["market_data_readiness"]
    assert not price_history.exists()


def test_primary_result_daily_closure_orchestrator_records_failed_terminal_for_failed_observation(tmp_path):
    exp_dir = _write_primary_result_artifacts(tmp_path)
    _promote_to_observing(exp_dir)
    db_path = _write_sqlite(tmp_path / "stock.db", failed=True)

    exit_code, payload = run_primary_result_daily_closure_orchestrator(
        sqlite_db_path=db_path,
        exp_dir=exp_dir,
        ts_code="000001.SZ",
        benchmark_ts_code="BENCHMARK",
        window_start="2026-04-15T09:30:00Z",
        window_end="2026-04-20T15:00:00Z",
        price_history_csv=tmp_path / "price_history.csv",
        price_history_manifest_json=tmp_path / "price_history_manifest.json",
        observation_output_json=exp_dir / "primary_result_observation_latest.json",
        terminal_output_json=exp_dir / "primary_result_terminal_latest.json",
        performance_ledger_jsonl=tmp_path / "ledger.jsonl",
        performance_summary_json=tmp_path / "summary.json",
    )

    assert exit_code == 1
    assert payload["status"] == "closed_failed"
    assert payload["terminal_outcome"] == "failed"
    assert json.loads((exp_dir / "primary_result_observation_latest.json").read_text(encoding="utf-8"))["observation_status"] == "failed"
    assert json.loads((exp_dir / "primary_result_terminal_latest.json").read_text(encoding="utf-8"))["terminal_outcome"] == "failed"
    assert json.loads((tmp_path / "summary.json").read_text(encoding="utf-8"))["failed_total"] == 1


def test_run_primary_result_daily_closure_cli_outputs_json(tmp_path):
    project_root = Path(__file__).resolve().parents[1]
    script_path = project_root / "scripts" / "run_primary_result_daily_closure.py"
    exp_dir = _write_primary_result_artifacts(tmp_path)
    _promote_to_observing(exp_dir)
    db_path = _write_sqlite(tmp_path / "stock.db")

    completed = subprocess.run(
        [
            sys.executable,
            str(script_path),
            "--sqlite-db",
            str(db_path),
            "--exp-dir",
            str(exp_dir),
            "--ts-code",
            "000001.SZ",
            "--benchmark-ts-code",
            "BENCHMARK",
            "--window-start",
            "2026-04-15T09:30:00Z",
            "--window-end",
            "2026-04-20T15:00:00Z",
            "--price-history-csv",
            str(tmp_path / "price_history.csv"),
            "--price-history-manifest-json",
            str(tmp_path / "price_history_manifest.json"),
            "--performance-ledger-jsonl",
            str(tmp_path / "ledger.jsonl"),
            "--performance-summary-json",
            str(tmp_path / "summary.json"),
            "--output",
            str(tmp_path / "daily_closure.json"),
            "--json",
        ],
        cwd=project_root,
        capture_output=True,
        text=True,
        check=True,
    )

    payload = json.loads(completed.stdout)
    assert payload["status"] == "closed_success"
    assert json.loads((tmp_path / "daily_closure.json").read_text(encoding="utf-8"))["status"] == "closed_success"
