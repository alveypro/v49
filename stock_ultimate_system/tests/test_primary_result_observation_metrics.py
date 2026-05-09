import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

from scripts.run_primary_result_observation_metrics import run_primary_result_observation_metrics
from tests.primary_result_test_support import seed_current_primary_pointer
from src.primary_result_audit import build_primary_result_audit
from src.primary_result_execution import build_primary_result_execution
from src.primary_result_observation import build_primary_result_observation
from src.primary_result_observation_metrics import calculate_primary_result_observation_metrics
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
    seed_current_primary_pointer(tmp_path, ts_code="000001.SZ", stock_name="平安银行")
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
    (exp_dir / "primary_result_observation_latest.json").write_text(
        json.dumps(observation, ensure_ascii=False),
        encoding="utf-8",
    )


def _write_price_history(path: Path) -> None:
    path.write_text(
        "ts_code,trade_date,close\n"
        "000001.SZ,2026-04-15,10.00\n"
        "000001.SZ,2026-04-16,10.80\n"
        "000001.SZ,2026-04-17,10.40\n"
        "000001.SZ,2026-04-20,11.00\n"
        "BENCHMARK,2026-04-15,100.00\n"
        "BENCHMARK,2026-04-16,101.00\n"
        "BENCHMARK,2026-04-17,100.50\n"
        "BENCHMARK,2026-04-20,102.00\n",
        encoding="utf-8",
    )


def test_calculate_primary_result_observation_metrics_from_price_history(tmp_path):
    price_history = tmp_path / "price_history.csv"
    _write_price_history(price_history)

    metrics = calculate_primary_result_observation_metrics(
        price_history_path=price_history,
        ts_code="000001.SZ",
        benchmark_ts_code="BENCHMARK",
        window_start="2026-04-15T09:30:00Z",
        window_end="2026-04-20T15:00:00Z",
    )

    assert metrics["metrics_version"] == "primary_result_observation_metrics.v1"
    assert metrics["observed_return"] == 0.1
    assert metrics["benchmark_return"] == 0.02
    assert metrics["excess_return"] == 0.08
    assert metrics["max_drawdown"] == -0.037037
    assert metrics["observed_price_summary"]["point_total"] == 4


def test_calculate_primary_result_observation_metrics_rejects_insufficient_window_data(tmp_path):
    price_history = tmp_path / "price_history.csv"
    price_history.write_text(
        "ts_code,trade_date,close\n"
        "000001.SZ,2026-04-15,10.00\n"
        "BENCHMARK,2026-04-15,100.00\n"
        "BENCHMARK,2026-04-16,101.00\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="observed price history"):
        calculate_primary_result_observation_metrics(
            price_history_path=price_history,
            ts_code="000001.SZ",
            benchmark_ts_code="BENCHMARK",
            window_start="2026-04-15",
            window_end="2026-04-20",
        )


def test_run_primary_result_observation_metrics_completes_observation(tmp_path):
    exp_dir = _write_primary_result_artifacts(tmp_path)
    _promote_to_observing(exp_dir)
    price_history = tmp_path / "price_history.csv"
    _write_price_history(price_history)

    exit_code, payload = run_primary_result_observation_metrics(
        exp_dir=exp_dir,
        price_history_path=price_history,
        benchmark_ts_code="BENCHMARK",
        window_end="2026-04-20T15:00:00Z",
        now=datetime(2026, 4, 20, 8, 0, 0, tzinfo=timezone.utc),
    )

    assert exit_code == 0
    assert payload["status"] == "passed"
    assert payload["metrics"]["observed_return"] == 0.1
    assert payload["observation"]["observation_status"] == "completed"
    assert payload["observation"]["completion_criteria"]["passed"] is True
    assert json.loads((exp_dir / "primary_result_observation_latest.json").read_text(encoding="utf-8"))[
        "observation_status"
    ] == "completed"


def test_run_primary_result_observation_metrics_cli_writes_outputs(tmp_path):
    project_root = Path(__file__).resolve().parents[1]
    script_path = project_root / "scripts" / "run_primary_result_observation_metrics.py"
    exp_dir = _write_primary_result_artifacts(tmp_path)
    _promote_to_observing(exp_dir)
    price_history = tmp_path / "price_history.csv"
    _write_price_history(price_history)
    metrics_output = tmp_path / "metrics.json"
    observation_output = tmp_path / "observation.json"

    completed = subprocess.run(
        [
            sys.executable,
            str(script_path),
            "--exp-dir",
            str(exp_dir),
            "--price-history-csv",
            str(price_history),
            "--benchmark-ts-code",
            "BENCHMARK",
            "--window-end",
            "2026-04-20T15:00:00Z",
            "--metrics-output",
            str(metrics_output),
            "--observation-output",
            str(observation_output),
            "--json",
        ],
        cwd=project_root,
        capture_output=True,
        text=True,
        check=True,
    )

    payload = json.loads(completed.stdout)
    assert payload["status"] == "passed"
    assert json.loads(metrics_output.read_text(encoding="utf-8"))["metrics"]["benchmark_return"] == 0.02
    assert json.loads(observation_output.read_text(encoding="utf-8"))["observation_status"] == "completed"
