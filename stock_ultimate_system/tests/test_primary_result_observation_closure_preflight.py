import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

from tests.primary_result_test_support import seed_current_primary_pointer
from src.primary_result_audit import build_primary_result_audit
from src.primary_result_execution import build_primary_result_execution
from src.primary_result_observation import build_primary_result_observation
from src.primary_result_observation_closure_preflight import build_primary_result_observation_closure_preflight
from src.primary_result_price_history_artifact import build_primary_result_price_history_artifact
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
    (exp_dir / "primary_result_observation_latest.json").write_text(json.dumps(observation, ensure_ascii=False), encoding="utf-8")


def _write_price_history(path: Path, *, failed: bool = False) -> None:
    end_close = "9.80" if failed else "11.00"
    path.write_text(
        "ts_code,trade_date,close\n"
        "000001.SZ,2026-04-15,10.00\n"
        "000001.SZ,2026-04-16,10.80\n"
        f"000001.SZ,2026-04-20,{end_close}\n"
        "BENCHMARK,2026-04-15,100.00\n"
        "BENCHMARK,2026-04-16,101.00\n"
        "BENCHMARK,2026-04-20,102.00\n",
        encoding="utf-8",
    )


def _write_price_history_manifest(path: Path, *, price_history: Path) -> Path:
    exit_code, payload = build_primary_result_price_history_artifact(
        price_history_path=price_history,
        ts_code="000001.SZ",
        benchmark_ts_code="BENCHMARK",
        window_start="2026-04-15T09:30:00Z",
        window_end="2026-04-20T15:00:00Z",
        output_path=path,
    )
    assert exit_code in {0, 1}
    return path


def test_primary_result_observation_closure_preflight_ready_for_terminal_success(tmp_path):
    exp_dir = _write_primary_result_artifacts(tmp_path)
    _promote_to_observing(exp_dir)
    price_history = tmp_path / "price_history.csv"
    _write_price_history(price_history)
    manifest = _write_price_history_manifest(tmp_path / "price_history_manifest.json", price_history=price_history)

    exit_code, payload = build_primary_result_observation_closure_preflight(
        exp_dir=exp_dir,
        price_history_path=price_history,
        price_history_manifest_path=manifest,
        benchmark_ts_code="BENCHMARK",
        window_end="2026-04-20T15:00:00Z",
    )

    assert exit_code == 0
    assert payload["preflight_version"] == "primary_result_observation_closure_preflight.v1"
    assert payload["status"] == "ready_for_terminal_success"
    assert payload["closure_outcome"] == "completed"
    assert payload["metrics"]["observed_return"] == 0.1
    assert payload["blocking_reasons"] == []


def test_primary_result_observation_closure_preflight_reports_failed_closure(tmp_path):
    exp_dir = _write_primary_result_artifacts(tmp_path)
    _promote_to_observing(exp_dir)
    price_history = tmp_path / "price_history.csv"
    _write_price_history(price_history, failed=True)
    manifest = _write_price_history_manifest(tmp_path / "price_history_manifest.json", price_history=price_history)

    exit_code, payload = build_primary_result_observation_closure_preflight(
        exp_dir=exp_dir,
        price_history_path=price_history,
        price_history_manifest_path=manifest,
        benchmark_ts_code="BENCHMARK",
        window_end="2026-04-20T15:00:00Z",
    )

    assert exit_code == 1
    assert payload["status"] == "blocked"
    assert payload["closure_outcome"] == "failed"
    assert "observed return must meet success floor" in payload["blocking_reasons"]


def test_primary_result_observation_closure_preflight_blocks_missing_price_history(tmp_path):
    exp_dir = _write_primary_result_artifacts(tmp_path)
    _promote_to_observing(exp_dir)

    exit_code, payload = build_primary_result_observation_closure_preflight(
        exp_dir=exp_dir,
        price_history_path=tmp_path / "missing.csv",
        price_history_manifest_path=tmp_path / "missing_manifest.json",
        benchmark_ts_code="BENCHMARK",
        window_end="2026-04-20T15:00:00Z",
    )

    assert exit_code == 1
    assert payload["status"] == "blocked"
    assert "local price history CSV must exist" in payload["blocking_reasons"]
    assert "validated price history manifest must exist" in payload["blocking_reasons"]


def test_inspect_primary_result_observation_closure_cli(tmp_path):
    project_root = Path(__file__).resolve().parents[1]
    script_path = project_root / "scripts" / "inspect_primary_result_observation_closure.py"
    exp_dir = _write_primary_result_artifacts(tmp_path)
    _promote_to_observing(exp_dir)
    price_history = tmp_path / "price_history.csv"
    _write_price_history(price_history)
    manifest = _write_price_history_manifest(tmp_path / "price_history_manifest.json", price_history=price_history)
    output_path = tmp_path / "closure_preflight.json"

    completed = subprocess.run(
        [
            sys.executable,
            str(script_path),
            "--exp-dir",
            str(exp_dir),
            "--price-history-csv",
            str(price_history),
            "--price-history-manifest-json",
            str(manifest),
            "--benchmark-ts-code",
            "BENCHMARK",
            "--window-end",
            "2026-04-20T15:00:00Z",
            "--output",
            str(output_path),
            "--json",
        ],
        cwd=project_root,
        capture_output=True,
        text=True,
        check=True,
    )

    payload = json.loads(completed.stdout)
    assert payload["status"] == "ready_for_terminal_success"
    assert json.loads(output_path.read_text(encoding="utf-8"))["closure_outcome"] == "completed"
