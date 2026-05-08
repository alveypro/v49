import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

from scripts.run_primary_result_execution import run_primary_result_execution
from tests.primary_result_test_support import seed_current_primary_pointer
from src.primary_result_audit import build_primary_result_audit
from src.primary_result_execution import build_primary_result_execution
from src.unified_result_builder import build_primary_result_api_payload


def _write_primary_result_artifacts(tmp_path, *, risk_level: str = "medium") -> Path:
    exp_dir = tmp_path / "data" / "experiments"
    exp_dir.mkdir(parents=True)
    (exp_dir / "candidates_top_latest.csv").write_text(
        "ts_code,stock_name,industry,signal,risk_level,final_score\n"
        f"000001.SZ,平安银行,银行,strong_buy,{risk_level},155\n",
        encoding="utf-8",
    )
    (exp_dir / "daily_research_status_latest.json").write_text('{"state":"completed"}', encoding="utf-8")
    (exp_dir / "buylist_latest.json").write_text('{"items":[{"ts_code":"000001.SZ"}]}', encoding="utf-8")
    (exp_dir / "governance_audit_latest.json").write_text("{}", encoding="utf-8")
    (exp_dir / "t1_execution_checklist_latest.json").write_text("{}", encoding="utf-8")
    (exp_dir / "t12_rollback_drill_latest.json").write_text('{"triggered": false}', encoding="utf-8")
    seed_current_primary_pointer(tmp_path, ts_code="000001.SZ", stock_name="平安银行")
    return exp_dir


def _write_audit_artifact(exp_dir: Path, *, max_source_age_hours: float = 100000) -> dict[str, object]:
    payload = build_primary_result_api_payload(exp_dir)
    audit = build_primary_result_audit(
        payload,
        now=datetime(2026, 4, 15, 7, 30, 20, tzinfo=timezone.utc),
        max_source_age_hours=max_source_age_hours,
    )
    (exp_dir / "primary_result_audit_latest.json").write_text(
        json.dumps(audit, ensure_ascii=False),
        encoding="utf-8",
    )
    return audit


def test_primary_result_execution_is_ready_after_passed_audit(tmp_path):
    exp_dir = _write_primary_result_artifacts(tmp_path)
    _write_audit_artifact(exp_dir)
    payload = build_primary_result_api_payload(exp_dir)

    execution = build_primary_result_execution(payload, now=datetime(2026, 4, 15, 7, 35, 20, tzinfo=timezone.utc))

    assert execution["execution_version"] == "primary_result_execution.v1"
    assert execution["execution_status"] == "ready"
    assert execution["result_id"] == "primary:000001.SZ"
    assert execution["source_audit_status"] == "passed"
    assert execution["execution_plan"]["mode"] == "local_protocol"
    assert execution["execution_plan"]["external_broker_connected"] is False
    assert all(item["passed"] for item in execution["checks"] if item["severity"] == "blocking")


def test_primary_result_execution_blocks_without_passed_audit(tmp_path):
    exp_dir = _write_primary_result_artifacts(tmp_path)
    payload = build_primary_result_api_payload(exp_dir)

    execution = build_primary_result_execution(payload, now=datetime(2026, 4, 15, 7, 35, 20, tzinfo=timezone.utc))

    assert execution["execution_status"] == "blocked"
    failed_checks = {item["check"] for item in execution["checks"] if item["passed"] is False}
    assert "audit_passed" in failed_checks
    assert "audited_lifecycle_stage" in failed_checks


def test_primary_result_execution_blocks_high_risk_review_candidate(tmp_path):
    exp_dir = _write_primary_result_artifacts(tmp_path, risk_level="high")
    _write_audit_artifact(exp_dir)
    payload = build_primary_result_api_payload(exp_dir)

    execution = build_primary_result_execution(payload, now=datetime(2026, 4, 15, 7, 35, 20, tzinfo=timezone.utc))

    assert payload["audit_status"] == "in_review"
    assert execution["execution_status"] == "blocked"
    failed_checks = {item["check"] for item in execution["checks"] if item["passed"] is False}
    assert "audit_passed" in failed_checks
    assert "risk_within_execution_tolerance" in failed_checks


def test_primary_result_builder_uses_execution_artifact_as_l4_source(tmp_path):
    exp_dir = _write_primary_result_artifacts(tmp_path)
    _write_audit_artifact(exp_dir)
    payload = build_primary_result_api_payload(exp_dir)
    execution = build_primary_result_execution(payload, now=datetime(2026, 4, 15, 7, 35, 20, tzinfo=timezone.utc))
    (exp_dir / "primary_result_execution_latest.json").write_text(
        json.dumps(execution, ensure_ascii=False),
        encoding="utf-8",
    )

    executable_payload = build_primary_result_api_payload(exp_dir)

    assert executable_payload["audit_status"] == "passed"
    assert executable_payload["execution_status"] == "ready"
    assert executable_payload["result_lifecycle_stage"] == "L4"
    assert executable_payload["result_type"] == "execution"
    assert executable_payload["history_source_file"] == "primary_result_execution_latest.json"
    assert "执行记录 ready" in executable_payload["history_summary"]


def test_run_primary_result_execution_cli_writes_json(tmp_path):
    project_root = Path(__file__).resolve().parents[1]
    script_path = project_root / "scripts" / "run_primary_result_execution.py"
    exp_dir = _write_primary_result_artifacts(tmp_path)
    _write_audit_artifact(exp_dir)
    output_path = tmp_path / "primary_result_execution_latest.json"

    completed = subprocess.run(
        [
            sys.executable,
            str(script_path),
            "--exp-dir",
            str(exp_dir),
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
    assert payload["execution_status"] == "ready"
    assert json.loads(output_path.read_text(encoding="utf-8"))["execution_status"] == "ready"


def test_run_primary_result_execution_function_defaults_to_experiment_artifact(tmp_path):
    exp_dir = _write_primary_result_artifacts(tmp_path)
    _write_audit_artifact(exp_dir)

    exit_code, execution = run_primary_result_execution(
        exp_dir=exp_dir,
        now=datetime(2026, 4, 15, 7, 35, 20, tzinfo=timezone.utc),
    )

    assert exit_code == 0
    assert execution["execution_status"] == "ready"
    assert (exp_dir / "primary_result_execution_latest.json").exists()
