import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

from scripts.run_primary_result_audit import run_primary_result_audit
from tests.primary_result_test_support import seed_current_primary_pointer
from src.primary_result_audit import build_primary_result_audit
from src.unified_result_builder import build_primary_result_api_payload


def _write_primary_result_artifacts(tmp_path, *, risk_level: str = "medium", include_candidate: bool = True) -> Path:
    exp_dir = tmp_path / "data" / "experiments"
    exp_dir.mkdir(parents=True)
    if include_candidate:
        candidate_rows = (
            "ts_code,stock_name,industry,signal,risk_level,final_score\n"
            f"000001.SZ,平安银行,银行,strong_buy,{risk_level},155\n"
        )
    else:
        candidate_rows = "ts_code,stock_name,industry,signal,risk_level,final_score\n"
    (exp_dir / "candidates_top_latest.csv").write_text(candidate_rows, encoding="utf-8")
    (exp_dir / "daily_research_status_latest.json").write_text('{"state":"completed"}', encoding="utf-8")
    (exp_dir / "buylist_latest.json").write_text('{"items":[{"ts_code":"000001.SZ"}]}', encoding="utf-8")
    (exp_dir / "governance_audit_latest.json").write_text("{}", encoding="utf-8")
    (exp_dir / "t1_execution_checklist_latest.json").write_text("{}", encoding="utf-8")
    (exp_dir / "t12_rollback_drill_latest.json").write_text('{"triggered": false}', encoding="utf-8")
    if include_candidate:
        seed_current_primary_pointer(tmp_path, ts_code="000001.SZ", stock_name="平安银行")
    return exp_dir


def test_primary_result_audit_passes_for_production_candidate(tmp_path):
    exp_dir = _write_primary_result_artifacts(tmp_path)
    payload = build_primary_result_api_payload(exp_dir)

    audit = build_primary_result_audit(
        payload,
        now=datetime(2026, 4, 15, 7, 30, 20, tzinfo=timezone.utc),
        max_source_age_hours=100000,
    )

    assert audit["audit_version"] == "primary_result_audit.v1"
    assert audit["audit_status"] == "passed"
    assert audit["result_id"] == "primary:000001.SZ"
    assert audit["ts_code"] == "000001.SZ"
    assert audit["stock_name"] == "平安银行"
    assert audit["content_quality"]["status"] == "passed"
    assert all(item["passed"] for item in audit["checks"] if item["severity"] == "blocking")


def test_primary_result_audit_requires_manual_review_for_high_risk_candidate(tmp_path):
    exp_dir = _write_primary_result_artifacts(tmp_path, risk_level="high")
    payload = build_primary_result_api_payload(exp_dir)

    audit = build_primary_result_audit(
        payload,
        now=datetime(2026, 4, 15, 7, 30, 20, tzinfo=timezone.utc),
        max_source_age_hours=100000,
    )

    assert audit["audit_status"] == "in_review"
    assert any(item["check"] == "risk_within_auto_pass_tolerance" and item["passed"] is False for item in audit["checks"])


def test_primary_result_audit_fails_when_no_actionable_candidate_exists(tmp_path):
    exp_dir = _write_primary_result_artifacts(tmp_path, include_candidate=False)
    payload = build_primary_result_api_payload(exp_dir)

    audit = build_primary_result_audit(
        payload,
        now=datetime(2026, 4, 15, 7, 30, 20, tzinfo=timezone.utc),
        max_source_age_hours=100000,
    )

    assert audit["audit_status"] == "failed"
    failed_checks = {item["check"] for item in audit["checks"] if item["passed"] is False}
    assert "content_quality_passed" in failed_checks
    assert "primary_result_identity_present" in failed_checks


def test_primary_result_builder_uses_per_result_audit_as_l3_source(tmp_path):
    exp_dir = _write_primary_result_artifacts(tmp_path)
    payload = build_primary_result_api_payload(exp_dir)
    audit = build_primary_result_audit(
        payload,
        now=datetime(2026, 4, 15, 7, 30, 20, tzinfo=timezone.utc),
        max_source_age_hours=100000,
    )
    (exp_dir / "primary_result_audit_latest.json").write_text(
        json.dumps(audit, ensure_ascii=False),
        encoding="utf-8",
    )

    promoted_payload = build_primary_result_api_payload(exp_dir)

    assert promoted_payload["audit_status"] == "passed"
    assert promoted_payload["result_lifecycle_stage"] == "L3"
    assert promoted_payload["result_type"] == "audit"
    assert promoted_payload["history_source_file"] == "primary_result_audit_latest.json"
    assert "审核记录 passed" in promoted_payload["history_summary"]


def test_run_primary_result_audit_cli_writes_json(tmp_path):
    project_root = Path(__file__).resolve().parents[1]
    script_path = project_root / "scripts" / "run_primary_result_audit.py"
    exp_dir = _write_primary_result_artifacts(tmp_path)
    output_path = tmp_path / "primary_result_audit_latest.json"

    completed = subprocess.run(
        [
            sys.executable,
            str(script_path),
            "--exp-dir",
            str(exp_dir),
            "--max-source-age-hours",
            "100000",
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
    assert payload["audit_status"] == "passed"
    assert json.loads(output_path.read_text(encoding="utf-8"))["audit_status"] == "passed"


def test_run_primary_result_audit_function_defaults_to_experiment_artifact(tmp_path):
    exp_dir = _write_primary_result_artifacts(tmp_path)

    exit_code, audit = run_primary_result_audit(
        exp_dir=exp_dir,
        max_source_age_hours=100000,
        now=datetime(2026, 4, 15, 7, 30, 20, tzinfo=timezone.utc),
    )

    assert exit_code == 0
    assert audit["audit_status"] == "passed"
    assert (exp_dir / "primary_result_audit_latest.json").exists()
