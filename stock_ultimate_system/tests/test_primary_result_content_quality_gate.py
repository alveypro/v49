import json
import subprocess
import sys
from datetime import datetime
from pathlib import Path

from scripts.check_primary_result_content_quality import check_primary_result_content_quality
from src.primary_result_content_quality import evaluate_primary_result_content_quality
from src.unified_result_builder import build_primary_result_api_payload


def _write_production_candidate_artifacts(tmp_path):
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


def test_primary_result_api_payload_exposes_actionable_identity(tmp_path):
    exp_dir = _write_production_candidate_artifacts(tmp_path)

    payload = build_primary_result_api_payload(exp_dir)

    assert payload["result_id"] == "primary:000001.SZ"
    assert payload["ts_code"] == "000001.SZ"
    assert payload["stock_name"] == "平安银行"
    assert payload["result_lifecycle_stage"] == "L2"
    assert payload["candidate_status"] == "shortlisted"
    assert payload["signal_level"] == "high"
    assert payload["risk_level"] == "medium"


def test_primary_result_content_quality_passes_with_actionable_candidate(tmp_path):
    exp_dir = _write_production_candidate_artifacts(tmp_path)
    payload = build_primary_result_api_payload(exp_dir)
    payload["history_source_timestamp"] = "2026-04-14 17:30:20"

    result = evaluate_primary_result_content_quality(
        payload,
        now=datetime(2026, 4, 15, 7, 30, 20),
    )

    assert result["status"] == "passed"
    assert result["readiness_level"] == "production_candidate"
    assert result["blocking_failures"] == []
    assert result["warnings"][0]["check"] == "governance_fields_degraded"


def test_primary_result_content_quality_rejects_empty_l1_payload():
    payload = {
        "schema_version": "primary_result_v1",
        "result_lifecycle_stage": "L1",
        "result_type": "research",
        "ts_code": "暂无",
        "stock_name": "",
        "history_source_timestamp": "-",
    }

    result = evaluate_primary_result_content_quality(payload, now=datetime(2026, 4, 15, 7, 30, 20))

    assert result["status"] == "failed"
    failure_names = {item["check"] for item in result["blocking_failures"]}
    assert "required_production_fields_present" in failure_names
    assert "result_has_actionable_lifecycle_stage" in failure_names
    assert "history_source_timestamp_parseable" in failure_names


def test_primary_result_content_quality_rejects_stale_source():
    payload = {
        "schema_version": "primary_result_v1",
        "result_id": "primary:000001.SZ",
        "ts_code": "000001.SZ",
        "stock_name": "平安银行",
        "result_lifecycle_stage": "L2",
        "result_type": "candidate",
        "research_status": "completed",
        "candidate_status": "shortlisted",
        "signal_level": "high",
        "risk_level": "medium",
        "history_source_file": "candidates_top_latest.csv",
        "history_source_timestamp": "2026-04-10 17:30:20",
    }

    result = evaluate_primary_result_content_quality(
        payload,
        now=datetime(2026, 4, 15, 7, 30, 20),
        max_source_age_hours=36,
    )

    assert result["status"] == "failed"
    assert any(item["check"] == "history_source_fresh" for item in result["blocking_failures"])


def test_check_primary_result_content_quality_cli_outputs_json(tmp_path):
    project_root = Path(__file__).resolve().parents[1]
    script_path = project_root / "scripts" / "check_primary_result_content_quality.py"
    exp_dir = _write_production_candidate_artifacts(tmp_path)
    output_path = tmp_path / "primary_result_quality.json"

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

    assert payload["content_quality_version"] == "primary_result_content_quality.v1"
    assert payload["status"] == "passed"
    assert json.loads(output_path.read_text(encoding="utf-8"))["status"] == "passed"
