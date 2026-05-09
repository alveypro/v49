import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

from scripts.check_current_result_pointer_integrity import check_current_result_pointer_integrity
from scripts.run_primary_result_lifecycle import run_primary_result_lifecycle
from tests.primary_result_test_support import seed_current_primary_pointer


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


def test_primary_result_lifecycle_orchestrates_to_observation_with_evidence(tmp_path):
    exp_dir = _write_primary_result_artifacts(tmp_path)

    exit_code, payload = run_primary_result_lifecycle(
        exp_dir=exp_dir,
        max_source_age_hours=100000,
        observation_window_start="2026-04-15T07:40:00Z",
        now=datetime(2026, 4, 15, 7, 40, 20, tzinfo=timezone.utc),
    )

    assert exit_code == 0
    assert payload["lifecycle_version"] == "primary_result_lifecycle.v1"
    assert payload["status"] == "passed"
    assert payload["result_id"] == "primary:000001.SZ"
    assert payload["final_payload"]["result_lifecycle_stage"] == "L4"
    assert payload["final_payload"]["audit_status"] == "passed"
    assert payload["final_payload"]["execution_status"] == "ready"
    assert payload["final_payload"]["rollback_status"] == "not_required"
    assert payload["final_payload"]["observation_status"] == "observing"
    assert payload["final_payload"]["terminal_outcome"] is None
    assert payload["lifecycle_id"].startswith("primary-lifecycle-")
    assert payload["run_id"].startswith("primary-run-")
    assert [step["step"] for step in payload["steps"]] == ["audit", "execution", "rollback", "observation"]
    assert all(step["exists"] is True for step in payload["steps"])
    assert all(step["sha256"] for step in payload["steps"])
    assert (exp_dir / "primary_result_lifecycle_evidence_latest.json").exists()
    assert payload["registry_chain"]["write_mode"] == "run_result_pointer_updated"
    artifacts_dir = tmp_path / "artifacts"
    current_result_pointer = json.loads((artifacts_dir / "current_result_pointer" / "current.json").read_text(encoding="utf-8"))
    current_result_record = json.loads((artifacts_dir / "result_registry" / "current.json").read_text(encoding="utf-8"))
    current_run_record = json.loads((artifacts_dir / "run_registry" / "current.json").read_text(encoding="utf-8"))
    assert current_result_pointer["result_id"] == "primary:000001.SZ"
    assert current_result_pointer["run_id"] == payload["run_id"]
    assert any(str(artifact_id).startswith(f"{payload['run_id']}:") for artifact_id in current_result_pointer["artifact_ids"])
    assert current_result_record["result_id"] == "primary:000001.SZ"
    assert current_run_record["run_id"] == payload["run_id"]
    assert payload["final_payload"]["run_id"] == payload["run_id"]
    assert payload["final_payload"]["lifecycle_id"] == payload["lifecycle_id"]
    assert payload["final_payload"]["artifact_ids"] == current_result_pointer["artifact_ids"]
    assert payload["final_payload"]["as_of_date"] == current_result_pointer["as_of_date"]
    stock_entry_guard = json.loads((artifacts_dir / "stock_entry_guard_latest.json").read_text(encoding="utf-8"))
    assert stock_entry_guard["ok"] is True
    assert stock_entry_guard["lifecycle_evidence"]["final_payload"]["run_id"] == payload["run_id"]
    assert stock_entry_guard["lifecycle_evidence"]["final_payload"]["lifecycle_id"] == payload["lifecycle_id"]
    assert stock_entry_guard["lifecycle_evidence"]["final_payload"]["artifact_ids"] == current_result_pointer["artifact_ids"]
    assert stock_entry_guard["lifecycle_evidence"]["final_payload"]["as_of_date"] == current_result_pointer["as_of_date"]
    assert payload["registry_chain"]["stock_entry_guard"]["ok"] is True
    integrity_exit, integrity_payload = check_current_result_pointer_integrity(
        pointer_dir=artifacts_dir / "current_result_pointer",
        results_dir=artifacts_dir / "result_registry",
        runs_dir=artifacts_dir / "run_registry",
    )
    assert integrity_exit == 0, integrity_payload
    assert integrity_payload["ok"] is True


def test_primary_result_lifecycle_reports_stale_artifacts_before_rebuild(tmp_path):
    exp_dir = _write_primary_result_artifacts(tmp_path)
    (exp_dir / "primary_result_execution_latest.json").write_text(
        json.dumps(
            {
                "execution_version": "primary_result_execution.v1",
                "generated_at": "2026-04-14T01:00:00Z",
                "execution_status": "ready",
                "result_id": "primary:999999.SZ",
                "ts_code": "999999.SZ",
                "stock_name": "旧对象",
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    exit_code, payload = run_primary_result_lifecycle(
        exp_dir=exp_dir,
        max_source_age_hours=100000,
        now=datetime(2026, 4, 15, 7, 40, 20, tzinfo=timezone.utc),
    )

    assert exit_code == 0
    assert payload["status"] == "passed"
    assert payload["stale_artifacts_detected"][0]["step"] == "execution"
    assert payload["stale_artifacts_detected"][0]["artifact_result_id"] == "primary:999999.SZ"
    assert payload["steps"][1]["result_id"] == "primary:000001.SZ"
    assert (tmp_path / "artifacts" / "artifact_registry.jsonl").exists()


def test_primary_result_lifecycle_defaults_to_trade_calendar_window_advice(tmp_path):
    exp_dir = _write_primary_result_artifacts(tmp_path)

    exit_code, payload = run_primary_result_lifecycle(
        exp_dir=exp_dir,
        max_source_age_hours=100000,
        now=datetime(2026, 4, 17, 9, 40, 20, tzinfo=timezone.utc),
    )

    observation = json.loads((exp_dir / "primary_result_observation_latest.json").read_text(encoding="utf-8"))
    assert exit_code == 0
    assert payload["resolved_observation_window_start"] == "2026-04-20T01:30:00Z"
    assert payload["observation_window_advice"]["suggested_window_start_local"] == "2026-04-20T09:30:00+08:00"
    assert observation["observation_window"]["started_at"] == "2026-04-20T01:30:00Z"


def test_primary_result_lifecycle_fails_when_audit_blocks(tmp_path):
    exp_dir = _write_primary_result_artifacts(tmp_path, risk_level="critical")

    exit_code, payload = run_primary_result_lifecycle(
        exp_dir=exp_dir,
        max_source_age_hours=100000,
        now=datetime(2026, 4, 15, 7, 40, 20, tzinfo=timezone.utc),
    )

    assert exit_code == 1
    assert payload["status"] == "failed"
    assert any(item["step"] == "audit" for item in payload["blocking_failures"])
    assert payload["registry_chain"]["write_mode"] == "run_only_failed_lifecycle"
    artifacts_dir = tmp_path / "artifacts"
    current_result_pointer = json.loads((artifacts_dir / "current_result_pointer" / "current.json").read_text(encoding="utf-8"))
    current_result_record = json.loads((artifacts_dir / "result_registry" / "current.json").read_text(encoding="utf-8"))
    current_run_record = json.loads((artifacts_dir / "run_registry" / "current.json").read_text(encoding="utf-8"))
    assert current_result_pointer["result_id"] == "primary:000001.SZ"
    assert current_result_pointer["run_id"] == "run-001"
    assert current_result_record["result_id"] == "primary:000001.SZ"
    assert current_run_record["run_id"] == payload["run_id"]


def test_primary_result_lifecycle_cli_writes_json(tmp_path):
    project_root = Path(__file__).resolve().parents[1]
    script_path = project_root / "scripts" / "run_primary_result_lifecycle.py"
    exp_dir = _write_primary_result_artifacts(tmp_path)
    output_path = tmp_path / "primary_result_lifecycle_evidence_latest.json"

    completed = subprocess.run(
        [
            sys.executable,
            str(script_path),
            "--exp-dir",
            str(exp_dir),
            "--max-source-age-hours",
            "100000",
            "--observation-window-start",
            "2026-04-15T07:40:00Z",
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
    assert payload["status"] == "passed"
    assert json.loads(output_path.read_text(encoding="utf-8"))["status"] == "passed"


def test_primary_result_lifecycle_resolves_relative_output_against_project_root(tmp_path, monkeypatch):
    exp_dir = _write_primary_result_artifacts(tmp_path)
    project_root = Path(__file__).resolve().parents[1]
    monkeypatch.chdir(project_root.parent)

    exit_code, payload = run_primary_result_lifecycle(
        exp_dir=exp_dir,
        max_source_age_hours=100000,
        observation_window_start="2026-04-15T07:40:00Z",
        output_path="stock_ultimate_system/data/experiments/primary_result_lifecycle_evidence_latest.json",
        now=datetime(2026, 4, 15, 7, 40, 20, tzinfo=timezone.utc),
    )

    assert exit_code == 0
    assert payload["status"] == "passed"
    assert (project_root / "data" / "experiments" / "primary_result_lifecycle_evidence_latest.json").exists()
