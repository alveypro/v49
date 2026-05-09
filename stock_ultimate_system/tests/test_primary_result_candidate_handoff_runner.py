import json
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from scripts.check_current_result_pointer_integrity import check_current_result_pointer_integrity
from src.current_result_pointer import CurrentResultPointerStore
from src.primary_result_candidate_handoff_runner import run_primary_result_candidate_handoff
from src.primary_result_lifecycle_registry import PrimaryResultLifecycleRegistry
from src.result_registry import ResultRegistry
from src.run_registry import RunRegistry


def _write_candidate_bundle(exp_dir: Path, ts_code: str) -> None:
    exp_dir.mkdir(parents=True, exist_ok=True)
    (exp_dir / "candidates_top_latest.csv").write_text(
        "ts_code,stock_name,industry,signal,risk_level,final_score\n"
        f"{ts_code},测试股,测试,strong_buy,medium,155\n",
        encoding="utf-8",
    )
    (exp_dir / "daily_research_status_latest.json").write_text('{"state":"completed"}', encoding="utf-8")
    (exp_dir / "buylist_latest.json").write_text(json.dumps({"items": [{"ts_code": ts_code}]}, ensure_ascii=False), encoding="utf-8")
    (exp_dir / "governance_audit_latest.json").write_text("{}", encoding="utf-8")
    (exp_dir / "t1_execution_checklist_latest.json").write_text("{}", encoding="utf-8")
    (exp_dir / "t12_rollback_drill_latest.json").write_text('{"triggered": false}', encoding="utf-8")


def _write_pointer(lifecycles: Path, ts_code: str) -> None:
    lifecycles.mkdir(parents=True, exist_ok=True)
    (lifecycles / "current.json").write_text(
        json.dumps(
            {
                "pointer_version": "primary_result_lifecycle_current_pointer.v1",
                "lifecycle_id": "existing",
                "snapshot_path": str(lifecycles / "history" / "existing.json"),
                "result_id": f"primary:{ts_code}",
                "ts_code": ts_code,
                "updated_at": "2026-04-17T00:00:00Z",
                "rollback_of_lifecycle_id": None,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )


def _write_current_primary_pointer(tmp_path: Path, ts_code: str) -> None:
    artifacts_dir = tmp_path / "artifacts"
    RunRegistry(runs_dir=artifacts_dir / "run_registry").register(
        run_id="run-001",
        run_type="daily_research",
        producer="test",
        config_hash="cfg-001",
        data_snapshot_id="data-001",
        code_revision="rev-001",
        make_current=True,
    )
    ResultRegistry(results_dir=artifacts_dir / "result_registry").register(
        record_id=f"result-record-{ts_code.replace('.', '-').lower()}",
        result_id=f"primary:{ts_code}",
        run_id="run-001",
        ts_code=ts_code,
        stock_name="测试股",
        lifecycle_stage="L2",
        artifact_ids=["artifact:a"],
        make_current=True,
    )
    CurrentResultPointerStore(pointer_dir=artifacts_dir / "current_result_pointer").point_to(
        pointer_snapshot_id=f"pointer-{ts_code.replace('.', '-').lower()}",
        result_id=f"primary:{ts_code}",
        run_id="run-001",
        lifecycle_id="lifecycle-001",
        artifact_ids=["artifact:a"],
        as_of_date="2026-04-28",
    )


def test_candidate_handoff_runner_skips_when_aligned(tmp_path):
    exp_dir = tmp_path / "data" / "experiments"
    lifecycles = tmp_path / "artifacts" / "primary_result_lifecycle"
    _write_candidate_bundle(exp_dir, "000001.SZ")
    _write_current_primary_pointer(tmp_path, "000001.SZ")
    _write_pointer(lifecycles, "000001.SZ")

    exit_code, payload = run_primary_result_candidate_handoff(
        exp_dir=exp_dir,
        lifecycles_dir=lifecycles,
        handoff_gate_output_path=tmp_path / "handoff_gate.json",
        output_path=tmp_path / "handoff_runner.json",
    )

    assert exit_code == 0
    assert payload["status"] == "skipped"
    assert payload["decision"] == "aligned"
    assert (tmp_path / "handoff_runner.json").exists()


def test_candidate_handoff_runner_requires_explicit_execution(tmp_path):
    exp_dir = tmp_path / "data" / "experiments"
    lifecycles = tmp_path / "artifacts" / "primary_result_lifecycle"
    _write_candidate_bundle(exp_dir, "000002.SZ")
    _write_current_primary_pointer(tmp_path, "000002.SZ")
    _write_pointer(lifecycles, "000001.SZ")

    exit_code, payload = run_primary_result_candidate_handoff(
        exp_dir=exp_dir,
        lifecycles_dir=lifecycles,
        handoff_gate_output_path=tmp_path / "handoff_gate.json",
    )

    assert exit_code == 0
    assert payload["status"] == "action_required"
    assert payload["decision"] == "handoff_required"
    assert "explicit --execute-handoff" in payload["reason"]
    assert payload["observation_window_advice"]["suggested_window_start"]


def test_candidate_handoff_runner_uses_trade_calendar_artifact(tmp_path):
    exp_dir = tmp_path / "data" / "experiments"
    lifecycles = tmp_path / "artifacts" / "primary_result_lifecycle"
    calendar = tmp_path / "artifacts" / "calendar.json"
    _write_candidate_bundle(exp_dir, "000002.SZ")
    _write_current_primary_pointer(tmp_path, "000002.SZ")
    _write_pointer(lifecycles, "000001.SZ")
    calendar.parent.mkdir(parents=True, exist_ok=True)
    calendar.write_text(json.dumps({"trade_dates": ["2026-05-08"]}), encoding="utf-8")
    candidate_mtime = datetime(2026, 5, 8, 8, 0, tzinfo=ZoneInfo("Asia/Shanghai")).timestamp()
    os.utime(exp_dir / "candidates_top_latest.csv", (candidate_mtime, candidate_mtime))

    exit_code, payload = run_primary_result_candidate_handoff(
        exp_dir=exp_dir,
        lifecycles_dir=lifecycles,
        trade_calendar_path=calendar,
        handoff_gate_output_path=tmp_path / "handoff_gate.json",
    )

    assert exit_code == 0
    assert payload["status"] == "action_required"
    assert payload["observation_window_advice"]["calendar_policy"] == "provided_trade_calendar_open_0930"


def test_candidate_handoff_runner_executes_and_registers_lifecycle(tmp_path):
    exp_dir = tmp_path / "data" / "experiments"
    lifecycles = tmp_path / "artifacts" / "primary_result_lifecycle"
    _write_candidate_bundle(exp_dir, "000002.SZ")
    _write_current_primary_pointer(tmp_path, "000002.SZ")
    _write_pointer(lifecycles, "000001.SZ")

    exit_code, payload = run_primary_result_candidate_handoff(
        exp_dir=exp_dir,
        lifecycles_dir=lifecycles,
        execute_handoff=True,
        observation_window_start="2026-04-20T01:30:00Z",
        lifecycle_id="primary-lifecycle-test-000002",
        handoff_gate_output_path=tmp_path / "handoff_gate.json",
        output_path=tmp_path / "handoff_runner.json",
    )

    assert exit_code == 0
    assert payload["status"] == "completed"
    assert payload["decision"] == "aligned"
    assert payload["lifecycle_id"] == "primary-lifecycle-test-000002"
    current = json.loads((lifecycles / "current.json").read_text(encoding="utf-8"))
    assert current["ts_code"] == "000002.SZ"
    assert (lifecycles / "history" / "primary-lifecycle-test-000002.json").exists()
    primary_pointer = json.loads((tmp_path / "artifacts" / "current_result_pointer" / "current.json").read_text(encoding="utf-8"))
    assert primary_pointer["result_id"] == "primary:000002.SZ"
    assert primary_pointer["lifecycle_id"] == "primary-lifecycle-test-000002"
    integrity_exit, integrity_payload = check_current_result_pointer_integrity(
        pointer_dir=tmp_path / "artifacts" / "current_result_pointer",
        results_dir=tmp_path / "artifacts" / "result_registry",
        runs_dir=tmp_path / "artifacts" / "run_registry",
    )
    assert integrity_exit == 0, integrity_payload
    assert integrity_payload["ok"] is True


def test_candidate_handoff_runner_rejects_execute_without_window_start(tmp_path):
    exp_dir = tmp_path / "data" / "experiments"
    lifecycles = tmp_path / "artifacts" / "primary_result_lifecycle"
    _write_candidate_bundle(exp_dir, "000002.SZ")
    _write_current_primary_pointer(tmp_path, "000002.SZ")
    _write_pointer(lifecycles, "000001.SZ")

    with pytest.raises(ValueError, match="suggested_observation_window_start"):
        run_primary_result_candidate_handoff(
            exp_dir=exp_dir,
            lifecycles_dir=lifecycles,
            execute_handoff=True,
            handoff_gate_output_path=tmp_path / "handoff_gate.json",
        )


def test_candidate_handoff_runner_cli_outputs_json(tmp_path):
    project_root = Path(__file__).resolve().parents[1]
    script = project_root / "scripts" / "run_primary_result_candidate_handoff.py"
    exp_dir = tmp_path / "data" / "experiments"
    lifecycles = tmp_path / "artifacts" / "primary_result_lifecycle"
    _write_candidate_bundle(exp_dir, "000001.SZ")
    _write_current_primary_pointer(tmp_path, "000001.SZ")
    _write_pointer(lifecycles, "000001.SZ")

    completed = subprocess.run(
        [
            sys.executable,
            str(script),
            "--exp-dir",
            str(exp_dir),
            "--lifecycles-dir",
            str(lifecycles),
                "--output",
                str(tmp_path / "handoff_runner.json"),
                "--handoff-gate-output",
                str(tmp_path / "handoff_gate.json"),
                "--json",
            ],
        cwd=project_root,
        capture_output=True,
        text=True,
        check=True,
    )

    payload = json.loads(completed.stdout)
    assert payload["status"] == "skipped"
    assert json.loads((tmp_path / "handoff_runner.json").read_text(encoding="utf-8"))["decision"] == "aligned"


def test_candidate_handoff_runner_fails_when_lifecycle_id_conflicts_with_other_candidate(tmp_path):
    exp_dir = tmp_path / "data" / "experiments"
    lifecycles = tmp_path / "artifacts" / "primary_result_lifecycle"
    evidence = tmp_path / "existing_evidence.json"
    _write_candidate_bundle(exp_dir, "000002.SZ")
    _write_current_primary_pointer(tmp_path, "000002.SZ")
    _write_pointer(lifecycles, "000001.SZ")
    registry = PrimaryResultLifecycleRegistry(lifecycles_dir=lifecycles)
    evidence.write_text(
        json.dumps(
            {
                "lifecycle_version": "primary_result_lifecycle.v1",
                "status": "passed",
                "result_id": "primary:000001.SZ",
                "ts_code": "000001.SZ",
                "stock_name": "旧对象",
                "final_payload": {
                    "result_id": "primary:000001.SZ",
                    "ts_code": "000001.SZ",
                    "audit_status": "passed",
                    "execution_status": "ready",
                    "observation_status": "observing",
                    "rollback_status": "not_required",
                },
                    "steps": [
                        {"step": "audit", "path": "audit.json", "exists": True, "sha256": "a", "result_id": "primary:000001.SZ", "ts_code": "000001.SZ", "exit_code": 0},
                        {"step": "execution", "path": "execution.json", "exists": True, "sha256": "b", "result_id": "primary:000001.SZ", "ts_code": "000001.SZ", "exit_code": 0},
                        {"step": "rollback", "path": "rollback.json", "exists": True, "sha256": "c", "result_id": "primary:000001.SZ", "ts_code": "000001.SZ", "exit_code": 0},
                        {"step": "observation", "path": "observation.json", "exists": True, "sha256": "d", "result_id": "primary:000001.SZ", "ts_code": "000001.SZ", "exit_code": 0},
                    ],
                "blocking_failures": [],
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    registry.register(evidence_path=evidence, lifecycle_id="conflict-id")

    exit_code, payload = run_primary_result_candidate_handoff(
        exp_dir=exp_dir,
        lifecycles_dir=lifecycles,
        execute_handoff=True,
        observation_window_start="2026-04-20T01:30:00Z",
        lifecycle_id="conflict-id",
        handoff_gate_output_path=tmp_path / "handoff_gate.json",
    )

    assert exit_code == 1
    assert payload["status"] == "failed"
    assert payload["reason"] == "lifecycle_id already exists for a different primary result"
    current = json.loads((lifecycles / "current.json").read_text(encoding="utf-8"))
    assert current["ts_code"] == "000001.SZ"
