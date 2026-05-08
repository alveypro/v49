import json
import subprocess
import sys
from pathlib import Path

import pytest

from src.artifact_registry import ArtifactRegistry
from src.primary_result_lifecycle_registry import PrimaryResultLifecycleRegistry


def _write_lifecycle_evidence(tmp_path: Path, *, status: str = "passed", ts_code: str = "000001.SZ") -> Path:
    exp_dir = tmp_path / "data" / "experiments"
    exp_dir.mkdir(parents=True, exist_ok=True)
    steps = []
    for step_name, step_status in (
        ("audit", "passed"),
        ("execution", "ready"),
        ("rollback", "not_required"),
        ("observation", "observing"),
    ):
        artifact_path = exp_dir / f"primary_result_{step_name}_latest.json"
        artifact_path.write_text(json.dumps({"step": step_name}, ensure_ascii=False), encoding="utf-8")
        steps.append(
            {
                "step": step_name,
                "path": str(artifact_path),
                "exists": True,
                "sha256": "abc123",
                "result_id": f"primary:{ts_code}",
                "ts_code": ts_code,
                "status": step_status,
                "generated_at": "2026-04-15T07:40:00Z",
                "exit_code": 0,
            }
        )
    payload = {
        "lifecycle_version": "primary_result_lifecycle.v1",
        "started_at": "2026-04-15T07:40:00Z",
        "completed_at": "2026-04-15T07:40:03Z",
        "status": status,
        "result_id": f"primary:{ts_code}",
        "ts_code": ts_code,
        "stock_name": "平安银行",
        "initial_payload": {"result_id": f"primary:{ts_code}", "ts_code": ts_code},
        "final_payload": {
            "result_id": f"primary:{ts_code}",
            "ts_code": ts_code,
            "stock_name": "平安银行",
            "audit_status": "passed",
            "execution_status": "ready",
            "rollback_status": "not_required",
            "observation_status": "observing",
            "terminal_outcome": None,
        },
        "stale_artifacts_detected": [],
        "steps": steps,
        "blocking_failures": [] if status == "passed" else [{"step": "audit"}],
    }
    evidence_path = exp_dir / "primary_result_lifecycle_evidence_latest.json"
    evidence_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return evidence_path


def test_primary_result_lifecycle_registry_registers_current_and_preserves_history(tmp_path):
    registry = PrimaryResultLifecycleRegistry(lifecycles_dir=tmp_path / "artifacts" / "primary_result_lifecycle")
    evidence_path = _write_lifecycle_evidence(tmp_path)

    snapshot = registry.register(evidence_path=evidence_path, lifecycle_id="lifecycle-001")

    assert snapshot["lifecycle_id"] == "lifecycle-001"
    assert snapshot["evidence_hash"]
    assert snapshot["result_id"] == "primary:000001.SZ"
    assert snapshot["final_payload"]["observation_status"] == "observing"
    current = registry.get_current_pointer()
    assert current["lifecycle_id"] == "lifecycle-001"
    assert current["rollback_of_lifecycle_id"] is None
    assert (tmp_path / "artifacts" / "primary_result_lifecycle" / "history" / "lifecycle-001.json").exists()
    artifact_entries = ArtifactRegistry(tmp_path / "artifacts" / "artifact_registry.jsonl").list_entries(run_id="lifecycle-001")
    assert {entry["artifact_type"] for entry in artifact_entries} == {
        "primary_result_lifecycle_evidence",
        "primary_result_lifecycle_snapshot",
        "primary_result_lifecycle_current_pointer",
    }
    assert all(entry["result_id"] == "primary:000001.SZ" for entry in artifact_entries)
    snapshot_entry = next(entry for entry in artifact_entries if entry["artifact_type"] == "primary_result_lifecycle_snapshot")
    assert snapshot_entry["parent_artifact_ids"] == ["lifecycle-001:evidence"]
    pointer_entry = next(entry for entry in artifact_entries if entry["artifact_type"] == "primary_result_lifecycle_current_pointer")
    assert pointer_entry["metadata"]["current_role"] == "index_only"

    with pytest.raises(FileExistsError):
        registry.register(evidence_path=evidence_path, lifecycle_id="lifecycle-001")


def test_primary_result_lifecycle_registry_rejects_failed_evidence(tmp_path):
    registry = PrimaryResultLifecycleRegistry(lifecycles_dir=tmp_path / "artifacts" / "primary_result_lifecycle")
    evidence_path = _write_lifecycle_evidence(tmp_path, status="failed")

    with pytest.raises(ValueError, match="must be passed"):
        registry.register(evidence_path=evidence_path, lifecycle_id="lifecycle-failed")

    assert registry.get_current_pointer()["lifecycle_id"] is None


def test_primary_result_lifecycle_registry_current_pointer_switches_and_rolls_back(tmp_path):
    registry = PrimaryResultLifecycleRegistry(lifecycles_dir=tmp_path / "artifacts" / "primary_result_lifecycle")
    first = _write_lifecycle_evidence(tmp_path / "first", ts_code="000001.SZ")
    second = _write_lifecycle_evidence(tmp_path / "second", ts_code="000002.SZ")

    registry.register(evidence_path=first, lifecycle_id="lifecycle-001")
    registry.register(evidence_path=second, lifecycle_id="lifecycle-002")

    assert registry.get_current_pointer()["lifecycle_id"] == "lifecycle-002"
    rolled_back = registry.rollback("lifecycle-001")
    assert rolled_back["lifecycle_id"] == "lifecycle-001"
    current = registry.get_current_pointer()
    assert current["lifecycle_id"] == "lifecycle-001"
    assert current["rollback_of_lifecycle_id"] == "lifecycle-001"
    artifact_entries = ArtifactRegistry(tmp_path / "artifacts" / "artifact_registry.jsonl").list_entries(run_id="lifecycle-001")
    current_pointer_entries = [entry for entry in artifact_entries if entry["artifact_type"] == "primary_result_lifecycle_current_pointer"]
    assert len(current_pointer_entries) == 2
    assert current_pointer_entries[-1]["metadata"]["rollback_of_lifecycle_id"] == "lifecycle-001"


def test_register_primary_result_lifecycle_cli_registers_snapshot(tmp_path):
    project_root = Path(__file__).resolve().parents[1]
    script_path = project_root / "scripts" / "register_primary_result_lifecycle.py"
    evidence_path = _write_lifecycle_evidence(tmp_path)
    lifecycles_dir = tmp_path / "artifacts" / "primary_result_lifecycle"

    completed = subprocess.run(
        [
            sys.executable,
            str(script_path),
            "--lifecycles-dir",
            str(lifecycles_dir),
            "--evidence-json",
            str(evidence_path),
            "--lifecycle-id",
            "lifecycle-cli",
        ],
        cwd=project_root,
        capture_output=True,
        text=True,
        check=True,
    )

    payload = json.loads(completed.stdout)
    assert payload["status"] == "registered"
    assert payload["lifecycle_id"] == "lifecycle-cli"
    assert json.loads((lifecycles_dir / "current.json").read_text(encoding="utf-8"))["lifecycle_id"] == "lifecycle-cli"
