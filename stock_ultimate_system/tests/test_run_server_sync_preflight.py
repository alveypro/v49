import json
import subprocess
import sys
from pathlib import Path

from scripts.run_server_sync_preflight import REQUIRED_ALLOWED_FILES, run_server_sync_preflight


def _create_syncable_project(tmp_path):
    project_root = tmp_path / "stock_ultimate_system"
    for required_file in REQUIRED_ALLOWED_FILES:
        path = project_root / required_file
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("x\n", encoding="utf-8")
    (project_root / "README.md").write_text("# test\n", encoding="utf-8")
    (project_root / "artifacts" / "baselines").mkdir(parents=True)
    (project_root / "artifacts" / "baselines" / "current.json").write_text("{}", encoding="utf-8")
    return project_root


def _passing_gate_runner(**kwargs):
    return 0, {
        "status": "passed",
        "failed_total": 0,
        "scope_registry": {
            "scope_ids": ["main_site", "stock", "t12"],
            "source": "src/airivo_scope_registry.py",
        },
    }


def _passing_full_readiness_runner(**kwargs):
    return 0, {
        "status": "passed",
        "scope_total": 3,
        "failed_total": 0,
    }


def test_server_sync_preflight_passes_with_manifest_and_scope_gate(tmp_path):
    project_root = _create_syncable_project(tmp_path)
    output_path = tmp_path / "preflight.json"

    exit_code, payload = run_server_sync_preflight(
        project_root=project_root,
        output_path=output_path,
        release_gate_runner=_passing_gate_runner,
        scope_readiness_runner=_passing_full_readiness_runner,
    )

    assert exit_code == 0
    assert payload["status"] == "passed"
    assert payload["sync_decision"] == {
        "allowed_to_sync": True,
        "decision": "sync_allowed",
        "next_action": "build server sync file list and stage rsync with the allowed file manifest",
        "blocking_checks": [],
    }
    assert payload["manifest_summary"]["unclassified_total"] == 0
    assert {check["check"]: check["passed"] for check in payload["checks"]} == {
        "manifest_classification": True,
        "required_sync_files": True,
        "release_gate_scope_readiness": True,
        "scope_full_readiness": True,
    }
    assert json.loads(output_path.read_text(encoding="utf-8"))["status"] == "passed"


def test_server_sync_preflight_rejects_blocking_scope_gate(tmp_path):
    project_root = _create_syncable_project(tmp_path)

    def failing_gate_runner(**kwargs):
        return 1, {
            "status": "failed",
            "failed_total": 1,
            "scope_registry": {
                "scope_ids": ["main_site", "stock", "t12"],
                "source": "src/airivo_scope_registry.py",
            },
        }

    exit_code, payload = run_server_sync_preflight(
        project_root=project_root,
        release_gate_runner=failing_gate_runner,
        scope_readiness_runner=_passing_full_readiness_runner,
    )

    checks = {check["check"]: check for check in payload["checks"]}
    assert exit_code == 1
    assert payload["status"] == "failed"
    assert payload["sync_decision"]["allowed_to_sync"] is False
    assert payload["sync_decision"]["decision"] == "sync_blocked"
    assert payload["sync_decision"]["blocking_checks"] == ["release_gate_scope_readiness"]
    assert checks["release_gate_scope_readiness"]["passed"] is False
    assert checks["release_gate_scope_readiness"]["details"]["failed_total"] == 1


def test_server_sync_preflight_rejects_unclassified_files(tmp_path):
    project_root = _create_syncable_project(tmp_path)
    (project_root / "runtime_state.json").write_text("{}", encoding="utf-8")

    exit_code, payload = run_server_sync_preflight(
        project_root=project_root,
        release_gate_runner=_passing_gate_runner,
        scope_readiness_runner=_passing_full_readiness_runner,
    )

    checks = {check["check"]: check for check in payload["checks"]}
    assert exit_code == 1
    assert payload["status"] == "failed"
    assert checks["manifest_classification"]["passed"] is False
    assert checks["manifest_classification"]["details"]["unclassified_files"] == ["runtime_state.json"]


def test_server_sync_preflight_cli_outputs_json():
    project_root = Path(__file__).resolve().parents[1]
    script_path = project_root / "scripts" / "run_server_sync_preflight.py"

    completed = subprocess.run(
        [sys.executable, str(script_path), "--json"],
        cwd=project_root,
        capture_output=True,
        text=True,
        check=True,
    )
    payload = json.loads(completed.stdout)

    assert payload["preflight_version"] == "server_sync_preflight.v1"
    assert payload["status"] == "passed"
    assert payload["manifest_summary"]["unclassified_total"] == 0
    assert "scripts/run_server_sync_preflight.py" in payload["allowed_files"]
