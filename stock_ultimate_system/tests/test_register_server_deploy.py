import json
import subprocess
import sys
from pathlib import Path

from server_deploy_test_helpers import write_server_deploy_evidence_bundle


def test_register_server_deploy_cli_registers_passed_evidence(tmp_path):
    project_root = Path(__file__).resolve().parents[1]
    script_path = project_root / "scripts" / "register_server_deploy.py"
    evidence = write_server_deploy_evidence_bundle(tmp_path / "release1", deployment_id="deploy-cli001")

    completed = subprocess.run(
        [
            sys.executable,
            str(script_path),
            "--deployments-dir",
            str(tmp_path / "artifacts" / "server_deployments"),
            "--evidence-bundle-json",
            str(evidence),
        ],
        cwd=project_root,
        capture_output=True,
        text=True,
        check=True,
    )
    payload = json.loads(completed.stdout)

    assert payload["status"] == "registered"
    assert payload["deployment_id"] == "deploy-cli001"
    assert payload["scope"] == "stock-scoped"
    assert (tmp_path / "artifacts" / "server_deployments" / "current.json").exists()


def test_register_server_deploy_cli_rejects_dry_run_evidence(tmp_path):
    project_root = Path(__file__).resolve().parents[1]
    script_path = project_root / "scripts" / "register_server_deploy.py"
    evidence = write_server_deploy_evidence_bundle(tmp_path / "release1", deployment_id="deploy-cli002", dry_run=True)

    completed = subprocess.run(
        [
            sys.executable,
            str(script_path),
            "--deployments-dir",
            str(tmp_path / "artifacts" / "server_deployments"),
            "--evidence-bundle-json",
            str(evidence),
        ],
        cwd=project_root,
        capture_output=True,
        text=True,
        check=False,
    )
    payload = json.loads(completed.stdout)

    assert completed.returncode == 1
    assert payload["status"] == "failed"
    assert "dry-run" in payload["error"]
