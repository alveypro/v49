import json
import subprocess
from pathlib import Path

from scripts.run_server_deploy_rollback import run_server_deploy_rollback
from server_deploy_test_helpers import write_server_deploy_evidence_bundle
from src.server_deploy_registry import ServerDeployRegistry


def _ok_runner(command, timeout=None):
    return subprocess.CompletedProcess(command, 0, stdout="ok", stderr="")


def _ok_url_fetcher(url, timeout):
    return 200, "ok"


def _create_app_dir():
    app_dir = Path("/opt/stock-ultimate/app")
    return app_dir


def test_server_deploy_rollback_dry_run_does_not_move_current_pointer(tmp_path):
    deployments_dir = tmp_path / "deployments"
    registry = ServerDeployRegistry(deployments_dir=deployments_dir)
    evidence_one = write_server_deploy_evidence_bundle(tmp_path / "release1", deployment_id="deploy-test001")
    evidence_two = write_server_deploy_evidence_bundle(tmp_path / "release2", deployment_id="deploy-test002")
    registry.register(evidence_bundle_path=evidence_one)
    registry.register(evidence_bundle_path=evidence_two)

    exit_code, payload = run_server_deploy_rollback(
        deployments_dir=deployments_dir,
        rollback_deployment_id="deploy-test001",
        confirm_deployment_id="deploy-test001",
        dry_run=True,
        command_runner=_ok_runner,
    )

    assert exit_code == 0
    assert payload["status"] == "passed"
    assert payload["scope"] == "stock-scoped"
    assert payload["dry_run"] is True
    assert registry.get_current_pointer()["deployment_id"] == "deploy-test002"
    assert payload["post_deploy_verification"]["status"] == "skipped"


def test_server_deploy_rollback_updates_pointer_after_execution_and_verification(tmp_path, monkeypatch):
    deployments_dir = tmp_path / "deployments"
    registry = ServerDeployRegistry(deployments_dir=deployments_dir)
    evidence_one = write_server_deploy_evidence_bundle(tmp_path / "release1", deployment_id="deploy-test001")
    evidence_two = write_server_deploy_evidence_bundle(tmp_path / "release2", deployment_id="deploy-test002")
    registry.register(evidence_bundle_path=evidence_one)
    registry.register(evidence_bundle_path=evidence_two)
    app_dir = _create_app_dir()

    def fake_verification(**kwargs):
        return 0, {
            "post_deploy_version": "server_post_deploy_verification.v1",
            "status": "passed",
            "checks": [{"check": "systemd_services_active", "passed": True}],
        }

    monkeypatch.setattr(
        "scripts.run_server_deploy_rollback.run_server_post_deploy_verification",
        fake_verification,
    )
    output_path = tmp_path / "rollback.json"
    exit_code, payload = run_server_deploy_rollback(
        deployments_dir=deployments_dir,
        rollback_deployment_id="deploy-test001",
        confirm_deployment_id="deploy-test001",
        output_path=output_path,
        command_runner=_ok_runner,
        url_fetcher=_ok_url_fetcher,
    )

    current = registry.get_current_pointer()
    assert exit_code == 0
    assert payload["status"] == "passed"
    assert payload["scope"] == "stock-scoped"
    assert payload["dry_run"] is False
    assert payload["post_deploy_verification"]["status"] == "passed"
    assert current["deployment_id"] == "deploy-test001"
    assert current["rollback_of_deployment_id"] == "deploy-test001"
    assert json.loads(output_path.read_text(encoding="utf-8"))["status"] == "passed"


def test_server_deploy_rollback_requires_confirmation(tmp_path):
    deployments_dir = tmp_path / "deployments"
    registry = ServerDeployRegistry(deployments_dir=deployments_dir)
    evidence = write_server_deploy_evidence_bundle(tmp_path / "release1", deployment_id="deploy-test001")
    registry.register(evidence_bundle_path=evidence)

    try:
        run_server_deploy_rollback(
            deployments_dir=deployments_dir,
            rollback_deployment_id="deploy-test001",
            confirm_deployment_id="wrong",
            dry_run=True,
            command_runner=_ok_runner,
        )
    except ValueError as exc:
        assert "confirm_deployment_id" in str(exc)
    else:
        raise AssertionError("rollback should require matching confirmation")
