import pytest

from server_deploy_test_helpers import write_server_deploy_evidence_bundle
from src.server_deploy_registry import ServerDeployRegistry


def test_server_deploy_registry_registers_current_and_preserves_history(tmp_path):
    registry = ServerDeployRegistry(deployments_dir=tmp_path / "artifacts" / "server_deployments")
    evidence = write_server_deploy_evidence_bundle(tmp_path / "release1", deployment_id="deploy-test001")

    snapshot = registry.register(evidence_bundle_path=evidence)

    assert snapshot["deployment_id"] == "deploy-test001"
    assert snapshot["scope"] == "stock-scoped"
    assert snapshot["evidence_bundle_hash"]
    current = registry.get_current_pointer()
    assert current["deployment_id"] == "deploy-test001"
    assert current["scope"] == "stock-scoped"
    assert current["rollback_of_deployment_id"] is None
    assert (tmp_path / "artifacts" / "server_deployments" / "history" / "deploy-test001.json").exists()

    with pytest.raises(FileExistsError):
        registry.register(evidence_bundle_path=evidence)


def test_server_deploy_registry_rejects_dry_run_evidence(tmp_path):
    registry = ServerDeployRegistry(deployments_dir=tmp_path / "artifacts" / "server_deployments")
    evidence = write_server_deploy_evidence_bundle(tmp_path / "release1", dry_run=True)

    with pytest.raises(ValueError, match="dry-run"):
        registry.register(evidence_bundle_path=evidence)

    assert registry.get_current_pointer()["deployment_id"] is None


def test_server_deploy_registry_current_pointer_switches_and_rolls_back(tmp_path):
    registry = ServerDeployRegistry(deployments_dir=tmp_path / "artifacts" / "server_deployments")
    evidence_one = write_server_deploy_evidence_bundle(tmp_path / "release1", deployment_id="deploy-test001")
    evidence_two = write_server_deploy_evidence_bundle(tmp_path / "release2", deployment_id="deploy-test002")

    registry.register(evidence_bundle_path=evidence_one)
    registry.register(evidence_bundle_path=evidence_two)

    assert registry.get_current_pointer()["deployment_id"] == "deploy-test002"
    rolled_back = registry.rollback("deploy-test001")
    assert rolled_back["deployment_id"] == "deploy-test001"
    current = registry.get_current_pointer()
    assert current["deployment_id"] == "deploy-test001"
    assert current["scope"] == "stock-scoped"
    assert current["rollback_of_deployment_id"] == "deploy-test001"
