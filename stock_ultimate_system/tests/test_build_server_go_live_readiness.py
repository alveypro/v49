import json
import subprocess
import sys
from pathlib import Path

from scripts.build_server_go_live_readiness import build_server_go_live_readiness
from src.server_deploy_registry import ServerDeployRegistry
from server_deploy_test_helpers import write_server_deploy_evidence_bundle


def test_build_server_go_live_readiness_passes_for_current_registered_deploy(tmp_path):
    bundle_path = write_server_deploy_evidence_bundle(tmp_path, deployment_id="deploy-ready001")
    deployments_dir = tmp_path / "deployments"
    ServerDeployRegistry(deployments_dir=deployments_dir).register(evidence_bundle_path=bundle_path)

    exit_code, payload = build_server_go_live_readiness(
        deployments_dir=deployments_dir,
        output_path=tmp_path / "go_live.json",
    )

    assert exit_code == 0
    assert payload["go_live_readiness_version"] == "server_go_live_readiness.v1"
    assert payload["status"] == "passed"
    assert payload["deployment_id"] == "deploy-ready001"
    assert payload["scope"] == "stock-scoped"
    assert payload["public_urls"]["stock"] == "https://airivo.online/stock/"
    assert set(payload["public_urls"]) == {"stock"}
    assert payload["blocking_failures"] == []


def test_build_server_go_live_readiness_rejects_missing_current_pointer(tmp_path):
    exit_code, payload = build_server_go_live_readiness(deployments_dir=tmp_path / "deployments")

    assert exit_code == 1
    assert payload["status"] == "failed"
    assert payload["scope"] is None
    assert payload["public_urls"] == {}
    assert payload["blocking_failures"][0]["check"] == "current_pointer_exists"


def test_build_server_go_live_readiness_allows_public_url_override(tmp_path):
    bundle_path = write_server_deploy_evidence_bundle(tmp_path, deployment_id="deploy-domain001")
    deployments_dir = tmp_path / "deployments"
    ServerDeployRegistry(deployments_dir=deployments_dir).register(evidence_bundle_path=bundle_path)

    exit_code, payload = build_server_go_live_readiness(
        deployments_dir=deployments_dir,
        public_urls={"main_site": "https://airivo.online/"},
    )

    assert exit_code == 0
    assert payload["scope"] == "stock-scoped"
    assert payload["public_urls"] == {"stock": "https://airivo.online/stock/"}
    assert payload["public_urls"]["stock"] == "https://airivo.online/stock/"


def test_build_server_go_live_readiness_rejects_tampered_evidence_bundle(tmp_path):
    bundle_path = write_server_deploy_evidence_bundle(tmp_path, deployment_id="deploy-tampered001")
    deployments_dir = tmp_path / "deployments"
    ServerDeployRegistry(deployments_dir=deployments_dir).register(evidence_bundle_path=bundle_path)
    payload = json.loads(bundle_path.read_text(encoding="utf-8"))
    payload["route_topology"].pop("stock")
    bundle_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")

    exit_code, readiness = build_server_go_live_readiness(deployments_dir=deployments_dir)

    assert exit_code == 1
    assert readiness["status"] == "failed"
    failure_names = {check["check"] for check in readiness["blocking_failures"]}
    assert "evidence_bundle_hash_matches" in failure_names
    assert "route_topology_complete" in failure_names


def test_build_server_go_live_readiness_rejects_missing_post_deploy_checks(tmp_path):
    bundle_path = write_server_deploy_evidence_bundle(tmp_path, deployment_id="deploy-missing-check001")
    payload = json.loads(bundle_path.read_text(encoding="utf-8"))
    payload["post_deploy_checks"] = [{"check": "systemd_services_active", "passed": True}]
    bundle_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    deployments_dir = tmp_path / "deployments"
    ServerDeployRegistry(deployments_dir=deployments_dir).register(evidence_bundle_path=bundle_path)

    exit_code, readiness = build_server_go_live_readiness(deployments_dir=deployments_dir)

    assert exit_code == 1
    assert readiness["status"] == "failed"
    post_deploy_failure = next(
        check for check in readiness["blocking_failures"] if check["check"] == "post_deploy_checks_complete"
    )
    assert "nginx_config_valid" in post_deploy_failure["details"]["missing_checks"]


def test_build_server_go_live_readiness_cli_outputs_json(tmp_path):
    project_root = Path(__file__).resolve().parents[1]
    script_path = project_root / "scripts" / "build_server_go_live_readiness.py"
    bundle_path = write_server_deploy_evidence_bundle(tmp_path, deployment_id="deploy-cli001")
    deployments_dir = tmp_path / "deployments"
    ServerDeployRegistry(deployments_dir=deployments_dir).register(evidence_bundle_path=bundle_path)
    output_path = tmp_path / "go_live.json"

    completed = subprocess.run(
        [
            sys.executable,
            str(script_path),
            "--deployments-dir",
            str(deployments_dir),
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
    assert payload["deployment_id"] == "deploy-cli001"
    assert payload["scope"] == "stock-scoped"
    assert json.loads(output_path.read_text(encoding="utf-8"))["status"] == "passed"
