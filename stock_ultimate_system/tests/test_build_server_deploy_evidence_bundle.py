import json
import subprocess
import sys
from pathlib import Path

import pytest

from scripts.build_server_deploy_evidence_bundle import build_server_deploy_evidence_bundle


def _write_json(path, payload):
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def _write_passed_deploy_artifacts(tmp_path):
    preflight = tmp_path / "preflight.json"
    file_list = tmp_path / "file_list.json"
    activation = tmp_path / "activation.json"
    activation_execution = tmp_path / "activation_execution.json"
    post_deploy = tmp_path / "post_deploy.json"
    _write_json(
        preflight,
        {
            "preflight_version": "server_sync_preflight.v1",
            "status": "passed",
            "sync_policy": "code_config_docs_tests_deploy_only",
            "manifest_summary": {"allowed_total": 10, "denied_total": 1, "unclassified_total": 0},
        },
    )
    _write_json(
        file_list,
        {
            "file_list_version": "server_sync_file_list.v1",
            "status": "passed",
            "file_total": 10,
        },
    )
    _write_json(
        activation,
        {
            "activation_plan_version": "server_activation_plan.v1",
            "status": "passed",
            "scope": "stock-scoped",
            "layout": {"app_dir": "/opt/stock-ultimate/app"},
        },
    )
    _write_json(
        activation_execution,
        {
            "activation_execution_version": "server_activation_execution.v1",
            "status": "passed",
            "action": "activate",
            "dry_run": False,
            "command_total": 2,
            "executed_total": 2,
            "failed_total": 0,
        },
    )
    _write_json(
        post_deploy,
        {
            "post_deploy_version": "server_post_deploy_verification.v1",
            "status": "passed",
            "rollout_scope": "stock-scoped",
            "checks": [
                {"check": "systemd_services_active", "passed": True},
                {"check": "dashboard_http_targets", "passed": True},
            ],
            "rollback_hint": {
                "available": True,
                "scope": "stock-scoped",
                "rollback_commands": ["systemctl restart stock-ultimate-dashboard.service"],
            },
        },
    )
    return preflight, file_list, activation, activation_execution, post_deploy


def test_build_server_deploy_evidence_bundle_records_hashes_and_routes(tmp_path):
    preflight, file_list, activation, activation_execution, post_deploy = _write_passed_deploy_artifacts(tmp_path)
    output_path = tmp_path / "server_deploy_evidence_bundle.json"

    bundle_path = build_server_deploy_evidence_bundle(
        deployment_id="deploy-20260414-001",
        preflight_json=preflight,
        file_list_json=file_list,
        activation_plan_json=activation,
        activation_execution_json=activation_execution,
        post_deploy_json=post_deploy,
        output_path=output_path,
    )

    payload = json.loads(bundle_path.read_text(encoding="utf-8"))
    assert payload["server_deploy_evidence_bundle_version"] == "server_deploy_evidence_bundle.v1"
    assert payload["deployment_id"] == "deploy-20260414-001"
    assert payload["status"] == "passed"
    assert payload["scope"] == "stock-scoped"
    assert payload["artifacts"]["preflight"]["sha256"]
    assert payload["artifacts"]["activation_execution"]["sha256"]
    assert payload["activation_execution"]["dry_run"] is False
    assert payload["rollback"]["available"] is True
    assert payload["route_topology"]["stock"]["route"] == "/stock/"
    assert set(payload["route_topology"]) == {"stock"}


def test_build_server_deploy_evidence_bundle_rejects_scope_mismatch(tmp_path):
    preflight, file_list, activation, activation_execution, post_deploy = _write_passed_deploy_artifacts(tmp_path)
    _write_json(
        post_deploy,
        {
            "post_deploy_version": "server_post_deploy_verification.v1",
            "status": "passed",
            "rollout_scope": "full-domain",
            "checks": [],
            "rollback_hint": {
                "available": True,
                "scope": "full-domain",
                "rollback_commands": ["systemctl restart stock-ultimate-dashboard.service"],
            },
        },
    )

    with pytest.raises(ValueError, match="scope"):
        build_server_deploy_evidence_bundle(
            deployment_id="deploy-20260414-scope-mismatch",
            preflight_json=preflight,
            file_list_json=file_list,
            activation_plan_json=activation,
            activation_execution_json=activation_execution,
            post_deploy_json=post_deploy,
            output_path=tmp_path / "bundle.json",
        )


def test_build_server_deploy_evidence_bundle_rejects_failed_post_deploy(tmp_path):
    preflight, file_list, activation, activation_execution, post_deploy = _write_passed_deploy_artifacts(tmp_path)
    _write_json(
        post_deploy,
        {
            "post_deploy_version": "server_post_deploy_verification.v1",
            "status": "failed",
        },
    )

    with pytest.raises(ValueError, match="post deploy verification"):
        build_server_deploy_evidence_bundle(
            deployment_id="deploy-20260414-002",
            preflight_json=preflight,
            file_list_json=file_list,
            activation_plan_json=activation,
            activation_execution_json=activation_execution,
            post_deploy_json=post_deploy,
            output_path=tmp_path / "bundle.json",
        )


def test_build_server_deploy_evidence_bundle_rejects_failed_activation_execution(tmp_path):
    preflight, file_list, activation, activation_execution, post_deploy = _write_passed_deploy_artifacts(tmp_path)
    _write_json(
        activation_execution,
        {
            "activation_execution_version": "server_activation_execution.v1",
            "status": "failed",
        },
    )

    with pytest.raises(ValueError, match="activation execution"):
        build_server_deploy_evidence_bundle(
            deployment_id="deploy-20260414-activation-failed",
            preflight_json=preflight,
            file_list_json=file_list,
            activation_plan_json=activation,
            activation_execution_json=activation_execution,
            post_deploy_json=post_deploy,
            output_path=tmp_path / "bundle.json",
        )


def test_build_server_deploy_evidence_bundle_rejects_dry_run_activation_execution(tmp_path):
    preflight, file_list, activation, activation_execution, post_deploy = _write_passed_deploy_artifacts(tmp_path)
    _write_json(
        activation_execution,
        {
            "activation_execution_version": "server_activation_execution.v1",
            "status": "passed",
            "action": "activate",
            "dry_run": True,
            "failed_total": 0,
        },
    )

    with pytest.raises(ValueError, match="dry-run"):
        build_server_deploy_evidence_bundle(
            deployment_id="deploy-20260414-dry-run",
            preflight_json=preflight,
            file_list_json=file_list,
            activation_plan_json=activation,
            activation_execution_json=activation_execution,
            post_deploy_json=post_deploy,
            output_path=tmp_path / "bundle.json",
        )


def test_build_server_deploy_evidence_bundle_rejects_rollback_activation_execution(tmp_path):
    preflight, file_list, activation, activation_execution, post_deploy = _write_passed_deploy_artifacts(tmp_path)
    _write_json(
        activation_execution,
        {
            "activation_execution_version": "server_activation_execution.v1",
            "status": "passed",
            "action": "rollback",
            "dry_run": False,
            "failed_total": 0,
        },
    )

    with pytest.raises(ValueError, match="action=activate"):
        build_server_deploy_evidence_bundle(
            deployment_id="deploy-20260414-rollback",
            preflight_json=preflight,
            file_list_json=file_list,
            activation_plan_json=activation,
            activation_execution_json=activation_execution,
            post_deploy_json=post_deploy,
            output_path=tmp_path / "bundle.json",
        )


def test_build_server_deploy_evidence_bundle_cli_outputs_status(tmp_path):
    project_root = Path(__file__).resolve().parents[1]
    script_path = project_root / "scripts" / "build_server_deploy_evidence_bundle.py"
    preflight, file_list, activation, activation_execution, post_deploy = _write_passed_deploy_artifacts(tmp_path)
    output_path = tmp_path / "bundle.json"

    completed = subprocess.run(
        [
            sys.executable,
            str(script_path),
            "--deployment-id",
            "deploy-20260414-003",
            "--preflight-json",
            str(preflight),
            "--file-list-json",
            str(file_list),
            "--activation-plan-json",
            str(activation),
            "--activation-execution-json",
            str(activation_execution),
            "--post-deploy-json",
            str(post_deploy),
            "--output",
            str(output_path),
        ],
        cwd=project_root,
        capture_output=True,
        text=True,
        check=True,
    )
    payload = json.loads(completed.stdout)

    assert payload["status"] == "passed"
    assert output_path.exists()
