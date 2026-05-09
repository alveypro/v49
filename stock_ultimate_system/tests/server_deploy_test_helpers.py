import json
from pathlib import Path


def write_server_activation_plan(tmp_path, *, release_id="release-test001"):
    plan_path = Path(tmp_path) / "server_activation_plan.json"
    plan_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "activation_plan_version": "server_activation_plan.v1",
        "status": "passed",
        "release_id": release_id,
        "scope": "stock-scoped",
        "activation_commands": ["echo activate"],
        "rollback_commands": ["echo rollback"],
    }
    plan_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    return plan_path


def write_server_deploy_evidence_bundle(tmp_path, *, deployment_id="deploy-test001", dry_run=False, activation_plan_path=None):
    root = Path(tmp_path)
    bundle_path = root / "server_deploy_evidence_bundle.json"
    bundle_path.parent.mkdir(parents=True, exist_ok=True)
    plan_path = activation_plan_path or write_server_activation_plan(root)
    payload = {
        "server_deploy_evidence_bundle_version": "server_deploy_evidence_bundle.v1",
        "deployment_id": deployment_id,
        "status": "passed",
        "scope": "stock-scoped",
        "activation_execution": {
            "action": "activate",
            "dry_run": dry_run,
            "command_total": 2,
            "executed_total": 2,
            "failed_total": 0,
        },
        "rollback": {
            "available": True,
            "commands": ["systemctl restart stock-ultimate-dashboard.service"],
        },
        "artifacts": {
            "activation_plan": {
                "path": str(plan_path),
                "status": "passed",
                "version": "server_activation_plan.v1",
            },
        },
        "post_deploy_checks": [
            {"check": "systemd_services_active", "passed": True},
            {"check": "nginx_config_valid", "passed": True},
            {"check": "dashboard_http_targets", "passed": True},
            {"check": "required_app_paths", "passed": True},
            {"check": "protected_runtime_paths_observed", "passed": True},
            {"check": "service_error_log_scan", "passed": True},
        ],
        "route_topology": {
            "stock": {"route": "/stock/", "local_url": "http://127.0.0.1:8765/stock/"},
        },
    }
    bundle_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    return bundle_path
