#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path


SERVER_DEPLOY_EVIDENCE_BUNDLE_VERSION = "server_deploy_evidence_bundle.v1"
DEFAULT_DEPLOY_SCOPE = "stock-scoped"


def _required_route_topology_for_scope(scope: str) -> dict[str, dict[str, str]]:
    if scope == "stock-scoped":
        return {
            "stock": {
                "route": "/stock/",
                "service": "stock-ultimate-dashboard.service",
                "local_url": "http://127.0.0.1:8765/stock/",
            }
        }
    if scope == "full-domain":
        return {
            "main_site": {
                "route": "/",
                "service": "stock-ultimate-main-site.service",
                "local_url": "http://127.0.0.1:8764/",
            },
            "stock": {
                "route": "/stock/",
                "service": "stock-ultimate-dashboard.service",
                "local_url": "http://127.0.0.1:8765/stock/",
            },
            "t12": {
                "route": "/T12/",
                "service": "stock-ultimate-t12.service",
                "local_url": "http://127.0.0.1:8766/T12/",
            },
        }
    raise ValueError(f"unsupported deploy scope: {scope}")


def _resolve_bundle_scope(
    activation_payload: dict[str, object],
    activation_execution_payload: dict[str, object],
    post_deploy_payload: dict[str, object],
) -> str:
    activation_scope = str(activation_payload.get("scope") or "").strip()
    verification_scope = str(post_deploy_payload.get("rollout_scope") or "").strip()
    rollback_scope = str((post_deploy_payload.get("rollback_hint") or {}).get("scope") or "").strip()
    candidate_scopes = [scope for scope in (activation_scope, verification_scope, rollback_scope) if scope]
    if not candidate_scopes:
        return DEFAULT_DEPLOY_SCOPE
    resolved_scope = candidate_scopes[0]
    for scope in candidate_scopes[1:]:
        if scope != resolved_scope:
            raise ValueError("activation scope, verification rollout_scope, and rollback scope must match")
    if activation_execution_payload.get("action") == "activate" and resolved_scope not in {"stock-scoped", "full-domain"}:
        raise ValueError(f"unsupported deploy scope: {resolved_scope}")
    return resolved_scope


def _sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _read_json(path: str | Path) -> dict[str, object]:
    resolved_path = Path(path)
    payload = json.loads(resolved_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"JSON artifact is not an object: {resolved_path}")
    return payload


def _artifact_entry(path: str | Path, expected_version_key: str, expected_version: str) -> dict[str, object]:
    resolved_path = Path(path)
    payload = _read_json(resolved_path)
    if payload.get(expected_version_key) != expected_version:
        raise ValueError(f"{resolved_path} is not {expected_version}")
    return {
        "path": str(resolved_path),
        "sha256": _sha256_file(resolved_path),
        "status": payload.get("status"),
        "version_key": expected_version_key,
        "version": payload.get(expected_version_key),
        "payload": payload,
    }


def _require_passed(artifact: dict[str, object], label: str) -> None:
    if artifact.get("status") != "passed":
        raise ValueError(f"{label} must be passed before building server deploy evidence bundle")


def _require_activation_execution_applied(artifact: dict[str, object]) -> None:
    payload = artifact["payload"]
    if payload.get("action") != "activate":
        raise ValueError("activation execution must use action=activate")
    if payload.get("dry_run") is not False:
        raise ValueError("activation execution must not be a dry-run")
    if payload.get("failed_total") != 0:
        raise ValueError("activation execution failed_total must be zero")


def build_server_deploy_evidence_bundle(
    *,
    deployment_id: str,
    preflight_json: str | Path,
    file_list_json: str | Path,
    activation_plan_json: str | Path,
    activation_execution_json: str | Path,
    post_deploy_json: str | Path,
    output_path: str | Path,
) -> Path:
    preflight = _artifact_entry(preflight_json, "preflight_version", "server_sync_preflight.v1")
    file_list = _artifact_entry(file_list_json, "file_list_version", "server_sync_file_list.v1")
    activation = _artifact_entry(activation_plan_json, "activation_plan_version", "server_activation_plan.v1")
    activation_execution = _artifact_entry(activation_execution_json, "activation_execution_version", "server_activation_execution.v1")
    post_deploy = _artifact_entry(post_deploy_json, "post_deploy_version", "server_post_deploy_verification.v1")
    _require_passed(preflight, "preflight")
    _require_passed(file_list, "file list")
    _require_passed(activation, "activation plan")
    _require_passed(activation_execution, "activation execution")
    _require_activation_execution_applied(activation_execution)
    _require_passed(post_deploy, "post deploy verification")

    preflight_payload = preflight["payload"]
    activation_payload = activation["payload"]
    activation_execution_payload = activation_execution["payload"]
    post_deploy_payload = post_deploy["payload"]
    scope = _resolve_bundle_scope(activation_payload, activation_execution_payload, post_deploy_payload)
    bundle = {
        "server_deploy_evidence_bundle_version": SERVER_DEPLOY_EVIDENCE_BUNDLE_VERSION,
        "deployment_id": deployment_id,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "status": "passed",
        "scope": scope,
        "sync_policy": preflight_payload.get("sync_policy"),
        "manifest_summary": preflight_payload.get("manifest_summary"),
        "artifacts": {
            "preflight": {key: preflight[key] for key in ("path", "sha256", "status", "version")},
            "file_list": {key: file_list[key] for key in ("path", "sha256", "status", "version")},
            "activation_plan": {key: activation[key] for key in ("path", "sha256", "status", "version")},
            "activation_execution": {key: activation_execution[key] for key in ("path", "sha256", "status", "version")},
            "post_deploy_verification": {key: post_deploy[key] for key in ("path", "sha256", "status", "version")},
        },
        "activation_execution": {
            "action": activation_execution_payload.get("action"),
            "dry_run": activation_execution_payload.get("dry_run"),
            "command_total": activation_execution_payload.get("command_total"),
            "executed_total": activation_execution_payload.get("executed_total"),
            "failed_total": activation_execution_payload.get("failed_total"),
        },
        "deployment_layout": activation_payload.get("layout"),
        "post_deploy_checks": [
            {
                "check": check.get("check"),
                "passed": check.get("passed"),
            }
            for check in post_deploy_payload.get("checks", [])
            if isinstance(check, dict)
        ],
        "rollback": {
            "available": (post_deploy_payload.get("rollback_hint") or {}).get("available"),
            "commands": (post_deploy_payload.get("rollback_hint") or {}).get("rollback_commands", []),
        },
        "route_topology": _required_route_topology_for_scope(scope),
    }
    destination = Path(output_path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(json.dumps(bundle, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return destination


def main() -> int:
    parser = argparse.ArgumentParser(description="Build a server deployment evidence bundle from passed deployment artifacts.")
    parser.add_argument("--deployment-id", required=True)
    parser.add_argument("--preflight-json", required=True)
    parser.add_argument("--file-list-json", required=True)
    parser.add_argument("--activation-plan-json", required=True)
    parser.add_argument("--activation-execution-json", required=True)
    parser.add_argument("--post-deploy-json", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    try:
        bundle_path = build_server_deploy_evidence_bundle(
            deployment_id=args.deployment_id,
            preflight_json=args.preflight_json,
            file_list_json=args.file_list_json,
            activation_plan_json=args.activation_plan_json,
            activation_execution_json=args.activation_execution_json,
            post_deploy_json=args.post_deploy_json,
            output_path=args.output,
        )
    except Exception as exc:
        print(json.dumps({"status": "failed", "error": str(exc)}, ensure_ascii=False, indent=2))
        return 1
    print(json.dumps({"status": "passed", "output": str(bundle_path)}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
