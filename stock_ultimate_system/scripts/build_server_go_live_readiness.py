#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.server_deploy_registry import ServerDeployRegistry


GO_LIVE_READINESS_VERSION = "server_go_live_readiness.v1"
DEFAULT_PUBLIC_URLS = {
    "main_site": "https://airivo.online/",
    "stock": "https://airivo.online/stock/",
    "t12": "https://airivo.online/T12/",
}
REQUIRED_POST_DEPLOY_CHECKS = (
    "systemd_services_active",
    "nginx_config_valid",
    "dashboard_http_targets",
    "required_app_paths",
    "protected_runtime_paths_observed",
    "service_error_log_scan",
)


def _required_route_scopes(scope: str) -> tuple[str, ...]:
    if scope == "stock-scoped":
        return ("stock",)
    if scope == "full-domain":
        return ("main_site", "stock", "t12")
    raise ValueError(f"unsupported go-live readiness scope: {scope}")


def _scoped_public_urls(scope: str, public_urls: dict[str, str]) -> dict[str, str]:
    return {route_scope: public_urls[route_scope] for route_scope in _required_route_scopes(scope)}


def _sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _read_json(path: str | Path) -> dict[str, object]:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"expected object json: {path}")
    return payload


def _write_output(path: str | Path | None, payload: dict[str, object]) -> None:
    if not path:
        return
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _check_result(name: str, passed: bool, details: dict[str, object] | None = None) -> dict[str, object]:
    return {
        "check": name,
        "passed": passed,
        "details": details or {},
    }


def _post_deploy_checks_from_bundle(bundle: dict[str, object]) -> dict[str, bool]:
    raw_checks = bundle.get("post_deploy_checks")
    if not isinstance(raw_checks, list):
        return {}
    checks: dict[str, bool] = {}
    for raw_check in raw_checks:
        if isinstance(raw_check, dict) and raw_check.get("check"):
            checks[str(raw_check["check"])] = raw_check.get("passed") is True
    return checks


def build_server_go_live_readiness(
    *,
    deployments_dir: str | Path = "/opt/stock-ultimate/deployments",
    output_path: str | Path | None = None,
    public_urls: dict[str, str] | None = None,
) -> tuple[int, dict[str, object]]:
    registry = ServerDeployRegistry(deployments_dir=deployments_dir)
    resolved_public_urls = dict(DEFAULT_PUBLIC_URLS)
    if public_urls:
        resolved_public_urls.update(public_urls)
    pointer = registry.get_current_pointer()
    snapshot = registry.get_current_snapshot()
    checks: list[dict[str, object]] = []

    checks.append(
        _check_result(
            "current_pointer_exists",
            bool(pointer.get("deployment_id") and pointer.get("snapshot_path")),
            {
                "deployment_id": pointer.get("deployment_id"),
                "snapshot_path": pointer.get("snapshot_path"),
            },
        )
    )
    if not snapshot:
        payload = {
            "go_live_readiness_version": GO_LIVE_READINESS_VERSION,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "status": "failed",
            "deployment_id": pointer.get("deployment_id"),
            "scope": None,
            "public_urls": {},
            "checks": checks,
            "blocking_failures": [check for check in checks if not check["passed"]],
        }
        _write_output(output_path, payload)
        return 1, payload

    evidence_path = Path(str(snapshot.get("source_evidence_bundle_path", "") or ""))
    evidence_bundle: dict[str, object] = {}
    if evidence_path.exists():
        evidence_bundle = _read_json(evidence_path)
    checks.append(
        _check_result(
            "current_snapshot_valid",
            snapshot.get("snapshot_version") == "server_deploy_snapshot.v1",
            {"snapshot_version": snapshot.get("snapshot_version")},
        )
    )
    checks.append(
        _check_result(
            "evidence_bundle_exists",
            evidence_path.exists(),
            {"source_evidence_bundle_path": str(evidence_path)},
        )
    )
    evidence_hash = _sha256_file(evidence_path) if evidence_path.exists() else None
    scope = str(snapshot.get("scope") or evidence_bundle.get("scope") or "").strip()
    checks.append(
        _check_result(
            "evidence_bundle_hash_matches",
            bool(evidence_hash and evidence_hash == snapshot.get("evidence_bundle_hash")),
            {
                "expected_hash": snapshot.get("evidence_bundle_hash"),
                "actual_hash": evidence_hash,
            },
        )
    )
    checks.append(
        _check_result(
            "evidence_bundle_passed",
            evidence_bundle.get("server_deploy_evidence_bundle_version") == "server_deploy_evidence_bundle.v1"
            and evidence_bundle.get("status") == "passed",
            {
                "version": evidence_bundle.get("server_deploy_evidence_bundle_version"),
                "status": evidence_bundle.get("status"),
            },
        )
    )
    checks.append(
        _check_result(
            "deployment_scope_valid",
            scope in {"stock-scoped", "full-domain"},
            {"scope": scope},
        )
    )
    activation_execution = evidence_bundle.get("activation_execution")
    activation_passed = (
        isinstance(activation_execution, dict)
        and activation_execution.get("action") == "activate"
        and activation_execution.get("dry_run") is False
        and activation_execution.get("failed_total") == 0
    )
    checks.append(
        _check_result(
            "activation_execution_applied",
            activation_passed,
            dict(activation_execution or {}) if isinstance(activation_execution, dict) else {},
        )
    )
    rollback = evidence_bundle.get("rollback")
    checks.append(
        _check_result(
            "rollback_available",
            isinstance(rollback, dict) and rollback.get("available") is True and bool(rollback.get("commands")),
            dict(rollback or {}) if isinstance(rollback, dict) else {},
        )
    )

    route_topology = evidence_bundle.get("route_topology")
    route_scopes = set(route_topology.keys()) if isinstance(route_topology, dict) else set()
    required_route_scopes = _required_route_scopes(scope) if scope in {"stock-scoped", "full-domain"} else ()
    checks.append(
        _check_result(
            "route_topology_complete",
            bool(required_route_scopes) and all(route_scope in route_scopes for route_scope in required_route_scopes),
            {
                "scope": scope,
                "required_scopes": list(required_route_scopes),
                "actual_scopes": sorted(route_scopes),
            },
        )
    )
    post_deploy_checks = _post_deploy_checks_from_bundle(evidence_bundle)
    missing_checks = [name for name in REQUIRED_POST_DEPLOY_CHECKS if name not in post_deploy_checks]
    failed_checks = [name for name in REQUIRED_POST_DEPLOY_CHECKS if post_deploy_checks.get(name) is False]
    checks.append(
        _check_result(
            "post_deploy_checks_complete",
            not missing_checks and not failed_checks,
            {
                "required_checks": list(REQUIRED_POST_DEPLOY_CHECKS),
                "missing_checks": missing_checks,
                "failed_checks": failed_checks,
            },
        )
    )
    blocking_failures = [check for check in checks if not check["passed"]]
    payload = {
        "go_live_readiness_version": GO_LIVE_READINESS_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "status": "passed" if not blocking_failures else "failed",
        "deployment_id": snapshot.get("deployment_id"),
        "scope": scope,
        "current_pointer_path": str(registry.current_path),
        "snapshot_path": pointer.get("snapshot_path"),
        "source_evidence_bundle_path": str(evidence_path),
        "public_urls": _scoped_public_urls(scope, resolved_public_urls) if scope in {"stock-scoped", "full-domain"} else {},
        "route_topology": route_topology if isinstance(route_topology, dict) else {},
        "checks": checks,
        "blocking_failures": blocking_failures,
    }
    _write_output(output_path, payload)
    return (0 if payload["status"] == "passed" else 1), payload


def main() -> int:
    parser = argparse.ArgumentParser(description="Build the final go-live readiness decision from the server deployment registry.")
    parser.add_argument("--deployments-dir", default="/opt/stock-ultimate/deployments")
    parser.add_argument("--output")
    parser.add_argument("--main-site-url", default=DEFAULT_PUBLIC_URLS["main_site"])
    parser.add_argument("--stock-url", default=DEFAULT_PUBLIC_URLS["stock"])
    parser.add_argument("--t12-url", default=DEFAULT_PUBLIC_URLS["t12"])
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    try:
        exit_code, payload = build_server_go_live_readiness(
            deployments_dir=args.deployments_dir,
            output_path=args.output,
            public_urls={
                "main_site": args.main_site_url,
                "stock": args.stock_url,
                "t12": args.t12_url,
            },
        )
    except Exception as exc:
        print(json.dumps({"status": "failed", "error": str(exc)}, ensure_ascii=False, indent=2))
        return 1
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print(
            json.dumps(
                {
                    "status": payload["status"],
                    "go_live_readiness_version": payload["go_live_readiness_version"],
                    "deployment_id": payload["deployment_id"],
                    "scope": payload["scope"],
                    "public_urls": payload["public_urls"],
                    "blocking_failure_total": len(payload["blocking_failures"]),
                },
                ensure_ascii=False,
                indent=2,
            )
        )
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
