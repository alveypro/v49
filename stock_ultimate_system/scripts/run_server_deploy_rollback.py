#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Callable


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.run_server_activation_plan import run_server_activation_plan
from scripts.run_server_post_deploy_verification import run_server_post_deploy_verification
from src.server_deploy_registry import ServerDeployRegistry


SERVER_DEPLOY_ROLLBACK_VERSION = "server_deploy_rollback.v1"
CommandRunner = Callable[..., subprocess.CompletedProcess[str]]


def _read_json(path: str | Path) -> dict[str, object]:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"expected object json: {path}")
    return payload


def _activation_plan_path_from_snapshot(snapshot: dict[str, object]) -> Path:
    evidence_path = Path(str(snapshot.get("source_evidence_bundle_path", "") or ""))
    evidence_payload = _read_json(evidence_path)
    artifacts = evidence_payload.get("artifacts")
    if not isinstance(artifacts, dict):
        raise ValueError("server deploy evidence bundle missing artifacts")
    activation_plan = artifacts.get("activation_plan")
    if not isinstance(activation_plan, dict) or not activation_plan.get("path"):
        raise ValueError("server deploy evidence bundle missing activation_plan artifact path")
    return Path(str(activation_plan["path"]))


def _snapshot_scope(snapshot: dict[str, object]) -> str:
    scope = str(snapshot.get("scope") or "").strip()
    if not scope:
        raise ValueError("server deployment snapshot missing scope")
    return scope


def _write_output(path: str | Path | None, payload: dict[str, object]) -> None:
    if not path:
        return
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def run_server_deploy_rollback(
    *,
    deployments_dir: str | Path,
    rollback_deployment_id: str,
    confirm_deployment_id: str,
    dry_run: bool = False,
    output_path: str | Path | None = None,
    activation_execution_output_path: str | Path | None = None,
    post_deploy_output_path: str | Path | None = None,
    command_runner: CommandRunner | None = None,
    url_fetcher=None,
) -> tuple[int, dict[str, object]]:
    if confirm_deployment_id != rollback_deployment_id:
        raise ValueError("confirm_deployment_id must match rollback_deployment_id")

    registry = ServerDeployRegistry(deployments_dir=deployments_dir)
    snapshot = registry.get_snapshot(rollback_deployment_id)
    snapshot_scope = _snapshot_scope(snapshot)
    activation_plan_path = _activation_plan_path_from_snapshot(snapshot)
    activation_kwargs = {}
    if command_runner is not None:
        activation_kwargs["command_runner"] = command_runner
    activation_exit_code, activation_payload = run_server_activation_plan(
        plan_path=activation_plan_path,
        confirm_release_id=str(_read_json(activation_plan_path).get("release_id", "") or ""),
        action="rollback",
        dry_run=dry_run,
        output_path=activation_execution_output_path,
        **activation_kwargs,
    )
    post_deploy_payload: dict[str, object] = {
        "status": "skipped",
        "reason": "dry-run rollback does not change server state",
    }
    post_deploy_exit_code = 0
    if activation_exit_code == 0 and not dry_run:
        verification_kwargs = {}
        if command_runner is not None:
            verification_kwargs["command_runner"] = command_runner
        if url_fetcher is not None:
            verification_kwargs["url_fetcher"] = url_fetcher
        post_deploy_exit_code, post_deploy_payload = run_server_post_deploy_verification(
            activation_plan_path=activation_plan_path,
            output_path=post_deploy_output_path,
            **verification_kwargs,
        )
    if activation_exit_code == 0 and post_deploy_exit_code == 0 and not dry_run:
        registry.rollback(rollback_deployment_id)

    payload = {
        "server_deploy_rollback_version": SERVER_DEPLOY_ROLLBACK_VERSION,
        "status": "passed" if activation_exit_code == 0 and post_deploy_exit_code == 0 else "failed",
        "rollback_deployment_id": rollback_deployment_id,
        "scope": snapshot_scope,
        "dry_run": dry_run,
        "activation_plan_path": str(activation_plan_path),
        "activation_execution": {
            "status": activation_payload.get("status"),
            "action": activation_payload.get("action"),
            "dry_run": activation_payload.get("dry_run"),
            "failed_total": activation_payload.get("failed_total"),
        },
        "post_deploy_verification": {
            "status": post_deploy_payload.get("status"),
        },
        "current_pointer_path": str(registry.current_path),
    }
    _write_output(output_path, payload)
    return (0 if payload["status"] == "passed" else 1), payload


def main() -> int:
    parser = argparse.ArgumentParser(description="Run rollback to a registered server deployment and update current pointer after verification.")
    parser.add_argument("--deployments-dir", default="/opt/stock-ultimate/deployments")
    parser.add_argument("--rollback-deployment-id", required=True)
    parser.add_argument("--confirm-deployment-id", required=True)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--output")
    parser.add_argument("--activation-execution-output")
    parser.add_argument("--post-deploy-output")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    try:
        exit_code, payload = run_server_deploy_rollback(
            deployments_dir=args.deployments_dir,
            rollback_deployment_id=args.rollback_deployment_id,
            confirm_deployment_id=args.confirm_deployment_id,
            dry_run=args.dry_run,
            output_path=args.output,
            activation_execution_output_path=args.activation_execution_output,
            post_deploy_output_path=args.post_deploy_output,
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
                    "server_deploy_rollback_version": payload["server_deploy_rollback_version"],
                    "rollback_deployment_id": payload["rollback_deployment_id"],
                    "dry_run": payload["dry_run"],
                },
                ensure_ascii=False,
                indent=2,
            )
        )
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
