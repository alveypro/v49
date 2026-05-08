#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Callable


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.check_release_gates import run_release_gates
from scripts.prepare_server_sync_manifest import build_server_sync_manifest
from scripts.run_airivo_scope_full_readiness import run_scope_full_readiness


PREFLIGHT_VERSION = "server_sync_preflight.v1"
REQUIRED_ALLOWED_FILES = (
    "src/candidate_quality/data_quality.py",
    "src/candidate_quality/explanation.py",
    "src/candidate_quality/lineage.py",
    "src/candidate_quality/observation.py",
    "src/candidate_quality/portfolio.py",
    "src/candidate_quality/proof_report.py",
    "src/candidate_quality/remediation.py",
    "src/candidate_quality/realistic_backtest.py",
    "src/candidate_quality/risk_state.py",
    "src/dashboard_context.py",
    "src/airivo_scope_registry.py",
    "scripts/build_candidate_data_quality_report.py",
    "scripts/build_candidate_explanations.py",
    "scripts/build_candidate_observation_closure.py",
    "scripts/build_candidate_portfolio.py",
    "scripts/build_candidate_quality_proof_report.py",
    "scripts/build_candidate_quality_remediation_plan.py",
    "scripts/build_candidate_realistic_backtest.py",
    "scripts/build_candidate_risk_state.py",
    "scripts/freeze_candidate_observation.py",
    "scripts/run_candidate_quality_daily_closure.py",
    "scripts/build_server_activation_plan.py",
    "scripts/build_server_deploy_evidence_bundle.py",
    "scripts/build_server_sync_file_list.py",
    "scripts/check_release_gates.py",
    "scripts/prepare_server_sync_manifest.py",
    "scripts/register_server_deploy.py",
    "scripts/run_airivo_scope_full_readiness.py",
    "scripts/run_server_activation_plan.py",
    "scripts/run_server_deploy_rollback.py",
    "scripts/run_server_post_deploy_verification.py",
    "scripts/run_server_sync_preflight.py",
    "deploy/aliyun/DEPLOY.md",
    "install_candidate_quality_daily_closure_launchd.py",
)

ReleaseGateRunner = Callable[..., tuple[int, dict[str, object]]]
ScopeReadinessRunner = Callable[..., tuple[int, dict[str, object]]]


def _build_check(name: str, passed: bool, details: dict[str, object]) -> dict[str, object]:
    return {
        "check": name,
        "passed": passed,
        "details": details,
    }


def _manifest_checks(manifest: dict[str, object]) -> list[dict[str, object]]:
    allowed_files = set(manifest["allowed_files"])
    missing_required_files = [
        path
        for path in REQUIRED_ALLOWED_FILES
        if path not in allowed_files
    ]
    return [
        _build_check(
            "manifest_classification",
            manifest["unclassified_total"] == 0,
            {
                "allowed_total": manifest["allowed_total"],
                "denied_total": manifest["denied_total"],
                "unclassified_total": manifest["unclassified_total"],
                "unclassified_files": manifest["unclassified_files"],
            },
        ),
        _build_check(
            "required_sync_files",
            not missing_required_files,
            {
                "required_files": list(REQUIRED_ALLOWED_FILES),
                "missing_required_files": missing_required_files,
            },
        ),
    ]


def _gate_check(
    release_gate_runner: ReleaseGateRunner,
) -> dict[str, object]:
    exit_code, payload = release_gate_runner(
        selected_gates=["scope_readiness"],
        capture_output=True,
    )
    return _build_check(
        "release_gate_scope_readiness",
        exit_code == 0 and payload.get("status") == "passed",
        {
            "exit_code": exit_code,
            "status": payload.get("status"),
            "failed_total": payload.get("failed_total"),
            "scope_registry": payload.get("scope_registry"),
        },
    )


def _full_readiness_check(
    scope_readiness_runner: ScopeReadinessRunner,
) -> dict[str, object]:
    exit_code, payload = scope_readiness_runner(capture_output=True)
    return _build_check(
        "scope_full_readiness",
        exit_code == 0 and payload.get("status") == "passed",
        {
            "exit_code": exit_code,
            "status": payload.get("status"),
            "scope_total": payload.get("scope_total"),
            "failed_total": payload.get("failed_total"),
        },
    )


def _skipped_full_readiness_check() -> dict[str, object]:
    return _build_check(
        "scope_full_readiness",
        True,
        {
            "skipped": True,
            "reason": "pass --full-readiness for nightly, staging, or release-before-sync checks",
        },
    )


def _build_sync_decision(checks: list[dict[str, object]]) -> dict[str, object]:
    blocking_checks = [
        str(check["check"])
        for check in checks
        if not bool(check.get("passed"))
    ]
    return {
        "allowed_to_sync": not blocking_checks,
        "decision": "sync_allowed" if not blocking_checks else "sync_blocked",
        "next_action": (
            "build server sync file list and stage rsync with the allowed file manifest"
            if not blocking_checks
            else "fix blocking preflight checks before building a file list or staging rsync"
        ),
        "blocking_checks": blocking_checks,
    }


def _write_output(path: str | Path | None, payload: dict[str, object]) -> None:
    if not path:
        return
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def run_server_sync_preflight(
    *,
    project_root: str | Path = PROJECT_ROOT,
    full_readiness: bool = False,
    output_path: str | Path | None = None,
    release_gate_runner: ReleaseGateRunner = run_release_gates,
    scope_readiness_runner: ScopeReadinessRunner = run_scope_full_readiness,
) -> tuple[int, dict[str, object]]:
    manifest = build_server_sync_manifest(project_root)
    checks = _manifest_checks(manifest)
    checks.append(_gate_check(release_gate_runner))
    checks.append(
        _full_readiness_check(scope_readiness_runner)
        if full_readiness
        else _skipped_full_readiness_check()
    )
    sync_decision = _build_sync_decision(checks)
    payload = {
        "preflight_version": PREFLIGHT_VERSION,
        "status": "passed" if sync_decision["allowed_to_sync"] else "failed",
        "project_root": manifest["project_root"],
        "sync_policy": manifest["sync_policy"],
        "sync_decision": sync_decision,
        "manifest_summary": {
            "allowed_total": manifest["allowed_total"],
            "denied_total": manifest["denied_total"],
            "unclassified_total": manifest["unclassified_total"],
        },
        "checks": checks,
        "allowed_files": manifest["allowed_files"],
    }
    _write_output(output_path, payload)
    return (0 if payload["status"] == "passed" else 1), payload


def main() -> int:
    parser = argparse.ArgumentParser(description="Run server sync preflight checks before staging rsync.")
    parser.add_argument("--project-root", default=str(PROJECT_ROOT))
    parser.add_argument("--full-readiness", action="store_true")
    parser.add_argument("--output")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    exit_code, payload = run_server_sync_preflight(
        project_root=args.project_root,
        full_readiness=args.full_readiness,
        output_path=args.output,
    )
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print(
            json.dumps(
                {
                    "status": payload["status"],
                    "preflight_version": payload["preflight_version"],
                    "manifest_summary": payload["manifest_summary"],
                    "checks": [
                        {
                            "check": check["check"],
                            "passed": check["passed"],
                        }
                        for check in payload["checks"]
                    ],
                },
                ensure_ascii=False,
                indent=2,
            )
        )
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
