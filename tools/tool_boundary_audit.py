#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
import time
from typing import Any


MANIFEST_VERSION = "airivo_tool_boundary_audit.v1"

STABLE_ENTRYPOINTS = {
    "run_daily_v9_evidence_pipeline.py",
    "record_top5_execution_observation.py",
    "check_top5_execution_observation_completeness.py",
    "import_top5_execution_observation_updates.py",
    "close_top5_execution_day.py",
    "build_top5_execution_evidence_summary.py",
    "check_top5_execution_ops_sla.py",
    "evaluate_v9_canary_promotion_readiness.py",
    "build_top5_execution_court_record.py",
    "rebuild_top5_trader_brief_exports.py",
    "evaluate_top5_forward_returns.py",
    "top5_audit_evidence_gate.py",
    "run_top5_competition_audit_then_gate.sh",
    "tool_boundary_audit.py",
}

ARCHIVE_PREFIXES = (
    "build_strategy_competition_",
    "review_strategy_competition_",
    "adjudicate_strategy_competition_",
    "reconcile_strategy_competition_",
    "ensemble_alpha_",
    "ensemble_rebuilt_",
    "archive_failed_",
    "predeclare_",
    "backfill_",
)

DAILY_SUPPORT_PREFIXES = (
    "top5_",
    "run_top5_",
    "gen_top5_",
    "install_top5_",
    "check_top5_",
)

CURRENT_TOP5_SUPPORT_ENTRYPOINTS = {
    "build_current_strategy_competition_audit.py",
}

DEPLOYMENT_SUPPORT_ENTRYPOINTS = {
    "auth_decision_alert.py",
    "converge_airivo_single_root.sh",
    "cosign_verify_top5_manifest.sh",
    "deploy_auth_to_release.sh",
    "deploy_auth_to_server.sh",
    "install_auth_alert_timer.sh",
}

RESEARCH_ARCHIVE_ENTRYPOINTS = {
    "all_strategy_evidence_run.py",
    "compare_hard_event_alpha_v5_v6_failures.py",
    "create_hard_event_alpha_v5_watch_oos_plan.py",
    "ensemble_allocator_throttle_attribution.py",
    "ensemble_observation_gate.py",
    "ensemble_observation_monitor.py",
    "ensemble_observation_promotion_apply.py",
    "ensemble_observation_promotion_decision.py",
    "ensemble_risk_off_alpha_repair_review.py",
    "promotion_decision_artifact.py",
    "rejected_backtest_artifacts.py",
    "research_repair_iteration_flow.py",
    "run_hard_event_alpha_v5_oos_monitoring.py",
    "stable_execution_evidence_fixture.py",
    "strategy_optimization_stage_audit.py",
    "v8_controlled_experiment.py",
}

STRATEGY_COMPETITION_ARCHIVE_ENTRYPOINTS = {
    "build_benchmark_industry_contract.py",
    "check_strategy_competition_broker_submission_guard.py",
    "check_strategy_competition_live_order_authority.py",
    "record_strategy_competition_shadow_feedback.py",
    "strategy_competition_portfolio_audit.py",
}

MAINTENANCE_ARCHIVE_ENTRYPOINTS = {
    "assemble_top5_rebuild_service.py",
    "extract_top5_rebuild_service.py",
    "openclaw_cleanup_audit.py",
    "run_top5_competition_audit.sh",
    "top5_audit_evidence_gate.sh",
}

GOVERNANCE_ARCHIVE_ENTRYPOINTS = {
    "run_governance_gate_ci.sh",
}


def _now_text() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S")


def classify_tool_name(name: str) -> tuple[str, str]:
    if name in STABLE_ENTRYPOINTS:
        return "stable", "declared_stable_entrypoint"
    if name in CURRENT_TOP5_SUPPORT_ENTRYPOINTS:
        return "support_review", "current_top5_support_entrypoint"
    if name in DEPLOYMENT_SUPPORT_ENTRYPOINTS:
        return "support_review", "deployment_or_auth_support_entrypoint"
    if name in RESEARCH_ARCHIVE_ENTRYPOINTS:
        return "archive_candidate", "research_or_shadow_experiment_not_daily_operator_surface"
    if name in STRATEGY_COMPETITION_ARCHIVE_ENTRYPOINTS:
        return "archive_candidate", "strategy_competition_shadow_or_transition_flow"
    if name in MAINTENANCE_ARCHIVE_ENTRYPOINTS:
        return "archive_candidate", "one_off_maintenance_or_extractor"
    if name in GOVERNANCE_ARCHIVE_ENTRYPOINTS:
        return "archive_candidate", "legacy_governance_gate_wrapper"
    if name.endswith((".py", ".sh")) and name.startswith(ARCHIVE_PREFIXES):
        return "archive_candidate", "one_off_strategy_competition_or_research_flow"
    if name.endswith((".py", ".sh")) and name.startswith(DAILY_SUPPORT_PREFIXES):
        return "support_review", "top5_support_or_scheduler_helper"
    if name.endswith((".py", ".sh")):
        return "manual_review", "not_in_stable_entrypoint_allowlist"
    return "ignore", "non_executable_tool_file"


def build_tool_boundary_manifest(tools_dir: Path) -> dict[str, Any]:
    tools_dir = tools_dir.resolve()
    entries: list[dict[str, Any]] = []
    for path in sorted(tools_dir.iterdir()):
        if not path.is_file():
            continue
        classification, reason = classify_tool_name(path.name)
        if classification == "ignore":
            continue
        entries.append(
            {
                "path": str(path),
                "relative_path": path.name,
                "classification": classification,
                "reason": reason,
                "size_bytes": path.stat().st_size,
            }
        )
    summary: dict[str, int] = {}
    for entry in entries:
        key = str(entry["classification"])
        summary[key] = summary.get(key, 0) + 1
    missing_stable = sorted(name for name in STABLE_ENTRYPOINTS if not (tools_dir / name).exists())
    archive_dir = tools_dir / "archive" / "strategy_competition"
    archived_entrypoints = sorted(path.name for path in archive_dir.glob("*.py")) if archive_dir.exists() else []
    archive_roots = {
        "strategy_competition": tools_dir / "archive" / "strategy_competition",
        "research": tools_dir / "archive" / "research",
        "maintenance": tools_dir / "archive" / "maintenance",
        "governance": tools_dir / "archive" / "governance",
    }
    archive_counts = {}
    archive_executable_counts = {}
    for key, path in archive_roots.items():
        archive_counts[key] = len(list(path.glob("*"))) if path.exists() else 0
        archive_executable_counts[key] = (
            len([item for item in path.glob("*") if item.is_file() and item.suffix in {".py", ".sh"}])
            if path.exists()
            else 0
        )
    return {
        "artifact_version": MANIFEST_VERSION,
        "created_at": _now_text(),
        "tools_dir": str(tools_dir),
        "policy": "tools/README.md",
        "summary": summary,
        "archive_summary": {
            "archive_counts": archive_counts,
            "archive_executable_counts": archive_executable_counts,
            "strategy_competition_archived_count": len(archived_entrypoints),
            "strategy_competition_archive_dir": str(archive_dir),
        },
        "missing_stable_entrypoints": missing_stable,
        "archived_entrypoints": archived_entrypoints,
        "archive_prefixes": list(ARCHIVE_PREFIXES),
        "stable_entrypoints": sorted(STABLE_ENTRYPOINTS),
        "entries": entries,
        "hard_boundaries": [
            "audit_only_no_file_moves",
            "archive_candidate_requires_reference_check",
            "stable_entrypoints_are_operator_surface",
        ],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Classify tools against the Airivo runtime boundary map.")
    parser.add_argument("--tools-dir", default="tools")
    parser.add_argument("--output", default="")
    parser.add_argument("--fail-on-archive-candidates", action="store_true")
    parser.add_argument("--max-manual-review", type=int, default=-1)
    parser.add_argument("--max-support-review", type=int, default=-1)
    args = parser.parse_args()

    manifest = build_tool_boundary_manifest(Path(args.tools_dir))
    text = json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    if args.output:
        out = Path(args.output)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(text, encoding="utf-8")
    print(text, end="")
    failures: list[str] = []
    if manifest.get("missing_stable_entrypoints"):
        failures.append("stable_entrypoints_missing")
    if args.fail_on_archive_candidates and int(manifest["summary"].get("archive_candidate", 0) or 0) > 0:
        failures.append("archive_candidates_present")
    if int(args.max_manual_review) >= 0 and int(manifest["summary"].get("manual_review", 0) or 0) > int(args.max_manual_review):
        failures.append("manual_review_budget_exceeded")
    if int(args.max_support_review) >= 0 and int(manifest["summary"].get("support_review", 0) or 0) > int(args.max_support_review):
        failures.append("support_review_budget_exceeded")
    if failures:
        print(json.dumps({"tool_boundary_failures": failures}, ensure_ascii=False, sort_keys=True))
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
