#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.primary_result_candidate_handoff_gate import build_primary_result_candidate_handoff_gate
from src.primary_result_competitive_gap_assessment import build_primary_result_competitive_gap_assessment
from src.primary_result_daily_operations_scoreboard import build_primary_result_daily_operations_scoreboard
from src.primary_result_daily_planner import build_primary_result_daily_planner
from src.primary_result_morning_operations_brief import build_primary_result_morning_operations_brief
from src.candidate_quality_density_progress import (
    build_candidate_quality_density_progress,
    write_candidate_quality_density_progress_artifact,
)
from src.candidate_quality_validation_history_archive import build_candidate_quality_validation_history_archive
from src.primary_result_performance_evidence import build_primary_result_performance_evidence
from src.primary_result_promotion_readiness_gate import build_primary_result_promotion_readiness_gate
from src.utils.project_paths import resolve_project_path


PRIMARY_RESULT_OPERATIONS_REFRESH_VERSION = "primary_result_operations_refresh.v1"


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _write_json(path: str | Path, payload: dict[str, Any]) -> None:
    resolved = resolve_project_path(path)
    resolved.parent.mkdir(parents=True, exist_ok=True)
    resolved.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _run_stage(name: str, fn: Callable[[], tuple[int, dict[str, Any]]]) -> dict[str, Any]:
    try:
        exit_code, payload = fn()
        return {
            "name": name,
            "status": payload.get("status") or payload.get("overall_status") or "completed",
            "decision": payload.get("decision"),
            "exit_code": int(exit_code),
            "generated_at": payload.get("generated_at"),
            "blocking_reasons": payload.get("blocking_reasons", []),
            "next_actions": payload.get("next_actions", []),
        }
    except Exception as exc:
        return {
            "name": name,
            "status": "failed",
            "decision": None,
            "exit_code": 1,
            "error": str(exc),
            "blocking_reasons": [str(exc)],
            "next_actions": ["fix the failed refresh stage before using operations summary as current evidence"],
        }


def _candidate_quality_density_progress_stage(exp_dir: Path) -> tuple[int, dict[str, Any]]:
    payload = build_candidate_quality_density_progress(exp_dir=exp_dir)
    output_path = write_candidate_quality_density_progress_artifact(payload, output_dir=exp_dir)
    return 0, {**payload, "output_path": output_path}


def _candidate_quality_validation_history_archive_stage(exp_dir: Path) -> tuple[int, dict[str, Any]]:
    return build_candidate_quality_validation_history_archive(exp_dir=exp_dir)


def refresh_primary_result_operations_artifacts(
    *,
    exp_dir: str | Path = "data/experiments",
    artifacts_dir: str | Path = "artifacts",
    output_path: str | Path = "artifacts/primary_result_operations_refresh_latest.json",
) -> tuple[int, dict[str, Any]]:
    resolved_artifacts_dir = resolve_project_path(artifacts_dir)
    resolved_exp_dir = resolve_project_path(exp_dir)

    stages = [
        _run_stage(
            "candidate_handoff_gate",
            lambda: build_primary_result_candidate_handoff_gate(
                exp_dir=resolved_exp_dir,
                lifecycles_dir=resolved_artifacts_dir / "primary_result_lifecycle",
                output_path=resolved_artifacts_dir / "primary_result_candidate_handoff_gate_latest.json",
            ),
        ),
        _run_stage(
            "performance_evidence",
            lambda: build_primary_result_performance_evidence(
                primary_ledger_jsonl=resolved_artifacts_dir / "primary_result_performance" / "ledger.jsonl",
                basket_ledger_jsonl=resolved_artifacts_dir / "primary_result_candidate_baskets" / "performance_ledger.jsonl",
                output_path=resolved_artifacts_dir / "primary_result_performance_evidence_latest.json",
            ),
        ),
        _run_stage(
            "promotion_readiness_gate",
            lambda: build_primary_result_promotion_readiness_gate(
                performance_evidence_path=resolved_artifacts_dir / "primary_result_performance_evidence_latest.json",
                feedback_queue_summary_path=resolved_artifacts_dir / "primary_result_feedback_review_queue" / "summary.json",
                baseline_current_path=resolved_artifacts_dir / "baselines" / "current.json",
                output_path=resolved_artifacts_dir / "primary_result_promotion_readiness_gate_latest.json",
            ),
        ),
        _run_stage(
            "daily_operations_scoreboard_pre_gap",
            lambda: build_primary_result_daily_operations_scoreboard(
                exp_dir=resolved_exp_dir,
                artifacts_dir=resolved_artifacts_dir,
                output_path=resolved_artifacts_dir / "primary_result_daily_operations_scoreboard_latest.json",
            ),
        ),
        _run_stage(
            "competitive_gap_assessment",
            lambda: build_primary_result_competitive_gap_assessment(
                exp_dir=resolved_exp_dir,
                artifacts_dir=resolved_artifacts_dir,
                output_path=resolved_artifacts_dir / "primary_result_competitive_gap_assessment_latest.json",
            ),
        ),
        _run_stage(
            "daily_operations_scoreboard_final",
            lambda: build_primary_result_daily_operations_scoreboard(
                exp_dir=resolved_exp_dir,
                artifacts_dir=resolved_artifacts_dir,
                output_path=resolved_artifacts_dir / "primary_result_daily_operations_scoreboard_latest.json",
            ),
        ),
        _run_stage(
            "candidate_quality_validation_history_archive",
            lambda: _candidate_quality_validation_history_archive_stage(resolved_exp_dir),
        ),
        _run_stage(
            "candidate_quality_density_progress",
            lambda: _candidate_quality_density_progress_stage(resolved_exp_dir),
        ),
        _run_stage(
            "daily_planner",
            lambda: build_primary_result_daily_planner(
                exp_dir=resolved_exp_dir,
                artifacts_dir=resolved_artifacts_dir,
                output_path=resolved_artifacts_dir / "primary_result_daily_planner_latest.json",
            ),
        ),
        _run_stage(
            "morning_operations_brief",
            lambda: build_primary_result_morning_operations_brief(
                planner_path=resolved_artifacts_dir / "primary_result_daily_planner_latest.json",
                output_path=resolved_artifacts_dir / "primary_result_morning_operations_brief_latest.md",
            ),
        ),
    ]

    failed_stages = [stage for stage in stages if int(stage.get("exit_code", 1)) != 0 and stage.get("status") == "failed"]
    blocking_reasons: list[str] = []
    next_actions: list[str] = []
    for stage in stages:
        for reason in stage.get("blocking_reasons", []) or []:
            text = str(reason).strip()
            if text and text not in blocking_reasons:
                blocking_reasons.append(text)
        for action in stage.get("next_actions", []) or []:
            text = str(action).strip()
            if text and text not in next_actions:
                next_actions.append(text)

    payload: dict[str, Any] = {
        "refresh_version": PRIMARY_RESULT_OPERATIONS_REFRESH_VERSION,
        "generated_at": _utc_now_iso(),
        "status": "failed" if failed_stages else "completed",
        "stages": stages,
        "failed_stages": [stage["name"] for stage in failed_stages],
        "blocking_reasons": blocking_reasons,
        "next_actions": next_actions,
        "artifacts": {
            "candidate_handoff_gate": str(resolved_artifacts_dir / "primary_result_candidate_handoff_gate_latest.json"),
            "performance_evidence": str(resolved_artifacts_dir / "primary_result_performance_evidence_latest.json"),
            "promotion_readiness_gate": str(resolved_artifacts_dir / "primary_result_promotion_readiness_gate_latest.json"),
            "competitive_gap_assessment": str(resolved_artifacts_dir / "primary_result_competitive_gap_assessment_latest.json"),
            "daily_operations_scoreboard": str(resolved_artifacts_dir / "primary_result_daily_operations_scoreboard_latest.json"),
            "candidate_quality_density_progress": str(resolved_exp_dir / "candidate_quality_density_progress.json"),
            "daily_planner": str(resolved_artifacts_dir / "primary_result_daily_planner_latest.json"),
            "morning_operations_brief": str(resolved_artifacts_dir / "primary_result_morning_operations_brief_latest.md"),
        },
        "production_boundary": (
            "operations artifact refresh only rebuilds local evidence summaries in dependency order; "
            "it does not trade, run candidate handoff, promote baselines, or deploy"
        ),
    }
    _write_json(output_path, payload)
    return (1 if failed_stages else 0), payload


def main() -> int:
    parser = argparse.ArgumentParser(description="Refresh /stock operations artifacts in dependency order.")
    parser.add_argument("--exp-dir", default="data/experiments")
    parser.add_argument("--artifacts-dir", default="artifacts")
    parser.add_argument("--output", default="artifacts/primary_result_operations_refresh_latest.json")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()
    exit_code, payload = refresh_primary_result_operations_artifacts(
        exp_dir=args.exp_dir,
        artifacts_dir=args.artifacts_dir,
        output_path=args.output,
    )
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print(
            json.dumps(
                {
                    "status": payload["status"],
                    "failed_stages": payload["failed_stages"],
                    "blocking_reasons": payload["blocking_reasons"],
                    "next_actions": payload["next_actions"],
                },
                ensure_ascii=False,
                indent=2,
            )
        )
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
