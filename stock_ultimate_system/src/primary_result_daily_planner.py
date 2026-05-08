from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.utils.project_paths import resolve_artifacts_path, resolve_experiments_path, resolve_project_path


PRIMARY_RESULT_DAILY_PLANNER_VERSION = "primary_result_daily_planner.v1"


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"expected object json: {path}")
    return payload


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _owner_schedule(queue_summary: dict[str, Any]) -> list[dict[str, Any]]:
    workloads = queue_summary.get("open_owner_workloads")
    if not isinstance(workloads, dict):
        return []
    rows: list[dict[str, Any]] = []
    for owner, raw in workloads.items():
        payload = raw if isinstance(raw, dict) else {}
        rows.append(
            {
                "owner": str(owner),
                "open_total": int(payload.get("open_total") or 0),
                "critical_priority_total": int(payload.get("critical_priority_total") or 0),
                "high_priority_total": int(payload.get("high_priority_total") or 0),
            }
        )
    rows.sort(
        key=lambda item: (
            -int(item["critical_priority_total"]),
            -int(item["high_priority_total"]),
            -int(item["open_total"]),
            str(item["owner"]),
        )
    )
    return rows


def _benchmark_execution_batches(
    promotion_gate: dict[str, Any],
    benchmark_plan_current: dict[str, Any],
) -> list[dict[str, Any]]:
    if not benchmark_plan_current:
        return []
    return [
        {
            "plan_id": benchmark_plan_current.get("plan_id"),
            "review_id": benchmark_plan_current.get("review_id"),
            "execution_priority": benchmark_plan_current.get("execution_priority"),
            "execution_batch": benchmark_plan_current.get("execution_batch"),
            "promotion_decision": promotion_gate.get("decision"),
        }
    ]


def _candidate_iteration_schedule(candidate_quality_diff: dict[str, Any]) -> list[dict[str, Any]]:
    schedule = candidate_quality_diff.get("iteration_schedule")
    if not isinstance(schedule, list):
        return []
    return [dict(item) for item in schedule if isinstance(item, dict)]


def _candidate_quality_sample_density(candidate_quality_summary: dict[str, Any]) -> dict[str, dict[str, Any]]:
    payload = candidate_quality_summary.get("multiwindow_sample_density")
    if not isinstance(payload, dict):
        return {}
    result: dict[str, dict[str, Any]] = {}
    for window in ("60d", "120d"):
        raw = payload.get(window)
        if not isinstance(raw, dict):
            continue
        result[window] = {
            "sample_total": int(raw.get("sample_total") or 0),
            "minimum_required": int(raw.get("minimum_required") or 0),
            "status": str(raw.get("status") or "blocked"),
        }
    return result


def _candidate_quality_density_progress(candidate_quality_density_progress: dict[str, Any]) -> dict[str, dict[str, Any]]:
    payload = candidate_quality_density_progress.get("progress")
    if not isinstance(payload, dict):
        return {}
    result: dict[str, dict[str, Any]] = {}
    for window in ("60d", "120d"):
        raw = payload.get(window)
        if not isinstance(raw, dict):
            continue
        result[window] = {
            "sample_total": int(raw.get("sample_total") or 0),
            "minimum_required": int(raw.get("minimum_required") or 0),
            "remaining_samples_needed": int(raw.get("remaining_samples_needed") or 0),
            "progress_ratio": float(raw.get("progress_ratio") or 0.0),
            "status": str(raw.get("status") or "blocked"),
            "latest_validation_date": raw.get("latest_validation_date"),
            "earliest_validation_date": raw.get("earliest_validation_date"),
        }
    return result


def _density_alerts(candidate_quality_summary: dict[str, Any]) -> list[str]:
    density = _candidate_quality_sample_density(candidate_quality_summary)
    alerts: list[str] = []
    for window, payload in density.items():
        if str(payload.get("status") or "").strip().lower() != "passed":
            alerts.append(
                f"{window} sample density is insufficient: sample_total={int(payload.get('sample_total') or 0)} "
                f"minimum_required={int(payload.get('minimum_required') or 0)}"
            )
    return alerts


def _density_progress_actions(candidate_quality_density_progress: dict[str, Any]) -> list[str]:
    progress = _candidate_quality_density_progress(candidate_quality_density_progress)
    actions: list[str] = []
    for window in ("120d", "60d"):
        payload = progress.get(window) or {}
        if str(payload.get("status") or "").strip().lower() == "passed":
            continue
        remaining = int(payload.get("remaining_samples_needed") or 0)
        sample_total = int(payload.get("sample_total") or 0)
        minimum_required = int(payload.get("minimum_required") or 0)
        if remaining > 0:
            actions.append(
                f"{window} density progress requires {remaining} more formal validation samples: "
                f"sample_total={sample_total} minimum_required={minimum_required}"
            )
    return actions


def _daily_actions(
    scoreboard: dict[str, Any],
    candidate_quality_diff: dict[str, Any],
    candidate_quality_summary: dict[str, Any],
    candidate_quality_density_progress: dict[str, Any],
) -> list[str]:
    actions: list[str] = []
    for value in scoreboard.get("next_actions", []) or []:
        text = str(value).strip()
        if text and text not in actions:
            actions.append(text)
    for value in candidate_quality_diff.get("recommended_remediation_actions", []) or []:
        text = str(value).strip()
        if text and text not in actions:
            actions.append(text)
    for value in _density_alerts(candidate_quality_summary):
        text = str(value).strip()
        if text and text not in actions:
            actions.append(text)
    for value in _density_progress_actions(candidate_quality_density_progress):
        text = str(value).strip()
        if text and text not in actions:
            actions.append(text)
    return actions


def build_primary_result_daily_planner(
    *,
    artifacts_dir: str | Path = "artifacts",
    exp_dir: str | Path = "data/experiments",
    output_path: str | Path | None = None,
) -> tuple[int, dict[str, Any]]:
    resolved_artifacts_dir = resolve_artifacts_path(artifacts_dir)
    resolved_exp_dir = resolve_experiments_path(exp_dir)

    scoreboard = _read_json(resolved_artifacts_dir / "primary_result_daily_operations_scoreboard_latest.json")
    queue_summary = _read_json(resolved_artifacts_dir / "primary_result_feedback_review_queue" / "summary.json")
    promotion_gate = _read_json(resolved_artifacts_dir / "primary_result_promotion_readiness_gate_latest.json")
    candidate_quality_diff = _read_json(resolved_exp_dir / "candidate_quality_diff.json")
    candidate_quality_summary = _read_json(resolved_exp_dir / "candidate_quality_summary.json")
    candidate_quality_density_progress = _read_json(resolved_exp_dir / "candidate_quality_density_progress.json")
    benchmark_plan_current = _read_json(resolved_artifacts_dir / "primary_result_benchmark_plans" / "current.json")

    owner_schedule = _owner_schedule(queue_summary)
    benchmark_batches = _benchmark_execution_batches(promotion_gate, benchmark_plan_current)
    iteration_schedule = _candidate_iteration_schedule(candidate_quality_diff)
    sample_density = _candidate_quality_sample_density(candidate_quality_summary)
    density_progress = _candidate_quality_density_progress(candidate_quality_density_progress)
    next_actions = _daily_actions(
        scoreboard,
        candidate_quality_diff,
        candidate_quality_summary,
        candidate_quality_density_progress,
    )

    payload = {
        "planner_version": PRIMARY_RESULT_DAILY_PLANNER_VERSION,
        "generated_at": _utc_now_iso(),
        "status": "passed",
        "source_scope": "stock",
        "scoreboard_status": scoreboard.get("overall_status"),
        "promotion_decision": promotion_gate.get("decision"),
        "owner_workload_schedule": owner_schedule,
        "benchmark_execution_batches": benchmark_batches,
        "candidate_iteration_schedule": iteration_schedule,
        "candidate_quality_sample_density": sample_density,
        "candidate_quality_density_progress": density_progress,
        "next_actions": next_actions,
        "evidence_paths": {
            "daily_operations_scoreboard": str(resolved_artifacts_dir / "primary_result_daily_operations_scoreboard_latest.json"),
            "feedback_queue_summary": str(resolved_artifacts_dir / "primary_result_feedback_review_queue" / "summary.json"),
            "promotion_readiness_gate": str(resolved_artifacts_dir / "primary_result_promotion_readiness_gate_latest.json"),
            "candidate_quality_diff": str(resolved_exp_dir / "candidate_quality_diff.json"),
            "candidate_quality_summary": str(resolved_exp_dir / "candidate_quality_summary.json"),
            "candidate_quality_density_progress": str(resolved_exp_dir / "candidate_quality_density_progress.json"),
            "benchmark_plan_current": str(resolved_artifacts_dir / "primary_result_benchmark_plans" / "current.json"),
        },
        "production_boundary": (
            "daily planner only composes governed review, benchmark, and iteration priorities into one execution artifact; "
            "it does not trade, mutate strategy rules, promote baselines, or deploy"
        ),
    }
    if output_path:
        _write_json(resolve_project_path(output_path), payload)
    return 0, payload
