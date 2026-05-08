from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.utils.project_paths import resolve_artifacts_path, resolve_project_path


PRIMARY_RESULT_MORNING_OPERATIONS_BRIEF_VERSION = "primary_result_morning_operations_brief.v1"


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"expected object json: {path}")
    return payload


def _write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def build_primary_result_morning_operations_brief(
    *,
    planner_path: str | Path = "artifacts/primary_result_daily_planner_latest.json",
    output_path: str | Path | None = None,
) -> tuple[int, dict[str, Any]]:
    resolved_planner_path = resolve_artifacts_path(planner_path)
    planner = _read_json(resolved_planner_path)

    owner_schedule = planner.get("owner_workload_schedule") or []
    benchmark_batches = planner.get("benchmark_execution_batches") or []
    iteration_schedule = planner.get("candidate_iteration_schedule") or []
    density = planner.get("candidate_quality_sample_density") or {}
    density_progress = planner.get("candidate_quality_density_progress") or {}
    next_actions = planner.get("next_actions") or []

    lines = [
        "# /stock Morning Operations Brief",
        "",
        f"- Generated at: {planner.get('generated_at') or _utc_now_iso()}",
        f"- Scoreboard status: {planner.get('scoreboard_status') or 'unknown'}",
        f"- Promotion decision: {planner.get('promotion_decision') or 'unknown'}",
        "",
        "## Review Workload",
    ]
    if isinstance(owner_schedule, list) and owner_schedule:
        for row in owner_schedule:
            if not isinstance(row, dict):
                continue
            lines.append(
                f"- {row.get('owner')}: open={row.get('open_total', 0)}, "
                f"critical={row.get('critical_priority_total', 0)}, high={row.get('high_priority_total', 0)}"
            )
    else:
        lines.append("- No open review workload.")

    lines.extend(["", "## Benchmark Batches"])
    if isinstance(benchmark_batches, list) and benchmark_batches:
        for row in benchmark_batches:
            if not isinstance(row, dict):
                continue
            lines.append(
                f"- {row.get('execution_batch') or 'unknown'} / {row.get('execution_priority') or 'unknown'}: "
                f"plan={row.get('plan_id') or '-'} review={row.get('review_id') or '-'}"
            )
    else:
        lines.append("- No active benchmark batch.")

    lines.extend(["", "## Candidate Iteration Schedule"])
    if isinstance(iteration_schedule, list) and iteration_schedule:
        for row in iteration_schedule:
            if not isinstance(row, dict):
                continue
            lines.append(
                f"- #{row.get('sequence')} {row.get('priority_band') or 'unknown'} {row.get('failure_field') or '-'}: "
                f"{row.get('recommended_action') or '-'}"
            )
    else:
        lines.append("- No candidate iteration items.")

    lines.extend(["", "## Sample Density"])
    if isinstance(density, dict) and density:
        for window in ("60d", "120d"):
            row = density.get(window)
            if not isinstance(row, dict):
                continue
            lines.append(
                f"- {window}: status={row.get('status') or 'unknown'}, "
                f"sample_total={row.get('sample_total', 0)}, minimum_required={row.get('minimum_required', 0)}"
            )
    else:
        lines.append("- No sample density evidence.")

    lines.extend(["", "## Density Progress"])
    if isinstance(density_progress, dict) and density_progress:
        for window in ("60d", "120d"):
            row = density_progress.get(window)
            if not isinstance(row, dict):
                continue
            lines.append(
                f"- {window}: remaining={row.get('remaining_samples_needed', 0)}, "
                f"progress_ratio={row.get('progress_ratio', 0.0)}, "
                f"latest_validation_date={row.get('latest_validation_date') or '-'}"
            )
    else:
        lines.append("- No density progress evidence.")

    lines.extend(["", "## Next Actions"])
    if isinstance(next_actions, list) and next_actions:
        for action in next_actions:
            lines.append(f"- {action}")
    else:
        lines.append("- No next actions.")

    markdown = "\n".join(lines) + "\n"
    resolved_output_path = resolve_project_path(output_path) if output_path is not None else None
    if resolved_output_path is not None:
        _write_text(resolved_output_path, markdown)
    payload = {
        "brief_version": PRIMARY_RESULT_MORNING_OPERATIONS_BRIEF_VERSION,
        "generated_at": _utc_now_iso(),
        "status": "passed",
        "planner_path": str(resolved_planner_path),
        "output_path": None if resolved_output_path is None else str(resolved_output_path),
        "line_total": len(lines),
    }
    return 0, payload
