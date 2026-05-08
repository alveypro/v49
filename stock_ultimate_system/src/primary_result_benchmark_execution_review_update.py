from __future__ import annotations

import json
from pathlib import Path

from src.artifact_registry import sha256_file
from src.primary_result_benchmark_plan_execution import PRIMARY_RESULT_BENCHMARK_PLAN_EXECUTION_VERSION
from src.primary_result_feedback_review_queue import PrimaryResultFeedbackReviewQueue
from src.utils.project_paths import resolve_project_path


def _read_json(path: Path) -> dict[str, object]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"expected object json: {path}")
    return payload


def apply_benchmark_execution_to_review_queue(
    *,
    execution_json_path: str | Path,
    queue_dir: str | Path = "artifacts/primary_result_feedback_review_queue",
    actor: str = "benchmark_executor",
) -> dict[str, object]:
    resolved_execution_path = resolve_project_path(execution_json_path)
    if not resolved_execution_path.exists():
        raise FileNotFoundError(f"benchmark plan execution evidence missing: {resolved_execution_path}")
    execution = _read_json(resolved_execution_path)
    if execution.get("execution_version") != PRIMARY_RESULT_BENCHMARK_PLAN_EXECUTION_VERSION:
        raise ValueError("benchmark plan execution evidence version is invalid")
    if execution.get("do_not_auto_apply") is not True:
        raise ValueError("benchmark plan execution evidence must keep do_not_auto_apply=true")
    review_id = str(execution.get("review_id", "") or "").strip()
    if not review_id:
        raise ValueError("benchmark plan execution evidence missing review_id")
    status = str(execution.get("status", "") or "").strip().lower()
    if status not in {"passed", "failed"}:
        raise ValueError("benchmark plan execution evidence status must be passed or failed")

    queue = PrimaryResultFeedbackReviewQueue(queue_dir=queue_dir)
    current_item = queue.get_item(review_id)
    if current_item.get("status") != "needs_benchmark":
        raise ValueError("benchmark execution can only update review items in needs_benchmark status")

    decision_status = "accepted" if status == "passed" else "rejected"
    reason = (
        f"benchmark plan execution {status}: plan_id={execution.get('plan_id')}, "
        f"exit_code={execution.get('exit_code')}"
    )
    item = queue.decide(
        review_id=review_id,
        status=decision_status,
        reason=reason,
        actor=actor,
        evidence_payload={
            "source_execution_path": str(resolved_execution_path),
            "source_execution_hash": sha256_file(resolved_execution_path),
            "benchmark_execution_status": status,
            "benchmark_execution_exit_code": execution.get("exit_code"),
            "plan_id": execution.get("plan_id"),
            "required_test_total": execution.get("required_test_total"),
        },
    )
    return {
        "status": "updated",
        "review_id": review_id,
        "item_status": item["status"],
        "review_priority": item.get("review_priority"),
        "benchmark_execution_status": status,
        "execution_priority": (
            "expedite" if str(item.get("review_priority") or "").strip().lower() in {"critical", "high"} else "normal"
        ),
        "execution_batch": str(execution.get("execution_batch") or "").strip() or None,
        "source_execution_hash": sha256_file(resolved_execution_path),
        "decision_history_path": str(queue.decisions_path),
        "summary_path": str(queue.summary_path),
        "do_not_auto_apply": item["do_not_auto_apply"],
    }
