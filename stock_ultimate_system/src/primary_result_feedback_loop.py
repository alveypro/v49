from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.primary_result_failure_attribution import build_primary_result_failure_attribution_from_paths
from src.primary_result_failure_attribution_ledger import PrimaryResultFailureAttributionLedger
from src.primary_result_feedback_review_queue import PrimaryResultFeedbackReviewQueue
from src.primary_result_learning_feedback import build_primary_result_learning_feedback
from src.utils.project_paths import resolve_project_path


PRIMARY_RESULT_FEEDBACK_LOOP_VERSION = "primary_result_feedback_loop.v1"
CLOSED_OBSERVATION_STATUSES = {"completed", "failed"}


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"json artifact missing: {path}")
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"expected object json: {path}")
    return payload


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _normalize_text(value: object) -> str:
    return str(value or "").strip()


def _stage(name: str, status: str, payload: dict[str, Any]) -> dict[str, Any]:
    return {"name": name, "status": status, "payload": payload}


def _safe_review_id(value: object) -> str:
    text = _normalize_text(value)
    allowed = []
    for ch in text.lower():
        if ch.isalnum():
            allowed.append(ch)
        elif ch in {".", ":", "-", "_"}:
            allowed.append("-")
        else:
            allowed.append("-")
    normalized = "".join(allowed).strip("-")
    while "--" in normalized:
        normalized = normalized.replace("--", "-")
    return normalized or "unknown"


def _write_stage_output(path: Path, payload: dict[str, Any]) -> dict[str, Any]:
    _write_json(path, payload)
    return {"path": str(path)}


def run_primary_result_feedback_loop(
    *,
    observation_path: str | Path = "data/experiments/primary_result_observation_latest.json",
    terminal_path: str | Path | None = "data/experiments/primary_result_terminal_latest.json",
    ledger_jsonl_path: str | Path = "artifacts/primary_result_performance/ledger.jsonl",
    attribution_output_path: str | Path = "data/experiments/primary_result_failure_attribution_latest.json",
    attribution_ledger_jsonl_path: str | Path = "artifacts/primary_result_failure_attribution/ledger.jsonl",
    attribution_summary_path: str | Path = "artifacts/primary_result_failure_attribution/summary.json",
    feedback_output_path: str | Path = "data/experiments/primary_result_learning_feedback_latest.json",
    queue_dir: str | Path = "artifacts/primary_result_feedback_review_queue",
    owner: str = "system",
    output_path: str | Path | None = None,
    min_success_return: float = 0.0,
    min_excess_return: float = 0.0,
    max_drawdown_floor: float = -0.08,
) -> tuple[int, dict[str, Any]]:
    resolved_observation_path = resolve_project_path(observation_path)
    resolved_terminal_path = resolve_project_path(terminal_path) if terminal_path else None
    resolved_ledger_path = resolve_project_path(ledger_jsonl_path)
    resolved_attribution_output = resolve_project_path(attribution_output_path)
    resolved_attribution_ledger_path = resolve_project_path(attribution_ledger_jsonl_path)
    resolved_attribution_summary_path = resolve_project_path(attribution_summary_path)
    resolved_feedback_output = resolve_project_path(feedback_output_path)

    observation = _read_json(resolved_observation_path)
    observation_status = _normalize_text(observation.get("observation_status")).lower()
    stages: list[dict[str, Any]] = []
    terminal_payload: dict[str, Any] | None = None
    if resolved_terminal_path and resolved_terminal_path.exists():
        terminal_payload = _read_json(resolved_terminal_path)

    if observation_status not in CLOSED_OBSERVATION_STATUSES:
        payload = _build_payload(
            status="skipped",
            reason="observation is not closed",
            stages=stages,
            observation_status=observation_status,
            terminal_outcome=_normalize_text((terminal_payload or {}).get("terminal_outcome")) or None,
        )
        if output_path:
            _write_json(resolve_project_path(output_path), payload)
        return 0, payload

    attribution = build_primary_result_failure_attribution_from_paths(
        observation_path=resolved_observation_path,
        ledger_jsonl_path=resolved_ledger_path,
        min_success_return=min_success_return,
        min_excess_return=min_excess_return,
        max_drawdown_floor=max_drawdown_floor,
    )
    stages.append(_stage("failure_attribution", "written", {**_write_stage_output(resolved_attribution_output, attribution), "attribution_required": attribution["attribution_required"]}))
    attribution_ledger = PrimaryResultFailureAttributionLedger(
        ledger_path=resolved_attribution_ledger_path,
        summary_path=resolved_attribution_summary_path,
    )
    attribution_ledger_entry = attribution_ledger.append_attribution(attribution_path=resolved_attribution_output)
    stages.append(
        _stage(
            "failure_attribution_ledger",
            "written",
            {
                "ledger_path": str(resolved_attribution_ledger_path),
                "summary_path": str(resolved_attribution_summary_path),
                "result_id": attribution_ledger_entry.get("result_id"),
                "primary_failure_category": attribution_ledger_entry.get("primary_failure_category"),
            },
        )
    )

    feedback = build_primary_result_learning_feedback(
        attribution,
        source_attribution_path=resolved_attribution_output,
    )
    stages.append(_stage("learning_feedback", "written", {**_write_stage_output(resolved_feedback_output, feedback), "change_total": feedback["change_total"]}))

    review_item: dict[str, Any] | None = None
    queue_status = "not_required"
    queue_error: str | None = None
    if feedback.get("attribution_required") is True and int(feedback.get("change_total") or 0) > 0:
        queue = PrimaryResultFeedbackReviewQueue(queue_dir=queue_dir)
        review_id = (
            "primary-feedback-"
            f"{_safe_review_id(feedback.get('ts_code'))}-"
            f"{_safe_review_id(feedback.get('primary_failure_category') or feedback.get('outcome'))}-"
            f"{_safe_review_id(feedback.get('result_id'))}"
        )
        try:
            review_item = queue.enqueue(feedback_path=resolved_feedback_output, review_id=review_id, owner=owner)
            queue_status = "enqueued"
        except FileExistsError as exc:
            review_item = queue.get_item(review_id)
            queue_status = "already_enqueued"
            queue_error = str(exc)
        stages.append(
            _stage(
                "feedback_review_queue",
                queue_status,
                {
                    "review_id": review_item.get("review_id") if review_item else review_id,
                    "item_status": review_item.get("status") if review_item else None,
                    "queue_dir": str(resolve_project_path(queue_dir)),
                    "error": queue_error,
                },
            )
        )
    else:
        stages.append(
            _stage(
                "feedback_review_queue",
                "not_required",
                {"reason": "attribution did not produce governed changes requiring review"},
            )
        )

    payload = _build_payload(
        status="completed",
        reason="closed observation processed through governed feedback loop",
        stages=stages,
        observation_status=observation_status,
        terminal_outcome=_normalize_text((terminal_payload or {}).get("terminal_outcome")) or None,
        attribution_required=bool(attribution.get("attribution_required")),
        change_total=int(feedback.get("change_total") or 0),
        queue_status=queue_status,
        review_id=review_item.get("review_id") if review_item else None,
    )
    if output_path:
        _write_json(resolve_project_path(output_path), payload)
    return 0, payload


def _build_payload(
    *,
    status: str,
    reason: str,
    stages: list[dict[str, Any]],
    observation_status: str,
    terminal_outcome: str | None,
    attribution_required: bool | None = None,
    change_total: int | None = None,
    queue_status: str | None = None,
    review_id: str | None = None,
) -> dict[str, Any]:
    return {
        "feedback_loop_version": PRIMARY_RESULT_FEEDBACK_LOOP_VERSION,
        "generated_at": _utc_now_iso(),
        "status": status,
        "reason": reason,
        "observation_status": observation_status,
        "terminal_outcome": terminal_outcome,
        "attribution_required": attribution_required,
        "change_total": change_total,
        "queue_status": queue_status,
        "review_id": review_id,
        "stages": stages,
        "production_boundary": (
            "feedback loop only turns closed observation evidence into attribution, governed feedback, and review queue items; "
            "it never mutates strategy rules, promotes baselines, trades, or deploys changes automatically"
        ),
    }
