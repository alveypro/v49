from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path

from src.artifact_registry import sha256_file
from src.utils.project_paths import resolve_project_path


PRIMARY_RESULT_FEEDBACK_REVIEW_QUEUE_VERSION = "primary_result_feedback_review_queue.v1"
PRIMARY_RESULT_FEEDBACK_REVIEW_EVENT_VERSION = "primary_result_feedback_review_event.v1"
SUPPORTED_FEEDBACK_VERSION = "primary_result_learning_feedback.v1"
REVIEW_STATUSES = {"open", "accepted", "rejected", "needs_benchmark", "closed"}
DECISION_STATUSES = {"accepted", "rejected", "needs_benchmark", "closed"}
REVIEW_PRIORITIES = {"critical", "high", "medium", "low", "none"}
PRIORITY_ORDER = {"critical": 0, "high": 1, "medium": 2, "low": 3, "none": 4}


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _read_json(path: Path) -> dict[str, object]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"expected object json: {path}")
    return payload


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _append_jsonl(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=False, sort_keys=True) + "\n")


def _normalize_text(value: object) -> str:
    return str(value or "").strip()


def _safe_id_part(value: object) -> str:
    text = _normalize_text(value).lower()
    text = re.sub(r"[^a-z0-9]+", "-", text)
    return text.strip("-") or "unknown"


def _normalize_review_id(value: str) -> str:
    review_id = _normalize_text(value)
    if not review_id:
        raise ValueError("review_id must not be empty")
    allowed = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_")
    if any(ch not in allowed for ch in review_id):
        raise ValueError("review_id must contain only letters, numbers, '-' or '_'")
    return review_id


def _default_review_id(feedback: dict[str, object]) -> str:
    return _normalize_review_id(
        "primary-feedback-"
        f"{_safe_id_part(feedback.get('ts_code'))}-"
        f"{_safe_id_part(feedback.get('primary_failure_category') or feedback.get('outcome'))}-"
        f"{_safe_id_part(feedback.get('generated_at'))}"
    )


def _load_jsonl(path: Path) -> list[dict[str, object]]:
    if not path.exists():
        return []
    entries: list[dict[str, object]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        payload = json.loads(line)
        if not isinstance(payload, dict):
            raise ValueError(f"expected object jsonl entry: {path}")
        entries.append(payload)
    return entries


class PrimaryResultFeedbackReviewQueue:
    def __init__(self, *, queue_dir: str | Path = "artifacts/primary_result_feedback_review_queue") -> None:
        self.queue_dir = resolve_project_path(queue_dir)
        self.items_dir = self.queue_dir / "items"
        self.decisions_path = self.queue_dir / "decision_history.jsonl"
        self.summary_path = self.queue_dir / "summary.json"
        self.ensure_layout()

    def ensure_layout(self) -> None:
        self.items_dir.mkdir(parents=True, exist_ok=True)
        if not self.summary_path.exists():
            self._write_summary()

    def list_items(self) -> list[dict[str, object]]:
        items: list[dict[str, object]] = []
        for path in sorted(self.items_dir.glob("*.json")):
            items.append(_read_json(path))
        items.sort(
            key=lambda item: (
                PRIORITY_ORDER.get(str(item.get("review_priority", "none")), PRIORITY_ORDER["none"]),
                str(item.get("created_at", "")),
                str(item.get("review_id", "")),
            )
        )
        return items

    def list_decision_history(self) -> list[dict[str, object]]:
        return _load_jsonl(self.decisions_path)

    def get_item(self, review_id: str) -> dict[str, object]:
        resolved_review_id = _normalize_review_id(review_id)
        path = self.items_dir / f"{resolved_review_id}.json"
        if not path.exists():
            raise FileNotFoundError(f"feedback review item not found: {path}")
        return _read_json(path)

    def enqueue(
        self,
        *,
        feedback_path: str | Path,
        review_id: str | None = None,
        owner: str = "unassigned",
        created_at: str | None = None,
    ) -> dict[str, object]:
        resolved_feedback_path = resolve_project_path(feedback_path)
        feedback = self._validate_feedback(resolved_feedback_path)
        resolved_review_id = _normalize_review_id(review_id or _default_review_id(feedback))
        item_path = self.items_dir / f"{resolved_review_id}.json"
        if item_path.exists():
            raise FileExistsError(f"feedback review item already exists: {item_path}")
        item = {
            "queue_version": PRIMARY_RESULT_FEEDBACK_REVIEW_QUEUE_VERSION,
            "review_id": resolved_review_id,
            "status": "open",
            "owner": _normalize_text(owner) or "unassigned",
            "created_at": created_at or _utc_now_iso(),
            "updated_at": created_at or _utc_now_iso(),
            "result_id": feedback.get("result_id"),
            "ts_code": feedback.get("ts_code"),
            "stock_name": feedback.get("stock_name"),
            "primary_failure_category": feedback.get("primary_failure_category"),
            "change_total": feedback.get("change_total"),
            "max_severity": feedback.get("max_severity"),
            "review_priority": feedback.get("review_priority"),
            "priority_reasons": list(feedback.get("priority_reasons", []) or []),
            "requires_baseline_revalidation": bool(feedback.get("requires_baseline_revalidation")),
            "do_not_auto_apply": True,
            "recommended_changes": list(feedback.get("recommended_changes", []) or []),
            "source_feedback_path": str(resolved_feedback_path),
            "source_feedback_hash": sha256_file(resolved_feedback_path),
            "decision_reason": None,
            "decision_at": None,
        }
        _write_json(item_path, item)
        self._append_event(
            review_id=resolved_review_id,
            action="enqueue",
            status="open",
            actor=item["owner"],
            reason="feedback queued for governed review",
            payload={"source_feedback_hash": item["source_feedback_hash"]},
            occurred_at=item["created_at"],
        )
        self._write_summary()
        return item

    def decide(
        self,
        *,
        review_id: str,
        status: str,
        reason: str,
        actor: str = "unassigned",
        decided_at: str | None = None,
        evidence_payload: dict[str, object] | None = None,
    ) -> dict[str, object]:
        resolved_review_id = _normalize_review_id(review_id)
        normalized_status = _normalize_text(status).lower()
        if normalized_status not in DECISION_STATUSES:
            raise ValueError(f"decision status must be one of {sorted(DECISION_STATUSES)}")
        normalized_reason = _normalize_text(reason)
        if not normalized_reason:
            raise ValueError("decision reason is required")
        item = self.get_item(resolved_review_id)
        if item.get("status") == "closed" and normalized_status != "closed":
            raise ValueError("closed feedback review item cannot be reopened")
        decision_time = decided_at or _utc_now_iso()
        item["status"] = normalized_status
        item["owner"] = _normalize_text(actor) or str(item.get("owner") or "unassigned")
        item["updated_at"] = decision_time
        item["decision_reason"] = normalized_reason
        item["decision_at"] = decision_time
        item["do_not_auto_apply"] = True
        _write_json(self.items_dir / f"{resolved_review_id}.json", item)
        self._append_event(
            review_id=resolved_review_id,
            action="decision",
            status=normalized_status,
            actor=item["owner"],
            reason=normalized_reason,
            payload={
                "requires_baseline_revalidation": bool(item.get("requires_baseline_revalidation")),
                "do_not_auto_apply": True,
                **dict(evidence_payload or {}),
            },
            occurred_at=decision_time,
        )
        self._write_summary()
        return item

    def _validate_feedback(self, feedback_path: Path) -> dict[str, object]:
        if not feedback_path.exists():
            raise FileNotFoundError(f"primary result learning feedback artifact missing: {feedback_path}")
        feedback = _read_json(feedback_path)
        if feedback.get("feedback_version") != SUPPORTED_FEEDBACK_VERSION:
            raise ValueError("primary result learning feedback version is invalid")
        if feedback.get("status") != "passed":
            raise ValueError("primary result learning feedback must be passed")
        if feedback.get("do_not_auto_apply") is not True:
            raise ValueError("primary result learning feedback must keep do_not_auto_apply=true")
        if str(feedback.get("review_priority") or "none") not in REVIEW_PRIORITIES:
            raise ValueError("primary result learning feedback review_priority is invalid")
        priority_reasons = feedback.get("priority_reasons")
        if priority_reasons is None:
            raise ValueError("primary result learning feedback missing priority_reasons")
        if not isinstance(priority_reasons, list):
            raise ValueError("primary result learning feedback priority_reasons must be a list")
        if not feedback.get("result_id"):
            raise ValueError("primary result learning feedback missing result_id")
        if not feedback.get("ts_code"):
            raise ValueError("primary result learning feedback missing ts_code")
        changes = feedback.get("recommended_changes")
        if changes is None:
            raise ValueError("primary result learning feedback missing recommended_changes")
        if not isinstance(changes, list):
            raise ValueError("primary result learning feedback recommended_changes must be a list")
        for change in changes:
            if not isinstance(change, dict):
                raise ValueError("primary result learning feedback recommended_changes must contain objects")
            if change.get("do_not_auto_apply") is not True:
                raise ValueError("primary result learning feedback change must keep do_not_auto_apply=true")
        return feedback

    def _append_event(
        self,
        *,
        review_id: str,
        action: str,
        status: str,
        actor: object,
        reason: str,
        payload: dict[str, object],
        occurred_at: str,
    ) -> None:
        _append_jsonl(
            self.decisions_path,
            {
                "event_version": PRIMARY_RESULT_FEEDBACK_REVIEW_EVENT_VERSION,
                "review_id": review_id,
                "action": action,
                "status": status,
                "actor": actor,
                "reason": reason,
                "occurred_at": occurred_at,
                "payload": payload,
            },
        )

    def _write_summary(self) -> None:
        items = self.list_items()
        status_counts = {status: 0 for status in sorted(REVIEW_STATUSES)}
        priority_counts = {priority: 0 for priority in ("critical", "high", "medium", "low", "none")}
        taxonomy_hotspots: dict[str, int] = {}
        open_owner_workloads: dict[str, dict[str, int]] = {}
        for item in items:
            status = str(item.get("status", ""))
            if status in status_counts:
                status_counts[status] += 1
            priority = str(item.get("review_priority", "none"))
            if priority in priority_counts:
                priority_counts[priority] += 1
            category = _normalize_text(item.get("primary_failure_category"))
            if category:
                taxonomy_hotspots[category] = taxonomy_hotspots.get(category, 0) + 1
            owner = _normalize_text(item.get("owner")) or "unassigned"
            if owner not in open_owner_workloads:
                open_owner_workloads[owner] = {
                    "open_total": 0,
                    "critical_priority_total": 0,
                    "high_priority_total": 0,
                }
            if status == "open":
                open_owner_workloads[owner]["open_total"] += 1
                if priority == "critical":
                    open_owner_workloads[owner]["critical_priority_total"] += 1
                if priority in {"critical", "high"}:
                    open_owner_workloads[owner]["high_priority_total"] += 1
        summary = {
            "queue_version": PRIMARY_RESULT_FEEDBACK_REVIEW_QUEUE_VERSION,
            "generated_at": _utc_now_iso(),
            "item_total": len(items),
            "status_counts": status_counts,
            "priority_counts": priority_counts,
            "requires_baseline_revalidation_total": sum(
                1 for item in items if bool(item.get("requires_baseline_revalidation"))
            ),
            "open_high_severity_total": sum(
                1 for item in items if item.get("status") == "open" and item.get("max_severity") in {"critical", "high"}
            ),
            "open_critical_priority_total": sum(
                1 for item in items if item.get("status") == "open" and item.get("review_priority") == "critical"
            ),
            "open_high_priority_total": sum(
                1 for item in items if item.get("status") == "open" and item.get("review_priority") in {"critical", "high"}
            ),
            "taxonomy_hotspots": dict(sorted(taxonomy_hotspots.items())),
            "open_owner_workloads": dict(sorted(open_owner_workloads.items())),
            "decision_event_total": len(self.list_decision_history()),
        }
        _write_json(self.summary_path, summary)
