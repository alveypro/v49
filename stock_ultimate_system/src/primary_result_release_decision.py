from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path

from src.artifact_registry import sha256_file
from src.utils.project_paths import resolve_project_path


PRIMARY_RESULT_RELEASE_DECISION_VERSION = "primary_result_release_decision.v1"
PRIMARY_RESULT_RELEASE_DECISION_POINTER_VERSION = "primary_result_release_decision_pointer.v1"
SUPPORTED_CHECKLIST_VERSION = "primary_result_release_evidence_checklist.v1"
DECISIONS = {"approved", "rejected"}


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


def _normalize_text(value: object) -> str:
    return str(value or "").strip()


def _safe_id_part(value: object) -> str:
    text = _normalize_text(value).lower()
    text = re.sub(r"[^a-z0-9]+", "-", text)
    return text.strip("-") or "unknown"


def _normalize_decision_id(value: str) -> str:
    decision_id = _normalize_text(value)
    if not decision_id:
        raise ValueError("decision_id must not be empty")
    allowed = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_")
    if any(ch not in allowed for ch in decision_id):
        raise ValueError("decision_id must contain only letters, numbers, '-' or '_'")
    return decision_id


def _default_decision_id(checklist: dict[str, object], decision: str) -> str:
    return _normalize_decision_id(
        "primary-release-decision-"
        f"{_safe_id_part(checklist.get('checklist_id'))}-"
        f"{_safe_id_part(decision)}"
    )


class PrimaryResultReleaseDecisionRegistry:
    def __init__(self, *, decisions_dir: str | Path = "artifacts/primary_result_release_decisions") -> None:
        self.decisions_dir = resolve_project_path(decisions_dir)
        self.history_dir = self.decisions_dir / "history"
        self.current_path = self.decisions_dir / "current.json"
        self.ensure_layout()

    def ensure_layout(self) -> None:
        self.history_dir.mkdir(parents=True, exist_ok=True)
        if not self.current_path.exists():
            _write_json(
                self.current_path,
                {
                    "pointer_version": PRIMARY_RESULT_RELEASE_DECISION_POINTER_VERSION,
                    "decision_id": None,
                    "decision_path": None,
                    "checklist_id": None,
                    "decision": None,
                    "updated_at": None,
                },
            )

    def get_current_pointer(self) -> dict[str, object]:
        return _read_json(self.current_path)

    def get_decision(self, decision_id: str) -> dict[str, object]:
        resolved_decision_id = _normalize_decision_id(decision_id)
        path = self.history_dir / f"{resolved_decision_id}.json"
        if not path.exists():
            raise FileNotFoundError(f"primary result release decision not found: {path}")
        payload = _read_json(path)
        payload["_decision_path"] = str(path)
        return payload

    def create_decision(
        self,
        *,
        checklist_path: str | Path,
        decision: str,
        actor: str,
        reason: str,
        decision_id: str | None = None,
        decided_at: str | None = None,
    ) -> dict[str, object]:
        resolved_checklist_path = resolve_project_path(checklist_path)
        checklist = self._validate_checklist(resolved_checklist_path)
        normalized_decision = _normalize_text(decision).lower()
        if normalized_decision not in DECISIONS:
            raise ValueError(f"decision must be one of {sorted(DECISIONS)}")
        normalized_actor = _normalize_text(actor)
        if not normalized_actor:
            raise ValueError("actor is required")
        normalized_reason = _normalize_text(reason)
        if not normalized_reason:
            raise ValueError("reason is required")
        if normalized_decision == "approved":
            self._validate_approval_ready(checklist)

        resolved_decision_id = _normalize_decision_id(
            decision_id or _default_decision_id(checklist, normalized_decision)
        )
        decision_path = self.history_dir / f"{resolved_decision_id}.json"
        if decision_path.exists():
            raise FileExistsError(f"primary result release decision already exists: {decision_path}")

        baseline_allowed = normalized_decision == "approved"
        payload = {
            "decision_version": PRIMARY_RESULT_RELEASE_DECISION_VERSION,
            "decision_id": resolved_decision_id,
            "decided_at": decided_at or _utc_now_iso(),
            "decision": normalized_decision,
            "actor": normalized_actor,
            "reason": normalized_reason,
            "checklist_id": checklist.get("checklist_id"),
            "checklist_status": checklist.get("status"),
            "review_id": checklist.get("review_id"),
            "result_id": checklist.get("result_id"),
            "ts_code": checklist.get("ts_code"),
            "missing_evidence": list(checklist.get("missing_evidence", []) or []),
            "blocking_gate_reason": checklist.get("blocking_gate_reason"),
            "release_pipeline_allowed": baseline_allowed,
            "baseline_promotion_allowed": baseline_allowed,
            "do_not_auto_apply": True,
            "decision_boundary": (
                "release decision authorizes the next governed step only; it does not release, promote baseline, "
                "or apply strategy changes by itself"
            ),
            "source_checklist_path": str(resolved_checklist_path),
            "source_checklist_hash": sha256_file(resolved_checklist_path),
        }
        _write_json(decision_path, payload)
        _write_json(
            self.current_path,
            {
                "pointer_version": PRIMARY_RESULT_RELEASE_DECISION_POINTER_VERSION,
                "decision_id": resolved_decision_id,
                "decision_path": str(decision_path),
                "checklist_id": checklist.get("checklist_id"),
                "decision": normalized_decision,
                "updated_at": payload["decided_at"],
            },
        )
        return payload

    def _validate_checklist(self, checklist_path: Path) -> dict[str, object]:
        if not checklist_path.exists():
            raise FileNotFoundError(f"release evidence checklist missing: {checklist_path}")
        checklist = _read_json(checklist_path)
        if checklist.get("checklist_version") != SUPPORTED_CHECKLIST_VERSION:
            raise ValueError("release evidence checklist version is invalid")
        if checklist.get("do_not_auto_apply") is not True:
            raise ValueError("release evidence checklist must keep do_not_auto_apply=true")
        if not checklist.get("checklist_id"):
            raise ValueError("release evidence checklist missing checklist_id")
        return checklist

    def _validate_approval_ready(self, checklist: dict[str, object]) -> None:
        if checklist.get("status") != "complete":
            raise ValueError("release approval requires complete checklist")
        missing_evidence = checklist.get("missing_evidence")
        if isinstance(missing_evidence, list) and missing_evidence:
            raise ValueError("release approval blocked: checklist has missing evidence")
        if checklist.get("blocking_gate_reason"):
            raise ValueError("release approval blocked: checklist has blocking gate")
