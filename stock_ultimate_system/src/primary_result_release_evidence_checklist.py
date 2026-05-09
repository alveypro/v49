from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path

from src.artifact_registry import sha256_file
from src.utils.project_paths import resolve_project_path


PRIMARY_RESULT_RELEASE_EVIDENCE_CHECKLIST_VERSION = "primary_result_release_evidence_checklist.v1"
PRIMARY_RESULT_RELEASE_EVIDENCE_CHECKLIST_POINTER_VERSION = "primary_result_release_evidence_checklist_pointer.v1"
SUPPORTED_REVIEW_QUEUE_VERSION = "primary_result_feedback_review_queue.v1"

REQUIRED_EVIDENCE_IDS = (
    "benchmark_report",
    "benchmark_diff",
    "release_gates",
    "release_evidence_bundle",
    "manifest",
    "baseline_policy_decision",
)


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


def _normalize_checklist_id(value: str) -> str:
    checklist_id = _normalize_text(value)
    if not checklist_id:
        raise ValueError("checklist_id must not be empty")
    allowed = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_")
    if any(ch not in allowed for ch in checklist_id):
        raise ValueError("checklist_id must contain only letters, numbers, '-' or '_'")
    return checklist_id


def _default_checklist_id(review_item: dict[str, object]) -> str:
    return _normalize_checklist_id(
        "primary-release-evidence-"
        f"{_safe_id_part(review_item.get('review_id'))}-"
        f"{_safe_id_part(review_item.get('updated_at') or review_item.get('decision_at'))}"
    )


def _resolve_optional_path(path: str | Path | None) -> Path | None:
    if path is None or _normalize_text(path) == "":
        return None
    return resolve_project_path(path)


def _gate_blocking_reason(path: Path | None) -> str | None:
    if path is None or not path.exists():
        return None
    payload = _read_json(path)
    status = _normalize_text(payload.get("status") or payload.get("overall_status")).lower()
    if status in {"failed", "failure", "blocked", "blocking"}:
        return f"release gates status is {status}"
    blocking_failures = payload.get("blocking_failures")
    if isinstance(blocking_failures, list) and blocking_failures:
        return "release gates contain blocking_failures"
    failures = payload.get("failures")
    if isinstance(failures, list):
        for failure in failures:
            if isinstance(failure, dict) and failure.get("blocking") is True:
                return "release gates contain a blocking failure"
    gates = payload.get("gates")
    if isinstance(gates, list):
        for gate in gates:
            if not isinstance(gate, dict):
                continue
            gate_status = _normalize_text(gate.get("status")).lower()
            if gate.get("blocking") is True and gate_status in {"failed", "failure", "blocked"}:
                return "release gates contain a blocking gate"
    return None


def _evidence_entry(evidence_id: str, path: Path | None) -> dict[str, object]:
    exists = path is not None and path.exists()
    return {
        "evidence_id": evidence_id,
        "required": True,
        "path": str(path) if path is not None else None,
        "exists": exists,
        "sha256": sha256_file(path) if exists and path is not None else None,
        "status": "present" if exists else "missing",
    }


class PrimaryResultReleaseEvidenceChecklistRegistry:
    def __init__(
        self,
        *,
        checklists_dir: str | Path = "artifacts/primary_result_release_evidence_checklists",
    ) -> None:
        self.checklists_dir = resolve_project_path(checklists_dir)
        self.history_dir = self.checklists_dir / "history"
        self.current_path = self.checklists_dir / "current.json"
        self.ensure_layout()

    def ensure_layout(self) -> None:
        self.history_dir.mkdir(parents=True, exist_ok=True)
        if not self.current_path.exists():
            _write_json(
                self.current_path,
                {
                    "pointer_version": PRIMARY_RESULT_RELEASE_EVIDENCE_CHECKLIST_POINTER_VERSION,
                    "checklist_id": None,
                    "checklist_path": None,
                    "review_id": None,
                    "status": None,
                    "updated_at": None,
                },
            )

    def get_current_pointer(self) -> dict[str, object]:
        return _read_json(self.current_path)

    def get_checklist(self, checklist_id: str) -> dict[str, object]:
        resolved_checklist_id = _normalize_checklist_id(checklist_id)
        path = self.history_dir / f"{resolved_checklist_id}.json"
        if not path.exists():
            raise FileNotFoundError(f"primary result release evidence checklist not found: {path}")
        payload = _read_json(path)
        payload["_checklist_path"] = str(path)
        return payload

    def create_checklist(
        self,
        *,
        review_item_path: str | Path,
        benchmark_report_path: str | Path | None = None,
        benchmark_diff_path: str | Path | None = None,
        release_gates_path: str | Path | None = None,
        release_evidence_bundle_path: str | Path | None = None,
        manifest_path: str | Path | None = None,
        baseline_policy_decision_path: str | Path | None = None,
        checklist_id: str | None = None,
        generated_at: str | None = None,
    ) -> dict[str, object]:
        resolved_review_item_path = resolve_project_path(review_item_path)
        review_item = self._validate_review_item(resolved_review_item_path)
        resolved_checklist_id = _normalize_checklist_id(checklist_id or _default_checklist_id(review_item))
        checklist_path = self.history_dir / f"{resolved_checklist_id}.json"
        if checklist_path.exists():
            raise FileExistsError(f"primary result release evidence checklist already exists: {checklist_path}")

        evidence_paths = {
            "benchmark_report": _resolve_optional_path(benchmark_report_path),
            "benchmark_diff": _resolve_optional_path(benchmark_diff_path),
            "release_gates": _resolve_optional_path(release_gates_path),
            "release_evidence_bundle": _resolve_optional_path(release_evidence_bundle_path),
            "manifest": _resolve_optional_path(manifest_path),
            "baseline_policy_decision": _resolve_optional_path(baseline_policy_decision_path),
        }
        required_evidence = [_evidence_entry(evidence_id, evidence_paths[evidence_id]) for evidence_id in REQUIRED_EVIDENCE_IDS]
        missing_evidence = [
            str(entry["evidence_id"])
            for entry in required_evidence
            if entry["required"] is True and entry["exists"] is not True
        ]
        blocking_gate_reason = _gate_blocking_reason(evidence_paths["release_gates"])
        status = "blocked" if blocking_gate_reason else "complete"
        if missing_evidence:
            status = "incomplete"

        checklist = {
            "checklist_version": PRIMARY_RESULT_RELEASE_EVIDENCE_CHECKLIST_VERSION,
            "checklist_id": resolved_checklist_id,
            "generated_at": generated_at or _utc_now_iso(),
            "status": status,
            "review_id": review_item.get("review_id"),
            "result_id": review_item.get("result_id"),
            "ts_code": review_item.get("ts_code"),
            "stock_name": review_item.get("stock_name"),
            "requires_baseline_revalidation": bool(review_item.get("requires_baseline_revalidation")),
            "required_evidence": required_evidence,
            "missing_evidence": missing_evidence,
            "blocking_gate_reason": blocking_gate_reason,
            "do_not_auto_apply": True,
            "release_boundary": (
                "release evidence checklist is readiness evidence only; it does not release, promote baseline, "
                "or apply strategy changes"
            ),
            "source_review_item_path": str(resolved_review_item_path),
            "source_review_item_hash": sha256_file(resolved_review_item_path),
        }
        _write_json(checklist_path, checklist)
        _write_json(
            self.current_path,
            {
                "pointer_version": PRIMARY_RESULT_RELEASE_EVIDENCE_CHECKLIST_POINTER_VERSION,
                "checklist_id": resolved_checklist_id,
                "checklist_path": str(checklist_path),
                "review_id": review_item.get("review_id"),
                "status": status,
                "updated_at": checklist["generated_at"],
            },
        )
        return checklist

    def _validate_review_item(self, review_item_path: Path) -> dict[str, object]:
        if not review_item_path.exists():
            raise FileNotFoundError(f"feedback review item missing: {review_item_path}")
        review_item = _read_json(review_item_path)
        if review_item.get("queue_version") != SUPPORTED_REVIEW_QUEUE_VERSION:
            raise ValueError("feedback review item version is invalid")
        if review_item.get("status") != "accepted":
            raise ValueError("release evidence checklist requires review item status accepted")
        if review_item.get("do_not_auto_apply") is not True:
            raise ValueError("feedback review item must keep do_not_auto_apply=true")
        if not review_item.get("review_id"):
            raise ValueError("feedback review item missing review_id")
        if not review_item.get("result_id"):
            raise ValueError("feedback review item missing result_id")
        if not review_item.get("ts_code"):
            raise ValueError("feedback review item missing ts_code")
        return review_item
