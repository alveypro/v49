from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from src.artifact_registry import ArtifactRegistry, sha256_file
from src.utils.project_paths import resolve_project_path


PRIMARY_RESULT_LIFECYCLE_SNAPSHOT_VERSION = "primary_result_lifecycle_snapshot.v1"
PRIMARY_RESULT_LIFECYCLE_CURRENT_POINTER_VERSION = "primary_result_lifecycle_current_pointer.v1"
EXPECTED_LIFECYCLE_VERSION = "primary_result_lifecycle.v1"
REQUIRED_STEPS = {"audit", "execution", "rollback", "observation"}
OBSERVATION_STATUSES = {"observing", "completed"}


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


def _normalize_lifecycle_id(value: str) -> str:
    lifecycle_id = str(value).strip()
    if not lifecycle_id:
        raise ValueError("lifecycle_id must not be empty")
    allowed = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_")
    if any(ch not in allowed for ch in lifecycle_id):
        raise ValueError("lifecycle_id must contain only letters, numbers, '-' or '_'")
    return lifecycle_id


def _safe_id_part(value: object) -> str:
    text = str(value or "").strip().lower()
    text = re.sub(r"[^a-z0-9]+", "-", text)
    return text.strip("-") or "unknown"


def _default_lifecycle_id(evidence: dict[str, object]) -> str:
    ts_code = _safe_id_part(evidence.get("ts_code"))
    completed_at = _safe_id_part(evidence.get("completed_at"))
    return _normalize_lifecycle_id(f"primary-lifecycle-{ts_code}-{completed_at}")


@dataclass(frozen=True)
class PrimaryResultLifecycleSnapshot:
    lifecycle_id: str
    registered_at: str
    source_evidence_path: str
    evidence_hash: str
    result_id: str
    ts_code: str
    stock_name: str | None
    lifecycle_status: str
    final_payload: dict[str, object]
    steps: list[dict[str, object]]
    stale_artifact_total: int
    snapshot_version: str = PRIMARY_RESULT_LIFECYCLE_SNAPSHOT_VERSION

    def as_dict(self) -> dict[str, object]:
        return {
            "snapshot_version": self.snapshot_version,
            "lifecycle_id": self.lifecycle_id,
            "registered_at": self.registered_at,
            "source_evidence_path": self.source_evidence_path,
            "evidence_hash": self.evidence_hash,
            "result_id": self.result_id,
            "ts_code": self.ts_code,
            "stock_name": self.stock_name,
            "lifecycle_status": self.lifecycle_status,
            "final_payload": self.final_payload,
            "steps": self.steps,
            "stale_artifact_total": self.stale_artifact_total,
        }


class PrimaryResultLifecycleRegistry:
    def __init__(
        self,
        *,
        lifecycles_dir: str | Path = "artifacts/primary_result_lifecycle",
        artifact_registry_path: str | Path | None = None,
    ) -> None:
        self.lifecycles_dir = resolve_project_path(lifecycles_dir)
        self.history_dir = self.lifecycles_dir / "history"
        self.current_path = self.lifecycles_dir / "current.json"
        resolved_registry_path = (
            resolve_project_path(artifact_registry_path)
            if artifact_registry_path is not None
            else self.lifecycles_dir.parent / "artifact_registry.jsonl"
        )
        self.artifact_registry = ArtifactRegistry(resolved_registry_path)
        self.ensure_layout()

    def ensure_layout(self) -> None:
        self.lifecycles_dir.mkdir(parents=True, exist_ok=True)
        self.history_dir.mkdir(parents=True, exist_ok=True)
        if not self.current_path.exists():
            _write_json(
                self.current_path,
                {
                    "pointer_version": PRIMARY_RESULT_LIFECYCLE_CURRENT_POINTER_VERSION,
                    "lifecycle_id": None,
                    "snapshot_path": None,
                    "result_id": None,
                    "ts_code": None,
                    "updated_at": None,
                    "rollback_of_lifecycle_id": None,
                },
            )

    def get_current_pointer(self) -> dict[str, object]:
        return _read_json(self.current_path)

    def get_current_snapshot(self) -> dict[str, object] | None:
        pointer = self.get_current_pointer()
        snapshot_path = pointer.get("snapshot_path")
        if not snapshot_path:
            return None
        return _read_json(Path(str(snapshot_path)))

    def get_snapshot(self, lifecycle_id: str) -> dict[str, object]:
        resolved_lifecycle_id = _normalize_lifecycle_id(lifecycle_id)
        snapshot_path = self.history_dir / f"{resolved_lifecycle_id}.json"
        if not snapshot_path.exists():
            raise FileNotFoundError(f"primary result lifecycle snapshot not found: {snapshot_path}")
        snapshot = _read_json(snapshot_path)
        snapshot["_snapshot_path"] = str(snapshot_path)
        return snapshot

    def list_history(self) -> list[dict[str, object]]:
        entries: list[dict[str, object]] = []
        for path in sorted(self.history_dir.glob("*.json")):
            payload = _read_json(path)
            payload["_snapshot_path"] = str(path)
            entries.append(payload)
        entries.sort(key=lambda item: (str(item.get("registered_at", "")), str(item.get("lifecycle_id", ""))))
        return entries

    def register(
        self,
        *,
        evidence_path: str | Path,
        lifecycle_id: str | None = None,
        registered_at: str | None = None,
    ) -> dict[str, object]:
        resolved_evidence_path = resolve_project_path(evidence_path)
        evidence = self._validate_evidence(resolved_evidence_path)
        resolved_lifecycle_id = _normalize_lifecycle_id(lifecycle_id or _default_lifecycle_id(evidence))
        snapshot_path = self.history_dir / f"{resolved_lifecycle_id}.json"
        if snapshot_path.exists():
            raise FileExistsError(f"primary result lifecycle snapshot already exists: {snapshot_path}")

        steps = [dict(step) for step in evidence.get("steps", []) if isinstance(step, dict)]
        final_payload = dict(evidence.get("final_payload", {}) or {})
        snapshot = PrimaryResultLifecycleSnapshot(
            lifecycle_id=resolved_lifecycle_id,
            registered_at=registered_at or _utc_now_iso(),
            source_evidence_path=str(resolved_evidence_path),
            evidence_hash=sha256_file(resolved_evidence_path),
            result_id=str(evidence.get("result_id", "") or ""),
            ts_code=str(evidence.get("ts_code", "") or ""),
            stock_name=str(evidence.get("stock_name")) if evidence.get("stock_name") is not None else None,
            lifecycle_status=str(evidence.get("status", "") or ""),
            final_payload=final_payload,
            steps=steps,
            stale_artifact_total=len(evidence.get("stale_artifacts_detected", []) or []),
        )
        _write_json(snapshot_path, snapshot.as_dict())
        current_pointer_updated_at = registered_at or _utc_now_iso()
        self._write_current_pointer(
            lifecycle_id=resolved_lifecycle_id,
            snapshot_path=snapshot_path,
            result_id=snapshot.result_id,
            ts_code=snapshot.ts_code,
            rollback_of_lifecycle_id=None,
            updated_at=current_pointer_updated_at,
        )
        self._register_artifacts(
            lifecycle_id=resolved_lifecycle_id,
            result_id=snapshot.result_id,
            evidence_path=resolved_evidence_path,
            snapshot_path=snapshot_path,
            current_pointer_path=self.current_path,
            registered_at=snapshot.registered_at,
            pointer_updated_at=current_pointer_updated_at,
        )
        return snapshot.as_dict()

    def rollback(self, lifecycle_id: str, *, rolled_back_at: str | None = None) -> dict[str, object]:
        resolved_lifecycle_id = _normalize_lifecycle_id(lifecycle_id)
        snapshot_path = self.history_dir / f"{resolved_lifecycle_id}.json"
        if not snapshot_path.exists():
            raise FileNotFoundError(f"primary result lifecycle snapshot not found: {snapshot_path}")
        snapshot = _read_json(snapshot_path)
        self._write_current_pointer(
            lifecycle_id=resolved_lifecycle_id,
            snapshot_path=snapshot_path,
            result_id=str(snapshot.get("result_id", "") or ""),
            ts_code=str(snapshot.get("ts_code", "") or ""),
            rollback_of_lifecycle_id=resolved_lifecycle_id,
            updated_at=rolled_back_at or _utc_now_iso(),
        )
        pointer_updated_at = str(self.get_current_pointer().get("updated_at") or "")
        rollback_suffix = _safe_id_part(rolled_back_at or pointer_updated_at or _utc_now_iso())
        rollback_pointer_path = self.history_dir / f"{resolved_lifecycle_id}.rollback-current-{rollback_suffix}.json"
        _write_json(rollback_pointer_path, self.get_current_pointer())
        self.artifact_registry.register_artifact(
            artifact_type="primary_result_lifecycle_current_pointer",
            run_id=resolved_lifecycle_id,
            result_id=str(snapshot.get("result_id", "") or "") or None,
            path=rollback_pointer_path,
            producer="src/primary_result_lifecycle_registry.py",
            artifact_id=f"{resolved_lifecycle_id}:current-pointer:rollback:{rollback_suffix}",
            created_at=pointer_updated_at,
            parent_artifact_ids=[f"{resolved_lifecycle_id}:snapshot"],
            metadata={
                "lifecycle_id": resolved_lifecycle_id,
                "ts_code": str(snapshot.get("ts_code", "") or ""),
                "rollback_of_lifecycle_id": resolved_lifecycle_id,
                "snapshot_path": str(snapshot_path),
                "current_pointer_path": str(self.current_path),
                "pointer_version": PRIMARY_RESULT_LIFECYCLE_CURRENT_POINTER_VERSION,
                "current_role": "index_only",
            },
        )
        return snapshot

    def _register_artifacts(
        self,
        *,
        lifecycle_id: str,
        result_id: str,
        evidence_path: Path,
        snapshot_path: Path,
        current_pointer_path: Path,
        registered_at: str,
        pointer_updated_at: str,
    ) -> None:
        evidence_artifact_id = f"{lifecycle_id}:evidence"
        snapshot_artifact_id = f"{lifecycle_id}:snapshot"
        current_pointer_artifact_id = f"{lifecycle_id}:current-pointer:{pointer_updated_at}"
        self.artifact_registry.register_artifact(
            artifact_type="primary_result_lifecycle_evidence",
            run_id=lifecycle_id,
            result_id=result_id,
            path=evidence_path,
            producer="src/primary_result_lifecycle_registry.py",
            artifact_id=evidence_artifact_id,
            created_at=registered_at,
            metadata={
                "lifecycle_id": lifecycle_id,
                "current_role": "immutable_source",
            },
        )
        self.artifact_registry.register_artifact(
            artifact_type="primary_result_lifecycle_snapshot",
            run_id=lifecycle_id,
            result_id=result_id,
            path=snapshot_path,
            producer="src/primary_result_lifecycle_registry.py",
            artifact_id=snapshot_artifact_id,
            created_at=registered_at,
            parent_artifact_ids=[evidence_artifact_id],
            metadata={
                "lifecycle_id": lifecycle_id,
                "current_role": "immutable_source",
                "source_evidence_path": str(evidence_path),
            },
        )
        self.artifact_registry.register_artifact(
            artifact_type="primary_result_lifecycle_current_pointer",
            run_id=lifecycle_id,
            result_id=result_id,
            path=current_pointer_path,
            producer="src/primary_result_lifecycle_registry.py",
            artifact_id=current_pointer_artifact_id,
            created_at=pointer_updated_at,
            parent_artifact_ids=[snapshot_artifact_id],
            metadata={
                "lifecycle_id": lifecycle_id,
                "current_role": "index_only",
                "snapshot_path": str(snapshot_path),
                "pointer_version": PRIMARY_RESULT_LIFECYCLE_CURRENT_POINTER_VERSION,
            },
        )

    def _write_current_pointer(
        self,
        *,
        lifecycle_id: str,
        snapshot_path: Path,
        result_id: str,
        ts_code: str,
        rollback_of_lifecycle_id: str | None,
        updated_at: str | None = None,
    ) -> None:
        _write_json(
            self.current_path,
            {
                "pointer_version": PRIMARY_RESULT_LIFECYCLE_CURRENT_POINTER_VERSION,
                "lifecycle_id": lifecycle_id,
                "snapshot_path": str(snapshot_path),
                "result_id": result_id,
                "ts_code": ts_code,
                "updated_at": updated_at or _utc_now_iso(),
                "rollback_of_lifecycle_id": rollback_of_lifecycle_id,
            },
        )

    def _validate_evidence(self, evidence_path: Path) -> dict[str, object]:
        if not evidence_path.exists():
            raise FileNotFoundError(f"primary result lifecycle evidence missing: {evidence_path}")
        evidence = _read_json(evidence_path)
        if evidence.get("lifecycle_version") != EXPECTED_LIFECYCLE_VERSION:
            raise ValueError("primary result lifecycle evidence version is invalid")
        if evidence.get("status") != "passed":
            raise ValueError("primary result lifecycle evidence must be passed")
        if evidence.get("blocking_failures") not in ([], None):
            raise ValueError("primary result lifecycle evidence must not include blocking failures")

        result_id = str(evidence.get("result_id", "") or "").strip()
        ts_code = str(evidence.get("ts_code", "") or "").strip()
        if not result_id:
            raise ValueError("primary result lifecycle evidence missing result_id")
        if not ts_code:
            raise ValueError("primary result lifecycle evidence missing ts_code")

        final_payload = evidence.get("final_payload")
        if not isinstance(final_payload, dict):
            raise ValueError("primary result lifecycle evidence missing final_payload")
        if final_payload.get("result_id") != result_id:
            raise ValueError("primary result lifecycle final_payload result_id mismatch")
        if final_payload.get("ts_code") != ts_code:
            raise ValueError("primary result lifecycle final_payload ts_code mismatch")
        if final_payload.get("audit_status") != "passed":
            raise ValueError("primary result lifecycle final_payload audit_status must be passed")
        if final_payload.get("execution_status") != "ready":
            raise ValueError("primary result lifecycle final_payload execution_status must be ready")
        if final_payload.get("observation_status") not in OBSERVATION_STATUSES:
            raise ValueError("primary result lifecycle final_payload observation_status is not promotable")
        if not final_payload.get("rollback_status"):
            raise ValueError("primary result lifecycle final_payload missing rollback_status")

        steps = evidence.get("steps")
        if not isinstance(steps, list):
            raise ValueError("primary result lifecycle evidence missing steps")
        step_by_name = {str(step.get("step", "") or ""): step for step in steps if isinstance(step, dict)}
        missing_steps = sorted(REQUIRED_STEPS - set(step_by_name))
        if missing_steps:
            raise ValueError(f"primary result lifecycle evidence missing steps: {', '.join(missing_steps)}")
        for step_name in sorted(REQUIRED_STEPS):
            step = step_by_name[step_name]
            if step.get("exists") is not True:
                raise ValueError(f"primary result lifecycle step artifact missing: {step_name}")
            if not step.get("sha256"):
                raise ValueError(f"primary result lifecycle step missing sha256: {step_name}")
            if not step.get("path"):
                raise ValueError(f"primary result lifecycle step missing path: {step_name}")
            if step.get("result_id") != result_id:
                raise ValueError(f"primary result lifecycle step result_id mismatch: {step_name}")
            if step.get("ts_code") != ts_code:
                raise ValueError(f"primary result lifecycle step ts_code mismatch: {step_name}")
            if step.get("exit_code") != 0:
                raise ValueError(f"primary result lifecycle step exit_code must be zero: {step_name}")
        return evidence
