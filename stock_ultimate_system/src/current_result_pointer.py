from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from src.utils.project_paths import resolve_project_path


CURRENT_RESULT_POINTER_VERSION = "current_result_pointer.v1"
CURRENT_RESULT_POINTER_SNAPSHOT_VERSION = "current_result_pointer_snapshot.v1"


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


def _normalize_pointer_snapshot_id(value: str) -> str:
    pointer_snapshot_id = _normalize_text(value)
    if not pointer_snapshot_id:
        raise ValueError("pointer_snapshot_id must not be empty")
    allowed = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_")
    if any(ch not in allowed for ch in pointer_snapshot_id):
        raise ValueError("pointer_snapshot_id must contain only letters, numbers, '-' or '_'")
    return pointer_snapshot_id


def _safe_id_part(value: object) -> str:
    text = _normalize_text(value).lower()
    text = re.sub(r"[^a-z0-9]+", "-", text)
    return text.strip("-") or "unknown"


def _default_pointer_snapshot_id(result_id: str, as_of_date: str, updated_at: str) -> str:
    return _normalize_pointer_snapshot_id(
        f"current-result-pointer-{_safe_id_part(result_id)}-{_safe_id_part(as_of_date)}-{_safe_id_part(updated_at)}"
    )


@dataclass(frozen=True)
class CurrentResultPointerSnapshot:
    pointer_snapshot_id: str
    result_id: str
    run_id: str
    lifecycle_id: str
    artifact_ids: tuple[str, ...]
    as_of_date: str
    updated_at: str
    source_scope: str
    pointer_version: str = CURRENT_RESULT_POINTER_VERSION
    snapshot_version: str = CURRENT_RESULT_POINTER_SNAPSHOT_VERSION
    metadata: dict[str, object] | None = None

    def as_dict(self) -> dict[str, object]:
        return {
            "snapshot_version": self.snapshot_version,
            "pointer_version": self.pointer_version,
            "pointer_snapshot_id": self.pointer_snapshot_id,
            "result_id": self.result_id,
            "run_id": self.run_id,
            "lifecycle_id": self.lifecycle_id,
            "artifact_ids": list(self.artifact_ids),
            "as_of_date": self.as_of_date,
            "updated_at": self.updated_at,
            "source_scope": self.source_scope,
            "metadata": dict(self.metadata or {}),
        }


class CurrentResultPointerStore:
    def __init__(self, *, pointer_dir: str | Path = "artifacts/current_result_pointer") -> None:
        self.pointer_dir = resolve_project_path(pointer_dir)
        self.history_dir = self.pointer_dir / "history"
        self.current_path = self.pointer_dir / "current.json"
        self.ensure_layout()

    def ensure_layout(self) -> None:
        self.history_dir.mkdir(parents=True, exist_ok=True)
        if not self.current_path.exists():
            _write_json(
                self.current_path,
                {
                    "pointer_version": CURRENT_RESULT_POINTER_VERSION,
                    "pointer_snapshot_id": None,
                    "result_id": None,
                    "run_id": None,
                    "lifecycle_id": None,
                    "artifact_ids": [],
                    "as_of_date": None,
                    "updated_at": None,
                    "source_scope": None,
                    "snapshot_path": None,
                },
            )

    def get_current_pointer(self) -> dict[str, object]:
        return _read_json(self.current_path)

    def get_snapshot(self, pointer_snapshot_id: str) -> dict[str, object]:
        resolved_pointer_snapshot_id = _normalize_pointer_snapshot_id(pointer_snapshot_id)
        path = self.history_dir / f"{resolved_pointer_snapshot_id}.json"
        if not path.exists():
            raise FileNotFoundError(f"current result pointer snapshot not found: {path}")
        payload = _read_json(path)
        payload["_snapshot_path"] = str(path)
        return payload

    def point_to(
        self,
        *,
        result_id: str,
        run_id: str,
        lifecycle_id: str,
        artifact_ids: list[str] | tuple[str, ...],
        as_of_date: str,
        source_scope: str = "stock",
        updated_at: str | None = None,
        pointer_snapshot_id: str | None = None,
        metadata: dict[str, object] | None = None,
    ) -> dict[str, object]:
        normalized_result_id = _normalize_text(result_id)
        normalized_run_id = _normalize_text(run_id)
        normalized_lifecycle_id = _normalize_text(lifecycle_id)
        normalized_as_of_date = _normalize_text(as_of_date)
        normalized_source_scope = _normalize_text(source_scope)
        normalized_artifact_ids = tuple(_normalize_text(item) for item in artifact_ids if _normalize_text(item))

        if not normalized_result_id:
            raise ValueError("result_id is required")
        if not normalized_run_id:
            raise ValueError("run_id is required")
        if not normalized_lifecycle_id:
            raise ValueError("lifecycle_id is required")
        if not normalized_artifact_ids:
            raise ValueError("artifact_ids must not be empty")
        if not normalized_as_of_date:
            raise ValueError("as_of_date is required")
        if not normalized_source_scope:
            raise ValueError("source_scope is required")

        resolved_updated_at = updated_at or _utc_now_iso()
        resolved_pointer_snapshot_id = _normalize_pointer_snapshot_id(
            pointer_snapshot_id or _default_pointer_snapshot_id(normalized_result_id, normalized_as_of_date, resolved_updated_at)
        )
        snapshot_path = self.history_dir / f"{resolved_pointer_snapshot_id}.json"
        if snapshot_path.exists():
            raise FileExistsError(f"current result pointer snapshot already exists: {snapshot_path}")

        snapshot = CurrentResultPointerSnapshot(
            pointer_snapshot_id=resolved_pointer_snapshot_id,
            result_id=normalized_result_id,
            run_id=normalized_run_id,
            lifecycle_id=normalized_lifecycle_id,
            artifact_ids=normalized_artifact_ids,
            as_of_date=normalized_as_of_date,
            updated_at=resolved_updated_at,
            source_scope=normalized_source_scope,
            metadata=metadata,
        )
        _write_json(snapshot_path, snapshot.as_dict())
        _write_json(
            self.current_path,
            {
                "pointer_version": CURRENT_RESULT_POINTER_VERSION,
                "pointer_snapshot_id": resolved_pointer_snapshot_id,
                "result_id": normalized_result_id,
                "run_id": normalized_run_id,
                "lifecycle_id": normalized_lifecycle_id,
                "artifact_ids": list(normalized_artifact_ids),
                "as_of_date": normalized_as_of_date,
                "updated_at": resolved_updated_at,
                "source_scope": normalized_source_scope,
                "snapshot_path": str(snapshot_path),
            },
        )
        return snapshot.as_dict()
