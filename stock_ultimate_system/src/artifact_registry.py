from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from src.utils.project_paths import resolve_project_path


ARTIFACT_REGISTRY_SCHEMA_VERSION = "artifact_registry.v1"


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def sha256_file(path: str | Path) -> str:
    resolved_path = resolve_project_path(path)
    return hashlib.sha256(resolved_path.read_bytes()).hexdigest()


@dataclass(frozen=True)
class ArtifactRegistryEntry:
    artifact_id: str
    artifact_type: str
    run_id: str
    path: str
    sha256: str
    created_at: str
    producer: str
    code_revision: str | None = None
    result_id: str | None = None
    parent_artifact_ids: tuple[str, ...] = ()
    schema_version: str = ARTIFACT_REGISTRY_SCHEMA_VERSION
    metadata: dict[str, object] | None = None

    def as_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "artifact_id": self.artifact_id,
            "artifact_type": self.artifact_type,
            "run_id": self.run_id,
            "path": self.path,
            "sha256": self.sha256,
            "created_at": self.created_at,
            "producer": self.producer,
            "code_revision": self.code_revision,
            "result_id": self.result_id,
            "parent_artifact_ids": list(self.parent_artifact_ids),
            "metadata": dict(self.metadata or {}),
        }


class ArtifactRegistry:
    def __init__(self, path: str | Path) -> None:
        self.path = resolve_project_path(path)

    def get_entry(self, artifact_id: str) -> dict[str, object]:
        resolved_artifact_id = str(artifact_id or "").strip()
        if not resolved_artifact_id:
            raise ValueError("artifact_id must not be empty")
        matches = [entry for entry in self.list_entries() if entry.get("artifact_id") == resolved_artifact_id]
        if len(matches) > 1:
            raise ValueError(f"artifact registry contains conflicting entries for artifact_id={resolved_artifact_id}")
        if matches:
            return matches[0]
        raise FileNotFoundError(f"artifact registry entry not found: {resolved_artifact_id}")

    def register_artifact(
        self,
        *,
        artifact_type: str,
        run_id: str,
        path: str | Path,
        producer: str,
        code_revision: str | None = None,
        artifact_id: str | None = None,
        created_at: str | None = None,
        result_id: str | None = None,
        parent_artifact_ids: list[str] | tuple[str, ...] | None = None,
        metadata: dict[str, object] | None = None,
    ) -> ArtifactRegistryEntry:
        resolved_path = resolve_project_path(path)
        if not resolved_path.exists():
            raise FileNotFoundError(f"artifact missing: {resolved_path}")
        resolved_artifact_id = artifact_id or f"{run_id}:{artifact_type}:{resolved_path.name}"
        for existing in self.list_entries():
            if existing.get("artifact_id") == resolved_artifact_id:
                raise FileExistsError(f"artifact registry entry already exists for artifact_id={resolved_artifact_id}")
            if existing.get("run_id") == run_id and str(existing.get("path") or "").strip() == str(resolved_path):
                raise FileExistsError(f"artifact registry path already registered: {resolved_path}")
        entry = ArtifactRegistryEntry(
            artifact_id=resolved_artifact_id,
            artifact_type=artifact_type,
            run_id=run_id,
            path=str(resolved_path),
            sha256=sha256_file(resolved_path),
            created_at=created_at or _utc_now_iso(),
            producer=producer,
            code_revision=str(code_revision).strip() or None if code_revision is not None else None,
            result_id=str(result_id).strip() or None if result_id is not None else None,
            parent_artifact_ids=tuple(str(item).strip() for item in (parent_artifact_ids or []) if str(item).strip()),
            metadata=metadata,
        )
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(entry.as_dict(), ensure_ascii=False, sort_keys=True) + "\n")
        return entry

    def list_entries(
        self,
        *,
        run_id: str | None = None,
        artifact_type: str | None = None,
        result_id: str | None = None,
    ) -> list[dict[str, object]]:
        if not self.path.exists():
            return []
        entries: list[dict[str, object]] = []
        for line in self.path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            payload = json.loads(line)
            if not isinstance(payload, dict):
                raise ValueError(f"expected object jsonl entry: {self.path}")
            if run_id is not None and payload.get("run_id") != run_id:
                continue
            if artifact_type is not None and payload.get("artifact_type") != artifact_type:
                continue
            if result_id is not None and payload.get("result_id") != result_id:
                continue
            entries.append(payload)
        return entries
