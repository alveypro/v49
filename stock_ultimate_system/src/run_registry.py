from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from src.utils.project_paths import resolve_project_path


RUN_REGISTRY_ENTRY_VERSION = "run_registry_entry.v1"
RUN_REGISTRY_CURRENT_POINTER_VERSION = "run_registry_current_pointer.v1"


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


def _normalize_run_id(value: str) -> str:
    run_id = _normalize_text(value)
    if not run_id:
        raise ValueError("run_id must not be empty")
    allowed = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_")
    if any(ch not in allowed for ch in run_id):
        raise ValueError("run_id must contain only letters, numbers, '-' or '_'")
    return run_id


@dataclass(frozen=True)
class RunRegistryEntry:
    run_id: str
    run_type: str
    status: str
    created_at: str
    producer: str
    config_hash: str
    data_snapshot_id: str
    code_revision: str
    schema_version: str = RUN_REGISTRY_ENTRY_VERSION
    metadata: dict[str, object] | None = None

    def as_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "run_id": self.run_id,
            "run_type": self.run_type,
            "status": self.status,
            "created_at": self.created_at,
            "producer": self.producer,
            "config_hash": self.config_hash,
            "data_snapshot_id": self.data_snapshot_id,
            "code_revision": self.code_revision,
            "metadata": dict(self.metadata or {}),
        }


class RunRegistry:
    def __init__(self, *, runs_dir: str | Path = "artifacts/run_registry") -> None:
        self.runs_dir = resolve_project_path(runs_dir)
        self.history_dir = self.runs_dir / "history"
        self.current_path = self.runs_dir / "current.json"
        self.ensure_layout()

    def ensure_layout(self) -> None:
        self.history_dir.mkdir(parents=True, exist_ok=True)
        if not self.current_path.exists():
            _write_json(
                self.current_path,
                {
                    "pointer_version": RUN_REGISTRY_CURRENT_POINTER_VERSION,
                    "run_id": None,
                    "run_type": None,
                    "updated_at": None,
                    "entry_path": None,
                },
            )

    def get_current_pointer(self) -> dict[str, object]:
        return _read_json(self.current_path)

    def get_run(self, run_id: str) -> dict[str, object]:
        resolved_run_id = _normalize_run_id(run_id)
        path = self.history_dir / f"{resolved_run_id}.json"
        if not path.exists():
            raise FileNotFoundError(f"run registry entry not found: {path}")
        payload = _read_json(path)
        payload["_entry_path"] = str(path)
        return payload

    def list_runs(self, *, run_type: str | None = None, status: str | None = None) -> list[dict[str, object]]:
        entries: list[dict[str, object]] = []
        for path in sorted(self.history_dir.glob("*.json")):
            payload = _read_json(path)
            if run_type is not None and payload.get("run_type") != run_type:
                continue
            if status is not None and payload.get("status") != status:
                continue
            payload["_entry_path"] = str(path)
            entries.append(payload)
        entries.sort(key=lambda item: (str(item.get("created_at", "")), str(item.get("run_id", ""))))
        return entries

    def register(
        self,
        *,
        run_id: str,
        run_type: str,
        producer: str,
        config_hash: str,
        data_snapshot_id: str,
        code_revision: str,
        status: str = "created",
        created_at: str | None = None,
        make_current: bool = False,
        metadata: dict[str, object] | None = None,
    ) -> dict[str, object]:
        normalized_run_id = _normalize_run_id(run_id)
        normalized_run_type = _normalize_text(run_type)
        normalized_status = _normalize_text(status)
        normalized_producer = _normalize_text(producer)
        normalized_config_hash = _normalize_text(config_hash)
        normalized_data_snapshot_id = _normalize_text(data_snapshot_id)
        normalized_code_revision = _normalize_text(code_revision)
        if not normalized_run_type:
            raise ValueError("run_type is required")
        if not normalized_status:
            raise ValueError("status is required")
        if not normalized_producer:
            raise ValueError("producer is required")
        if not normalized_config_hash:
            raise ValueError("config_hash is required")
        if not normalized_data_snapshot_id:
            raise ValueError("data_snapshot_id is required")
        if not normalized_code_revision:
            raise ValueError("code_revision is required")

        path = self.history_dir / f"{normalized_run_id}.json"
        if path.exists():
            raise FileExistsError(f"run registry entry already exists: {path}")

        resolved_created_at = created_at or _utc_now_iso()
        entry = RunRegistryEntry(
            run_id=normalized_run_id,
            run_type=normalized_run_type,
            status=normalized_status,
            created_at=resolved_created_at,
            producer=normalized_producer,
            config_hash=normalized_config_hash,
            data_snapshot_id=normalized_data_snapshot_id,
            code_revision=normalized_code_revision,
            metadata=metadata,
        )
        _write_json(path, entry.as_dict())
        if make_current:
            _write_json(
                self.current_path,
                {
                    "pointer_version": RUN_REGISTRY_CURRENT_POINTER_VERSION,
                    "run_id": normalized_run_id,
                    "run_type": normalized_run_type,
                    "updated_at": resolved_created_at,
                    "entry_path": str(path),
                },
            )
        return entry.as_dict()
