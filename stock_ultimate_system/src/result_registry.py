from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from src.utils.project_paths import resolve_project_path


RESULT_REGISTRY_ENTRY_VERSION = "result_registry_entry.v1"
RESULT_REGISTRY_CURRENT_POINTER_VERSION = "result_registry_current_pointer.v1"


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


def _normalize_record_id(value: str) -> str:
    record_id = _normalize_text(value)
    if not record_id:
        raise ValueError("record_id must not be empty")
    allowed = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_")
    if any(ch not in allowed for ch in record_id):
        raise ValueError("record_id must contain only letters, numbers, '-' or '_'")
    return record_id


def _safe_id_part(value: object) -> str:
    text = _normalize_text(value).lower()
    text = re.sub(r"[^a-z0-9]+", "-", text)
    return text.strip("-") or "unknown"


def _default_record_id(result_id: str, lifecycle_stage: str, registered_at: str) -> str:
    return _normalize_record_id(
        f"result-record-{_safe_id_part(result_id)}-{_safe_id_part(lifecycle_stage)}-{_safe_id_part(registered_at)}"
    )


@dataclass(frozen=True)
class ResultRegistryEntry:
    record_id: str
    result_id: str
    run_id: str
    ts_code: str
    stock_name: str | None
    lifecycle_stage: str
    artifact_ids: tuple[str, ...]
    registered_at: str
    source_scope: str
    schema_version: str = RESULT_REGISTRY_ENTRY_VERSION
    metadata: dict[str, object] | None = None

    def as_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "record_id": self.record_id,
            "result_id": self.result_id,
            "run_id": self.run_id,
            "ts_code": self.ts_code,
            "stock_name": self.stock_name,
            "lifecycle_stage": self.lifecycle_stage,
            "artifact_ids": list(self.artifact_ids),
            "registered_at": self.registered_at,
            "source_scope": self.source_scope,
            "metadata": dict(self.metadata or {}),
        }


class ResultRegistry:
    def __init__(self, *, results_dir: str | Path = "artifacts/result_registry") -> None:
        self.results_dir = resolve_project_path(results_dir)
        self.history_dir = self.results_dir / "history"
        self.current_path = self.results_dir / "current.json"
        self.ensure_layout()

    def ensure_layout(self) -> None:
        self.history_dir.mkdir(parents=True, exist_ok=True)
        if not self.current_path.exists():
            _write_json(
                self.current_path,
                {
                    "pointer_version": RESULT_REGISTRY_CURRENT_POINTER_VERSION,
                    "record_id": None,
                    "result_id": None,
                    "run_id": None,
                    "source_scope": None,
                    "updated_at": None,
                    "entry_path": None,
                },
            )

    def get_current_pointer(self) -> dict[str, object]:
        return _read_json(self.current_path)

    def get_record(self, record_id: str) -> dict[str, object]:
        resolved_record_id = _normalize_record_id(record_id)
        path = self.history_dir / f"{resolved_record_id}.json"
        if not path.exists():
            raise FileNotFoundError(f"result registry record not found: {path}")
        payload = _read_json(path)
        payload["_entry_path"] = str(path)
        return payload

    def list_records(self, *, result_id: str | None = None, run_id: str | None = None) -> list[dict[str, object]]:
        entries: list[dict[str, object]] = []
        for path in sorted(self.history_dir.glob("*.json")):
            payload = _read_json(path)
            if result_id is not None and payload.get("result_id") != result_id:
                continue
            if run_id is not None and payload.get("run_id") != run_id:
                continue
            payload["_entry_path"] = str(path)
            entries.append(payload)
        entries.sort(key=lambda item: (str(item.get("registered_at", "")), str(item.get("record_id", ""))))
        return entries

    def get_latest_record_for_result(self, result_id: str) -> dict[str, object] | None:
        records = self.list_records(result_id=_normalize_text(result_id))
        return records[-1] if records else None

    def register(
        self,
        *,
        result_id: str,
        run_id: str,
        ts_code: str,
        lifecycle_stage: str,
        artifact_ids: list[str] | tuple[str, ...],
        stock_name: str | None = None,
        source_scope: str = "stock",
        registered_at: str | None = None,
        record_id: str | None = None,
        make_current: bool = False,
        metadata: dict[str, object] | None = None,
    ) -> dict[str, object]:
        normalized_result_id = _normalize_text(result_id)
        normalized_run_id = _normalize_text(run_id)
        normalized_ts_code = _normalize_text(ts_code)
        normalized_lifecycle_stage = _normalize_text(lifecycle_stage)
        normalized_artifact_ids = tuple(_normalize_text(item) for item in artifact_ids if _normalize_text(item))
        normalized_source_scope = _normalize_text(source_scope)
        normalized_stock_name = _normalize_text(stock_name) if stock_name is not None else None

        if not normalized_result_id:
            raise ValueError("result_id is required")
        if not normalized_run_id:
            raise ValueError("run_id is required")
        if not normalized_ts_code:
            raise ValueError("ts_code is required")
        if not normalized_lifecycle_stage:
            raise ValueError("lifecycle_stage is required")
        if not normalized_artifact_ids:
            raise ValueError("artifact_ids must not be empty")
        if not normalized_source_scope:
            raise ValueError("source_scope is required")

        resolved_registered_at = registered_at or _utc_now_iso()
        resolved_record_id = _normalize_record_id(
            record_id or _default_record_id(normalized_result_id, normalized_lifecycle_stage, resolved_registered_at)
        )
        path = self.history_dir / f"{resolved_record_id}.json"
        if path.exists():
            raise FileExistsError(f"result registry record already exists: {path}")

        entry = ResultRegistryEntry(
            record_id=resolved_record_id,
            result_id=normalized_result_id,
            run_id=normalized_run_id,
            ts_code=normalized_ts_code,
            stock_name=normalized_stock_name or None,
            lifecycle_stage=normalized_lifecycle_stage,
            artifact_ids=normalized_artifact_ids,
            registered_at=resolved_registered_at,
            source_scope=normalized_source_scope,
            metadata=metadata,
        )
        _write_json(path, entry.as_dict())
        if make_current:
            _write_json(
                self.current_path,
                {
                    "pointer_version": RESULT_REGISTRY_CURRENT_POINTER_VERSION,
                    "record_id": resolved_record_id,
                    "result_id": normalized_result_id,
                    "run_id": normalized_run_id,
                    "source_scope": normalized_source_scope,
                    "updated_at": resolved_registered_at,
                    "entry_path": str(path),
                },
            )
        return entry.as_dict()
