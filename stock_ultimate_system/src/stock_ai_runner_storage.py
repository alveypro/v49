from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.stock_ai_provider_adapter import build_stock_ai_provider_telemetry_summary
from src.utils.project_paths import resolve_project_path


STOCK_AI_RUNNER_STORAGE_SCHEMA_VERSION = "stock_ai_runner_storage.v1"
STOCK_AI_RUNNER_HISTORY_LIMIT = 32
STOCK_AI_RUNNER_RECENT_ATTEMPT_LIMIT = 8
_FAILURE_STATES = {"timeout", "blocked", "error"}


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        payload = json.loads(line)
        if not isinstance(payload, dict):
            raise ValueError(f"expected object jsonl entry: {path}")
        rows.append(payload)
    return rows


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    rendered = "\n".join(json.dumps(row, ensure_ascii=False, sort_keys=True) for row in rows)
    path.write_text(rendered + ("\n" if rendered else ""), encoding="utf-8")


def _truncate_rows(rows: list[dict[str, Any]], *, limit: int = STOCK_AI_RUNNER_HISTORY_LIMIT) -> list[dict[str, Any]]:
    if len(rows) <= limit:
        return rows
    return rows[-limit:]


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _normalize_recorded_at(value: Any) -> str:
    text = str(value or "").strip()
    return text or _utc_now_iso()


def _attempt_snapshot(row: dict[str, Any]) -> dict[str, Any]:
    state = str(row.get("state", "") or "")
    return {
        "result_id": str(row.get("result_id", "") or ""),
        "provider_name": str(row.get("provider_name", "") or ""),
        "request_id": str(row.get("request_id", "") or ""),
        "recorded_at": _normalize_recorded_at(row.get("recorded_at")),
        "state": state,
        "reason": str(row.get("reason", "") or ""),
        "final_status": str(row.get("final_status", "") or ""),
        "is_problem": state in _FAILURE_STATES,
    }


def _telemetry_snapshot(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "provider_name": str(row.get("provider_name", "") or ""),
        "request_id": str(row.get("request_id", "") or ""),
        "result_id": str(row.get("result_id", "") or ""),
        "recorded_at": _normalize_recorded_at(row.get("recorded_at")),
        "status": str(row.get("status", "") or ""),
        "status_code": int(row.get("status_code", 0) or 0),
        "elapsed_ms": int(row.get("elapsed_ms", 0) or 0),
        "response_bytes": int(row.get("response_bytes", 0) or 0),
        "final_status": str(row.get("final_status", "") or ""),
    }


@dataclass
class StockAIRunnerStorage:
    storage_dir: Path

    @classmethod
    def from_path(cls, storage_dir: str | Path = "artifacts/stock_ai_runner") -> "StockAIRunnerStorage":
        return cls(storage_dir=resolve_project_path(storage_dir))

    @property
    def ledger_path(self) -> Path:
        return self.storage_dir / "attempt_ledger.jsonl"

    @property
    def telemetry_path(self) -> Path:
        return self.storage_dir / "telemetry.jsonl"

    @property
    def provider_summary_path(self) -> Path:
        return self.storage_dir / "provider_summary.json"

    @property
    def read_model_path(self) -> Path:
        return self.storage_dir / "read_model.json"

    def load_attempt_ledger_rows(self) -> list[dict[str, Any]]:
        return _read_jsonl(self.ledger_path)

    def load_telemetry_rows(self) -> list[dict[str, Any]]:
        return _read_jsonl(self.telemetry_path)

    def build_read_model(self) -> dict[str, Any]:
        ledger_rows = self.load_attempt_ledger_rows()
        telemetry_rows = self.load_telemetry_rows()

        provider_groups: dict[str, list[dict[str, Any]]] = {}
        for row in telemetry_rows:
            provider_groups.setdefault(str(row.get("provider_name", "") or "unknown"), []).append(row)
        provider_latest_telemetry: dict[str, dict[str, Any]] = {}
        for row in telemetry_rows:
            snapshot = _telemetry_snapshot(row)
            provider_name = snapshot["provider_name"] or "unknown"
            provider_latest_telemetry[provider_name] = snapshot

        recent_attempts = [_attempt_snapshot(row) for row in ledger_rows[-STOCK_AI_RUNNER_RECENT_ATTEMPT_LIMIT:]]

        failure_buckets: dict[str, int] = {}
        failure_top_cause_buckets: dict[str, dict[str, Any]] = {}
        provider_latest_status: dict[str, dict[str, Any]] = {}
        result_recent_attempts: dict[str, list[dict[str, Any]]] = {}
        for row in ledger_rows:
            snapshot = _attempt_snapshot(row)
            state = snapshot["state"]
            provider_name = snapshot["provider_name"] or "unknown"
            result_id = snapshot["result_id"] or "primary:unavailable"
            provider_latest_status[provider_name] = snapshot
            result_recent_attempts.setdefault(result_id, []).append(snapshot)
            if len(result_recent_attempts[result_id]) > STOCK_AI_RUNNER_RECENT_ATTEMPT_LIMIT:
                result_recent_attempts[result_id] = result_recent_attempts[result_id][-STOCK_AI_RUNNER_RECENT_ATTEMPT_LIMIT:]
            if state not in _FAILURE_STATES:
                continue
            failure_buckets[state] = failure_buckets.get(state, 0) + 1
            failure_cause = snapshot["reason"] or state
            bucket = failure_top_cause_buckets.setdefault(
                failure_cause,
                {
                    "reason": failure_cause,
                    "count": 0,
                    "latest_state": state,
                    "latest_provider_name": provider_name,
                    "latest_result_id": result_id,
                },
            )
            bucket["count"] = int(bucket.get("count", 0) or 0) + 1
            bucket["latest_state"] = state
            bucket["latest_provider_name"] = provider_name
            bucket["latest_result_id"] = result_id

        failure_top_causes = sorted(
            failure_top_cause_buckets.values(),
            key=lambda item: (-int(item.get("count", 0) or 0), str(item.get("reason", "") or "")),
        )

        provider_latest_health: dict[str, dict[str, Any]] = {}
        all_provider_names = sorted(set(provider_groups) | set(provider_latest_status) | set(provider_latest_telemetry))
        for provider_name in all_provider_names:
            latest_attempt = dict(provider_latest_status.get(provider_name, {}) or {})
            latest_telemetry = dict(provider_latest_telemetry.get(provider_name, {}) or {})
            latest_state = str(latest_attempt.get("state", "") or latest_telemetry.get("status", "") or "")
            provider_latest_health[provider_name] = {
                "provider_name": provider_name,
                "latest_state": latest_state,
                "is_problem": bool(latest_attempt.get("is_problem")) or latest_state in _FAILURE_STATES,
                "last_request_id": str(
                    latest_attempt.get("request_id", "") or latest_telemetry.get("request_id", "") or ""
                ),
                "last_recorded_at": str(
                    latest_attempt.get("recorded_at", "") or latest_telemetry.get("recorded_at", "") or ""
                ),
                "last_result_id": str(
                    latest_attempt.get("result_id", "") or latest_telemetry.get("result_id", "") or ""
                ),
                "last_reason": str(latest_attempt.get("reason", "") or ""),
                "last_status_code": int(latest_telemetry.get("status_code", 0) or 0),
                "last_elapsed_ms": int(latest_telemetry.get("elapsed_ms", 0) or 0),
                "last_response_bytes": int(latest_telemetry.get("response_bytes", 0) or 0),
                "last_final_status": str(
                    latest_attempt.get("final_status", "") or latest_telemetry.get("final_status", "") or ""
                ),
            }

        return {
            "schema_version": STOCK_AI_RUNNER_STORAGE_SCHEMA_VERSION,
            "history_limit": STOCK_AI_RUNNER_HISTORY_LIMIT,
            "recent_attempt_limit": STOCK_AI_RUNNER_RECENT_ATTEMPT_LIMIT,
            "provider_rollups": {
                provider: build_stock_ai_provider_telemetry_summary(rows)
                for provider, rows in sorted(provider_groups.items())
            },
            "provider_latest_status": provider_latest_status,
            "provider_latest_health": provider_latest_health,
            "recent_attempts": recent_attempts,
            "failure_buckets": failure_buckets,
            "failure_top_causes": failure_top_causes,
            "result_recent_attempts": result_recent_attempts,
        }

    def persist(
        self,
        *,
        result_id: str,
        provider_name: str,
        final_status: str,
        attempt_ledger: list[dict[str, Any]],
        telemetry_buffer: list[dict[str, Any]],
    ) -> dict[str, Any]:
        ledger_rows = self.load_attempt_ledger_rows()
        telemetry_rows = self.load_telemetry_rows()

        for row in attempt_ledger:
            ledger_rows.append(
                {
                    "schema_version": STOCK_AI_RUNNER_STORAGE_SCHEMA_VERSION,
                    "result_id": str(result_id or "").strip(),
                    "provider_name": str(provider_name or "").strip(),
                    "final_status": str(final_status or "").strip(),
                    "recorded_at": _normalize_recorded_at(row.get("recorded_at")),
                    **dict(row),
                }
            )
        for row in telemetry_buffer:
            telemetry_rows.append(
                {
                    "schema_version": STOCK_AI_RUNNER_STORAGE_SCHEMA_VERSION,
                    "result_id": str(result_id or "").strip(),
                    "provider_name": str(provider_name or "").strip(),
                    "final_status": str(final_status or "").strip(),
                    "recorded_at": _normalize_recorded_at(row.get("recorded_at")),
                    **dict(row),
                }
            )

        ledger_rows = _truncate_rows(ledger_rows)
        telemetry_rows = _truncate_rows(telemetry_rows)
        _write_jsonl(self.ledger_path, ledger_rows)
        _write_jsonl(self.telemetry_path, telemetry_rows)

        read_model = self.build_read_model()
        provider_summary = {
            "schema_version": STOCK_AI_RUNNER_STORAGE_SCHEMA_VERSION,
            "history_limit": STOCK_AI_RUNNER_HISTORY_LIMIT,
            "providers": dict(read_model.get("provider_rollups", {})),
        }
        self.provider_summary_path.parent.mkdir(parents=True, exist_ok=True)
        self.provider_summary_path.write_text(json.dumps(provider_summary, ensure_ascii=False, sort_keys=True), encoding="utf-8")
        self.read_model_path.write_text(json.dumps(read_model, ensure_ascii=False, sort_keys=True), encoding="utf-8")
        return {
            "ledger_path": str(self.ledger_path),
            "telemetry_path": str(self.telemetry_path),
            "provider_summary_path": str(self.provider_summary_path),
            "read_model_path": str(self.read_model_path),
            "ledger_entries": len(ledger_rows),
            "telemetry_entries": len(telemetry_rows),
        }
