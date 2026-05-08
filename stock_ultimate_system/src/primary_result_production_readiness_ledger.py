from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path

from src.artifact_registry import sha256_file
from src.utils.project_paths import resolve_project_path


PRIMARY_RESULT_PRODUCTION_READINESS_VERSION = "primary_result_production_readiness.v1"
PRIMARY_RESULT_PRODUCTION_READINESS_POINTER_VERSION = "primary_result_production_readiness_pointer.v1"


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _read_json(path: Path) -> dict[str, object]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"expected object json: {path}")
    return payload


def _load_jsonl(path: Path) -> list[dict[str, object]]:
    if not path.exists():
        return []
    rows: list[dict[str, object]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        payload = json.loads(line)
        if not isinstance(payload, dict):
            raise ValueError(f"expected object jsonl entry: {path}")
        rows.append(payload)
    return rows


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _normalize_text(value: object) -> str:
    return str(value or "").strip()


def _safe_id_part(value: object) -> str:
    text = _normalize_text(value).lower()
    text = re.sub(r"[^a-z0-9]+", "-", text)
    return text.strip("-") or "unknown"


def _normalize_readiness_id(value: str) -> str:
    readiness_id = _normalize_text(value)
    if not readiness_id:
        raise ValueError("readiness_id must not be empty")
    allowed = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_")
    if any(ch not in allowed for ch in readiness_id):
        raise ValueError("readiness_id must contain only letters, numbers, '-' or '_'")
    return readiness_id


def _artifact_entry(artifact_id: str, path: Path | None) -> dict[str, object]:
    exists = path is not None and path.exists()
    return {
        "artifact_id": artifact_id,
        "path": str(path) if path is not None else None,
        "exists": exists,
        "sha256": sha256_file(path) if exists and path is not None else None,
    }


def _check(name: str, passed: bool, detail: str) -> dict[str, object]:
    return {"name": name, "passed": passed, "detail": detail}


def _default_readiness_id(decision: dict[str, object], baseline_pointer: dict[str, object]) -> str:
    return _normalize_readiness_id(
        "primary-production-readiness-"
        f"{_safe_id_part(decision.get('decision_id'))}-"
        f"{_safe_id_part(baseline_pointer.get('baseline_id'))}"
    )


class PrimaryResultProductionReadinessLedger:
    def __init__(
        self,
        *,
        readiness_dir: str | Path = "artifacts/primary_result_production_readiness",
    ) -> None:
        self.readiness_dir = resolve_project_path(readiness_dir)
        self.history_dir = self.readiness_dir / "history"
        self.current_path = self.readiness_dir / "current.json"
        self.ensure_layout()

    def ensure_layout(self) -> None:
        self.history_dir.mkdir(parents=True, exist_ok=True)
        if not self.current_path.exists():
            _write_json(
                self.current_path,
                {
                    "pointer_version": PRIMARY_RESULT_PRODUCTION_READINESS_POINTER_VERSION,
                    "readiness_id": None,
                    "readiness_path": None,
                    "status": None,
                    "baseline_id": None,
                    "updated_at": None,
                },
            )

    def get_current_pointer(self) -> dict[str, object]:
        return _read_json(self.current_path)

    def create_readiness(
        self,
        *,
        release_decision_path: str | Path,
        baseline_current_path: str | Path,
        terminal_path: str | Path,
        performance_ledger_path: str | Path,
        performance_summary_path: str | Path,
        readiness_id: str | None = None,
        generated_at: str | None = None,
    ) -> dict[str, object]:
        decision_path = resolve_project_path(release_decision_path)
        current_path = resolve_project_path(baseline_current_path)
        terminal_file = resolve_project_path(terminal_path)
        ledger_path = resolve_project_path(performance_ledger_path)
        summary_path = resolve_project_path(performance_summary_path)

        decision = _read_json(decision_path)
        baseline_pointer = _read_json(current_path)
        terminal = _read_json(terminal_file)
        performance_entries = _load_jsonl(ledger_path)
        performance_summary = _read_json(summary_path)

        resolved_readiness_id = _normalize_readiness_id(
            readiness_id or _default_readiness_id(decision, baseline_pointer)
        )
        readiness_path = self.history_dir / f"{resolved_readiness_id}.json"
        if readiness_path.exists():
            raise FileExistsError(f"primary result production readiness already exists: {readiness_path}")

        baseline_snapshot_path = baseline_pointer.get("snapshot_path")
        baseline_snapshot_file = Path(str(baseline_snapshot_path)) if baseline_snapshot_path else None
        baseline_snapshot = _read_json(baseline_snapshot_file) if baseline_snapshot_file and baseline_snapshot_file.exists() else {}
        latest_entry_id = performance_summary.get("latest_entry_id")
        latest_entry = next(
            (entry for entry in reversed(performance_entries) if entry.get("entry_id") == latest_entry_id),
            performance_entries[-1] if performance_entries else {},
        )

        decision_hash = sha256_file(decision_path)
        checks = [
            _check(
                "release_decision_approved",
                decision.get("decision_version") == "primary_result_release_decision.v1"
                and decision.get("decision") == "approved"
                and decision.get("baseline_promotion_allowed") is True,
                "release decision must be approved and allow baseline promotion",
            ),
            _check(
                "baseline_pointer_active",
                bool(baseline_pointer.get("baseline_id")) and bool(baseline_pointer.get("snapshot_path")),
                "baseline current pointer must reference an immutable snapshot",
            ),
            _check(
                "baseline_snapshot_exists",
                bool(baseline_snapshot),
                "baseline snapshot file must exist",
            ),
            _check(
                "baseline_uses_release_decision",
                bool(baseline_snapshot)
                and baseline_snapshot.get("release_decision_hash") == decision_hash
                and baseline_snapshot.get("release_decision_path") == str(decision_path),
                "baseline snapshot must bind to the approved release decision hash",
            ),
            _check(
                "terminal_success",
                terminal.get("terminal_version") == "primary_result_terminal.v1"
                and terminal.get("terminal_outcome") == "success",
                "terminal artifact must record explicit success",
            ),
            _check(
                "performance_summary_present",
                performance_summary.get("summary_version") == "primary_result_performance_summary.v1"
                and int(performance_summary.get("entry_total", 0) or 0) > 0,
                "performance summary must contain at least one closed observation",
            ),
            _check(
                "latest_performance_success",
                latest_entry.get("ledger_version") == "primary_result_performance_ledger.v1"
                and latest_entry.get("outcome") == "success",
                "latest performance ledger entry must be success",
            ),
        ]
        blocking_reasons = [str(check["detail"]) for check in checks if check["passed"] is not True]
        status = "ready" if not blocking_reasons else "blocked"

        payload = {
            "readiness_version": PRIMARY_RESULT_PRODUCTION_READINESS_VERSION,
            "readiness_id": resolved_readiness_id,
            "generated_at": generated_at or _utc_now_iso(),
            "status": status,
            "baseline_id": baseline_pointer.get("baseline_id"),
            "decision_id": decision.get("decision_id"),
            "terminal_outcome": terminal.get("terminal_outcome"),
            "latest_performance_entry_id": latest_entry.get("entry_id"),
            "checks": checks,
            "blocking_reasons": blocking_reasons,
            "artifacts": [
                _artifact_entry("release_decision", decision_path),
                _artifact_entry("baseline_current_pointer", current_path),
                _artifact_entry("baseline_snapshot", baseline_snapshot_file),
                _artifact_entry("terminal", terminal_file),
                _artifact_entry("performance_ledger", ledger_path),
                _artifact_entry("performance_summary", summary_path),
            ],
            "do_not_auto_apply": True,
            "production_boundary": (
                "production readiness ledger is an evidence conclusion only; it does not deploy, trade, "
                "promote baseline, or change strategy state"
            ),
        }
        _write_json(readiness_path, payload)
        _write_json(
            self.current_path,
            {
                "pointer_version": PRIMARY_RESULT_PRODUCTION_READINESS_POINTER_VERSION,
                "readiness_id": resolved_readiness_id,
                "readiness_path": str(readiness_path),
                "status": status,
                "baseline_id": baseline_pointer.get("baseline_id"),
                "updated_at": payload["generated_at"],
            },
        )
        return payload
