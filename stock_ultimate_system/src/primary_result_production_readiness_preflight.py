from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from src.artifact_registry import sha256_file
from src.utils.project_paths import resolve_project_path


PRIMARY_RESULT_PRODUCTION_READINESS_PREFLIGHT_VERSION = "primary_result_production_readiness_preflight.v1"


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


def _check(name: str, passed: bool, detail: str, *, path: Path | None = None) -> dict[str, object]:
    return {
        "name": name,
        "passed": passed,
        "detail": detail,
        "path": str(path) if path is not None else None,
    }


def _pointer_target(pointer: dict[str, object], key: str) -> Path | None:
    value = pointer.get(key)
    if not value:
        return None
    return Path(str(value))


def build_primary_result_production_readiness_preflight(
    *,
    release_decision_current_path: str | Path = "artifacts/primary_result_release_decisions/current.json",
    baseline_current_path: str | Path = "artifacts/baselines/current.json",
    terminal_path: str | Path = "data/experiments/primary_result_terminal_latest.json",
    performance_ledger_path: str | Path = "artifacts/primary_result_performance/ledger.jsonl",
    performance_summary_path: str | Path = "artifacts/primary_result_performance/summary.json",
    output_path: str | Path | None = None,
) -> tuple[int, dict[str, object]]:
    decision_current = resolve_project_path(release_decision_current_path)
    baseline_current = resolve_project_path(baseline_current_path)
    terminal_file = resolve_project_path(terminal_path)
    ledger_file = resolve_project_path(performance_ledger_path)
    summary_file = resolve_project_path(performance_summary_path)

    checks: list[dict[str, object]] = []
    missing_artifacts: list[str] = []

    decision_pointer: dict[str, object] = {}
    decision_path: Path | None = None
    decision_payload: dict[str, object] = {}
    checks.append(
        _check(
            "release_decision_current_exists",
            decision_current.exists(),
            "release decision current pointer must exist",
            path=decision_current,
        )
    )
    if decision_current.exists():
        decision_pointer = _read_json(decision_current)
        decision_path = _pointer_target(decision_pointer, "decision_path")
    else:
        missing_artifacts.append("release_decision_current")
    checks.append(
        _check(
            "release_decision_pointer_active",
            bool(decision_path),
            "release decision current pointer must reference a decision snapshot",
            path=decision_current,
        )
    )
    if decision_path is None:
        missing_artifacts.append("release_decision_snapshot")
    elif not decision_path.exists():
        missing_artifacts.append("release_decision_snapshot")
    checks.append(
        _check(
            "release_decision_snapshot_exists",
            decision_path is not None and decision_path.exists(),
            "release decision snapshot must exist",
            path=decision_path,
        )
    )
    if decision_path is not None and decision_path.exists():
        decision_payload = _read_json(decision_path)
    checks.append(
        _check(
            "release_decision_approved",
            decision_payload.get("decision_version") == "primary_result_release_decision.v1"
            and decision_payload.get("decision") == "approved"
            and decision_payload.get("baseline_promotion_allowed") is True,
            "release decision must be approved and allow baseline promotion",
            path=decision_path,
        )
    )

    baseline_pointer: dict[str, object] = {}
    baseline_snapshot_path: Path | None = None
    baseline_snapshot: dict[str, object] = {}
    checks.append(
        _check(
            "baseline_current_exists",
            baseline_current.exists(),
            "baseline current pointer must exist",
            path=baseline_current,
        )
    )
    if baseline_current.exists():
        baseline_pointer = _read_json(baseline_current)
        baseline_snapshot_path = _pointer_target(baseline_pointer, "snapshot_path")
    else:
        missing_artifacts.append("baseline_current")
    checks.append(
        _check(
            "baseline_pointer_active",
            bool(baseline_pointer.get("baseline_id")) and bool(baseline_snapshot_path),
            "baseline current pointer must reference an immutable snapshot",
            path=baseline_current,
        )
    )
    if baseline_snapshot_path is None:
        missing_artifacts.append("baseline_snapshot")
    elif not baseline_snapshot_path.exists():
        missing_artifacts.append("baseline_snapshot")
    checks.append(
        _check(
            "baseline_snapshot_exists",
            baseline_snapshot_path is not None and baseline_snapshot_path.exists(),
            "baseline snapshot must exist",
            path=baseline_snapshot_path,
        )
    )
    if baseline_snapshot_path is not None and baseline_snapshot_path.exists():
        baseline_snapshot = _read_json(baseline_snapshot_path)
    expected_decision_hash = sha256_file(decision_path) if decision_path is not None and decision_path.exists() else None
    checks.append(
        _check(
            "baseline_binds_release_decision",
            bool(expected_decision_hash)
            and baseline_snapshot.get("release_decision_hash") == expected_decision_hash
            and baseline_snapshot.get("release_decision_path") == str(decision_path),
            "baseline snapshot must bind to the approved release decision",
            path=baseline_snapshot_path,
        )
    )

    terminal_payload: dict[str, object] = {}
    if not terminal_file.exists():
        missing_artifacts.append("terminal")
    checks.append(
        _check(
            "terminal_exists",
            terminal_file.exists(),
            "terminal artifact must exist",
            path=terminal_file,
        )
    )
    if terminal_file.exists():
        terminal_payload = _read_json(terminal_file)
    checks.append(
        _check(
            "terminal_success",
            terminal_payload.get("terminal_version") == "primary_result_terminal.v1"
            and terminal_payload.get("terminal_outcome") == "success",
            "terminal artifact must record explicit success",
            path=terminal_file,
        )
    )

    performance_entries = _load_jsonl(ledger_file)
    performance_summary: dict[str, object] = {}
    if not ledger_file.exists():
        missing_artifacts.append("performance_ledger")
    if not summary_file.exists():
        missing_artifacts.append("performance_summary")
    checks.append(
        _check(
            "performance_ledger_exists",
            ledger_file.exists(),
            "performance ledger must exist",
            path=ledger_file,
        )
    )
    checks.append(
        _check(
            "performance_summary_exists",
            summary_file.exists(),
            "performance summary must exist",
            path=summary_file,
        )
    )
    if summary_file.exists():
        performance_summary = _read_json(summary_file)
    latest_entry_id = performance_summary.get("latest_entry_id")
    latest_entry = next(
        (entry for entry in reversed(performance_entries) if entry.get("entry_id") == latest_entry_id),
        performance_entries[-1] if performance_entries else {},
    )
    checks.append(
        _check(
            "performance_summary_has_entries",
            performance_summary.get("summary_version") == "primary_result_performance_summary.v1"
            and int(performance_summary.get("entry_total", 0) or 0) > 0,
            "performance summary must contain at least one closed observation",
            path=summary_file,
        )
    )
    checks.append(
        _check(
            "latest_performance_success",
            latest_entry.get("ledger_version") == "primary_result_performance_ledger.v1"
            and latest_entry.get("outcome") == "success",
            "latest performance ledger entry must be success",
            path=ledger_file,
        )
    )

    blocking_checks = [check for check in checks if check["passed"] is not True]
    payload = {
        "preflight_version": PRIMARY_RESULT_PRODUCTION_READINESS_PREFLIGHT_VERSION,
        "generated_at": _utc_now_iso(),
        "status": "ready" if not blocking_checks else "blocked",
        "baseline_id": baseline_pointer.get("baseline_id"),
        "decision_id": decision_pointer.get("decision_id"),
        "missing_artifacts": sorted(set(missing_artifacts)),
        "checks": checks,
        "blocking_reasons": [str(check["detail"]) for check in blocking_checks],
        "production_boundary": (
            "production readiness preflight is diagnostics only; it does not write readiness history, deploy, trade, "
            "promote baseline, or change strategy state"
        ),
    }
    if output_path:
        _write_json(resolve_project_path(output_path), payload)
    return (0 if payload["status"] == "ready" else 1), payload
