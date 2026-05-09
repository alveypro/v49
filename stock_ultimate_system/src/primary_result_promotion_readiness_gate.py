from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.utils.project_paths import resolve_artifacts_path, resolve_project_path


PRIMARY_RESULT_PROMOTION_READINESS_GATE_VERSION = "primary_result_promotion_readiness_gate.v1"


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"expected object json: {path}")
    return payload


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _check(name: str, passed: bool, detail: str, *, severity: str = "blocking", details: dict[str, Any] | None = None) -> dict[str, Any]:
    return {
        "name": name,
        "passed": passed,
        "severity": severity,
        "detail": detail,
        "details": details or {},
    }


def _queue_open_counts(summary: dict[str, Any]) -> tuple[int, int, int]:
    status_counts = summary.get("status_counts")
    if not isinstance(status_counts, dict):
        status_counts = {}
    open_total = int(status_counts.get("open") or 0)
    open_high = int(summary.get("open_high_severity_total") or 0)
    open_high_priority = int(summary.get("open_high_priority_total") or 0)
    return open_total, open_high, open_high_priority


def _first_floor_status(performance_evidence: dict[str, Any]) -> list[dict[str, Any]]:
    streams = performance_evidence.get("streams")
    if not isinstance(streams, list):
        return []
    floors: list[dict[str, Any]] = []
    for stream in streams:
        if not isinstance(stream, dict):
            continue
        windows = stream.get("windows")
        if isinstance(windows, list) and windows:
            first_window = windows[0] if isinstance(windows[0], dict) else {}
        else:
            first_window = {}
        floors.append(
            {
                "stream_id": stream.get("stream_id"),
                "stream_status": stream.get("status"),
                "entry_total": stream.get("entry_total"),
                "first_floor": first_window.get("floor"),
                "first_floor_status": first_window.get("status"),
                "blocking_reasons": first_window.get("blocking_reasons", []),
            }
        )
    return floors


def build_primary_result_promotion_readiness_gate(
    *,
    performance_evidence_path: str | Path = "artifacts/primary_result_performance_evidence_latest.json",
    feedback_queue_summary_path: str | Path = "artifacts/primary_result_feedback_review_queue/summary.json",
    baseline_current_path: str | Path = "artifacts/baselines/current.json",
    output_path: str | Path | None = None,
) -> tuple[int, dict[str, Any]]:
    evidence_path = resolve_artifacts_path(performance_evidence_path)
    queue_path = resolve_artifacts_path(feedback_queue_summary_path)
    baseline_path = resolve_artifacts_path(baseline_current_path)

    evidence = _read_json(evidence_path)
    queue_summary = _read_json(queue_path)
    baseline_current = _read_json(baseline_path)
    open_total, open_high, open_high_priority = _queue_open_counts(queue_summary)
    first_floor_statuses = _first_floor_status(evidence)

    checks = [
        _check("performance_evidence_exists", bool(evidence), "performance evidence report must exist", details={"path": str(evidence_path)}),
        _check(
            "performance_evidence_ready",
            evidence.get("evidence_version") == "primary_result_performance_evidence.v1" and evidence.get("status") == "ready",
            "performance evidence must be ready before promotion review",
            details={"status": evidence.get("status"), "first_floor_statuses": first_floor_statuses},
        ),
        _check(
            "feedback_queue_summary_exists",
            bool(queue_summary),
            "feedback review queue summary should exist for governance visibility",
            severity="warning",
            details={"path": str(queue_path)},
        ),
        _check(
            "no_open_high_severity_review_items",
            open_high == 0,
            "open high-severity review items block promotion",
            details={"open_high_severity_total": open_high},
        ),
        _check(
            "no_open_high_priority_review_items",
            open_high_priority == 0,
            "open high-priority review items block promotion until benchmark and review handling is completed",
            details={"open_high_priority_total": open_high_priority},
        ),
        _check(
            "no_open_review_items_requiring_manual_closure",
            open_total == 0,
            "open review items should be closed or explicitly decided before promotion",
            details={"open_total": open_total},
        ),
        _check(
            "baseline_registry_exists",
            bool(baseline_current),
            "baseline current pointer should exist before promotion review",
            severity="warning",
            details={"path": str(baseline_path), "baseline_id": baseline_current.get("baseline_id")},
        ),
    ]
    blocking = [check for check in checks if check["severity"] == "blocking" and check["passed"] is not True]
    if not evidence:
        decision = "blocked"
    elif evidence.get("status") == "failed":
        decision = "freeze"
    elif blocking:
        decision = "blocked"
    else:
        decision = "promotion_review_allowed"
    payload = {
        "gate_version": PRIMARY_RESULT_PROMOTION_READINESS_GATE_VERSION,
        "generated_at": _utc_now_iso(),
        "status": "passed" if decision == "promotion_review_allowed" else "blocked",
        "decision": decision,
        "checks": checks,
        "blocking_reasons": [str(check["detail"]) for check in blocking],
        "first_floor_statuses": first_floor_statuses,
        "baseline_id": baseline_current.get("baseline_id"),
        "next_actions": _next_actions(decision=decision, evidence=evidence, open_total=open_total, open_high=open_high),
        "review_queue_priority": {
            "open_total": open_total,
            "open_high_severity_total": open_high,
            "open_high_priority_total": open_high_priority,
            "priority_counts": queue_summary.get("priority_counts", {}),
            "taxonomy_hotspots": queue_summary.get("taxonomy_hotspots", {}),
        },
        "evidence_paths": {
            "performance_evidence": str(evidence_path),
            "feedback_queue_summary": str(queue_path),
            "baseline_current": str(baseline_path),
        },
        "production_boundary": (
            "promotion readiness gate only decides whether governed promotion review is allowed; "
            "it does not promote baselines, mutate strategy rules, trade, or deploy"
        ),
    }
    if output_path:
        _write_json(resolve_project_path(output_path), payload)
    return (0 if payload["status"] == "passed" else 1), payload


def _next_actions(*, decision: str, evidence: dict[str, Any], open_total: int, open_high: int) -> list[str]:
    if decision == "promotion_review_allowed":
        return ["promotion review is allowed; require explicit release decision before baseline promotion"]
    actions: list[str] = []
    if not evidence:
        actions.append("build performance evidence before any promotion review")
    elif evidence.get("status") == "accumulating":
        actions.extend(str(item) for item in evidence.get("next_actions", []) or [])
    elif evidence.get("status") == "failed":
        actions.append("freeze promotion and route failed evidence to governed review")
    if open_high:
        actions.append(f"close or decide {open_high} open high-severity review items")
    if open_total:
        actions.append(f"close or decide {open_total} open review items before promotion")
    if not actions:
        actions.append("resolve blocking checks before promotion review")
    return actions
