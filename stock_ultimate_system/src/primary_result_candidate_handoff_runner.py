from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from scripts.run_primary_result_lifecycle import run_primary_result_lifecycle
from src.primary_result_candidate_handoff_gate import build_primary_result_candidate_handoff_gate
from src.primary_result_lifecycle_registry import PrimaryResultLifecycleRegistry
from src.primary_result_observation_window_advisor import suggest_observation_window_start
from src.utils.project_paths import resolve_project_path


PRIMARY_RESULT_CANDIDATE_HANDOFF_RUNNER_VERSION = "primary_result_candidate_handoff_runner.v1"
DEFAULT_HANDOFF_GATE_OUTPUT = "artifacts/primary_result_candidate_handoff_gate_latest.json"
DEFAULT_LIFECYCLE_EVIDENCE_OUTPUT = "data/experiments/primary_result_lifecycle_evidence_latest.json"
DEFAULT_RUNNER_OUTPUT = "data/experiments/primary_result_candidate_handoff_latest.json"


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _safe_id_part(value: object) -> str:
    text = str(value or "").strip().lower()
    text = re.sub(r"[^a-z0-9]+", "-", text)
    return text.strip("-") or "unknown"


def _default_lifecycle_id(ts_code: object, now: str | None = None) -> str:
    stamp = _safe_id_part(now or _utc_now_iso())
    return f"primary-lifecycle-{stamp}-{_safe_id_part(ts_code)}"


def _resolve_default_scoped_output(
    value: str | Path | None,
    *,
    default_value: str,
    resolved_exp_dir: Path,
) -> str | Path | None:
    if value is None:
        return None
    if isinstance(value, Path) and value.is_absolute():
        return value
    text = str(value)
    if text == DEFAULT_LIFECYCLE_EVIDENCE_OUTPUT:
        return resolved_exp_dir / "primary_result_lifecycle_evidence_latest.json"
    if text == DEFAULT_RUNNER_OUTPUT:
        return resolved_exp_dir / "primary_result_candidate_handoff_latest.json"
    if text == DEFAULT_HANDOFF_GATE_OUTPUT:
        return resolved_exp_dir.parent.parent / "artifacts" / "primary_result_candidate_handoff_gate_latest.json"
    if isinstance(value, Path):
        return value
    return value


def run_primary_result_candidate_handoff(
    *,
    exp_dir: str | Path = "data/experiments",
    lifecycles_dir: str | Path = "artifacts/primary_result_lifecycle",
    candidate_index: int = 0,
    max_source_age_hours: float = 72.0,
    execute_handoff: bool = False,
    observation_window_start: str | None = None,
    trade_calendar_path: str | Path | None = None,
    lifecycle_id: str | None = None,
    handoff_gate_output_path: str | Path = DEFAULT_HANDOFF_GATE_OUTPUT,
    lifecycle_evidence_output_path: str | Path = DEFAULT_LIFECYCLE_EVIDENCE_OUTPUT,
    output_path: str | Path | None = None,
) -> tuple[int, dict[str, Any]]:
    resolved_exp_dir = resolve_project_path(exp_dir)
    resolved_handoff_gate_output_path = _resolve_default_scoped_output(
        handoff_gate_output_path,
        default_value=DEFAULT_HANDOFF_GATE_OUTPUT,
        resolved_exp_dir=resolved_exp_dir,
    )
    resolved_lifecycle_evidence_output_path = _resolve_default_scoped_output(
        lifecycle_evidence_output_path,
        default_value=DEFAULT_LIFECYCLE_EVIDENCE_OUTPUT,
        resolved_exp_dir=resolved_exp_dir,
    )
    resolved_output_path = _resolve_default_scoped_output(
        output_path,
        default_value=DEFAULT_RUNNER_OUTPUT,
        resolved_exp_dir=resolved_exp_dir,
    )
    window_advice = suggest_observation_window_start(
        candidate_file_path=resolved_exp_dir / "candidates_top_latest.csv",
        trade_calendar_path=trade_calendar_path,
    )
    gate_code, gate = build_primary_result_candidate_handoff_gate(
        exp_dir=resolved_exp_dir,
        lifecycles_dir=lifecycles_dir,
        candidate_index=candidate_index,
        output_path=resolved_handoff_gate_output_path,
    )
    decision = str(gate.get("decision") or "")
    stages: list[dict[str, Any]] = [
        {
            "name": "candidate_handoff_gate",
            "status": gate.get("status"),
            "decision": decision,
            "exit_code": gate_code,
        }
    ]
    if decision == "aligned":
        payload = _build_payload(
            status="skipped",
            decision=decision,
            reason="candidate and lifecycle are already aligned",
            gate=gate,
            stages=stages,
            window_advice=window_advice,
        )
        if resolved_output_path:
            _write_json(resolve_project_path(resolved_output_path), payload)
        return 0, payload

    if decision != "handoff_required":
        payload = _build_payload(
            status="blocked",
            decision=decision,
            reason="candidate handoff gate did not allow handoff execution",
            gate=gate,
            stages=stages,
            window_advice=window_advice,
        )
        if resolved_output_path:
            _write_json(resolve_project_path(resolved_output_path), payload)
        return 1, payload

    if not execute_handoff:
        payload = _build_payload(
            status="action_required",
            decision=decision,
            reason="handoff execution requires explicit --execute-handoff",
            gate=gate,
            stages=stages,
            window_advice=window_advice,
        )
        if resolved_output_path:
            _write_json(resolve_project_path(resolved_output_path), payload)
        return 0, payload

    if not observation_window_start:
        raise ValueError(
            "observation_window_start is required when execute_handoff=true; "
            f"suggested_observation_window_start={window_advice['suggested_window_start']}"
        )

    current_candidate = dict(gate.get("current_candidate", {}) or {})
    ts_code = current_candidate.get("ts_code")
    resolved_lifecycle_id = lifecycle_id or _default_lifecycle_id(ts_code)
    lifecycle_code, lifecycle_payload = run_primary_result_lifecycle(
        exp_dir=exp_dir,
        candidate_index=candidate_index,
        max_source_age_hours=max_source_age_hours,
        observation_status="observing",
        observation_reason="candidate handoff opened controlled observation window",
        observation_window_start=observation_window_start,
        lifecycle_id=resolved_lifecycle_id,
        output_path=resolved_lifecycle_evidence_output_path,
        seed_from_latest_candidate=True,
    )
    stages.append(
        {
            "name": "run_primary_result_lifecycle",
            "status": lifecycle_payload.get("status"),
            "exit_code": lifecycle_code,
            "run_id": lifecycle_payload.get("run_id"),
            "lifecycle_id": lifecycle_payload.get("lifecycle_id"),
            "result_id": lifecycle_payload.get("result_id"),
            "ts_code": lifecycle_payload.get("ts_code"),
        }
    )
    if lifecycle_code != 0:
        payload = _build_payload(
            status="failed",
            decision=decision,
            reason="lifecycle run failed during handoff",
            gate=gate,
            stages=stages,
            lifecycle_id=resolved_lifecycle_id,
            window_advice=window_advice,
        )
        if resolved_output_path:
            _write_json(resolve_project_path(resolved_output_path), payload)
        return 1, payload

    registry = PrimaryResultLifecycleRegistry(lifecycles_dir=lifecycles_dir)
    try:
        snapshot = registry.register(
            evidence_path=resolved_lifecycle_evidence_output_path,
            lifecycle_id=resolved_lifecycle_id,
        )
        registry_status = "registered"
    except FileExistsError:
        snapshot = registry.get_snapshot(resolved_lifecycle_id)
        snapshot_result_id = str(snapshot.get("result_id") or "").strip()
        snapshot_ts_code = str(snapshot.get("ts_code") or "").strip()
        expected_result_id = str(lifecycle_payload.get("result_id") or "").strip()
        expected_ts_code = str(lifecycle_payload.get("ts_code") or "").strip()
        if snapshot_result_id != expected_result_id or snapshot_ts_code != expected_ts_code:
            stages.append(
                {
                    "name": "register_lifecycle_snapshot",
                    "status": "failed_id_conflict",
                    "lifecycle_id": resolved_lifecycle_id,
                    "result_id": snapshot_result_id,
                    "ts_code": snapshot_ts_code,
                    "expected_result_id": expected_result_id,
                    "expected_ts_code": expected_ts_code,
                }
            )
            payload = _build_payload(
                status="failed",
                decision=decision,
                reason="lifecycle_id already exists for a different primary result",
                gate=gate,
                stages=stages,
                lifecycle_id=resolved_lifecycle_id,
                window_advice=window_advice,
            )
            if resolved_output_path:
                _write_json(resolve_project_path(resolved_output_path), payload)
            return 1, payload
        registry.rollback(resolved_lifecycle_id)
        registry_status = "already_registered_pointer_restored"
    stages.append(
        {
            "name": "register_lifecycle_snapshot",
            "status": registry_status,
            "lifecycle_id": snapshot.get("lifecycle_id"),
            "result_id": snapshot.get("result_id"),
            "ts_code": snapshot.get("ts_code"),
        }
    )

    verify_code, verify_gate = build_primary_result_candidate_handoff_gate(
        exp_dir=exp_dir,
        lifecycles_dir=lifecycles_dir,
        candidate_index=candidate_index,
        output_path=resolved_handoff_gate_output_path,
    )
    stages.append(
        {
            "name": "verify_candidate_handoff_gate",
            "status": verify_gate.get("status"),
            "decision": verify_gate.get("decision"),
            "exit_code": verify_code,
        }
    )
    status = "completed" if verify_gate.get("decision") == "aligned" else "failed"
    payload = _build_payload(
        status=status,
        decision=str(verify_gate.get("decision") or decision),
        reason="candidate handoff completed" if status == "completed" else "candidate handoff verification failed",
        gate=verify_gate,
        stages=stages,
        lifecycle_id=resolved_lifecycle_id,
        window_advice=window_advice,
    )
    if resolved_output_path:
        _write_json(resolve_project_path(resolved_output_path), payload)
    return (0 if status == "completed" else 1), payload


def _build_payload(
    *,
    status: str,
    decision: str,
    reason: str,
    gate: dict[str, Any],
    stages: list[dict[str, Any]],
    lifecycle_id: str | None = None,
    window_advice: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "runner_version": PRIMARY_RESULT_CANDIDATE_HANDOFF_RUNNER_VERSION,
        "generated_at": _utc_now_iso(),
        "status": status,
        "decision": decision,
        "reason": reason,
        "lifecycle_id": lifecycle_id,
        "current_ts_code": dict(gate.get("current_candidate", {}) or {}).get("ts_code"),
        "pointer_ts_code": dict(gate.get("lifecycle_pointer", {}) or {}).get("ts_code"),
        "blocking_reasons": gate.get("blocking_reasons", []),
        "next_actions": gate.get("next_actions", []),
        "observation_window_advice": window_advice,
        "stages": stages,
        "production_boundary": (
            "candidate handoff runner only performs controlled lifecycle handoff when explicitly requested; "
            "it does not trade, promote baselines, close observations, or deploy"
        ),
    }
