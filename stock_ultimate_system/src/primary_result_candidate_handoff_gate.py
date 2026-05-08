from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.artifact_registry import sha256_file
from src.artifact_source_guard import assert_not_temp_source_path, should_enforce_production_source_guard
from src.unified_result_builder import build_primary_result_api_payload
from src.utils.project_paths import resolve_project_path


PRIMARY_RESULT_CANDIDATE_HANDOFF_GATE_VERSION = "primary_result_candidate_handoff_gate.v1"
LIFECYCLE_ARTIFACTS = {
    "audit": "primary_result_audit_latest.json",
    "execution": "primary_result_execution_latest.json",
    "rollback": "primary_result_rollback_latest.json",
    "observation": "primary_result_observation_latest.json",
    "terminal": "primary_result_terminal_latest.json",
}


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


def _check(name: str, passed: bool, detail: str, *, details: dict[str, Any] | None = None) -> dict[str, Any]:
    return {"name": name, "passed": passed, "severity": "blocking", "detail": detail, "details": details or {}}


def _stale_artifacts(exp_dir: Path, *, current_result_id: str, current_ts_code: str) -> list[dict[str, Any]]:
    stale: list[dict[str, Any]] = []
    for step, file_name in LIFECYCLE_ARTIFACTS.items():
        path = exp_dir / file_name
        if not path.exists():
            continue
        payload = _read_json(path)
        artifact_result_id = str(payload.get("result_id") or "").strip()
        artifact_ts_code = str(payload.get("ts_code") or "").strip()
        if artifact_result_id == current_result_id or (artifact_ts_code and artifact_ts_code == current_ts_code):
            continue
        stale.append(
            {
                "step": step,
                "path": str(path),
                "sha256": sha256_file(path),
                "artifact_result_id": artifact_result_id or None,
                "artifact_ts_code": artifact_ts_code or None,
                "current_result_id": current_result_id,
                "current_ts_code": current_ts_code,
            }
        )
    return stale


def build_primary_result_candidate_handoff_gate(
    *,
    exp_dir: str | Path = "data/experiments",
    lifecycles_dir: str | Path = "artifacts/primary_result_lifecycle",
    candidate_index: int = 0,
    output_path: str | Path | None = None,
    enforce_production_source_guard: bool | None = None,
) -> tuple[int, dict[str, Any]]:
    enforce_guard = (
        should_enforce_production_source_guard(output_path)
        if enforce_production_source_guard is None
        else bool(enforce_production_source_guard)
    )
    resolved_exp_dir = resolve_project_path(exp_dir)
    resolved_lifecycles_dir = resolve_project_path(lifecycles_dir)
    current_candidate = build_primary_result_api_payload(
        resolved_exp_dir,
        candidate_index=candidate_index,
        require_current_pointer=True,
    )
    current_result_id = str(current_candidate.get("result_id") or "").strip()
    current_ts_code = str(current_candidate.get("ts_code") or "").strip()
    lifecycle_pointer = _read_json(resolved_lifecycles_dir / "current.json")
    pointer_result_id = str(lifecycle_pointer.get("result_id") or "").strip()
    pointer_ts_code = str(lifecycle_pointer.get("ts_code") or "").strip()
    pointer_snapshot_path = str(lifecycle_pointer.get("snapshot_path") or "").strip()
    stale = _stale_artifacts(resolved_exp_dir, current_result_id=current_result_id, current_ts_code=current_ts_code)
    lifecycle_matches_candidate = bool(pointer_result_id) and pointer_result_id == current_result_id and pointer_ts_code == current_ts_code
    has_candidate = bool(current_ts_code) and current_ts_code not in {"-", "暂无"}

    temp_source_blockers: list[str] = []
    if enforce_guard:
        try:
            assert_not_temp_source_path(pointer_snapshot_path, field_name="snapshot_path")
        except ValueError as exc:
            temp_source_blockers.append(str(exc))

    checks = [
        _check(
            "current_candidate_is_pointer_governed",
            current_candidate.get("history_generation_mode") != "blocked",
            "current candidate must come from a valid current_result_pointer before handoff gate evaluation",
            details={
                "result_id": current_result_id or None,
                "history_generation_mode": current_candidate.get("history_generation_mode"),
                "disabled_reason": current_candidate.get("disabled_reason"),
            },
        ),
        _check("candidate_present", has_candidate, "current candidate must have a ts_code", details={"current_ts_code": current_ts_code}),
        _check(
            "lifecycle_pointer_matches_candidate",
            lifecycle_matches_candidate,
            "lifecycle current pointer must match the current top candidate before closure",
            details={
                "current_result_id": current_result_id,
                "current_ts_code": current_ts_code,
                "pointer_result_id": pointer_result_id or None,
                "pointer_ts_code": pointer_ts_code or None,
            },
        ),
        _check(
            "no_stale_lifecycle_artifacts_for_current_candidate",
            not stale,
            "stale lifecycle artifacts must be rebuilt or ignored before current candidate closure",
            details={"stale_artifact_total": len(stale)},
        ),
        _check(
            "lifecycle_snapshot_path_is_not_temp",
            not temp_source_blockers,
            "lifecycle snapshot_path must not point to pytest or temporary directories",
            details={"snapshot_path": pointer_snapshot_path or None},
        ),
    ]
    blocking = [check for check in checks if check["passed"] is not True]
    result_stage = str(current_candidate.get("result_lifecycle_stage") or "")
    if not has_candidate:
        decision = "blocked"
    elif temp_source_blockers:
        decision = "blocked"
    elif lifecycle_matches_candidate and not stale:
        decision = "aligned"
    elif result_stage == "L2" or not lifecycle_matches_candidate:
        decision = "handoff_required"
    else:
        decision = "blocked"
    payload = {
        "gate_version": PRIMARY_RESULT_CANDIDATE_HANDOFF_GATE_VERSION,
        "generated_at": _utc_now_iso(),
        "status": "passed" if decision == "aligned" else "blocked",
        "decision": decision,
        "current_candidate": current_candidate,
        "lifecycle_pointer": lifecycle_pointer,
        "fact_source_role": "gate_only",
        "stale_artifacts": stale,
        "checks": checks,
        "blocking_reasons": [str(check["detail"]) for check in blocking],
        "next_actions": _next_actions(decision=decision, current_ts_code=current_ts_code),
        "production_boundary": (
            "candidate handoff gate only detects whether current candidate and lifecycle evidence are aligned; "
            "it does not overwrite lifecycle artifacts, close observations, promote baselines, trade, or deploy"
        ),
    }
    if output_path:
        _write_json(resolve_project_path(output_path), payload)
    return (0 if payload["status"] == "passed" else 1), payload


def _next_actions(*, decision: str, current_ts_code: str) -> list[str]:
    if decision == "aligned":
        return ["current candidate and lifecycle pointer are aligned; daily closure may proceed when data gates pass"]
    if decision == "handoff_required":
        return [
            f"run primary result lifecycle for current candidate {current_ts_code}",
            "register the passed lifecycle evidence as the current lifecycle snapshot",
            "do not run daily closure against stale observation artifacts",
        ]
    return ["resolve candidate or lifecycle pointer mismatch before daily closure"]
