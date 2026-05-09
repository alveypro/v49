#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.run_primary_result_audit import run_primary_result_audit
from scripts.run_primary_result_execution import run_primary_result_execution
from scripts.run_primary_result_observation import run_primary_result_observation
from scripts.run_primary_result_rollback import run_primary_result_rollback
from src.artifact_registry import ArtifactRegistry, sha256_file
from src.current_result_pointer import CurrentResultPointerStore
from src.dashboard_support import read_json
from src.primary_result_observation_window_advisor import suggest_observation_window_start
from src.result_registry import ResultRegistry
from src.run_registry import RunRegistry
from src.stock_entry_guard import write_stock_entry_guard_artifact
from src.unified_result_builder import build_primary_result_api_payload
from src.utils.project_paths import resolve_project_path


PRIMARY_RESULT_LIFECYCLE_VERSION = "primary_result_lifecycle.v1"
LIFECYCLE_ARTIFACTS = {
    "audit": "primary_result_audit_latest.json",
    "execution": "primary_result_execution_latest.json",
    "rollback": "primary_result_rollback_latest.json",
    "observation": "primary_result_observation_latest.json",
    "terminal": "primary_result_terminal_latest.json",
}


def _isoformat_z(value: datetime | None = None) -> str:
    current = value or datetime.now(timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    return current.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _safe_id_part(value: object) -> str:
    text = "".join(ch.lower() if ch.isalnum() else "-" for ch in str(value or "").strip())
    return "-".join(part for part in text.split("-") if part) or "unknown"


def _default_lifecycle_id(ts_code: object, started_at: str) -> str:
    return f"primary-lifecycle-{_safe_id_part(started_at)}-{_safe_id_part(ts_code)}"


def _default_run_id(lifecycle_id: str) -> str:
    return f"primary-run-{_safe_id_part(lifecycle_id)}"


def _sha256_json(payload: dict[str, object]) -> str:
    canonical = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _write_output(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _build_data_snapshot_id(exp_dir: Path, result_id: str, ts_code: str) -> str:
    source_files = [
        exp_dir / "candidates_top_latest.csv",
        exp_dir / "daily_research_status_latest.json",
        exp_dir / "buylist_latest.json",
        exp_dir / "governance_audit_latest.json",
        exp_dir / "t1_execution_checklist_latest.json",
        exp_dir / "t12_rollback_drill_latest.json",
    ]
    manifest: list[dict[str, object]] = []
    for path in source_files:
        exists = path.exists()
        stat = path.stat() if exists else None
        manifest.append(
            {
                "path": str(path),
                "exists": exists,
                "size": stat.st_size if stat is not None else None,
                "mtime_ns": stat.st_mtime_ns if stat is not None else None,
            }
        )
    return _sha256_json(
        {
            "result_id": result_id,
            "ts_code": ts_code,
            "sources": manifest,
        }
    )


def _register_primary_result_chain(
    *,
    exp_dir: Path,
    output_path: Path,
    payload: dict[str, object],
    started_at: str,
    lifecycle_id: str,
    run_id: str,
    candidate_index: int,
    max_source_age_hours: float,
    observation_status: str,
    observation_reason: str,
    observation_window_start: str | None,
    observation_window_end: str | None,
    observed_return: float | None,
    benchmark_return: float | None,
    max_drawdown: float | None,
    source_scope: str = "stock",
) -> dict[str, object]:
    artifacts_dir = exp_dir.parent.parent / "artifacts"
    artifact_registry = ArtifactRegistry(artifacts_dir / "artifact_registry.jsonl")
    run_registry = RunRegistry(runs_dir=artifacts_dir / "run_registry")
    result_registry = ResultRegistry(results_dir=artifacts_dir / "result_registry")
    pointer_store = CurrentResultPointerStore(pointer_dir=artifacts_dir / "current_result_pointer")

    final_payload = dict(payload.get("final_payload") or {})
    result_id = str(final_payload.get("result_id") or payload.get("result_id") or "").strip()
    ts_code = str(final_payload.get("ts_code") or payload.get("ts_code") or "").strip()
    stock_name = str(final_payload.get("stock_name") or payload.get("stock_name") or "").strip() or None
    lifecycle_stage = str(final_payload.get("result_lifecycle_stage") or "").strip()
    completed_at = str(payload.get("completed_at") or started_at).strip() or started_at
    as_of_date = completed_at[:10]

    config_hash = _sha256_json(
        {
            "candidate_index": candidate_index,
            "max_source_age_hours": max_source_age_hours,
            "observation_status": observation_status,
            "observation_reason": observation_reason,
            "observation_window_start": observation_window_start,
            "observation_window_end": observation_window_end,
            "observed_return": observed_return,
            "benchmark_return": benchmark_return,
            "max_drawdown": max_drawdown,
        }
    )
    data_snapshot_id = _build_data_snapshot_id(exp_dir, result_id=result_id, ts_code=ts_code)
    run_entry = run_registry.register(
        run_id=run_id,
        run_type="primary_result_lifecycle",
        status=str(payload.get("status") or "unknown"),
        producer="scripts.run_primary_result_lifecycle",
        config_hash=config_hash,
        data_snapshot_id=data_snapshot_id,
        code_revision="workspace",
        created_at=started_at,
        make_current=True,
        metadata={
            "lifecycle_id": lifecycle_id,
            "result_id": result_id,
            "ts_code": ts_code,
            "evidence_path": str(output_path),
            "source_scope": source_scope,
            "producer": "scripts.run_primary_result_lifecycle",
            "code_revision": "workspace",
        },
    )

    step_artifact_ids: list[str] = []
    for step in payload.get("steps", []):
        if not isinstance(step, dict) or step.get("exists") is not True:
            continue
        step_name = str(step.get("step") or "").strip()
        if not step_name:
            continue
        step_artifact_id = f"{run_id}:{step_name}"
        artifact_registry.register_artifact(
            artifact_type=f"primary_result_{step_name}",
            run_id=run_id,
            path=Path(str(step["path"])),
            producer="scripts.run_primary_result_lifecycle",
            artifact_id=step_artifact_id,
            created_at=str(step.get("generated_at") or completed_at),
            result_id=result_id,
            code_revision="workspace",
            metadata={
                "lifecycle_id": lifecycle_id,
                "step": step_name,
                "status": step.get("status"),
                "ts_code": ts_code,
            },
        )
        step_artifact_ids.append(step_artifact_id)

    lifecycle_evidence_artifact_id = f"{run_id}:lifecycle-evidence"
    artifact_ids = [*step_artifact_ids, lifecycle_evidence_artifact_id]

    registry_chain: dict[str, object] = {
        "artifacts_dir": str(artifacts_dir),
        "artifact_registry_path": str(artifacts_dir / "artifact_registry.jsonl"),
        "lifecycle_evidence_artifact_id": lifecycle_evidence_artifact_id,
        "lifecycle_evidence_parent_artifact_ids": list(step_artifact_ids),
        "run_registry": {
            "run_id": run_entry["run_id"],
            "status": run_entry["status"],
            "producer": run_entry["producer"],
            "code_revision": run_entry["code_revision"],
            "entry_path": str((artifacts_dir / "run_registry" / "history" / f"{run_id}.json")),
        },
        "result_registry": None,
        "current_result_pointer": None,
    }

    if str(payload.get("status") or "") != "passed":
        registry_chain["write_mode"] = "run_only_failed_lifecycle"
        return registry_chain

    result_entry = result_registry.register(
        record_id=f"result-record-{_safe_id_part(run_id)}-{_safe_id_part(lifecycle_stage)}",
        result_id=result_id,
        run_id=run_id,
        ts_code=ts_code,
        stock_name=stock_name,
        lifecycle_stage=lifecycle_stage,
        artifact_ids=artifact_ids,
        registered_at=completed_at,
        source_scope=source_scope,
        make_current=True,
        metadata={
            "lifecycle_id": lifecycle_id,
            "evidence_path": str(output_path),
            "lifecycle_status": payload.get("status"),
            "artifact_registry_path": str(artifacts_dir / "artifact_registry.jsonl"),
            "source_scope": source_scope,
            "producer": "scripts.run_primary_result_lifecycle",
            "code_revision": "workspace",
        },
    )
    pointer_snapshot = pointer_store.point_to(
        pointer_snapshot_id=f"current-result-pointer-{_safe_id_part(run_id)}",
        result_id=result_id,
        run_id=run_id,
        lifecycle_id=lifecycle_id,
        artifact_ids=artifact_ids,
        as_of_date=as_of_date,
        source_scope=source_scope,
        updated_at=completed_at,
        metadata={
            "result_registry_record_id": result_entry["record_id"],
            "evidence_path": str(output_path),
            "artifact_registry_path": str(artifacts_dir / "artifact_registry.jsonl"),
            "source_scope": source_scope,
            "producer": "scripts.run_primary_result_lifecycle",
            "code_revision": "workspace",
        },
    )
    registry_chain["write_mode"] = "run_result_pointer_updated"
    registry_chain["result_registry"] = {
        "record_id": result_entry["record_id"],
        "lifecycle_stage": result_entry["lifecycle_stage"],
        "source_scope": result_entry["source_scope"],
        "producer": "scripts.run_primary_result_lifecycle",
        "code_revision": "workspace",
        "entry_path": str((artifacts_dir / "result_registry" / "history" / f"{result_entry['record_id']}.json")),
    }
    registry_chain["current_result_pointer"] = {
        "pointer_snapshot_id": pointer_snapshot["pointer_snapshot_id"],
        "result_id": pointer_snapshot["result_id"],
        "run_id": pointer_snapshot["run_id"],
        "lifecycle_id": pointer_snapshot["lifecycle_id"],
        "artifact_ids": list(pointer_snapshot["artifact_ids"]),
        "as_of_date": pointer_snapshot["as_of_date"],
        "source_scope": pointer_snapshot["source_scope"],
        "producer": "scripts.run_primary_result_lifecycle",
        "code_revision": "workspace",
        "snapshot_path": str((artifacts_dir / "current_result_pointer" / "history" / f"{pointer_snapshot['pointer_snapshot_id']}.json")),
    }
    registry_chain["pointer"] = pointer_snapshot
    guard_payload = write_stock_entry_guard_artifact(
        output_path=artifacts_dir / "stock_entry_guard_latest.json",
        exp_dir=exp_dir,
        artifacts_dir=artifacts_dir,
    )
    guard_output_path = Path(str(guard_payload.get("output_path") or artifacts_dir / "stock_entry_guard_latest.json"))
    artifact_registry.register_artifact(
        artifact_type="stock_entry_guard",
        run_id=run_id,
        path=guard_output_path,
        producer="scripts.run_primary_result_lifecycle",
        artifact_id=f"{run_id}:stock-entry-guard",
        created_at=completed_at,
        result_id=result_id,
        parent_artifact_ids=artifact_ids,
        code_revision="workspace",
        metadata={
            "lifecycle_id": lifecycle_id,
            "guard_ok": guard_payload.get("ok"),
        },
    )
    registry_chain["stock_entry_guard"] = {
        "ok": guard_payload.get("ok"),
        "output_path": str(guard_output_path),
        "problems": list(guard_payload.get("problems") or []),
    }
    return registry_chain


def _register_lifecycle_evidence_artifact(
    *,
    artifacts_dir: Path,
    output_path: Path,
    payload: dict[str, object],
    registry_chain: dict[str, object],
    run_id: str,
    lifecycle_id: str,
) -> None:
    artifact_registry = ArtifactRegistry(artifacts_dir / "artifact_registry.jsonl")
    final_payload = dict(payload.get("final_payload") or {})
    result_id = str(final_payload.get("result_id") or payload.get("result_id") or "").strip()
    ts_code = str(final_payload.get("ts_code") or payload.get("ts_code") or "").strip()
    created_at = str(payload.get("completed_at") or payload.get("started_at") or "").strip() or _isoformat_z()
    artifact_id = str(registry_chain.get("lifecycle_evidence_artifact_id") or f"{run_id}:lifecycle-evidence").strip()
    parent_artifact_ids = list(registry_chain.get("lifecycle_evidence_parent_artifact_ids") or [])
    artifact_registry.register_artifact(
        artifact_type="primary_result_lifecycle_evidence",
        run_id=run_id,
        path=output_path,
        producer="scripts.run_primary_result_lifecycle",
        artifact_id=artifact_id,
        created_at=created_at,
        result_id=result_id,
        parent_artifact_ids=parent_artifact_ids,
        code_revision="workspace",
        metadata={
            "lifecycle_id": lifecycle_id,
            "ts_code": ts_code,
            "status": payload.get("status"),
        },
    )


def _status_for_step(step_name: str, payload: dict[str, object]) -> str | None:
    return {
        "audit": payload.get("audit_status"),
        "execution": payload.get("execution_status"),
        "rollback": payload.get("rollback_status"),
        "observation": payload.get("observation_status"),
        "terminal": payload.get("terminal_outcome"),
    }.get(step_name)  # type: ignore[return-value]


def _artifact_entry(exp_dir: Path, step_name: str, payload: dict[str, object]) -> dict[str, object]:
    path = exp_dir / LIFECYCLE_ARTIFACTS[step_name]
    return {
        "step": step_name,
        "path": str(path),
        "exists": path.exists(),
        "sha256": sha256_file(path) if path.exists() else None,
        "result_id": payload.get("result_id"),
        "ts_code": payload.get("ts_code"),
        "status": _status_for_step(step_name, payload),
        "generated_at": payload.get("generated_at"),
    }


def _synchronize_final_payload_identity(
    final_payload: dict[str, object],
    *,
    registry_chain: dict[str, object],
    run_id: str,
    lifecycle_id: str,
) -> dict[str, object]:
    synchronized = dict(final_payload)
    synchronized["run_id"] = run_id
    synchronized["lifecycle_id"] = lifecycle_id

    current_pointer = registry_chain.get("current_result_pointer")
    if not isinstance(current_pointer, dict):
        return synchronized
    synchronized["result_id"] = current_pointer.get("result_id") or synchronized.get("result_id")
    synchronized["artifact_ids"] = list(current_pointer.get("artifact_ids") or synchronized.get("artifact_ids") or [])
    synchronized["as_of_date"] = current_pointer.get("as_of_date") or synchronized.get("as_of_date")
    synchronized["source_scope"] = current_pointer.get("source_scope") or synchronized.get("source_scope")

    result_registry = registry_chain.get("result_registry")
    if isinstance(result_registry, dict):
        synchronized["result_lifecycle_stage"] = (
            result_registry.get("lifecycle_stage") or synchronized.get("result_lifecycle_stage")
        )
    return synchronized


def _refresh_stock_entry_guard(
    *,
    exp_dir: Path,
    artifacts_dir: Path,
    registry_chain: dict[str, object],
) -> dict[str, object]:
    guard_payload = write_stock_entry_guard_artifact(
        output_path=artifacts_dir / "stock_entry_guard_latest.json",
        exp_dir=exp_dir,
        artifacts_dir=artifacts_dir,
    )
    refreshed = {
        "ok": guard_payload.get("ok"),
        "output_path": str(guard_payload.get("output_path") or artifacts_dir / "stock_entry_guard_latest.json"),
        "problems": list(guard_payload.get("problems") or []),
    }
    registry_chain["stock_entry_guard"] = refreshed
    return refreshed


def _detect_stale_artifacts(exp_dir: Path, current_result_id: str, current_ts_code: str) -> list[dict[str, object]]:
    stale: list[dict[str, object]] = []
    for step_name, file_name in LIFECYCLE_ARTIFACTS.items():
        path = exp_dir / file_name
        if not path.exists():
            continue
        payload = read_json(path)
        artifact_result_id = str(payload.get("result_id", "") or "").strip()
        artifact_ts_code = str(payload.get("ts_code", "") or "").strip()
        if artifact_result_id == current_result_id or (artifact_ts_code and artifact_ts_code == current_ts_code):
            continue
        stale.append(
            {
                "step": step_name,
                "path": str(path),
                "sha256": sha256_file(path),
                "artifact_result_id": artifact_result_id or None,
                "artifact_ts_code": artifact_ts_code or None,
                "current_result_id": current_result_id,
                "current_ts_code": current_ts_code,
            }
        )
    return stale


def run_primary_result_lifecycle(
    *,
    exp_dir: str | Path = "data/experiments",
    candidate_index: int = 0,
    max_source_age_hours: float = 72.0,
    observation_status: str = "observing",
    observation_reason: str = "local observation window opened",
    observation_window_start: str | None = None,
    observation_window_end: str | None = None,
    observed_return: float | None = None,
    benchmark_return: float | None = None,
    max_drawdown: float | None = None,
    lifecycle_id: str | None = None,
    run_id: str | None = None,
    output_path: str | Path | None = None,
    now: datetime | None = None,
    seed_from_latest_candidate: bool = False,
) -> tuple[int, dict[str, object]]:
    resolved_exp_dir = resolve_project_path(exp_dir)
    started_at = _isoformat_z(now)
    if not seed_from_latest_candidate:
        initial_payload = build_primary_result_api_payload(
            resolved_exp_dir,
            candidate_index=candidate_index,
            require_current_pointer=True,
        )
    else:
        initial_payload = build_primary_result_api_payload(
            resolved_exp_dir,
            candidate_index=candidate_index,
            require_current_pointer=False,
            ignore_current_pointer=True,
        )
    current_result_id = str(initial_payload.get("result_id", "") or "")
    current_ts_code = str(initial_payload.get("ts_code", "") or "")
    stale_artifacts = _detect_stale_artifacts(resolved_exp_dir, current_result_id, current_ts_code)
    window_advice = suggest_observation_window_start(
        reference_time=now,
        candidate_file_path=resolved_exp_dir / "candidates_top_latest.csv",
    )
    resolved_observation_window_start = observation_window_start or str(window_advice["suggested_window_start"])

    audit_exit, audit_payload = run_primary_result_audit(
        exp_dir=resolved_exp_dir,
        candidate_index=candidate_index,
        max_source_age_hours=max_source_age_hours,
        output_path=resolved_exp_dir / LIFECYCLE_ARTIFACTS["audit"],
        now=now,
        ignore_current_pointer=seed_from_latest_candidate,
    )
    execution_exit, execution_payload = run_primary_result_execution(
        exp_dir=resolved_exp_dir,
        candidate_index=candidate_index,
        output_path=resolved_exp_dir / LIFECYCLE_ARTIFACTS["execution"],
        now=now,
        ignore_current_pointer=seed_from_latest_candidate,
    )
    rollback_exit, rollback_payload = run_primary_result_rollback(
        exp_dir=resolved_exp_dir,
        candidate_index=candidate_index,
        output_path=resolved_exp_dir / LIFECYCLE_ARTIFACTS["rollback"],
        now=now,
        ignore_current_pointer=seed_from_latest_candidate,
    )
    observation_exit, observation_payload = run_primary_result_observation(
        exp_dir=resolved_exp_dir,
        candidate_index=candidate_index,
        observation_status=observation_status,
        reason=observation_reason,
        window_start=resolved_observation_window_start,
        window_end=observation_window_end,
        observed_return=observed_return,
        benchmark_return=benchmark_return,
        max_drawdown=max_drawdown,
        output_path=resolved_exp_dir / LIFECYCLE_ARTIFACTS["observation"],
        now=now,
        ignore_current_pointer=seed_from_latest_candidate,
    )

    steps = [
        {**_artifact_entry(resolved_exp_dir, "audit", audit_payload), "exit_code": audit_exit},
        {**_artifact_entry(resolved_exp_dir, "execution", execution_payload), "exit_code": execution_exit},
        {**_artifact_entry(resolved_exp_dir, "rollback", rollback_payload), "exit_code": rollback_exit},
        {**_artifact_entry(resolved_exp_dir, "observation", observation_payload), "exit_code": observation_exit},
    ]
    if not seed_from_latest_candidate:
        final_payload = build_primary_result_api_payload(
            resolved_exp_dir,
            candidate_index=candidate_index,
            require_current_pointer=True,
        )
    else:
        final_payload = build_primary_result_api_payload(
            resolved_exp_dir,
            candidate_index=candidate_index,
            require_current_pointer=False,
            ignore_current_pointer=True,
        )
    blocking_failures = [
        {"step": step["step"], "status": step["status"], "exit_code": step["exit_code"]}
        for step in steps
        if step["exit_code"] != 0
    ]
    payload = {
        "lifecycle_version": PRIMARY_RESULT_LIFECYCLE_VERSION,
        "started_at": started_at,
        "completed_at": _isoformat_z(),
        "status": "passed" if not blocking_failures else "failed",
        "result_id": final_payload.get("result_id"),
        "ts_code": final_payload.get("ts_code"),
        "stock_name": final_payload.get("stock_name"),
        "initial_payload": initial_payload,
        "final_payload": final_payload,
        "observation_window_advice": window_advice,
        "resolved_observation_window_start": resolved_observation_window_start,
        "stale_artifacts_detected": stale_artifacts,
        "steps": steps,
        "blocking_failures": blocking_failures,
    }
    output = (
        resolve_project_path(output_path)
        if output_path is not None
        else resolved_exp_dir / "primary_result_lifecycle_evidence_latest.json"
    )
    resolved_lifecycle_id = lifecycle_id or _default_lifecycle_id(payload.get("ts_code"), started_at)
    resolved_run_id = run_id or _default_run_id(resolved_lifecycle_id)
    payload["lifecycle_id"] = resolved_lifecycle_id
    payload["run_id"] = resolved_run_id
    _write_output(output, payload)
    payload["registry_chain"] = _register_primary_result_chain(
        exp_dir=resolved_exp_dir,
        output_path=output,
        payload=payload,
        started_at=started_at,
        lifecycle_id=resolved_lifecycle_id,
        run_id=resolved_run_id,
        candidate_index=candidate_index,
        max_source_age_hours=max_source_age_hours,
        observation_status=observation_status,
        observation_reason=observation_reason,
        observation_window_start=resolved_observation_window_start,
        observation_window_end=observation_window_end,
        observed_return=observed_return,
        benchmark_return=benchmark_return,
        max_drawdown=max_drawdown,
    )
    if payload["registry_chain"].get("write_mode") == "run_result_pointer_updated":
        payload["final_payload"] = _synchronize_final_payload_identity(
            dict(payload["final_payload"]),
            registry_chain=payload["registry_chain"],
            run_id=resolved_run_id,
            lifecycle_id=resolved_lifecycle_id,
        )
    _write_output(output, payload)
    _register_lifecycle_evidence_artifact(
        artifacts_dir=Path(str(payload["registry_chain"]["artifacts_dir"])),
        output_path=output,
        payload=payload,
        registry_chain=payload["registry_chain"],
        run_id=resolved_run_id,
        lifecycle_id=resolved_lifecycle_id,
    )
    if payload["registry_chain"].get("write_mode") == "run_result_pointer_updated":
        artifacts_dir = Path(str(payload["registry_chain"]["artifacts_dir"]))
        _refresh_stock_entry_guard(
            exp_dir=resolved_exp_dir,
            artifacts_dir=artifacts_dir,
            registry_chain=payload["registry_chain"],
        )
    return (0 if payload["status"] == "passed" else 1), payload


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the local /stock primary result lifecycle and write evidence.")
    parser.add_argument("--exp-dir", default="data/experiments")
    parser.add_argument("--candidate-index", type=int, default=0)
    parser.add_argument("--max-source-age-hours", type=float, default=72.0)
    parser.add_argument("--observation-status", default="observing")
    parser.add_argument("--observation-reason", default="local observation window opened")
    parser.add_argument("--observation-window-start")
    parser.add_argument("--observation-window-end")
    parser.add_argument("--observed-return", type=float)
    parser.add_argument("--benchmark-return", type=float)
    parser.add_argument("--max-drawdown", type=float)
    parser.add_argument("--lifecycle-id")
    parser.add_argument("--run-id")
    parser.add_argument("--output")
    parser.add_argument("--seed-from-latest-candidate", action="store_true")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()
    exit_code, payload = run_primary_result_lifecycle(
        exp_dir=args.exp_dir,
        candidate_index=args.candidate_index,
        max_source_age_hours=args.max_source_age_hours,
        observation_status=args.observation_status,
        observation_reason=args.observation_reason,
        observation_window_start=args.observation_window_start,
        observation_window_end=args.observation_window_end,
        observed_return=args.observed_return,
        benchmark_return=args.benchmark_return,
        max_drawdown=args.max_drawdown,
        lifecycle_id=args.lifecycle_id,
        run_id=args.run_id,
        output_path=args.output,
        seed_from_latest_candidate=args.seed_from_latest_candidate,
    )
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print(
            json.dumps(
                {
                    "status": payload["status"],
                    "result_id": payload["result_id"],
                    "ts_code": payload["ts_code"],
                    "step_total": len(payload["steps"]),
                    "blocking_failure_total": len(payload["blocking_failures"]),
                    "stale_artifact_total": len(payload["stale_artifacts_detected"]),
                },
                ensure_ascii=False,
                indent=2,
            )
        )
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
