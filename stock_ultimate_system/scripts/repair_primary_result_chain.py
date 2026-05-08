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

from scripts.run_primary_result_lifecycle import run_primary_result_lifecycle
from src.artifact_registry import ArtifactRegistry
from src.artifact_registry import sha256_file
from src.current_result_pointer import CurrentResultPointerStore
from src.primary_result_audit import build_primary_result_audit
from src.primary_result_execution import build_primary_result_execution
from src.primary_result_lifecycle_registry import PrimaryResultLifecycleRegistry
from src.primary_result_observation import build_primary_result_observation
from src.primary_result_rollback import build_primary_result_rollback
from src.result_registry import ResultRegistry
from src.run_registry import RunRegistry
from src.stock_entry_guard import write_stock_entry_guard_artifact
from src.unified_result_builder import build_primary_result_api_payload
from src.utils.project_paths import resolve_project_path


CHAIN_REPAIR_VERSION = "primary_result_chain_repair.v1"
BOOTSTRAP_STEP_ORDER = ("audit", "execution", "rollback", "observation")


def _isoformat_z(value: datetime | None = None) -> str:
    current = value or datetime.now(timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    return current.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _safe_id_part(value: object) -> str:
    text = "".join(ch.lower() if ch.isalnum() else "-" for ch in str(value or "").strip())
    parts = [part for part in text.split("-") if part]
    return "-".join(parts) or "unknown"


def _seed_payload_usable(payload: dict[str, object]) -> bool:
    result_id = str(payload.get("result_id") or "").strip()
    ts_code = str(payload.get("ts_code") or "").strip()
    if not result_id or result_id == "primary:unavailable":
        return False
    if not ts_code:
        return False
    if "." not in ts_code:
        return False
    code_part, market_part = ts_code.split(".", 1)
    if not (len(code_part) == 6 and code_part.isdigit()):
        return False
    if market_part not in {"SZ", "SH", "BJ"}:
        return False
    return True


def _write_output(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _sha256_json(payload: dict[str, object]) -> str:
    canonical = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _build_data_snapshot_id(exp_dir: Path, payload: dict[str, object]) -> str:
    candidate_path = exp_dir / "candidates_top_latest.csv"
    buylist_path = exp_dir / "buylist_latest.json"
    daily_status_path = exp_dir / "daily_research_status_latest.json"
    manifest = []
    for path in (candidate_path, buylist_path, daily_status_path):
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
            "result_id": payload.get("result_id"),
            "ts_code": payload.get("ts_code"),
            "manifest": manifest,
        }
    )


def _bootstrap_artifact_path(exp_dir: Path, step_name: str) -> Path:
    return exp_dir / f"primary_result_{step_name}_latest.json"


def _build_bootstrap_step_payload(
    *,
    step_name: str,
    primary_result_payload: dict[str, object],
    observation_status: str,
    observation_reason: str,
    observation_window_start: str | None,
    observation_window_end: str | None,
    observed_return: float | None,
    benchmark_return: float | None,
    max_drawdown: float | None,
    max_source_age_hours: float,
    now: datetime | None,
) -> tuple[int, dict[str, object]]:
    if step_name == "audit":
        payload = build_primary_result_audit(
            primary_result_payload,
            max_source_age_hours=max_source_age_hours,
            now=now,
        )
        return (0 if payload["audit_status"] == "passed" else 1), payload
    if step_name == "execution":
        payload = build_primary_result_execution(primary_result_payload, now=now)
        return (0 if payload["execution_status"] == "ready" else 1), payload
    if step_name == "rollback":
        payload = build_primary_result_rollback(primary_result_payload, now=now)
        return (0 if payload["rollback_status"] in {"not_required", "completed"} else 1), payload
    if step_name == "observation":
        payload = build_primary_result_observation(
            primary_result_payload,
            observation_status=observation_status,
            reason=observation_reason,
            window_start=observation_window_start,
            window_end=observation_window_end,
            observed_return=observed_return,
            benchmark_return=benchmark_return,
            max_drawdown=max_drawdown,
            now=now,
        )
        return (0 if payload["observation_status"] in {"observing", "completed"} else 1), payload
    raise ValueError(f"unsupported bootstrap step: {step_name}")


def _register_chain_state(
    *,
    artifacts_dir: Path,
    run_id: str,
    lifecycle_id: str,
    payload: dict[str, object],
    artifact_ids: list[str],
    updated_at: str,
    snapshot_suffix: str,
    source_scope: str = "stock",
) -> dict[str, object]:
    run_registry = RunRegistry(runs_dir=artifacts_dir / "run_registry")
    result_registry = ResultRegistry(results_dir=artifacts_dir / "result_registry")
    pointer_store = CurrentResultPointerStore(pointer_dir=artifacts_dir / "current_result_pointer")

    result_id = str(payload.get("result_id") or "").strip()
    ts_code = str(payload.get("ts_code") or "").strip()
    stock_name = str(payload.get("stock_name") or "").strip() or None
    lifecycle_stage = str(payload.get("result_lifecycle_stage") or "").strip() or "L1"
    as_of_date = updated_at[:10]

    result_entry = result_registry.register(
        record_id=(
            f"result-repair-{_safe_id_part(run_id)}-"
            f"{_safe_id_part(lifecycle_stage)}-{_safe_id_part(updated_at)}-{_safe_id_part(snapshot_suffix)}"
        ),
        result_id=result_id,
        run_id=run_id,
        ts_code=ts_code,
        stock_name=stock_name,
        lifecycle_stage=lifecycle_stage,
        artifact_ids=artifact_ids,
        registered_at=updated_at,
        source_scope=source_scope,
        make_current=True,
        metadata={
            "lifecycle_id": lifecycle_id,
            "source_scope": source_scope,
            "repair_mode": "bootstrap",
            "producer": "scripts.repair_primary_result_chain",
            "code_revision": "workspace",
        },
    )
    pointer_snapshot = pointer_store.point_to(
        pointer_snapshot_id=(
            f"current-result-pointer-repair-{_safe_id_part(run_id)}-"
            f"{_safe_id_part(updated_at)}-{_safe_id_part(snapshot_suffix)}"
        ),
        result_id=result_id,
        run_id=run_id,
        lifecycle_id=lifecycle_id,
        artifact_ids=artifact_ids,
        as_of_date=as_of_date,
        source_scope=source_scope,
        updated_at=updated_at,
        metadata={
            "result_registry_record_id": result_entry["record_id"],
            "source_scope": source_scope,
            "repair_mode": "bootstrap",
            "producer": "scripts.repair_primary_result_chain",
            "code_revision": "workspace",
        },
    )
    return {
        "result_registry_record_id": result_entry["record_id"],
        "pointer_snapshot_id": pointer_snapshot["pointer_snapshot_id"],
        "result_id": result_id,
        "ts_code": ts_code,
    }


def _reconcile_result_registry_to_current_pointer(
    *,
    artifacts_dir: Path,
    payload: dict[str, object],
    registered_at: str,
) -> dict[str, object]:
    pointer_store = CurrentResultPointerStore(pointer_dir=artifacts_dir / "current_result_pointer")
    result_registry = ResultRegistry(results_dir=artifacts_dir / "result_registry")
    pointer = pointer_store.get_current_pointer()
    result_id = str(pointer.get("result_id") or payload.get("result_id") or "").strip()
    run_id = str(pointer.get("run_id") or payload.get("run_id") or "").strip()
    lifecycle_id = str(pointer.get("lifecycle_id") or payload.get("lifecycle_id") or "").strip()
    ts_code = str(payload.get("ts_code") or "").strip()
    stock_name = str(payload.get("stock_name") or "").strip() or None
    lifecycle_stage = str(payload.get("result_lifecycle_stage") or "L1").strip() or "L1"
    artifact_ids = [str(item).strip() for item in (pointer.get("artifact_ids") or []) if str(item).strip()]
    if not result_id or not run_id or not artifact_ids:
        raise ValueError("cannot reconcile result_registry without current pointer identity and artifact_ids")
    result_entry = result_registry.register(
        record_id=f"result-repair-reconcile-{_safe_id_part(run_id)}-{_safe_id_part(registered_at)}",
        result_id=result_id,
        run_id=run_id,
        ts_code=ts_code,
        stock_name=stock_name,
        lifecycle_stage=lifecycle_stage,
        artifact_ids=artifact_ids,
        registered_at=registered_at,
        source_scope="stock",
        make_current=True,
        metadata={
            "lifecycle_id": lifecycle_id,
            "source_scope": "stock",
            "repair_mode": "post_lifecycle_reconcile",
            "producer": "scripts.run_primary_result_lifecycle",
            "code_revision": "workspace",
        },
    )
    pointer_snapshot = pointer_store.point_to(
        pointer_snapshot_id=f"current-result-pointer-reconcile-{_safe_id_part(run_id)}-{_safe_id_part(registered_at)}",
        result_id=result_id,
        run_id=run_id,
        lifecycle_id=lifecycle_id,
        artifact_ids=artifact_ids,
        as_of_date=str(pointer.get("as_of_date") or registered_at[:10]).strip() or registered_at[:10],
        source_scope=str(pointer.get("source_scope") or "stock").strip() or "stock",
        updated_at=registered_at,
        metadata={
            "result_registry_record_id": result_entry["record_id"],
            "source_scope": str(pointer.get("source_scope") or "stock").strip() or "stock",
            "repair_mode": "post_lifecycle_reconcile",
            "producer": "scripts.run_primary_result_lifecycle",
            "code_revision": "workspace",
        },
    )
    return {
        "result_record": result_entry,
        "pointer_snapshot": pointer_snapshot,
    }


def _synchronize_lifecycle_evidence_registry_chain_after_reconcile(
    *,
    lifecycle_output_path: Path,
    reconcile_record: dict[str, object],
    artifacts_dir: Path,
    run_id: str,
) -> None:
    payload = json.loads(lifecycle_output_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("lifecycle evidence payload is not a valid object json")
    registry_chain = payload.get("registry_chain")
    if not isinstance(registry_chain, dict):
        return
    result_record = reconcile_record.get("result_record")
    pointer_snapshot = reconcile_record.get("pointer_snapshot")
    if isinstance(result_record, dict):
        registry_chain["result_registry"] = {
            "record_id": result_record.get("record_id"),
            "lifecycle_stage": result_record.get("lifecycle_stage"),
            "source_scope": result_record.get("source_scope"),
            "producer": "scripts.run_primary_result_lifecycle",
            "code_revision": "workspace",
            "entry_path": str(artifacts_dir / "result_registry" / "history" / f"{result_record.get('record_id')}.json"),
        }
    if isinstance(pointer_snapshot, dict):
        registry_chain["current_result_pointer"] = {
            "pointer_snapshot_id": pointer_snapshot.get("pointer_snapshot_id"),
            "result_id": pointer_snapshot.get("result_id"),
            "run_id": pointer_snapshot.get("run_id"),
            "lifecycle_id": pointer_snapshot.get("lifecycle_id"),
            "artifact_ids": list(pointer_snapshot.get("artifact_ids") or []),
            "as_of_date": pointer_snapshot.get("as_of_date"),
            "source_scope": pointer_snapshot.get("source_scope"),
            "producer": "scripts.run_primary_result_lifecycle",
            "code_revision": "workspace",
            "snapshot_path": str(
                artifacts_dir / "current_result_pointer" / "history" / f"{pointer_snapshot.get('pointer_snapshot_id')}.json"
            ),
        }
        registry_chain["pointer"] = pointer_snapshot
    lifecycle_output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    artifact_registry_path = artifacts_dir / "artifact_registry.jsonl"
    registry_lines: list[str] = []
    for line in artifact_registry_path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        entry = json.loads(line)
        if entry.get("artifact_id") == f"{run_id}:lifecycle-evidence":
            entry["sha256"] = sha256_file(lifecycle_output_path)
        registry_lines.append(json.dumps(entry, ensure_ascii=False, sort_keys=True))
    artifact_registry_path.write_text("\n".join(registry_lines) + "\n", encoding="utf-8")


def repair_primary_result_chain(
    *,
    exp_dir: str | Path = "data/experiments",
    artifacts_dir: str | Path = "artifacts",
    candidate_index: int = 0,
    max_source_age_hours: float = 72.0,
    observation_status: str = "observing",
    observation_reason: str = "pointer bootstrap opened controlled observation window",
    observation_window_start: str | None = None,
    observation_window_end: str | None = None,
    observed_return: float | None = None,
    benchmark_return: float | None = None,
    max_drawdown: float | None = None,
    bootstrap_run_id: str | None = None,
    bootstrap_lifecycle_id: str | None = None,
    lifecycle_run_id: str | None = None,
    lifecycle_id: str | None = None,
    output_path: str | Path | None = None,
    now: datetime | None = None,
) -> tuple[int, dict[str, object]]:
    resolved_exp_dir = resolve_project_path(exp_dir)
    resolved_artifacts_dir = resolve_project_path(artifacts_dir)
    started_at = _isoformat_z(now)

    seed_payload = build_primary_result_api_payload(
        resolved_exp_dir,
        candidate_index=candidate_index,
        require_current_pointer=False,
        ignore_current_pointer=True,
    )
    result_id = str(seed_payload.get("result_id") or "").strip()
    ts_code = str(seed_payload.get("ts_code") or "").strip()
    if not _seed_payload_usable(seed_payload):
        payload = {
            "repair_version": CHAIN_REPAIR_VERSION,
            "generated_at": _isoformat_z(),
            "status": "failed",
            "reason": "seed payload unavailable; cannot bootstrap current result chain",
            "seed_payload": seed_payload,
        }
        if output_path is not None:
            _write_output(resolve_project_path(output_path), payload)
        return 1, payload

    resolved_bootstrap_lifecycle_id = bootstrap_lifecycle_id or f"primary-bootstrap-{_safe_id_part(ts_code)}-{_safe_id_part(started_at)}"
    resolved_bootstrap_run_id = bootstrap_run_id or f"primary_bootstrap_{_safe_id_part(ts_code)}_{_safe_id_part(started_at)}"
    resolved_lifecycle_id = lifecycle_id or f"primary-lifecycle-{_safe_id_part(started_at)}-{_safe_id_part(ts_code)}"
    resolved_lifecycle_run_id = lifecycle_run_id or f"primary_run_{_safe_id_part(started_at)}_{_safe_id_part(ts_code)}"
    existing_pointer_path = resolved_artifacts_dir / "current_result_pointer" / "current.json"
    seed_differs_from_existing_pointer = False
    if existing_pointer_path.exists():
        try:
            existing_pointer = json.loads(existing_pointer_path.read_text(encoding="utf-8"))
            seed_differs_from_existing_pointer = str(existing_pointer.get("result_id") or "").strip() != result_id
        except Exception:
            seed_differs_from_existing_pointer = True

    bootstrap_steps: list[dict[str, object]] = []
    if not seed_differs_from_existing_pointer:
        artifact_registry = ArtifactRegistry(resolved_artifacts_dir / "artifact_registry.jsonl")
        run_registry = RunRegistry(runs_dir=resolved_artifacts_dir / "run_registry")
        run_registry.register(
            run_id=resolved_bootstrap_run_id,
            run_type="primary_result_chain_repair",
            producer="scripts.repair_primary_result_chain",
            config_hash=_sha256_json(
                {
                    "candidate_index": candidate_index,
                    "max_source_age_hours": max_source_age_hours,
                    "observation_status": observation_status,
                    "observation_reason": observation_reason,
                    "observation_window_start": observation_window_start,
                }
            ),
            data_snapshot_id=_build_data_snapshot_id(resolved_exp_dir, seed_payload),
            code_revision="workspace",
            status="bootstrap_ready",
            created_at=started_at,
            make_current=True,
            metadata={
                "lifecycle_id": resolved_bootstrap_lifecycle_id,
                "result_id": result_id,
                "ts_code": ts_code,
                "source_scope": "stock",
                "repair_mode": "bootstrap",
                "producer": "scripts.repair_primary_result_chain",
                "code_revision": "workspace",
            },
        )
        bootstrap_artifact_ids: list[str] = []
        current_payload = seed_payload
        for step_name in BOOTSTRAP_STEP_ORDER:
            exit_code, step_payload = _build_bootstrap_step_payload(
                step_name=step_name,
                primary_result_payload=current_payload,
                observation_status=observation_status,
                observation_reason=observation_reason,
                observation_window_start=observation_window_start,
                observation_window_end=observation_window_end,
                observed_return=observed_return,
                benchmark_return=benchmark_return,
                max_drawdown=max_drawdown,
                max_source_age_hours=max_source_age_hours,
                now=now,
            )
            step_path = _bootstrap_artifact_path(resolved_exp_dir, step_name)
            _write_output(step_path, step_payload)
            artifact_id = f"{resolved_bootstrap_run_id}:{step_name}"
            artifact_registry.register_artifact(
                artifact_type=f"primary_result_{step_name}",
                run_id=resolved_bootstrap_run_id,
                path=step_path,
                producer="scripts.repair_primary_result_chain",
                artifact_id=artifact_id,
                created_at=str(step_payload.get("generated_at") or started_at),
                result_id=result_id,
                code_revision="workspace",
                metadata={
                    "lifecycle_id": resolved_bootstrap_lifecycle_id,
                    "repair_mode": "bootstrap",
                    "step": step_name,
                    "status": step_payload.get(f"{step_name}_status") or step_payload.get("observation_status"),
                    "exit_code": exit_code,
                },
            )
            bootstrap_artifact_ids.append(artifact_id)
            chain_state = _register_chain_state(
                artifacts_dir=resolved_artifacts_dir,
                run_id=resolved_bootstrap_run_id,
                lifecycle_id=resolved_bootstrap_lifecycle_id,
                payload=step_payload.get("primary_result_payload") or current_payload,
                artifact_ids=bootstrap_artifact_ids,
                updated_at=str(step_payload.get("generated_at") or started_at),
                snapshot_suffix=f"{len(bootstrap_artifact_ids)}-{step_name}",
            )
            current_payload = build_primary_result_api_payload(
                resolved_exp_dir,
                candidate_index=candidate_index,
                require_current_pointer=True,
            )
            bootstrap_steps.append(
                {
                    "step": step_name,
                    "exit_code": exit_code,
                    "artifact_id": artifact_id,
                    "path": str(step_path),
                    "status": (
                        step_payload.get("audit_status")
                        or step_payload.get("execution_status")
                        or step_payload.get("rollback_status")
                        or step_payload.get("observation_status")
                    ),
                    "chain_state": chain_state,
                    "primary_result_payload": current_payload,
                }
            )

    lifecycle_output_path = (
        resolve_project_path(output_path)
        if output_path is not None
        else resolved_exp_dir / "primary_result_lifecycle_evidence_latest.json"
    )
    lifecycle_exit, lifecycle_payload = run_primary_result_lifecycle(
        exp_dir=resolved_exp_dir,
        candidate_index=candidate_index,
        max_source_age_hours=max_source_age_hours,
        observation_status=observation_status,
        observation_reason=observation_reason,
        observation_window_start=observation_window_start,
        observation_window_end=observation_window_end,
        observed_return=observed_return,
        benchmark_return=benchmark_return,
        max_drawdown=max_drawdown,
        lifecycle_id=resolved_lifecycle_id,
        run_id=resolved_lifecycle_run_id,
        output_path=lifecycle_output_path,
        now=now,
        seed_from_latest_candidate=seed_differs_from_existing_pointer,
    )

    lifecycle_registry_status: dict[str, object] | None = None
    result_registry_reconcile_status: dict[str, object] | None = None
    if lifecycle_payload.get("status") == "passed":
        registry = PrimaryResultLifecycleRegistry(lifecycles_dir=resolved_artifacts_dir / "primary_result_lifecycle")
        try:
            snapshot = registry.register(
                evidence_path=lifecycle_output_path,
                lifecycle_id=resolved_lifecycle_id,
                registered_at=str(lifecycle_payload.get("completed_at") or _isoformat_z()),
            )
            lifecycle_registry_status = {"status": "registered", "snapshot": snapshot}
        except FileExistsError:
            lifecycle_registry_status = {
                "status": "already_exists",
                "snapshot": registry.get_snapshot(resolved_lifecycle_id),
            }
        result_registry_reconcile_status = {
            "status": "registered",
            "record": _reconcile_result_registry_to_current_pointer(
                artifacts_dir=resolved_artifacts_dir,
                payload=lifecycle_payload.get("final_payload") or {},
                registered_at=str(lifecycle_payload.get("completed_at") or _isoformat_z()),
            ),
        }
        _synchronize_lifecycle_evidence_registry_chain_after_reconcile(
            lifecycle_output_path=lifecycle_output_path,
            reconcile_record=result_registry_reconcile_status["record"],
            artifacts_dir=resolved_artifacts_dir,
            run_id=resolved_lifecycle_run_id,
        )

    guard_payload = write_stock_entry_guard_artifact(
        output_path=resolved_artifacts_dir / "stock_entry_guard_latest.json",
        exp_dir=resolved_exp_dir,
        artifacts_dir=resolved_artifacts_dir,
    )
    payload = {
        "repair_version": CHAIN_REPAIR_VERSION,
        "generated_at": _isoformat_z(),
        "status": "passed" if guard_payload.get("ok") is True else "failed",
        "seed_payload": seed_payload,
        "bootstrap_run_id": resolved_bootstrap_run_id,
        "bootstrap_lifecycle_id": resolved_bootstrap_lifecycle_id,
        "bootstrap_steps": bootstrap_steps,
        "lifecycle_run_id": resolved_lifecycle_run_id,
        "lifecycle_id": resolved_lifecycle_id,
        "lifecycle_exit_code": lifecycle_exit,
        "lifecycle_payload": lifecycle_payload,
        "lifecycle_registry_status": lifecycle_registry_status,
        "result_registry_reconcile_status": result_registry_reconcile_status,
        "guard_payload": guard_payload,
    }
    _write_output(lifecycle_output_path.parent / "primary_result_chain_repair_latest.json", payload)
    return (0 if payload["status"] == "passed" else 1), payload


def main() -> int:
    parser = argparse.ArgumentParser(description="Bootstrap and repair the primary result current chain, then rerun lifecycle.")
    parser.add_argument("--exp-dir", default="data/experiments")
    parser.add_argument("--artifacts-dir", default="artifacts")
    parser.add_argument("--candidate-index", type=int, default=0)
    parser.add_argument("--max-source-age-hours", type=float, default=72.0)
    parser.add_argument("--observation-status", default="observing")
    parser.add_argument("--observation-reason", default="pointer bootstrap opened controlled observation window")
    parser.add_argument("--observation-window-start")
    parser.add_argument("--observation-window-end")
    parser.add_argument("--observed-return", type=float)
    parser.add_argument("--benchmark-return", type=float)
    parser.add_argument("--max-drawdown", type=float)
    parser.add_argument("--bootstrap-run-id")
    parser.add_argument("--bootstrap-lifecycle-id")
    parser.add_argument("--lifecycle-run-id")
    parser.add_argument("--lifecycle-id")
    parser.add_argument("--output")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    exit_code, payload = repair_primary_result_chain(
        exp_dir=args.exp_dir,
        artifacts_dir=args.artifacts_dir,
        candidate_index=args.candidate_index,
        max_source_age_hours=args.max_source_age_hours,
        observation_status=args.observation_status,
        observation_reason=args.observation_reason,
        observation_window_start=args.observation_window_start,
        observation_window_end=args.observation_window_end,
        observed_return=args.observed_return,
        benchmark_return=args.benchmark_return,
        max_drawdown=args.max_drawdown,
        bootstrap_run_id=args.bootstrap_run_id,
        bootstrap_lifecycle_id=args.bootstrap_lifecycle_id,
        lifecycle_run_id=args.lifecycle_run_id,
        lifecycle_id=args.lifecycle_id,
        output_path=args.output,
    )
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print(
            json.dumps(
                {
                    "status": payload["status"],
                    "bootstrap_run_id": payload["bootstrap_run_id"],
                    "lifecycle_run_id": payload["lifecycle_run_id"],
                    "guard_ok": payload["guard_payload"]["ok"],
                    "lifecycle_status": payload["lifecycle_payload"]["status"],
                },
                ensure_ascii=False,
                indent=2,
            )
        )
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
