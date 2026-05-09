#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.artifact_registry import ArtifactRegistry, sha256_file
from src.artifact_source_guard import is_rejected_temp_source_path
from src.current_result_pointer import CurrentResultPointerStore
from src.result_registry import ResultRegistry
from src.run_registry import RunRegistry
from src.utils.project_paths import resolve_project_path

_EXPECTED_ARTIFACT_TYPES_BY_STAGE: dict[str, tuple[str, ...]] = {
    "L3": ("primary_result_audit", "primary_result_lifecycle_evidence"),
    "L4": (
        "primary_result_audit",
        "primary_result_execution",
        "primary_result_rollback",
        "primary_result_observation",
        "primary_result_lifecycle_evidence",
    ),
    "L5": (
        "primary_result_audit",
        "primary_result_execution",
        "primary_result_rollback",
        "primary_result_observation",
        "primary_result_terminal",
        "primary_result_lifecycle_evidence",
    ),
}
_EXPECTED_MAIN_CHAIN_RUN_TYPE = "primary_result_lifecycle"
_EXPECTED_MAIN_CHAIN_ARTIFACT_PRODUCER = "scripts.run_primary_result_lifecycle"


def _parse_iso_timestamp(value: object) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None


def check_current_result_pointer_integrity(
    *,
    pointer_dir: str | Path = "artifacts/current_result_pointer",
    results_dir: str | Path = "artifacts/result_registry",
    runs_dir: str | Path = "artifacts/run_registry",
    artifact_registry_path: str | Path | None = None,
) -> tuple[int, dict[str, object]]:
    pointer_store = CurrentResultPointerStore(pointer_dir=pointer_dir)
    result_registry = ResultRegistry(results_dir=results_dir)
    run_registry = RunRegistry(runs_dir=runs_dir)
    resolved_pointer_dir = resolve_project_path(pointer_dir)
    resolved_results_dir = resolve_project_path(results_dir)
    resolved_runs_dir = resolve_project_path(runs_dir)
    resolved_artifact_registry_path = (
        resolve_project_path(artifact_registry_path)
        if artifact_registry_path is not None
        else resolved_pointer_dir.parent / "artifact_registry.jsonl"
    )
    artifact_registry = ArtifactRegistry(resolved_artifact_registry_path)
    run_entry_payload: dict[str, object] | None = None

    pointer = pointer_store.get_current_pointer()
    required_fields = ("result_id", "run_id", "lifecycle_id", "artifact_ids", "as_of_date", "source_scope", "snapshot_path")
    missing_fields = [field for field in required_fields if not pointer.get(field)]
    problems: list[str] = []
    if missing_fields:
        problems.append(f"missing pointer fields: {', '.join(missing_fields)}")

    snapshot_path = pointer.get("snapshot_path")
    if snapshot_path and not Path(str(snapshot_path)).exists():
        problems.append(f"pointer snapshot missing: {snapshot_path}")
    if snapshot_path and is_rejected_temp_source_path(snapshot_path):
        resolved_snapshot_path = Path(str(snapshot_path)).resolve()
        try:
            resolved_snapshot_path.relative_to(resolved_pointer_dir)
        except ValueError:
            problems.append(f"pointer snapshot points to a temporary or pytest-derived path: {snapshot_path}")
    pointer_snapshot_payload: dict[str, object] | None = None
    if snapshot_path and Path(str(snapshot_path)).exists():
        raw_snapshot_payload = json.loads(Path(str(snapshot_path)).read_text(encoding="utf-8"))
        if isinstance(raw_snapshot_payload, dict):
            pointer_snapshot_payload = raw_snapshot_payload
    pointer_updated_at = _parse_iso_timestamp(pointer.get("updated_at"))
    if str(pointer.get("updated_at") or "").strip() and pointer_updated_at is None:
        problems.append("current result pointer updated_at unreadable")

    result_id = str(pointer.get("result_id") or "").strip()
    run_id = str(pointer.get("run_id") or "").strip()
    artifact_ids = pointer.get("artifact_ids")
    ordered_artifact_types: list[str] = []
    artifact_entries: list[dict[str, object]] = []
    if artifact_ids is not None and not isinstance(artifact_ids, list):
        problems.append("artifact_ids must be a list")
    elif isinstance(artifact_ids, list) and not artifact_ids:
        problems.append("artifact_ids must not be empty")
    elif isinstance(artifact_ids, list) and resolved_artifact_registry_path.exists():
        for artifact_id in artifact_ids:
            try:
                entry = artifact_registry.get_entry(str(artifact_id))
            except FileNotFoundError:
                problems.append(f"artifact registry entry missing for artifact_id={artifact_id}")
                continue
            if result_id and str(entry.get("result_id") or "").strip() not in {"", result_id}:
                problems.append(f"artifact registry result_id mismatch for artifact_id={artifact_id}")
            if run_id and entry.get("run_id") != run_id:
                problems.append(f"artifact registry run_id mismatch for artifact_id={artifact_id}")
            artifact_path = Path(str(entry.get("path") or "")).resolve()
            if not artifact_path.exists():
                problems.append(f"artifact registry path missing for artifact_id={artifact_id}")
                continue
            expected_sha256 = str(entry.get("sha256") or "").strip()
            actual_sha256 = sha256_file(artifact_path)
            if expected_sha256 and actual_sha256 != expected_sha256:
                problems.append(f"artifact registry sha256 mismatch for artifact_id={artifact_id}")
            artifact_type = str(entry.get("artifact_type") or "").strip()
            if artifact_type:
                ordered_artifact_types.append(artifact_type)
            artifact_entries.append(entry)

    current_result_pointer = result_registry.get_current_pointer()
    if result_id and current_result_pointer.get("result_id") != result_id:
        problems.append("result registry current pointer does not match current result pointer")
    pointer_source_scope = str(pointer.get("source_scope") or "").strip()
    result_current_source_scope = str(current_result_pointer.get("source_scope") or "").strip()
    if pointer_source_scope and result_current_source_scope and result_current_source_scope != pointer_source_scope:
        problems.append("result registry current source_scope does not match current result pointer")
    current_record_id = str(current_result_pointer.get("record_id") or "").strip()
    current_entry_path = str(current_result_pointer.get("entry_path") or "").strip()
    result_current_updated_at = _parse_iso_timestamp(current_result_pointer.get("updated_at"))
    if str(current_result_pointer.get("updated_at") or "").strip() and result_current_updated_at is None:
        problems.append("result registry current pointer updated_at unreadable")
    if current_record_id and not current_entry_path:
        problems.append("result registry current pointer entry_path missing")
    elif current_entry_path:
        resolved_current_entry_path = Path(current_entry_path).resolve()
        try:
            resolved_current_entry_path.relative_to(resolved_results_dir / "history")
        except ValueError:
            problems.append("result registry current pointer entry_path is outside managed history dir")
        if not resolved_current_entry_path.exists():
            problems.append(f"result registry current pointer entry missing: {current_entry_path}")
        else:
            current_entry_payload = json.loads(resolved_current_entry_path.read_text(encoding="utf-8"))
            if not isinstance(current_entry_payload, dict):
                problems.append("result registry current pointer entry is not a valid object json")
            else:
                if current_record_id and current_entry_payload.get("record_id") != current_record_id:
                    problems.append("result registry current pointer record_id does not match history entry")
                if result_id and current_entry_payload.get("result_id") != result_id:
                    problems.append("result registry current pointer result_id does not match current result pointer")
                if run_id and current_entry_payload.get("run_id") != run_id:
                    problems.append("result registry current pointer run_id does not match current result pointer")
                current_entry_source_scope = str(current_entry_payload.get("source_scope") or "").strip()
                if pointer_source_scope and current_entry_source_scope and current_entry_source_scope != pointer_source_scope:
                    problems.append("result registry current history source_scope does not match current result pointer")
                current_entry_artifact_ids = current_entry_payload.get("artifact_ids")
                if isinstance(current_entry_artifact_ids, list) and isinstance(artifact_ids, list) and current_entry_artifact_ids != artifact_ids:
                    problems.append("result registry current history artifact_ids do not match current result pointer")

    if run_id:
        try:
            run_entry = run_registry.get_run(run_id)
        except FileNotFoundError:
            problems.append(f"run registry entry missing for run_id={run_id}")
        else:
            run_entry_payload = run_entry
            if run_entry.get("run_id") != run_id:
                problems.append(f"run registry returned mismatched run_id={run_entry.get('run_id')}")
            if str(run_entry.get("run_type") or "").strip() != _EXPECTED_MAIN_CHAIN_RUN_TYPE:
                problems.append("run registry history entry run_type does not match main chain lifecycle type")
            expected_code_revision = str(run_entry.get("code_revision") or "").strip()
            for entry in artifact_entries:
                artifact_id = str(entry.get("artifact_id") or "").strip()
                artifact_producer = str(entry.get("producer") or "").strip()
                if artifact_producer and artifact_producer != _EXPECTED_MAIN_CHAIN_ARTIFACT_PRODUCER:
                    problems.append(f"artifact registry producer is not allowed for current formal main chain: {artifact_id}")
                artifact_code_revision = str(entry.get("code_revision") or "").strip()
                if expected_code_revision and artifact_code_revision and artifact_code_revision != expected_code_revision:
                    problems.append(f"artifact registry code_revision does not match main chain run history: {artifact_id}")
    run_current_pointer = run_registry.get_current_pointer()
    current_run_id = str(run_current_pointer.get("run_id") or "").strip()
    current_run_type = str(run_current_pointer.get("run_type") or "").strip()
    current_run_entry_path = str(run_current_pointer.get("entry_path") or "").strip()
    run_current_updated_at = _parse_iso_timestamp(run_current_pointer.get("updated_at"))
    if str(run_current_pointer.get("updated_at") or "").strip() and run_current_updated_at is None:
        problems.append("run registry current pointer updated_at unreadable")
    if run_id and current_run_id and current_run_id != run_id:
        problems.append("run registry current pointer does not match current result pointer run_id")
    if current_run_type and current_run_type != _EXPECTED_MAIN_CHAIN_RUN_TYPE:
        problems.append("run registry current pointer run_type does not match main chain lifecycle type")
    if run_id and current_run_id and current_run_id != run_id and current_run_type and current_run_type != _EXPECTED_MAIN_CHAIN_RUN_TYPE:
        problems.append("run registry current pointer diverged to a non-main-chain run while formal main-chain pointer remains active")
    if current_run_id and not current_run_entry_path:
        problems.append("run registry current pointer entry_path missing")
    elif current_run_entry_path:
        resolved_current_run_entry_path = Path(current_run_entry_path).resolve()
        try:
            resolved_current_run_entry_path.relative_to(resolved_runs_dir / "history")
        except ValueError:
            problems.append("run registry current pointer entry_path is outside managed history dir")
        if not resolved_current_run_entry_path.exists():
            problems.append(f"run registry current pointer entry missing: {current_run_entry_path}")
        else:
            current_run_entry_payload = json.loads(resolved_current_run_entry_path.read_text(encoding="utf-8"))
            if not isinstance(current_run_entry_payload, dict):
                problems.append("run registry current pointer entry is not a valid object json")
            else:
                if current_run_id and current_run_entry_payload.get("run_id") != current_run_id:
                    problems.append("run registry current pointer run_id does not match history entry")
                if current_run_type and current_run_entry_payload.get("run_type") != current_run_type:
                    problems.append("run registry current pointer run_type does not match history entry")
                if run_id and current_run_entry_payload.get("run_id") != run_id:
                    problems.append("run registry current pointer history entry does not match current result pointer run_id")
                current_run_entry_created_at = _parse_iso_timestamp(current_run_entry_payload.get("created_at"))
                if str(current_run_entry_payload.get("created_at") or "").strip() and current_run_entry_created_at is None:
                    problems.append("run registry current history entry created_at unreadable")
                if run_current_updated_at is not None and current_run_entry_created_at is not None and run_current_updated_at != current_run_entry_created_at:
                    problems.append("run registry current pointer updated_at does not match current run history created_at")

    latest_result_record = result_registry.get_latest_record_for_result(result_id) if result_id else None
    if result_id and latest_result_record is None:
        problems.append(f"result registry entry missing for result_id={result_id}")
    elif latest_result_record is not None:
        latest_record_id = str(latest_result_record.get("record_id") or "").strip()
        if current_record_id and latest_record_id and current_record_id != latest_record_id:
            problems.append("result registry current record_id does not match chronological latest record for result_id")
        if latest_result_record.get("run_id") != run_id:
            problems.append("result registry latest record run_id does not match current result pointer")
        latest_source_scope = str(latest_result_record.get("source_scope") or "").strip()
        if pointer_source_scope and latest_source_scope and latest_source_scope != pointer_source_scope:
            problems.append("result registry latest record source_scope does not match current result pointer")
        latest_artifact_ids = latest_result_record.get("artifact_ids")
        if isinstance(latest_artifact_ids, list) and isinstance(artifact_ids, list) and latest_artifact_ids != artifact_ids:
            problems.append("result registry artifact_ids do not match current result pointer")
        lifecycle_stage = str(latest_result_record.get("lifecycle_stage") or "").strip().upper()
        latest_result_registered_at = _parse_iso_timestamp(latest_result_record.get("registered_at"))
        if str(latest_result_record.get("registered_at") or "").strip() and latest_result_registered_at is None:
            problems.append("result registry current history registered_at unreadable")
        expected_artifact_types = _EXPECTED_ARTIFACT_TYPES_BY_STAGE.get(lifecycle_stage)
        if expected_artifact_types and isinstance(artifact_ids, list) and len(artifact_ids) > 1:
            if tuple(ordered_artifact_types) != expected_artifact_types:
                problems.append("artifact_ids order/types do not match lifecycle stage sequence")
        if run_entry_payload is not None:
            run_metadata = run_entry_payload.get("metadata")
            if isinstance(run_metadata, dict):
                metadata_lifecycle_id = str(run_metadata.get("lifecycle_id") or "").strip()
                metadata_result_id = str(run_metadata.get("result_id") or "").strip()
                metadata_ts_code = str(run_metadata.get("ts_code") or "").strip()
                metadata_source_scope = str(run_metadata.get("source_scope") or "").strip()
                metadata_evidence_path = str(run_metadata.get("evidence_path") or "").strip()
                metadata_artifact_registry_path = str(run_metadata.get("artifact_registry_path") or "").strip()
                metadata_repair_mode = str(run_metadata.get("repair_mode") or "").strip()
                pointer_lifecycle_id = str(pointer.get("lifecycle_id") or "").strip()
                if pointer_lifecycle_id and metadata_lifecycle_id and metadata_lifecycle_id != pointer_lifecycle_id:
                    problems.append("run registry history metadata lifecycle_id does not match current result pointer")
                if result_id and metadata_result_id and metadata_result_id != result_id:
                    problems.append("run registry history metadata result_id does not match current result pointer")
                if pointer_source_scope and metadata_source_scope and metadata_source_scope != pointer_source_scope:
                    problems.append("run registry history metadata source_scope does not match current result pointer")
                if result_current_source_scope and metadata_source_scope and metadata_source_scope != result_current_source_scope:
                    problems.append("run registry history metadata source_scope does not match result registry current source_scope")
                expected_ts_code = str(latest_result_record.get("ts_code") or "").strip()
                if expected_ts_code and metadata_ts_code and metadata_ts_code != expected_ts_code:
                    problems.append("run registry history metadata ts_code does not match result registry current ts_code")
                latest_source_scope = str(latest_result_record.get("source_scope") or "").strip()
                if latest_source_scope and metadata_source_scope and metadata_source_scope != latest_source_scope:
                    problems.append("run registry history metadata source_scope does not match result registry current source_scope")
                result_metadata = latest_result_record.get("metadata")
                if isinstance(result_metadata, dict):
                    result_evidence_path = str(result_metadata.get("evidence_path") or "").strip()
                    if metadata_evidence_path and result_evidence_path:
                        if Path(metadata_evidence_path).resolve() != Path(result_evidence_path).resolve():
                            problems.append("run registry history metadata evidence_path does not match result registry current evidence_path")
                if metadata_artifact_registry_path and Path(metadata_artifact_registry_path).resolve() != resolved_artifact_registry_path.resolve():
                    problems.append("run registry history metadata artifact_registry_path does not match active artifact registry")
                if metadata_repair_mode and metadata_repair_mode != "post_lifecycle_reconcile":
                    problems.append("run registry history metadata repair_mode is not allowed for current formal main chain")
                if (
                    run_current_updated_at is not None
                    and result_current_updated_at is not None
                    and run_current_updated_at <= result_current_updated_at
                    and (
                        (metadata_repair_mode and metadata_repair_mode != "post_lifecycle_reconcile")
                        or (metadata_source_scope and pointer_source_scope and metadata_source_scope != pointer_source_scope)
                    )
                ):
                    problems.append(
                        "run registry current updated_at is chronologically aligned but history metadata carries non-main-chain semantics"
                    )
        if pointer_snapshot_payload is not None:
            pointer_snapshot_updated_at = _parse_iso_timestamp(pointer_snapshot_payload.get("updated_at"))
            if str(pointer_snapshot_payload.get("updated_at") or "").strip() and pointer_snapshot_updated_at is None:
                problems.append("current result pointer snapshot updated_at unreadable")
            if pointer_updated_at is not None and pointer_snapshot_updated_at is not None and pointer_updated_at != pointer_snapshot_updated_at:
                problems.append("current result pointer updated_at does not match pointer snapshot updated_at")
            snapshot_metadata = pointer_snapshot_payload.get("metadata")
            if isinstance(snapshot_metadata, dict):
                snapshot_result_record_id = str(snapshot_metadata.get("result_registry_record_id") or "").strip()
                if latest_record_id and snapshot_result_record_id and snapshot_result_record_id != latest_record_id:
                    problems.append("current result pointer snapshot metadata result_registry_record_id does not match result registry current record")
                snapshot_source_scope = str(snapshot_metadata.get("source_scope") or "").strip()
                if pointer_source_scope and snapshot_source_scope and snapshot_source_scope != pointer_source_scope:
                    problems.append("current result pointer snapshot metadata source_scope does not match active current result pointer")
                if result_current_source_scope and snapshot_source_scope and snapshot_source_scope != result_current_source_scope:
                    problems.append("current result pointer snapshot metadata source_scope does not match result registry current source_scope")
                if run_entry_payload is not None:
                    expected_run_producer = str(run_entry_payload.get("producer") or "").strip()
                    snapshot_producer = str(snapshot_metadata.get("producer") or "").strip()
                    if expected_run_producer and snapshot_producer and snapshot_producer != expected_run_producer:
                        problems.append("current result pointer snapshot metadata producer does not match main chain run history")
                    expected_run_code_revision = str(run_entry_payload.get("code_revision") or "").strip()
                    snapshot_code_revision = str(snapshot_metadata.get("code_revision") or "").strip()
                    if expected_run_code_revision and snapshot_code_revision and snapshot_code_revision != expected_run_code_revision:
                        problems.append("current result pointer snapshot metadata code_revision does not match main chain run history")
                result_metadata = latest_result_record.get("metadata")
                if isinstance(result_metadata, dict):
                    snapshot_producer = str(snapshot_metadata.get("producer") or "").strip()
                    result_metadata_producer = str(result_metadata.get("producer") or "").strip()
                    if result_metadata_producer and snapshot_producer and snapshot_producer != result_metadata_producer:
                        problems.append("current result pointer snapshot metadata producer does not match result registry current metadata producer")
                    snapshot_code_revision = str(snapshot_metadata.get("code_revision") or "").strip()
                    result_metadata_code_revision = str(result_metadata.get("code_revision") or "").strip()
                    if result_metadata_code_revision and snapshot_code_revision and snapshot_code_revision != result_metadata_code_revision:
                        problems.append("current result pointer snapshot metadata code_revision does not match result registry current metadata code_revision")
                    snapshot_evidence_path = str(snapshot_metadata.get("evidence_path") or "").strip()
                    result_evidence_path = str(result_metadata.get("evidence_path") or "").strip()
                    if snapshot_evidence_path and result_evidence_path:
                        if Path(snapshot_evidence_path).resolve() != Path(result_evidence_path).resolve():
                            problems.append(
                                "current result pointer snapshot metadata evidence_path does not match result registry current evidence_path"
                            )
                    snapshot_artifact_registry_path = str(snapshot_metadata.get("artifact_registry_path") or "").strip()
                    if snapshot_artifact_registry_path and Path(snapshot_artifact_registry_path).resolve() != resolved_artifact_registry_path.resolve():
                        problems.append(
                            "current result pointer snapshot metadata artifact_registry_path does not match active artifact registry"
                        )
        result_metadata = latest_result_record.get("metadata")
        if isinstance(result_metadata, dict):
            metadata_lifecycle_id = str(result_metadata.get("lifecycle_id") or "").strip()
            pointer_lifecycle_id = str(pointer.get("lifecycle_id") or "").strip()
            if pointer_lifecycle_id and metadata_lifecycle_id and metadata_lifecycle_id != pointer_lifecycle_id:
                problems.append("result registry metadata lifecycle_id does not match current result pointer")
            if run_entry_payload is not None:
                expected_run_producer = str(run_entry_payload.get("producer") or "").strip()
                result_metadata_producer = str(result_metadata.get("producer") or "").strip()
                if expected_run_producer and result_metadata_producer and result_metadata_producer != expected_run_producer:
                    problems.append("result registry metadata producer does not match main chain run history")
                expected_run_code_revision = str(run_entry_payload.get("code_revision") or "").strip()
                result_metadata_code_revision = str(result_metadata.get("code_revision") or "").strip()
                if expected_run_code_revision and result_metadata_code_revision and result_metadata_code_revision != expected_run_code_revision:
                    problems.append("result registry metadata code_revision does not match main chain run history")
            metadata_repair_mode = str(result_metadata.get("repair_mode") or "").strip()
            if metadata_repair_mode and metadata_repair_mode != "post_lifecycle_reconcile":
                problems.append("result registry metadata repair_mode is not allowed for current formal main chain")
            metadata_artifact_registry_path = str(result_metadata.get("artifact_registry_path") or "").strip()
            if metadata_artifact_registry_path and Path(metadata_artifact_registry_path).resolve() != resolved_artifact_registry_path.resolve():
                problems.append("result registry metadata artifact_registry_path does not match active artifact registry")
            lifecycle_completed_at = None
            metadata_evidence_path = str(result_metadata.get("evidence_path") or "").strip()
            if metadata_evidence_path:
                evidence_path = Path(metadata_evidence_path)
                if evidence_path.exists():
                    raw_evidence_payload = json.loads(evidence_path.read_text(encoding="utf-8"))
                    if isinstance(raw_evidence_payload, dict):
                        lifecycle_completed_at = _parse_iso_timestamp(raw_evidence_payload.get("completed_at"))
                        if str(raw_evidence_payload.get("completed_at") or "").strip() and lifecycle_completed_at is None:
                            problems.append("primary result lifecycle evidence completed_at unreadable")
            if run_current_updated_at is not None and result_current_updated_at is not None and run_current_updated_at > result_current_updated_at:
                problems.append("run registry current updated_at is after result registry current updated_at")
            if result_current_updated_at is not None and pointer_updated_at is not None and result_current_updated_at != pointer_updated_at:
                problems.append("result registry current updated_at does not match current result pointer updated_at")
            if lifecycle_completed_at is not None:
                if result_current_updated_at is not None and result_current_updated_at != lifecycle_completed_at:
                    problems.append("result registry current updated_at does not match lifecycle evidence completed_at")
                if pointer_updated_at is not None and pointer_updated_at != lifecycle_completed_at:
                    problems.append("current result pointer updated_at does not match lifecycle evidence completed_at")
                if run_current_updated_at is not None and run_current_updated_at > lifecycle_completed_at:
                    problems.append("run registry current updated_at is after lifecycle evidence completed_at")

    payload = {
        "ok": not problems,
        "pointer_dir": str(Path(pointer_dir)),
        "results_dir": str(Path(results_dir)),
        "runs_dir": str(Path(runs_dir)),
        "artifact_registry_path": str(resolved_artifact_registry_path),
        "problems": problems,
        "pointer": pointer,
        "run_entry": run_entry_payload,
    }
    return (0 if not problems else 1), payload


def main() -> int:
    parser = argparse.ArgumentParser(description="Check current result pointer integrity.")
    parser.add_argument("--pointer-dir", default="artifacts/current_result_pointer")
    parser.add_argument("--results-dir", default="artifacts/result_registry")
    parser.add_argument("--runs-dir", default="artifacts/run_registry")
    parser.add_argument("--artifact-registry-path")
    args = parser.parse_args()

    exit_code, payload = check_current_result_pointer_integrity(
        pointer_dir=args.pointer_dir,
        results_dir=args.results_dir,
        runs_dir=args.runs_dir,
        artifact_registry_path=args.artifact_registry_path,
    )
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
