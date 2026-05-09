from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

from src.artifact_registry import ArtifactRegistry
from src.artifact_registry import sha256_file
from scripts.check_current_result_pointer_integrity import check_current_result_pointer_integrity
from src.result_registry import ResultRegistry
from src.utils.project_paths import resolve_artifacts_path, resolve_experiments_path, resolve_project_path


_DERIVED_LIFECYCLE_STAGE_BY_STEPS: dict[tuple[str, ...], str] = {
    ("audit",): "L3",
    ("audit", "execution", "rollback", "observation"): "L4",
    ("audit", "execution", "rollback", "observation", "terminal"): "L5",
}
_EXPECTED_MAIN_CHAIN_ARTIFACT_PRODUCER = "scripts.run_primary_result_lifecycle"


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _parse_iso_timestamp(value: object) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        normalized = text.replace("Z", "+00:00")
        return datetime.fromisoformat(normalized)
    except ValueError:
        return None


def evaluate_stock_entry_guard(
    *,
    exp_dir: str | Path = "data/experiments",
    artifacts_dir: str | Path = "artifacts",
) -> dict[str, Any]:
    resolved_exp_dir = resolve_experiments_path(exp_dir)
    resolved_artifacts_dir = resolve_artifacts_path(artifacts_dir)
    integrity_exit, integrity_payload = check_current_result_pointer_integrity(
        pointer_dir=resolved_artifacts_dir / "current_result_pointer",
        results_dir=resolved_artifacts_dir / "result_registry",
        runs_dir=resolved_artifacts_dir / "run_registry",
        artifact_registry_path=resolved_artifacts_dir / "artifact_registry.jsonl",
    )
    problems = list(integrity_payload.get("problems") or [])
    pointer = integrity_payload.get("pointer") or {}
    pointer_result_id = str(pointer.get("result_id") or "").strip()
    pointer_run_id = str(pointer.get("run_id") or "").strip()
    pointer_source_scope = str(pointer.get("source_scope") or "").strip()
    pointer_snapshot_path = Path(str(pointer.get("snapshot_path") or ""))
    pointer_snapshot_payload = _read_json(pointer_snapshot_path) if pointer_snapshot_path.exists() else {}
    current_result_record = (
        ResultRegistry(results_dir=resolved_artifacts_dir / "result_registry").get_latest_record_for_result(pointer_result_id)
        if pointer_result_id
        else None
    )
    artifact_registry = ArtifactRegistry(resolved_artifacts_dir / "artifact_registry.jsonl")
    expected_ts_code = str((current_result_record or {}).get("ts_code") or "").strip()
    run_registry_payload = (integrity_payload or {}).get("run_entry")

    lifecycle_evidence_path = resolved_exp_dir / "primary_result_lifecycle_evidence_latest.json"
    lifecycle_evidence = _read_json(lifecycle_evidence_path)
    if not lifecycle_evidence_path.exists():
        problems.append("primary_result_lifecycle_evidence_latest.json missing")
    elif not lifecycle_evidence:
        problems.append("primary_result_lifecycle_evidence_latest.json unreadable")
    else:
        lifecycle_started_at = _parse_iso_timestamp(lifecycle_evidence.get("started_at"))
        lifecycle_completed_at = _parse_iso_timestamp(lifecycle_evidence.get("completed_at"))
        if str(lifecycle_evidence.get("started_at") or "").strip() and lifecycle_started_at is None:
            problems.append("lifecycle evidence started_at unreadable")
        if str(lifecycle_evidence.get("completed_at") or "").strip() and lifecycle_completed_at is None:
            problems.append("lifecycle evidence completed_at unreadable")
        if lifecycle_started_at is not None and lifecycle_completed_at is not None and lifecycle_started_at > lifecycle_completed_at:
            problems.append("lifecycle evidence started_at is after completed_at")
        if str(lifecycle_evidence.get("status") or "").strip() != "passed":
            problems.append("primary result lifecycle evidence status is not passed")
        pointer_lifecycle_id = str(pointer.get("lifecycle_id") or "").strip()
        if pointer_result_id and str(lifecycle_evidence.get("result_id") or "").strip() != pointer_result_id:
            problems.append("lifecycle evidence result_id does not match current_result_pointer")
        if pointer_run_id and str(lifecycle_evidence.get("run_id") or "").strip() != pointer_run_id:
            problems.append("lifecycle evidence run_id does not match current_result_pointer")
        if pointer_lifecycle_id and str(lifecycle_evidence.get("lifecycle_id") or "").strip() != pointer_lifecycle_id:
            problems.append("lifecycle evidence lifecycle_id does not match current_result_pointer")
        if pointer_snapshot_payload:
            pointer_snapshot_metadata = pointer_snapshot_payload.get("metadata")
            if isinstance(pointer_snapshot_metadata, dict):
                snapshot_evidence_path = str(pointer_snapshot_metadata.get("evidence_path") or "").strip()
                if snapshot_evidence_path and Path(snapshot_evidence_path).resolve() != lifecycle_evidence_path.resolve():
                    problems.append(
                        "current result pointer snapshot metadata evidence_path does not match primary_result_lifecycle_evidence_latest.json"
                    )
                snapshot_artifact_registry_path = str(pointer_snapshot_metadata.get("artifact_registry_path") or "").strip()
                if snapshot_artifact_registry_path and Path(snapshot_artifact_registry_path).resolve() != (
                    resolved_artifacts_dir / "artifact_registry.jsonl"
                ).resolve():
                    problems.append(
                        "current result pointer snapshot metadata artifact_registry_path does not match active artifact registry"
                    )
                    snapshot_source_scope = str(pointer_snapshot_metadata.get("source_scope") or "").strip()
                    if pointer_source_scope and snapshot_source_scope and snapshot_source_scope != pointer_source_scope:
                        problems.append("current result pointer snapshot metadata source_scope does not match active current result pointer")
                    if isinstance(run_registry_payload, dict):
                        snapshot_producer = str(pointer_snapshot_metadata.get("producer") or "").strip()
                    expected_run_producer = str(run_registry_payload.get("producer") or "").strip()
                    if expected_run_producer and snapshot_producer and snapshot_producer != expected_run_producer:
                        problems.append("current result pointer snapshot metadata producer does not match main chain run history")
                    snapshot_code_revision = str(pointer_snapshot_metadata.get("code_revision") or "").strip()
                    expected_run_code_revision = str(run_registry_payload.get("code_revision") or "").strip()
                    if expected_run_code_revision and snapshot_code_revision and snapshot_code_revision != expected_run_code_revision:
                        problems.append("current result pointer snapshot metadata code_revision does not match main chain run history")
        final_payload = lifecycle_evidence.get("final_payload")
        if not isinstance(final_payload, dict):
            problems.append("lifecycle evidence final_payload missing")
        else:
            if pointer_result_id and str(final_payload.get("result_id") or "").strip() != pointer_result_id:
                problems.append("lifecycle evidence final_payload result_id does not match current_result_pointer")
            if pointer_run_id and str(final_payload.get("run_id") or "").strip() != pointer_run_id:
                problems.append("lifecycle evidence final_payload run_id does not match current_result_pointer")
            if pointer_lifecycle_id and str(final_payload.get("lifecycle_id") or "").strip() != pointer_lifecycle_id:
                problems.append("lifecycle evidence final_payload lifecycle_id does not match current_result_pointer")
            pointer_artifact_ids = list(pointer.get("artifact_ids") or [])
            final_payload_artifact_ids = list(final_payload.get("artifact_ids") or [])
            if pointer_artifact_ids and final_payload_artifact_ids != pointer_artifact_ids:
                problems.append("lifecycle evidence final_payload artifact_ids do not match current_result_pointer")
            pointer_as_of_date = str(pointer.get("as_of_date") or "").strip()
            if pointer_as_of_date and str(final_payload.get("as_of_date") or "").strip() != pointer_as_of_date:
                problems.append("lifecycle evidence final_payload as_of_date does not match current_result_pointer")
            final_source_scope = str(final_payload.get("source_scope") or "").strip()
            if pointer_source_scope and final_source_scope and final_source_scope != pointer_source_scope:
                problems.append("lifecycle evidence final_payload source_scope does not match current_result_pointer")
            registry_stage = str((current_result_record or {}).get("lifecycle_stage") or "").strip().upper()
            final_stage = str(final_payload.get("result_lifecycle_stage") or "").strip().upper()
            if registry_stage and final_stage and registry_stage != final_stage:
                problems.append("lifecycle evidence final_payload result_lifecycle_stage does not match result_registry current stage")
            current_result_source_scope = str((current_result_record or {}).get("source_scope") or "").strip()
            if current_result_source_scope and final_source_scope and final_source_scope != current_result_source_scope:
                problems.append("lifecycle evidence final_payload source_scope does not match result_registry current source_scope")
            if pointer_snapshot_payload:
                pointer_snapshot_metadata = pointer_snapshot_payload.get("metadata")
                if isinstance(pointer_snapshot_metadata, dict):
                    snapshot_source_scope = str(pointer_snapshot_metadata.get("source_scope") or "").strip()
                    if final_source_scope and snapshot_source_scope and snapshot_source_scope != final_source_scope:
                        problems.append("current result pointer snapshot metadata source_scope does not match lifecycle evidence final_payload source_scope")
            result_metadata = (current_result_record or {}).get("metadata")
            if isinstance(result_metadata, dict):
                metadata_lifecycle_id = str(result_metadata.get("lifecycle_id") or "").strip()
                if pointer_lifecycle_id and metadata_lifecycle_id and metadata_lifecycle_id != pointer_lifecycle_id:
                    problems.append("result registry metadata lifecycle_id does not match current result pointer")
                if run_registry_payload:
                    active_result_producer = str(result_metadata.get("producer") or "").strip()
                    expected_run_producer = str(run_registry_payload.get("producer") or "").strip()
                    if expected_run_producer and active_result_producer and active_result_producer != expected_run_producer:
                        problems.append("result registry metadata producer does not match main chain run history")
                    active_result_code_revision = str(result_metadata.get("code_revision") or "").strip()
                    expected_run_code_revision = str(run_registry_payload.get("code_revision") or "").strip()
                    if expected_run_code_revision and active_result_code_revision and active_result_code_revision != expected_run_code_revision:
                        problems.append("result registry metadata code_revision does not match main chain run history")
                metadata_repair_mode = str(result_metadata.get("repair_mode") or "").strip()
                if metadata_repair_mode and metadata_repair_mode != "post_lifecycle_reconcile":
                    problems.append("result registry metadata repair_mode is not allowed for current formal main chain")
                evidence_path = str(result_metadata.get("evidence_path") or "").strip()
                if evidence_path and Path(evidence_path).resolve() != lifecycle_evidence_path.resolve():
                    problems.append("result registry metadata evidence_path does not match primary_result_lifecycle_evidence_latest.json")
                metadata_artifact_registry_path = str(result_metadata.get("artifact_registry_path") or "").strip()
                if metadata_artifact_registry_path and Path(metadata_artifact_registry_path).resolve() != (resolved_artifacts_dir / "artifact_registry.jsonl").resolve():
                    problems.append("result registry metadata artifact_registry_path does not match active artifact registry")
                metadata_lifecycle_status = str(result_metadata.get("lifecycle_status") or "").strip()
                lifecycle_status = str(lifecycle_evidence.get("status") or "").strip()
                if metadata_lifecycle_status and lifecycle_status and metadata_lifecycle_status != lifecycle_status:
                    problems.append("result registry metadata lifecycle_status does not match lifecycle evidence status")
        registry_chain = lifecycle_evidence.get("registry_chain")
        if isinstance(registry_chain, dict):
            write_mode = str(registry_chain.get("write_mode") or "").strip()
            if write_mode and write_mode != "run_result_pointer_updated":
                problems.append("lifecycle evidence registry_chain write_mode is not allowed for current formal main chain")
        steps = lifecycle_evidence.get("steps")
        registry_chain = lifecycle_evidence.get("registry_chain")
        if isinstance(registry_chain, dict):
            embedded_result_registry = registry_chain.get("result_registry")
            if isinstance(embedded_result_registry, dict):
                active_result_record_id = str((current_result_record or {}).get("record_id") or "").strip()
                embedded_record_id = str(embedded_result_registry.get("record_id") or "").strip()
                if active_result_record_id and embedded_record_id and embedded_record_id != active_result_record_id:
                    problems.append("lifecycle evidence registry_chain result_registry record_id does not match active result registry current record")
                active_result_stage = str((current_result_record or {}).get("lifecycle_stage") or "").strip()
                embedded_result_stage = str(embedded_result_registry.get("lifecycle_stage") or "").strip()
                if active_result_stage and embedded_result_stage and embedded_result_stage != active_result_stage:
                    problems.append("lifecycle evidence registry_chain result_registry lifecycle_stage does not match active result registry current stage")
                active_result_source_scope = str((current_result_record or {}).get("source_scope") or "").strip()
                embedded_result_source_scope = str(embedded_result_registry.get("source_scope") or "").strip()
                if active_result_source_scope and embedded_result_source_scope and embedded_result_source_scope != active_result_source_scope:
                    problems.append("lifecycle evidence registry_chain result_registry source_scope does not match active result registry current source_scope")
                result_metadata = (current_result_record or {}).get("metadata")
                if isinstance(result_metadata, dict):
                    embedded_result_producer = str(embedded_result_registry.get("producer") or "").strip()
                    active_result_producer = str(result_metadata.get("producer") or "").strip()
                    if active_result_producer and embedded_result_producer and embedded_result_producer != active_result_producer:
                        problems.append("lifecycle evidence registry_chain result_registry producer does not match active result registry current metadata producer")
                    embedded_result_code_revision = str(embedded_result_registry.get("code_revision") or "").strip()
                    active_result_code_revision = str(result_metadata.get("code_revision") or "").strip()
                    if active_result_code_revision and embedded_result_code_revision and embedded_result_code_revision != active_result_code_revision:
                        problems.append("lifecycle evidence registry_chain result_registry code_revision does not match active result registry current metadata code_revision")
                if run_registry_payload:
                    expected_run_producer = str(run_registry_payload.get("producer") or "").strip()
                    embedded_result_producer = str(embedded_result_registry.get("producer") or "").strip()
                    if expected_run_producer and embedded_result_producer and embedded_result_producer != expected_run_producer:
                        problems.append("lifecycle evidence registry_chain result_registry producer does not match main chain run history")
                    expected_run_code_revision = str(run_registry_payload.get("code_revision") or "").strip()
                    embedded_result_code_revision = str(embedded_result_registry.get("code_revision") or "").strip()
                    if expected_run_code_revision and embedded_result_code_revision and embedded_result_code_revision != expected_run_code_revision:
                        problems.append("lifecycle evidence registry_chain result_registry code_revision does not match main chain run history")
            embedded_pointer = registry_chain.get("current_result_pointer")
            if isinstance(embedded_pointer, dict):
                active_pointer_snapshot_id = str(pointer.get("pointer_snapshot_id") or "").strip()
                embedded_pointer_snapshot_id = str(embedded_pointer.get("pointer_snapshot_id") or "").strip()
                if active_pointer_snapshot_id and embedded_pointer_snapshot_id and embedded_pointer_snapshot_id != active_pointer_snapshot_id:
                    problems.append("lifecycle evidence registry_chain current_result_pointer snapshot_id does not match active current result pointer")
                if pointer_result_id and str(embedded_pointer.get("result_id") or "").strip() not in {"", pointer_result_id}:
                    problems.append("lifecycle evidence registry_chain current_result_pointer result_id does not match active current result pointer")
                if pointer_run_id and str(embedded_pointer.get("run_id") or "").strip() not in {"", pointer_run_id}:
                    problems.append("lifecycle evidence registry_chain current_result_pointer run_id does not match active current result pointer")
                if pointer_lifecycle_id and str(embedded_pointer.get("lifecycle_id") or "").strip() not in {"", pointer_lifecycle_id}:
                    problems.append("lifecycle evidence registry_chain current_result_pointer lifecycle_id does not match active current result pointer")
                pointer_artifact_ids = list(pointer.get("artifact_ids") or [])
                embedded_artifact_ids = list(embedded_pointer.get("artifact_ids") or [])
                if pointer_artifact_ids and embedded_artifact_ids and embedded_artifact_ids != pointer_artifact_ids:
                    problems.append("lifecycle evidence registry_chain current_result_pointer artifact_ids do not match active current result pointer")
                pointer_as_of_date = str(pointer.get("as_of_date") or "").strip()
                embedded_as_of_date = str(embedded_pointer.get("as_of_date") or "").strip()
                if pointer_as_of_date and embedded_as_of_date and embedded_as_of_date != pointer_as_of_date:
                    problems.append("lifecycle evidence registry_chain current_result_pointer as_of_date does not match active current result pointer")
                embedded_pointer_source_scope = str(embedded_pointer.get("source_scope") or "").strip()
                if pointer_source_scope and embedded_pointer_source_scope and embedded_pointer_source_scope != pointer_source_scope:
                    problems.append("lifecycle evidence registry_chain current_result_pointer source_scope does not match active current result pointer")
                if pointer_snapshot_payload:
                    pointer_snapshot_metadata = pointer_snapshot_payload.get("metadata")
                    if isinstance(pointer_snapshot_metadata, dict):
                        embedded_pointer_producer = str(embedded_pointer.get("producer") or "").strip()
                        snapshot_producer = str(pointer_snapshot_metadata.get("producer") or "").strip()
                        if snapshot_producer and embedded_pointer_producer and embedded_pointer_producer != snapshot_producer:
                            problems.append("lifecycle evidence registry_chain current_result_pointer producer does not match active current result pointer snapshot metadata producer")
                        embedded_pointer_code_revision = str(embedded_pointer.get("code_revision") or "").strip()
                        snapshot_code_revision = str(pointer_snapshot_metadata.get("code_revision") or "").strip()
                        if snapshot_code_revision and embedded_pointer_code_revision and embedded_pointer_code_revision != snapshot_code_revision:
                            problems.append("lifecycle evidence registry_chain current_result_pointer code_revision does not match active current result pointer snapshot metadata code_revision")
                if run_registry_payload:
                    expected_run_producer = str(run_registry_payload.get("producer") or "").strip()
                    embedded_pointer_producer = str(embedded_pointer.get("producer") or "").strip()
                    if expected_run_producer and embedded_pointer_producer and embedded_pointer_producer != expected_run_producer:
                        problems.append("lifecycle evidence registry_chain current_result_pointer producer does not match main chain run history")
                    expected_run_code_revision = str(run_registry_payload.get("code_revision") or "").strip()
                    embedded_pointer_code_revision = str(embedded_pointer.get("code_revision") or "").strip()
                    if expected_run_code_revision and embedded_pointer_code_revision and embedded_pointer_code_revision != expected_run_code_revision:
                        problems.append("lifecycle evidence registry_chain current_result_pointer code_revision does not match main chain run history")
        if not isinstance(steps, list) or not steps:
            problems.append("lifecycle evidence steps missing")
        else:
            previous_step_generated_at: datetime | None = None
            step_names: list[str] = []
            pointer_step_artifact_ids = list(pointer.get("artifact_ids") or [])[:-1] if list(pointer.get("artifact_ids") or []) else []
            for step in steps:
                if not isinstance(step, dict):
                    problems.append("lifecycle evidence contains malformed step")
                    continue
                step_name = str(step.get("step") or "").strip()
                if step_name:
                    step_names.append(step_name)
                if step.get("exists") is not True:
                    problems.append(f"lifecycle step missing: {step.get('step')}")
                path = Path(str(step.get("path") or ""))
                if not path.exists():
                    problems.append(f"lifecycle step path missing: {step.get('step')}")
                    continue
                expected_sha256 = str(step.get("sha256") or "").strip()
                if expected_sha256 and sha256_file(path) != expected_sha256:
                    problems.append(f"lifecycle step sha256 mismatch: {step.get('step')}")
                step_payload = _read_json(path)
                if not step_payload:
                    problems.append(f"lifecycle step unreadable: {step.get('step')}")
                    continue
                step_meta_result_id = str(step.get("result_id") or "").strip()
                step_result_id = str(step_payload.get("result_id") or "").strip()
                if step_meta_result_id and step_result_id and step_meta_result_id != step_result_id:
                    problems.append(f"lifecycle evidence step result_id metadata mismatch: {step.get('step')}")
                if pointer_result_id and step_result_id and step_result_id != pointer_result_id:
                    problems.append(f"lifecycle step result_id mismatch: {step.get('step')}")
                step_meta_ts_code = str(step.get("ts_code") or "").strip()
                step_ts_code = str(step_payload.get("ts_code") or "").strip()
                if step_meta_ts_code and step_ts_code and step_meta_ts_code != step_ts_code:
                    problems.append(f"lifecycle evidence step ts_code metadata mismatch: {step.get('step')}")
                if expected_ts_code and step_ts_code and step_ts_code != expected_ts_code:
                    problems.append(f"lifecycle step ts_code mismatch: {step.get('step')}")
                step_meta_generated_at = str(step.get("generated_at") or "").strip()
                step_generated_at = str(step_payload.get("generated_at") or "").strip()
                if step_meta_generated_at and step_generated_at and step_meta_generated_at != step_generated_at:
                    problems.append(f"lifecycle evidence step generated_at metadata mismatch: {step.get('step')}")
                parsed_step_generated_at = _parse_iso_timestamp(step_generated_at)
                if step_generated_at and parsed_step_generated_at is None:
                    problems.append(f"lifecycle step generated_at unreadable: {step.get('step')}")
                elif parsed_step_generated_at is not None:
                    if lifecycle_started_at is not None and parsed_step_generated_at < lifecycle_started_at:
                        problems.append(f"lifecycle step generated_at before lifecycle start: {step.get('step')}")
                    if lifecycle_completed_at is not None and parsed_step_generated_at > lifecycle_completed_at:
                        problems.append(f"lifecycle step generated_at after lifecycle completion: {step.get('step')}")
                    if previous_step_generated_at is not None and parsed_step_generated_at < previous_step_generated_at:
                        problems.append(f"lifecycle step generated_at sequence mismatch: {step.get('step')}")
                    previous_step_generated_at = parsed_step_generated_at
                step_run_id = str(step_payload.get("run_id") or "").strip()
                if pointer_run_id and step_run_id and step_run_id != pointer_run_id:
                    problems.append(f"lifecycle step run_id mismatch: {step.get('step')}")
            if pointer_step_artifact_ids and len(pointer_step_artifact_ids) == len(steps):
                for index, step in enumerate(steps):
                    if not isinstance(step, dict):
                        continue
                    current_step_name = str(step.get("step") or "").strip()
                    step_artifact_id = str(pointer_step_artifact_ids[index]).strip()
                    if not step_artifact_id:
                        continue
                    try:
                        step_artifact_entry = artifact_registry.get_entry(step_artifact_id)
                    except Exception:
                        continue
                    expected_artifact_type = f"primary_result_{str(step.get('step') or '').strip()}"
                    if expected_artifact_type and str(step_artifact_entry.get("artifact_type") or "").strip() != expected_artifact_type:
                        problems.append(f"lifecycle step artifact mapping type mismatch: {step.get('step')}")
                    step_path = Path(str(step.get("path") or "")).resolve()
                    entry_path = Path(str(step_artifact_entry.get("path") or "")).resolve()
                    if step.get("exists") is True and step_path != entry_path:
                        problems.append(f"lifecycle step artifact mapping path mismatch: {step.get('step')}")
                    step_sha = str(step.get("sha256") or "").strip()
                    entry_sha = str(step_artifact_entry.get("sha256") or "").strip()
                    if step_sha and entry_sha and step_sha != entry_sha:
                        problems.append(f"lifecycle step artifact mapping sha256 mismatch: {step.get('step')}")
                    step_artifact_metadata = step_artifact_entry.get("metadata")
                    step_artifact_producer = str(step_artifact_entry.get("producer") or "").strip()
                    if step_artifact_producer and step_artifact_producer != _EXPECTED_MAIN_CHAIN_ARTIFACT_PRODUCER:
                        problems.append(f"lifecycle step artifact producer is not allowed for current formal main chain: {step.get('step')}")
                    expected_code_revision = ""
                    run_registry_payload = (integrity_payload or {}).get("run_entry")
                    if isinstance(run_registry_payload, dict):
                        expected_code_revision = str(run_registry_payload.get("code_revision") or "").strip()
                    step_artifact_code_revision = str(step_artifact_entry.get("code_revision") or "").strip()
                    if expected_code_revision and step_artifact_code_revision and step_artifact_code_revision != expected_code_revision:
                        problems.append(f"lifecycle step artifact code_revision does not match main chain run history: {step.get('step')}")
                    if isinstance(step_artifact_metadata, dict):
                        metadata_step = str(step_artifact_metadata.get("step") or "").strip()
                        if current_step_name and metadata_step and metadata_step != current_step_name:
                            problems.append(f"lifecycle step artifact metadata step mismatch: {step.get('step')}")
                        metadata_status = str(step_artifact_metadata.get("status") or "").strip()
                        step_status = str(step.get("status") or "").strip()
                        if step_status and metadata_status and metadata_status != step_status:
                            problems.append(f"lifecycle step artifact metadata status mismatch: {step.get('step')}")
                        metadata_lifecycle_id = str(step_artifact_metadata.get("lifecycle_id") or "").strip()
                        if pointer_lifecycle_id and metadata_lifecycle_id and metadata_lifecycle_id != pointer_lifecycle_id:
                            problems.append(f"lifecycle step artifact metadata lifecycle_id mismatch: {step.get('step')}")
                        metadata_ts_code = str(step_artifact_metadata.get("ts_code") or "").strip()
                        if expected_ts_code and metadata_ts_code and metadata_ts_code != expected_ts_code:
                            problems.append(f"lifecycle step artifact metadata ts_code mismatch: {step.get('step')}")
            final_payload = lifecycle_evidence.get("final_payload")
            if isinstance(final_payload, dict):
                declared_stage = str(final_payload.get("result_lifecycle_stage") or "").strip().upper()
                derived_stage = _DERIVED_LIFECYCLE_STAGE_BY_STEPS.get(tuple(step_names))
                if declared_stage and derived_stage and declared_stage != derived_stage:
                    problems.append("lifecycle evidence final_payload result_lifecycle_stage does not match steps chain")
                lifecycle_evidence_artifact_id = list(pointer.get("artifact_ids") or [])[-1] if list(pointer.get("artifact_ids") or []) else ""
                if lifecycle_evidence_artifact_id:
                    try:
                        lifecycle_evidence_entry = artifact_registry.get_entry(str(lifecycle_evidence_artifact_id))
                    except Exception:
                        lifecycle_evidence_entry = None
                    if isinstance(lifecycle_evidence_entry, dict):
                        lifecycle_artifact_metadata = lifecycle_evidence_entry.get("metadata")
                        lifecycle_artifact_producer = str(lifecycle_evidence_entry.get("producer") or "").strip()
                        if lifecycle_artifact_producer and lifecycle_artifact_producer != _EXPECTED_MAIN_CHAIN_ARTIFACT_PRODUCER:
                            problems.append("lifecycle evidence artifact producer is not allowed for current formal main chain")
                        expected_code_revision = ""
                        run_registry_payload = (integrity_payload or {}).get("run_entry")
                        if isinstance(run_registry_payload, dict):
                            expected_code_revision = str(run_registry_payload.get("code_revision") or "").strip()
                        lifecycle_artifact_code_revision = str(lifecycle_evidence_entry.get("code_revision") or "").strip()
                        if expected_code_revision and lifecycle_artifact_code_revision and lifecycle_artifact_code_revision != expected_code_revision:
                            problems.append("lifecycle evidence artifact code_revision does not match main chain run history")
                        if isinstance(lifecycle_artifact_metadata, dict):
                            metadata_status = str(lifecycle_artifact_metadata.get("status") or "").strip()
                            lifecycle_status = str(lifecycle_evidence.get("status") or "").strip()
                            if metadata_status and lifecycle_status and metadata_status != lifecycle_status:
                                problems.append("lifecycle evidence artifact metadata status does not match lifecycle evidence status")
                            metadata_ts_code = str(lifecycle_artifact_metadata.get("ts_code") or "").strip()
                            if expected_ts_code and metadata_ts_code and metadata_ts_code != expected_ts_code:
                                problems.append("lifecycle evidence artifact metadata ts_code does not match result registry current ts_code")
                            metadata_lifecycle_id = str(lifecycle_artifact_metadata.get("lifecycle_id") or "").strip()
                            if pointer_lifecycle_id and metadata_lifecycle_id and metadata_lifecycle_id != pointer_lifecycle_id:
                                problems.append("lifecycle evidence artifact metadata lifecycle_id does not match current result pointer")
                        expected_parent_artifact_ids = list(pointer.get("artifact_ids") or [])[:-1]
                        parent_artifact_ids = list(lifecycle_evidence_entry.get("parent_artifact_ids") or [])
                        final_payload_artifact_ids = list(final_payload.get("artifact_ids") or [])
                        if expected_parent_artifact_ids and parent_artifact_ids != expected_parent_artifact_ids:
                            problems.append("lifecycle evidence parent_artifact_ids do not match current step artifact chain")
                        if final_payload_artifact_ids and parent_artifact_ids != final_payload_artifact_ids[:-1]:
                            problems.append("lifecycle evidence parent_artifact_ids do not match final_payload step artifact chain")
    return {
        "ok": integrity_exit == 0 and not problems,
        "problems": problems,
        "pointer": pointer,
        "integrity_ok": integrity_exit == 0,
        "integrity_payload": integrity_payload,
        "lifecycle_evidence_path": str(lifecycle_evidence_path),
        "lifecycle_evidence": lifecycle_evidence,
    }


def write_stock_entry_guard_artifact(
    *,
    output_path: str | Path = "artifacts/stock_entry_guard_latest.json",
    exp_dir: str | Path = "data/experiments",
    artifacts_dir: str | Path = "artifacts",
) -> dict[str, Any]:
    payload = evaluate_stock_entry_guard(exp_dir=exp_dir, artifacts_dir=artifacts_dir)
    resolved_output_path = resolve_project_path(output_path)
    resolved_output_path.parent.mkdir(parents=True, exist_ok=True)
    resolved_output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    payload["output_path"] = str(resolved_output_path)
    return payload
