#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import shutil
from pathlib import Path
import sys


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.build_release_evidence_bundle import build_release_evidence_bundle
from scripts.check_release_gates import run_release_gates
from src.artifact_registry import ArtifactRegistry
from src.release_pipeline_steps import (
    create_release_pipeline_context,
    finalize_context,
    run_candidate_quality_diff_step,
    run_benchmark_diff_step,
    run_benchmark_report_step,
    run_release_gates_step,
    run_timed_stage,
    write_manifest_step,
)
from src.stock_baseline_registry import StockBaselineRegistry


def _read_json(path: Path) -> dict[str, object]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


def _build_stable_release_reference(
    *,
    payload: dict[str, object],
    previous_reference: dict[str, object],
) -> dict[str, object]:
    return {
        "reference_version": "v1",
        "run_id": payload["run_id"],
        "status": payload["status"],
        "updated_at": payload["finished_at"],
        "summary_json_path": payload["summary_json_path"],
        "manifest_json_path": payload["manifest"]["json_path"],
        "release_evidence_bundle_json_path": payload["release_evidence_bundle"]["json_path"],
        "release_gates_json_path": payload["release_gates"]["json_path"],
        "benchmark_report_json_path": payload["benchmark_report"]["json_path"],
        "previous_stable_run_id": previous_reference.get("run_id"),
    }


def _build_rollback_readiness(
    *,
    previous_stable_reference: dict[str, object],
    status: str,
) -> dict[str, object]:
    previous_run_id = str(previous_stable_reference.get("run_id", "") or "").strip()
    has_previous_stable_release = bool(previous_run_id)
    return {
        "has_previous_stable_release": has_previous_stable_release,
        "previous_stable_run_id": previous_run_id or None,
        "fully_release_ready": status == "passed" and has_previous_stable_release,
        "release_classification": "standard_release" if has_previous_stable_release else "bootstrap_release",
    }


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _run_release_gates_with_optional_context(
    *,
    output_path: Path,
    require_active_pointer: bool,
    artifact_root: Path,
    artifact_registry_path: Path,
) -> tuple[int, dict[str, object]]:
    kwargs = {
        "capture_output": True,
        "output_path": output_path,
        "require_active_pointer": require_active_pointer,
    }
    if require_active_pointer:
        kwargs.update(
            {
                "pointer_dir": artifact_root / "current_result_pointer",
                "results_dir": artifact_root / "result_registry",
                "runs_dir": artifact_root / "run_registry",
                "artifact_registry_path": artifact_registry_path,
            }
        )
    return run_release_gates(**kwargs)


def _sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _resolve_project_path(path: str | Path) -> Path:
    resolved = Path(path)
    if not resolved.is_absolute():
        resolved = PROJECT_ROOT / resolved
    return resolved


def _validate_release_decision_json(release_decision_json: str | Path | None) -> tuple[Path, dict[str, object]]:
    if not release_decision_json:
        raise ValueError("baseline promotion requires approved release decision")
    decision_path = _resolve_project_path(release_decision_json)
    if not decision_path.exists():
        raise FileNotFoundError(f"release decision json missing: {decision_path}")
    decision_payload = _read_json(decision_path)
    if decision_payload.get("decision_version") != "primary_result_release_decision.v1":
        raise ValueError("release decision version is invalid")
    if str(decision_payload.get("decision", "")).strip().lower() != "approved":
        raise ValueError("release decision is not approved")
    if decision_payload.get("baseline_promotion_allowed") is not True:
        raise ValueError("release decision does not allow baseline promotion")
    if decision_payload.get("do_not_auto_apply") is not True:
        raise ValueError("release decision must keep do_not_auto_apply=true")
    return decision_path, decision_payload


def _update_evidence_bundle_with_baseline_promotion(
    evidence_bundle_path: Path,
    baseline_promotion: dict[str, object],
) -> None:
    bundle_payload = _read_json(evidence_bundle_path)
    if not bundle_payload:
        return
    bundle_payload["baseline_promotion"] = baseline_promotion
    _write_json(evidence_bundle_path, bundle_payload)


def _use_existing_release_gates_json(context, release_gates_json: str | Path) -> tuple[int, dict[str, object]]:
    source_path = Path(release_gates_json)
    if not source_path.is_absolute():
        source_path = PROJECT_ROOT / source_path
    if not source_path.exists():
        raise FileNotFoundError(f"release gates json missing: {source_path}")
    gate_payload = _read_json(source_path)
    if not gate_payload:
        raise ValueError(f"release gates json is not a valid object: {source_path}")
    destination_path = context.output_dir / "release_gates.json"
    destination_path.parent.mkdir(parents=True, exist_ok=True)
    if source_path.resolve() != destination_path.resolve():
        shutil.copyfile(source_path, destination_path)
    context.release_gates_path = destination_path
    failed_total = int(gate_payload.get("failed_total", 0) or 0)
    gate_passed = str(gate_payload.get("status", "")).strip().lower() == "passed" and failed_total == 0
    if not gate_passed:
        context.blocking_failures.append("release_gates")
    return (0 if gate_passed else 1), gate_payload


def _release_gate_result_by_name(gate_payload: dict[str, object], gate_name: str) -> dict[str, object] | None:
    results = gate_payload.get("results")
    if not isinstance(results, list):
        return None
    for item in results:
        if isinstance(item, dict) and item.get("gate") == gate_name:
            return item
    return None


def _release_gates_support_active_pointer_policy(gate_payload: dict[str, object], *, require_active_pointer: bool) -> str | None:
    if not require_active_pointer:
        return None
    pointer_integrity = _release_gate_result_by_name(gate_payload, "pointer_integrity")
    if pointer_integrity is None:
        return "release gates json missing pointer_integrity result required for release-context execution"
    if pointer_integrity.get("passed") is not True:
        return "release gates json failed pointer_integrity required for release-context execution"
    if str(pointer_integrity.get("status", "")).strip().lower() == "skipped_no_active_pointer":
        return "release gates json skipped active current_result_pointer validation required for release-context execution"
    return None


def _artifact_path_from_payload(payload: dict[str, object], *keys: str) -> Path | None:
    cursor: object = payload
    for key in keys:
        if not isinstance(cursor, dict):
            return None
        cursor = cursor.get(key)
    if not cursor:
        return None
    return Path(str(cursor))


def _planned_release_artifacts(pipeline_payload: dict[str, object]) -> list[tuple[str, Path, dict[str, object]]]:
    planned: list[tuple[str, Path, dict[str, object]]] = []
    for artifact_type, keys in (
        ("benchmark_report", ("benchmark_report", "json_path")),
        ("benchmark_diff", ("benchmark_diff", "json_path")),
        ("candidate_quality_diff", ("candidate_quality_diff", "json_path")),
        ("release_gates", ("release_gates", "json_path")),
        ("release_evidence_bundle", ("release_evidence_bundle", "json_path")),
        ("release_pipeline_manifest", ("manifest", "json_path")),
        ("release_pipeline_summary", ("summary_json_path",)),
    ):
        path = _artifact_path_from_payload(pipeline_payload, *keys)
        if path is not None:
            planned.append((artifact_type, path, {}))

    stable_reference_path = None
    if "stable_release_reference" in pipeline_payload:
        summary_path = _artifact_path_from_payload(pipeline_payload, "summary_json_path")
        if summary_path is not None:
            stable_reference_path = summary_path.parent / "stable_release_reference.json"
    if stable_reference_path is not None and stable_reference_path.exists():
        planned.append(("stable_release_reference", stable_reference_path, {}))

    baseline_promotion = pipeline_payload.get("baseline_promotion", {})
    if isinstance(baseline_promotion, dict) and baseline_promotion.get("status") == "promoted":
        release_decision_path = baseline_promotion.get("release_decision_path")
        if release_decision_path:
            planned.append(("primary_result_release_decision", Path(str(release_decision_path)), {}))
        baseline_id = str(baseline_promotion.get("baseline_id", "") or "")
        snapshot_path = baseline_promotion.get("snapshot_path")
        current_pointer_path = baseline_promotion.get("current_pointer_path")
        if snapshot_path:
            planned.append(("baseline_snapshot", Path(str(snapshot_path)), {"baseline_id": baseline_id}))
        if current_pointer_path:
            planned.append(("baseline_current_pointer", Path(str(current_pointer_path)), {"baseline_id": baseline_id}))
    return planned


def _register_release_artifacts(
    *,
    registry_path: str | Path,
    pipeline_payload: dict[str, object],
) -> list[dict[str, object]]:
    registry = ArtifactRegistry(registry_path)
    run_id = str(pipeline_payload["run_id"])
    created_at = str(pipeline_payload.get("finished_at") or "")
    entries: list[dict[str, object]] = []
    for artifact_type, path, metadata in _planned_release_artifacts(pipeline_payload):
        entry = registry.register_artifact(
            artifact_type=artifact_type,
            run_id=run_id,
            path=path,
            producer="scripts/run_stock_release_pipeline.py",
            artifact_id=f"{run_id}:{artifact_type}",
            created_at=created_at or None,
            metadata=metadata,
        )
        entries.append(entry.as_dict())
    return entries


def build_stock_release_pipeline_summary(
    output_dir: str | Path,
    *,
    baseline_report: str | None = None,
    promote_baseline: bool = False,
    baseline_id: str | None = None,
    baselines_dir: str | Path = "artifacts/baselines",
    baseline_policy_path: str | Path = "STOCK_PRIMARY_RESULT_BASELINE_POLICY.md",
    artifact_registry_path: str | Path | None = None,
    release_gates_json: str | Path | None = None,
    release_decision_json: str | Path | None = None,
    require_active_pointer: bool = False,
) -> dict[str, object]:
    context = create_release_pipeline_context(output_dir)
    stable_reference_path = context.output_dir / "stable_release_reference.json"
    previous_stable_reference = _read_json(stable_reference_path)
    resolved_artifact_registry_path = Path(artifact_registry_path or "artifacts/artifact_registry.jsonl")
    artifact_root = resolved_artifact_registry_path.parent

    benchmark_report_payload = run_timed_stage(
        context,
        "benchmark_report",
        lambda: run_benchmark_report_step(context),
    )
    benchmark_diff_payload = run_timed_stage(
        context,
        "benchmark_diff",
        lambda: run_benchmark_diff_step(context, baseline_report=baseline_report),
    )
    candidate_quality_diff_payload = run_timed_stage(
        context,
        "_candidate_quality_diff",
        lambda: run_candidate_quality_diff_step(context),
    )
    context.stage_timings.pop("_candidate_quality_diff", None)
    release_gate_exit_code, release_gate_payload = run_timed_stage(
        context,
        "release_gates",
        lambda: _use_existing_release_gates_json(context, release_gates_json)
        if release_gates_json
        else run_release_gates_step(
            context,
            gate_runner=lambda output_path: _run_release_gates_with_optional_context(
                output_path=output_path,
                require_active_pointer=require_active_pointer,
                artifact_root=artifact_root,
                artifact_registry_path=resolved_artifact_registry_path,
            ),
        ),
    )
    active_pointer_policy_error = _release_gates_support_active_pointer_policy(
        release_gate_payload,
        require_active_pointer=require_active_pointer,
    )
    if active_pointer_policy_error:
        release_gate_exit_code = 1
        release_gate_payload["status"] = "failed"
        release_gate_payload["failed_total"] = max(int(release_gate_payload.get("failed_total", 0) or 0), 1)
        release_gate_payload["active_pointer_policy_error"] = active_pointer_policy_error
    if release_gate_exit_code != 0 and "release_gates" not in context.blocking_failures:
        context.blocking_failures.append("release_gates")
    finalize_context(context)
    run_timed_stage(
        context,
        "manifest",
        lambda: write_manifest_step(
            context,
            project_root=PROJECT_ROOT,
            baseline_report=baseline_report,
            previous_stable_release=previous_stable_reference,
        ),
    )
    evidence_bundle_path = run_timed_stage(
        context,
        "release_evidence_bundle",
        lambda: build_release_evidence_bundle(
            context.output_dir / "release_evidence_bundle",
            benchmark_report_json=context.benchmark_report_path,
            benchmark_diff_json=context.benchmark_diff_path,
            candidate_quality_diff_json=context.candidate_quality_diff_path,
            release_gates_json=context.release_gates_path,
            manifest_json=context.manifest_path,
        ),
    )
    context.evidence_bundle_path = evidence_bundle_path

    baseline_promotion: dict[str, object] = {
        "status": "not_requested",
        "baseline_id": None,
        "snapshot_path": None,
        "current_pointer_path": None,
        "release_decision_path": None,
        "release_decision_hash": None,
        "error": None,
    }
    if promote_baseline:
        if context.blocking_failures:
            baseline_promotion = {
                "status": "skipped",
                "baseline_id": baseline_id,
                "snapshot_path": None,
                "current_pointer_path": None,
                "release_decision_path": None,
                "release_decision_hash": None,
                "error": "pipeline has blocking failures",
            }
        else:
            try:
                decision_path, _decision_payload = _validate_release_decision_json(release_decision_json)
                registry = StockBaselineRegistry(
                    baselines_dir=baselines_dir,
                    policy_path=baseline_policy_path,
                )
                promoted_snapshot = registry.promote(
                    baseline_id=baseline_id,
                    benchmark_report_path=context.benchmark_report_path,
                    benchmark_diff_path=context.benchmark_diff_path,
                    release_gates_path=context.release_gates_path,
                    evidence_bundle_path=context.evidence_bundle_path,
                    manifest_path=context.manifest_path,
                    release_decision_path=decision_path,
                )
                promoted_baseline_id = str(promoted_snapshot["baseline_id"])
                baseline_promotion = {
                    "status": "promoted",
                    "baseline_id": promoted_baseline_id,
                    "snapshot_path": str(registry.history_dir / f"{promoted_baseline_id}.json"),
                    "current_pointer_path": str(registry.current_path),
                    "release_decision_path": str(decision_path),
                    "release_decision_hash": _sha256_file(decision_path),
                    "error": None,
                }
            except Exception as exc:
                context.blocking_failures.append("baseline_promotion")
                baseline_promotion = {
                    "status": "failed",
                    "baseline_id": baseline_id,
                    "snapshot_path": None,
                    "current_pointer_path": str(StockBaselineRegistry(
                        baselines_dir=baselines_dir,
                        policy_path=baseline_policy_path,
                    ).current_path),
                    "release_decision_path": str(_resolve_project_path(release_decision_json))
                    if release_decision_json
                    else None,
                    "release_decision_hash": _sha256_file(_resolve_project_path(release_decision_json))
                    if release_decision_json and _resolve_project_path(release_decision_json).exists()
                    else None,
                    "error": str(exc),
                }
    _update_evidence_bundle_with_baseline_promotion(context.evidence_bundle_path, baseline_promotion)

    status = "passed" if not context.blocking_failures else "failed"
    registry_path = Path(artifact_registry_path) if artifact_registry_path is not None else context.output_dir / "artifact_registry.jsonl"
    pipeline_payload = {
        "pipeline_version": "v1",
        "run_id": context.run_id,
        "started_at": context.started_at,
        "finished_at": context.finished_at,
        "steps": [
            "benchmark_report",
            "benchmark_diff",
            "candidate_quality_diff",
            "release_gates",
            "manifest",
            "release_evidence_bundle",
        ],
        "benchmark_report": {
            "json_path": str(context.benchmark_report_path),
            "benchmark_version": benchmark_report_payload["benchmark_version"],
        },
        "benchmark_diff": {
            "json_path": str(context.benchmark_diff_path),
            "status": "passed" if not benchmark_diff_payload.get("has_blocking_regression") else "failed",
            "has_blocking_regression": bool(benchmark_diff_payload.get("has_blocking_regression")),
        },
        "candidate_quality_diff": {
            "json_path": str(context.candidate_quality_diff_path),
            "status": str(candidate_quality_diff_payload.get("pass_or_fail") or "not_available"),
            "blocking_reasons": list(candidate_quality_diff_payload.get("blocking_reasons") or []),
        },
        "release_gates": {
            "json_path": str(context.release_gates_path),
            "status": release_gate_payload.get("status", "failed"),
            "exit_code": release_gate_exit_code,
            "source": "existing_json" if release_gates_json else "executed",
            "require_active_pointer": require_active_pointer,
        },
        "release_evidence_bundle": {
            "json_path": str(context.evidence_bundle_path),
        },
        "manifest": {
            "json_path": str(context.manifest_path),
        },
        "baseline_promotion": baseline_promotion,
        "previous_stable_release": previous_stable_reference,
        "rollback_readiness": _build_rollback_readiness(
            previous_stable_reference=previous_stable_reference,
            status=status,
        ),
        "blocking_failures": list(context.blocking_failures),
        "stage_timings": dict(context.stage_timings),
        "artifact_registry": {
            "jsonl_path": str(registry_path),
            "registered_total": 0,
        },
        "status": status,
    }
    summary_path = context.output_dir / "stock_release_pipeline_summary.json"
    pipeline_payload["summary_json_path"] = str(summary_path)
    if pipeline_payload["status"] == "passed":
        stable_reference = _build_stable_release_reference(
            payload=pipeline_payload,
            previous_reference=previous_stable_reference,
        )
        _write_json(stable_reference_path, stable_reference)
        pipeline_payload["stable_release_reference"] = stable_reference
    pipeline_payload["artifact_registry"]["registered_total"] = len(_planned_release_artifacts(pipeline_payload))
    _write_json(summary_path, pipeline_payload)
    _register_release_artifacts(
        registry_path=registry_path,
        pipeline_payload=pipeline_payload,
    )
    return pipeline_payload


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the unified stock release pipeline.")
    parser.add_argument("--output-dir", default="artifacts/stock_release_pipeline")
    parser.add_argument("--baseline-report", help="Optional official baseline benchmark report.")
    parser.add_argument("--promote-baseline", action="store_true", help="Promote the passed release as the official /stock baseline.")
    parser.add_argument("--baseline-id", help="Optional explicit baseline id for promotion.")
    parser.add_argument("--baselines-dir", default="artifacts/baselines", help="Baseline registry directory.")
    parser.add_argument("--baseline-policy-path", default="STOCK_PRIMARY_RESULT_BASELINE_POLICY.md")
    parser.add_argument("--artifact-registry-path", default="artifacts/artifact_registry.jsonl")
    parser.add_argument("--release-gates-json", help="Reuse an existing release_gates.json instead of executing gates.")
    parser.add_argument("--release-decision-json", help="Approved release decision required for baseline promotion.")
    parser.add_argument(
        "--require-active-pointer",
        action="store_true",
        help="Require active current_result_pointer validation when running release gates for this release context.",
    )
    parser.add_argument(
        "--allow-missing-active-pointer",
        action="store_true",
        help="Opt out of active current_result_pointer enforcement for non-release diagnostics or isolated tests.",
    )
    args = parser.parse_args()

    payload = build_stock_release_pipeline_summary(
        args.output_dir,
        baseline_report=args.baseline_report,
        promote_baseline=args.promote_baseline,
        baseline_id=args.baseline_id,
        baselines_dir=args.baselines_dir,
        baseline_policy_path=args.baseline_policy_path,
        artifact_registry_path=args.artifact_registry_path,
        release_gates_json=args.release_gates_json,
        release_decision_json=args.release_decision_json,
        require_active_pointer=(args.require_active_pointer or not args.allow_missing_active_pointer),
    )
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0 if payload["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
