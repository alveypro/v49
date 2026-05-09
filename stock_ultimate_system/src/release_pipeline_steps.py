from __future__ import annotations

import hashlib
import json
import subprocess
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

from src.release_pipeline_context import ReleasePipelineContext
from src.candidate_quality_diff import build_candidate_quality_diff, write_candidate_quality_diff_artifact
from src.stock_primary_result_benchmark_diff import build_stock_primary_result_benchmark_diff
from src.stock_primary_result_benchmark_report import (
    BENCHMARK_VERSION,
    BENCHMARK_REGISTRY_VERSION,
    RUNTIME_OBSERVABILITY_VERSION,
    build_stock_primary_result_benchmark_report,
    render_stock_primary_result_benchmark_report_json,
    write_stock_primary_result_benchmark_report_artifacts,
)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _sha256_json(payload: dict[str, object]) -> str:
    text = json.dumps(payload, ensure_ascii=False, sort_keys=True)
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _maybe_git_commit(project_root: Path) -> str | None:
    completed = subprocess.run(
        ["git", "-C", str(project_root), "rev-parse", "HEAD"],
        capture_output=True,
        text=True,
        check=False,
    )
    if completed.returncode != 0:
        return None
    return completed.stdout.strip() or None


def create_release_pipeline_context(output_dir: str | Path) -> ReleasePipelineContext:
    destination = Path(output_dir)
    destination.mkdir(parents=True, exist_ok=True)
    return ReleasePipelineContext(
        run_id=f"stock-release-{uuid.uuid4().hex[:12]}",
        output_dir=destination,
        started_at=utc_now_iso(),
    )


def run_timed_stage(context: ReleasePipelineContext, stage_name: str, fn):
    started = time.perf_counter()
    result = fn()
    context.stage_timings[stage_name] = round(time.perf_counter() - started, 6)
    return result


def run_benchmark_report_step(context: ReleasePipelineContext) -> dict[str, object]:
    report = build_stock_primary_result_benchmark_report()
    benchmark_dir = context.output_dir / "benchmark_report"
    json_path, _markdown_path = write_stock_primary_result_benchmark_report_artifacts(benchmark_dir, report)
    context.benchmark_report_path = json_path
    return report.as_dict()


def run_benchmark_diff_step(
    context: ReleasePipelineContext,
    *,
    baseline_report: str | None = None,
) -> dict[str, object]:
    if context.benchmark_report_path is None:
        raise ValueError("benchmark report must exist before diff step")
    if baseline_report:
        diff_payload = build_stock_primary_result_benchmark_diff(
            baseline_report,
            context.benchmark_report_path,
        ).as_dict()
    else:
        diff_payload = {
            "base_benchmark_version": "none",
            "target_benchmark_version": BENCHMARK_VERSION,
            "change_total": 0,
            "has_blocking_regression": False,
            "blocking_regressions": [],
            "observation_changes": [],
            "enhancements": [],
        }
    diff_path = context.output_dir / "benchmark_diff.json"
    diff_path.write_text(json.dumps(diff_payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    context.benchmark_diff_path = diff_path
    if diff_payload.get("has_blocking_regression"):
        context.blocking_failures.append("benchmark_diff")
    return diff_payload


def run_candidate_quality_diff_step(context: ReleasePipelineContext) -> dict[str, object]:
    try:
        diff_payload = build_candidate_quality_diff()
        output_path = write_candidate_quality_diff_artifact(diff_payload, output_dir=context.output_dir)
        context.candidate_quality_diff_path = Path(output_path)
        return diff_payload
    except FileNotFoundError:
        diff_payload = {
            "diff_version": "candidate_quality_diff.v1",
            "status": "not_available",
            "pass_or_fail": "not_available",
            "blocking_reasons": ["current_candidate_quality_summary_missing"],
        }
        output_path = context.output_dir / "candidate_quality_diff.json"
        output_path.write_text(json.dumps(diff_payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        context.candidate_quality_diff_path = output_path
        return diff_payload


def run_release_gates_step(context: ReleasePipelineContext, *, gate_runner) -> tuple[int, dict[str, object]]:
    gate_exit_code, gate_payload = gate_runner(output_path=context.output_dir / "release_gates.json")
    context.release_gates_path = context.output_dir / "release_gates.json"
    if gate_exit_code != 0:
        context.blocking_failures.append("release_gates")
    return gate_exit_code, gate_payload


def write_manifest_step(
    context: ReleasePipelineContext,
    *,
    project_root: Path,
    baseline_report: str | None = None,
    previous_stable_release: dict[str, object] | None = None,
) -> dict[str, object]:
    if not all([context.benchmark_report_path, context.benchmark_diff_path, context.release_gates_path]):
        raise ValueError("manifest requires benchmark report, diff and release gates")
    config_payload = {
        "baseline_report": baseline_report or "",
        "benchmark_version": BENCHMARK_VERSION,
        "registry_version": BENCHMARK_REGISTRY_VERSION,
        "runtime_observability_version": RUNTIME_OBSERVABILITY_VERSION,
    }
    manifest_payload = {
        "run_id": context.run_id,
        "started_at": context.started_at,
        "finished_at": context.finished_at,
        "config_hash": _sha256_json(config_payload),
        "benchmark_report_hash": _sha256_file(context.benchmark_report_path),
        "benchmark_diff_hash": _sha256_file(context.benchmark_diff_path),
        "release_gates_hash": _sha256_file(context.release_gates_path),
        "dataset_identity": "canonical_single_track_dataset",
        "market_calendar_identity": "default_market_calendar",
        "git_commit": _maybe_git_commit(project_root),
        "previous_stable_release": previous_stable_release or {},
    }
    manifest_path = context.output_dir / "release_pipeline_manifest.json"
    manifest_path.write_text(json.dumps(manifest_payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    context.manifest_path = manifest_path
    return manifest_payload


def finalize_context(context: ReleasePipelineContext) -> None:
    context.finished_at = utc_now_iso()
