import hashlib
import json
from pathlib import Path


def _sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def write_release_artifacts(
    tmp_path: Path,
    *,
    gate_status: str = "passed",
    failed_total: int = 0,
    has_blocking_regression: bool = False,
    run_id: str = "stock-release-test001",
) -> dict[str, Path]:
    tmp_path.mkdir(parents=True, exist_ok=True)
    report_path = tmp_path / "stock_primary_result_benchmark_report.json"
    diff_path = tmp_path / "benchmark_diff.json"
    gates_path = tmp_path / "release_gates.json"
    bundle_path = tmp_path / "release_evidence_bundle.json"
    manifest_path = tmp_path / "release_pipeline_manifest.json"

    report_path.write_text(
        json.dumps(
            {
                "benchmark_version": "v1",
                "registry_version": "v1",
                "sample_total": 7,
                "core_sample_total": 4,
                "extended_sample_total": 3,
                "blocking_total": 4,
                "observation_total": 3,
                "render_contract_version": "v1",
                "runtime_observability_version": "v1",
                "has_blocking_regression": False,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    diff_path.write_text(
        json.dumps(
            {
                "base_benchmark_version": "v1",
                "target_benchmark_version": "v1",
                "change_total": 0,
                "has_blocking_regression": has_blocking_regression,
                "blocking_regressions": [],
                "observation_changes": [],
                "enhancements": [],
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    gates_path.write_text(
        json.dumps(
            {
                "status": gate_status,
                "gate_total": 5,
                "failed_total": failed_total,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    bundle_path.write_text(
        json.dumps(
            {
                "run_id": run_id,
                "benchmark_diff": {
                    "json_path": diff_path.name,
                    "has_blocking_regression": has_blocking_regression,
                },
                "release_gate_result": {
                    "json_path": gates_path.name,
                    "status": gate_status,
                    "gate_total": 5,
                    "failed_total": failed_total,
                },
                "blocking_status_summary": {
                    "has_blocking_regression": has_blocking_regression,
                    "release_gate_failed": gate_status != "passed" or failed_total > 0,
                },
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    manifest_path.write_text(
        json.dumps(
            {
                "run_id": run_id,
                "benchmark_report_hash": _sha256_file(report_path),
                "benchmark_diff_hash": _sha256_file(diff_path),
                "release_gates_hash": _sha256_file(gates_path),
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    return {
        "report": report_path,
        "diff": diff_path,
        "gates": gates_path,
        "bundle": bundle_path,
        "manifest": manifest_path,
    }


def write_release_decision(tmp_path: Path, *, decision: str = "approved") -> Path:
    decision_path = tmp_path / "primary_result_release_decision.json"
    decision_path.parent.mkdir(parents=True, exist_ok=True)
    decision_path.write_text(
        json.dumps(
            {
                "decision_version": "primary_result_release_decision.v1",
                "decision_id": "release-decision-test001",
                "decided_at": "2026-04-20T08:50:00Z",
                "decision": decision,
                "actor": "release_manager",
                "reason": "test release decision",
                "checklist_id": "checklist-test001",
                "checklist_status": "complete",
                "review_id": "review-test001",
                "result_id": "primary:000001.SZ",
                "ts_code": "000001.SZ",
                "missing_evidence": [],
                "blocking_gate_reason": None,
                "release_pipeline_allowed": decision == "approved",
                "baseline_promotion_allowed": decision == "approved",
                "do_not_auto_apply": True,
                "decision_boundary": "test decision only",
                "source_checklist_path": "checklist.json",
                "source_checklist_hash": "abc123",
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    return decision_path
