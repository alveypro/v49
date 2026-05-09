#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

EVIDENCE_BUNDLE_VERSION = "v1"


def build_release_evidence_bundle(
    output_dir: str | Path,
    *,
    benchmark_report_json: str | Path,
    benchmark_diff_json: str | Path,
    candidate_quality_diff_json: str | Path | None = None,
    release_gates_json: str | Path,
    manifest_json: str | Path,
) -> Path:
    destination = Path(output_dir)
    destination.mkdir(parents=True, exist_ok=True)
    benchmark_report_path = Path(benchmark_report_json)
    benchmark_diff_path = Path(benchmark_diff_json)
    candidate_quality_diff_path = Path(candidate_quality_diff_json) if candidate_quality_diff_json else None
    release_gates_path = Path(release_gates_json)
    manifest_path = Path(manifest_json)
    report_payload = json.loads(benchmark_report_path.read_text(encoding="utf-8"))
    diff_payload = json.loads(benchmark_diff_path.read_text(encoding="utf-8"))
    candidate_quality_diff_payload = (
        json.loads(candidate_quality_diff_path.read_text(encoding="utf-8"))
        if candidate_quality_diff_path is not None and candidate_quality_diff_path.exists()
        else None
    )
    gate_payload = json.loads(release_gates_path.read_text(encoding="utf-8"))
    manifest_payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    pointer_integrity_result = next(
        (
            item
            for item in gate_payload.get("results", [])
            if isinstance(item, dict) and item.get("gate") == "pointer_integrity"
        ),
        {},
    )
    bundle_payload = {
        "evidence_bundle_version": EVIDENCE_BUNDLE_VERSION,
        "run_id": manifest_payload["run_id"],
        "benchmark_report": {
            "json_path": benchmark_report_path.name,
            "benchmark_version": report_payload["benchmark_version"],
            "registry_version": report_payload["registry_version"],
            "render_contract_version": report_payload["render_contract_version"],
            "runtime_observability_version": report_payload["runtime_observability_version"],
        },
        "benchmark_diff": {
            "json_path": benchmark_diff_path.name,
            "has_blocking_regression": diff_payload["has_blocking_regression"],
        },
        "candidate_quality_diff": {
            "json_path": None if candidate_quality_diff_path is None else candidate_quality_diff_path.name,
            "status": None if not isinstance(candidate_quality_diff_payload, dict) else candidate_quality_diff_payload.get("pass_or_fail"),
            "blocking_reasons": [] if not isinstance(candidate_quality_diff_payload, dict) else list(candidate_quality_diff_payload.get("blocking_reasons") or []),
            "improvement_gate": {} if not isinstance(candidate_quality_diff_payload, dict) else dict(candidate_quality_diff_payload.get("improvement_gate") or {}),
        },
        "release_gate_result": {
            "json_path": release_gates_path.name,
            "status": gate_payload["status"],
            "gate_total": gate_payload["gate_total"],
            "failed_total": gate_payload["failed_total"],
            "gate_policy": gate_payload.get("gate_policy", {}),
            "pointer_integrity": {
                "status": pointer_integrity_result.get("status"),
                "passed": pointer_integrity_result.get("passed"),
            },
        },
        "manifest": {
            "json_path": manifest_path.name,
            "run_id": manifest_payload["run_id"],
        },
        "previous_stable_release": manifest_payload.get("previous_stable_release", {}),
        "rollback_readiness": {
            "has_previous_stable_release": bool((manifest_payload.get("previous_stable_release", {}) or {}).get("run_id")),
            "previous_stable_run_id": (manifest_payload.get("previous_stable_release", {}) or {}).get("run_id"),
        },
        "blocking_status_summary": {
            "has_blocking_regression": diff_payload["has_blocking_regression"],
            "candidate_quality_diff_blocked": (
                isinstance(candidate_quality_diff_payload, dict)
                and str(candidate_quality_diff_payload.get("pass_or_fail") or "").strip().lower() == "blocked"
            ),
            "release_gate_failed": gate_payload["status"] != "passed",
        },
    }
    bundle_path = destination / "release_evidence_bundle.json"
    bundle_path.write_text(json.dumps(bundle_payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return bundle_path


def main() -> int:
    parser = argparse.ArgumentParser(description="Build Airivo release evidence bundle.")
    parser.add_argument("--output-dir", default="artifacts/release_evidence_bundle")
    parser.add_argument("--benchmark-report-json", required=True)
    parser.add_argument("--benchmark-diff-json", required=True)
    parser.add_argument("--candidate-quality-diff-json")
    parser.add_argument("--release-gates-json", required=True)
    parser.add_argument("--manifest-json", required=True)
    args = parser.parse_args()

    build_release_evidence_bundle(
        args.output_dir,
        benchmark_report_json=args.benchmark_report_json,
        benchmark_diff_json=args.benchmark_diff_json,
        candidate_quality_diff_json=args.candidate_quality_diff_json,
        release_gates_json=args.release_gates_json,
        manifest_json=args.manifest_json,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
