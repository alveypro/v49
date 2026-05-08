import json
from pathlib import Path

import pytest

from scripts.build_release_evidence_bundle import build_release_evidence_bundle


pytestmark = pytest.mark.fast


def test_release_evidence_bundle_is_built_with_minimum_artifacts(tmp_path):
    benchmark_report = tmp_path / "stock_primary_result_benchmark_report.json"
    benchmark_diff = tmp_path / "benchmark_diff.json"
    candidate_quality_diff = tmp_path / "candidate_quality_diff.json"
    release_gates = tmp_path / "release_gates.json"
    manifest = tmp_path / "release_pipeline_manifest.json"
    benchmark_report.write_text(
        json.dumps(
            {
                "benchmark_version": "v1",
                "registry_version": "v1",
                "render_contract_version": "v1",
                "runtime_observability_version": "v1",
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    benchmark_diff.write_text(
        json.dumps({"has_blocking_regression": False}, ensure_ascii=False),
        encoding="utf-8",
    )
    candidate_quality_diff.write_text(
        json.dumps(
            {
                "pass_or_fail": "passed",
                "blocking_reasons": [],
                "improvement_gate": {"pass_or_fail": "passed"},
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    release_gates.write_text(
        json.dumps(
            {
                "status": "passed",
                "gate_total": 5,
                "failed_total": 0,
                "gate_policy": {"require_active_pointer": True},
                "results": [
                    {"gate": "pointer_integrity", "passed": True, "status": "passed"},
                ],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    manifest.write_text(
        json.dumps({"run_id": "stock-release-test", "previous_stable_release": {"run_id": "stock-release-prev"}}, ensure_ascii=False),
        encoding="utf-8",
    )

    bundle_path = build_release_evidence_bundle(
        tmp_path / "bundle",
        benchmark_report_json=benchmark_report,
        benchmark_diff_json=benchmark_diff,
        candidate_quality_diff_json=candidate_quality_diff,
        release_gates_json=release_gates,
        manifest_json=manifest,
    )
    assert bundle_path.exists()

    bundle_payload = json.loads(bundle_path.read_text(encoding="utf-8"))
    assert bundle_payload["evidence_bundle_version"] == "v1"
    assert bundle_payload["run_id"] == "stock-release-test"
    assert bundle_payload["benchmark_report"]["benchmark_version"] == "v1"
    assert bundle_payload["candidate_quality_diff"]["status"] == "passed"
    assert bundle_payload["candidate_quality_diff"]["json_path"] == "candidate_quality_diff.json"
    assert bundle_payload["release_gate_result"]["status"] == "passed"
    assert bundle_payload["release_gate_result"]["json_path"] == "release_gates.json"
    assert bundle_payload["release_gate_result"]["gate_policy"]["require_active_pointer"] is True
    assert bundle_payload["release_gate_result"]["pointer_integrity"]["status"] == "passed"
    assert bundle_payload["previous_stable_release"]["run_id"] == "stock-release-prev"
    assert "has_blocking_regression" in bundle_payload["blocking_status_summary"]
    assert "release_gate_failed" in bundle_payload["blocking_status_summary"]
