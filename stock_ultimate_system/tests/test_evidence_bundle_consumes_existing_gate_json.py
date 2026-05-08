import json

import pytest

from scripts.build_release_evidence_bundle import build_release_evidence_bundle


pytestmark = pytest.mark.fast


def test_evidence_bundle_consumes_existing_gate_json(tmp_path):
    benchmark_report = tmp_path / "report.json"
    benchmark_diff = tmp_path / "diff.json"
    release_gates = tmp_path / "gates.json"
    manifest = tmp_path / "manifest.json"

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
        json.dumps({"has_blocking_regression": True}, ensure_ascii=False),
        encoding="utf-8",
    )
    release_gates.write_text(
        json.dumps({"status": "failed", "gate_total": 5, "failed_total": 1}, ensure_ascii=False),
        encoding="utf-8",
    )
    manifest.write_text(json.dumps({"run_id": "stock-release-manifest"}, ensure_ascii=False), encoding="utf-8")

    bundle_path = build_release_evidence_bundle(
        tmp_path / "bundle",
        benchmark_report_json=benchmark_report,
        benchmark_diff_json=benchmark_diff,
        release_gates_json=release_gates,
        manifest_json=manifest,
    )
    payload = json.loads(bundle_path.read_text(encoding="utf-8"))

    assert payload["run_id"] == "stock-release-manifest"
    assert payload["release_gate_result"]["status"] == "failed"
    assert payload["release_gate_result"]["failed_total"] == 1
    assert payload["blocking_status_summary"]["has_blocking_regression"] is True
