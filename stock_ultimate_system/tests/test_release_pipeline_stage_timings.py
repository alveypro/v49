import json

import pytest

from scripts.run_stock_release_pipeline import build_stock_release_pipeline_summary


pytestmark = pytest.mark.integration


def test_release_pipeline_summary_contains_stage_timings_and_manifest(tmp_path):
    payload = build_stock_release_pipeline_summary(tmp_path)

    assert payload["run_id"].startswith("stock-release-")
    assert set(payload["stage_timings"].keys()) == {
        "benchmark_report",
        "benchmark_diff",
        "release_gates",
        "manifest",
        "release_evidence_bundle",
    }
    assert payload["manifest"]["json_path"].endswith("release_pipeline_manifest.json")

    manifest_payload = json.loads((tmp_path / "release_pipeline_manifest.json").read_text(encoding="utf-8"))
    assert manifest_payload["run_id"] == payload["run_id"]
    assert manifest_payload["benchmark_report_hash"]
    assert manifest_payload["benchmark_diff_hash"]
    assert manifest_payload["release_gates_hash"]
