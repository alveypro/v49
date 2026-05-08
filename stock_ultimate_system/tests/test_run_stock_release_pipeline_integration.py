from __future__ import annotations

import json

import pytest

import scripts.run_stock_release_pipeline as pipeline


pytestmark = pytest.mark.integration


def _write_existing_release_gates(path, *, require_active_pointer: bool = False):
    path.write_text(
        json.dumps(
            {
                "status": "passed",
                "gate_total": 3,
                "passed_total": 3,
                "failed_total": 0,
                "gate_policy": {"require_active_pointer": require_active_pointer},
                "artifact_support": {},
                "results": [
                    {"gate": "benchmark", "gate_level": "blocking", "passed": True},
                    {"gate": "latest_path", "gate_level": "blocking", "passed": True},
                    {"gate": "pointer_integrity", "gate_level": "blocking", "passed": True, "status": "passed"},
                ],
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    return path


def test_run_stock_release_pipeline_outputs_stable_summary(tmp_path):
    release_gates_path = _write_existing_release_gates(tmp_path / "existing_release_gates.json")
    payload = pipeline.build_stock_release_pipeline_summary(
        tmp_path,
        require_active_pointer=False,
        artifact_registry_path=tmp_path / "artifact_registry.jsonl",
        release_gates_json=release_gates_path,
    )

    assert payload["pipeline_version"] == "v1"
    assert payload["run_id"].startswith("stock-release-")
    assert payload["started_at"]
    assert payload["finished_at"]
    assert payload["steps"] == [
        "benchmark_report",
        "benchmark_diff",
        "candidate_quality_diff",
        "release_gates",
        "manifest",
        "release_evidence_bundle",
    ]
    assert payload["status"] == "passed"
    assert payload["release_gates"]["require_active_pointer"] is False
    assert payload["baseline_promotion"]["status"] == "not_requested"
    assert payload["artifact_registry"]["registered_total"] == 8
    assert payload["release_gates"]["source"] == "existing_json"
    assert payload["release_evidence_bundle"]["json_path"].endswith("release_evidence_bundle.json")
    assert payload["candidate_quality_diff"]["json_path"].endswith("candidate_quality_diff.json")
    assert payload["manifest"]["json_path"].endswith("release_pipeline_manifest.json")
    assert "release_gates" in payload["stage_timings"]
    assert (tmp_path / "stock_release_pipeline_summary.json").exists()
    assert (tmp_path / "stable_release_reference.json").exists()
    manifest_payload = json.loads((tmp_path / "release_pipeline_manifest.json").read_text(encoding="utf-8"))
    evidence_payload = json.loads(
        (tmp_path / "release_evidence_bundle" / "release_evidence_bundle.json").read_text(encoding="utf-8")
    )
    stable_reference_payload = json.loads((tmp_path / "stable_release_reference.json").read_text(encoding="utf-8"))
    artifact_registry_lines = (tmp_path / "artifact_registry.jsonl").read_text(encoding="utf-8").splitlines()
    assert manifest_payload["run_id"] == payload["run_id"] == evidence_payload["run_id"]
    assert manifest_payload["previous_stable_release"] == {}
    assert payload["stable_release_reference"]["run_id"] == payload["run_id"]
    assert stable_reference_payload["run_id"] == payload["run_id"]
    assert evidence_payload["previous_stable_release"] == {}
    assert evidence_payload["candidate_quality_diff"]["status"] == "not_available"
    assert evidence_payload["baseline_promotion"]["status"] == "not_requested"
    assert len(artifact_registry_lines) == 8
    assert payload["rollback_readiness"]["has_previous_stable_release"] is False
    assert payload["rollback_readiness"]["fully_release_ready"] is False
    assert payload["rollback_readiness"]["release_classification"] == "bootstrap_release"


def test_run_stock_release_pipeline_carries_previous_stable_reference(tmp_path):
    release_gates_path = _write_existing_release_gates(tmp_path / "existing_release_gates.json")
    (tmp_path / "stable_release_reference.json").write_text(
        json.dumps(
            {
                "reference_version": "v1",
                "run_id": "stock-release-prev001",
                "status": "passed",
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    payload = pipeline.build_stock_release_pipeline_summary(
        tmp_path,
        require_active_pointer=False,
        artifact_registry_path=tmp_path / "artifact_registry.jsonl",
        release_gates_json=release_gates_path,
    )

    manifest_payload = json.loads((tmp_path / "release_pipeline_manifest.json").read_text(encoding="utf-8"))
    evidence_payload = json.loads(
        (tmp_path / "release_evidence_bundle" / "release_evidence_bundle.json").read_text(encoding="utf-8")
    )
    stable_reference_payload = json.loads((tmp_path / "stable_release_reference.json").read_text(encoding="utf-8"))

    assert payload["previous_stable_release"]["run_id"] == "stock-release-prev001"
    assert manifest_payload["previous_stable_release"]["run_id"] == "stock-release-prev001"
    assert evidence_payload["previous_stable_release"]["run_id"] == "stock-release-prev001"
    assert stable_reference_payload["previous_stable_run_id"] == "stock-release-prev001"
    assert payload["rollback_readiness"]["has_previous_stable_release"] is True
    assert payload["rollback_readiness"]["fully_release_ready"] is True
    assert payload["rollback_readiness"]["release_classification"] == "standard_release"
    assert payload["release_gates"]["require_active_pointer"] is False
