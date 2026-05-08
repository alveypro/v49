import json
from pathlib import Path

import pytest

import scripts.run_stock_release_pipeline as pipeline
from src.artifact_registry import ArtifactRegistry


pytestmark = pytest.mark.fast


def test_release_pipeline_executes_gate_once(monkeypatch, tmp_path):
    call_count = {"value": 0}

    def fake_run_release_gates(*, selected_gates=None, capture_output=False, output_path=None, require_active_pointer=False):
        call_count["value"] += 1
        payload = {
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
        }
        Path(output_path).write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        return 0, payload

    monkeypatch.setattr(pipeline, "run_release_gates", fake_run_release_gates)

    payload = pipeline.build_stock_release_pipeline_summary(tmp_path)

    assert payload["status"] == "passed"
    assert call_count["value"] == 1
    entries = ArtifactRegistry(tmp_path / "artifact_registry.jsonl").list_entries(run_id=payload["run_id"])
    assert payload["artifact_registry"]["registered_total"] == 8
    assert {entry["artifact_type"] for entry in entries} == {
        "benchmark_report",
        "benchmark_diff",
        "candidate_quality_diff",
        "release_gates",
        "release_evidence_bundle",
        "release_pipeline_manifest",
        "release_pipeline_summary",
        "stable_release_reference",
    }


def test_release_pipeline_reuses_existing_release_gates_json(monkeypatch, tmp_path):
    def fail_if_called(*, selected_gates=None, capture_output=False, output_path=None, require_active_pointer=False):
        raise AssertionError("release gates should not execute when release_gates_json is supplied")

    existing_gates_path = tmp_path / "existing_release_gates.json"
    existing_gates_path.write_text(
        json.dumps(
            {
                "status": "passed",
                "gate_total": 3,
                "passed_total": 3,
                "failed_total": 0,
                "gate_policy": {"require_active_pointer": False},
                "artifact_support": {},
                "results": [
                    {"gate": "benchmark", "gate_level": "blocking", "passed": True},
                    {"gate": "latest_path", "gate_level": "blocking", "passed": True},
                    {"gate": "pointer_integrity", "gate_level": "blocking", "passed": True, "status": "passed"},
                ],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(pipeline, "run_release_gates", fail_if_called)

    payload = pipeline.build_stock_release_pipeline_summary(
        tmp_path / "release",
        release_gates_json=existing_gates_path,
    )

    assert payload["status"] == "passed"
    assert payload["release_gates"]["source"] == "existing_json"
    assert payload["release_gates"]["json_path"].endswith("release_gates.json")
    assert json.loads((tmp_path / "release" / "release_gates.json").read_text(encoding="utf-8"))["status"] == "passed"


def test_release_pipeline_rejects_existing_blocking_release_gates_json(monkeypatch, tmp_path):
    def fail_if_called(*, selected_gates=None, capture_output=False, output_path=None, require_active_pointer=False):
        raise AssertionError("release gates should not execute when release_gates_json is supplied")

    existing_gates_path = tmp_path / "existing_release_gates.json"
    existing_gates_path.write_text(
        json.dumps(
            {
                "status": "failed",
                "gate_total": 3,
                "passed_total": 0,
                "failed_total": 1,
                "gate_policy": {"require_active_pointer": False},
                "artifact_support": {},
                "results": [
                    {"gate": "benchmark", "gate_level": "blocking", "passed": False},
                    {"gate": "latest_path", "gate_level": "blocking", "passed": True},
                    {"gate": "pointer_integrity", "gate_level": "blocking", "passed": True, "status": "passed"},
                ],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(pipeline, "run_release_gates", fail_if_called)

    payload = pipeline.build_stock_release_pipeline_summary(
        tmp_path / "release",
        release_gates_json=existing_gates_path,
    )

    assert payload["status"] == "failed"
    assert payload["release_gates"]["source"] == "existing_json"
    assert payload["blocking_failures"] == ["release_gates"]


def test_release_pipeline_rejects_existing_release_gates_json_without_active_pointer(monkeypatch, tmp_path):
    def fail_if_called(*, selected_gates=None, capture_output=False, output_path=None, require_active_pointer=False):
        raise AssertionError("release gates should not execute when release_gates_json is supplied")

    existing_gates_path = tmp_path / "existing_release_gates.json"
    existing_gates_path.write_text(
        json.dumps(
            {
                "status": "passed",
                "gate_total": 3,
                "passed_total": 3,
                "failed_total": 0,
                "gate_policy": {"require_active_pointer": False},
                "artifact_support": {},
                "results": [
                    {"gate": "benchmark", "gate_level": "blocking", "passed": True},
                    {"gate": "latest_path", "gate_level": "blocking", "passed": True},
                    {
                        "gate": "pointer_integrity",
                        "gate_level": "blocking",
                        "passed": True,
                        "status": "skipped_no_active_pointer",
                    },
                ],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(pipeline, "run_release_gates", fail_if_called)

    payload = pipeline.build_stock_release_pipeline_summary(
        tmp_path / "release",
        release_gates_json=existing_gates_path,
        require_active_pointer=True,
    )

    assert payload["status"] == "failed"
    assert payload["release_gates"]["require_active_pointer"] is True
    assert payload["blocking_failures"] == ["release_gates"]
