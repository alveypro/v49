from __future__ import annotations

import json
from pathlib import Path

import pytest

import scripts.run_stock_release_pipeline as pipeline
from tests.release_pipeline_cli_test_support import write_release_decision


pytestmark = pytest.mark.fast


def _write_existing_release_gates(path: Path, *, require_active_pointer: bool = False) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
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


def _write_blocking_baseline(path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "benchmark_version": "v1",
                "registry_version": "v1",
                "sample_total": 7,
                "core_sample_total": 4,
                "extended_sample_total": 3,
                "blocking_total": 0,
                "observation_total": 3,
                "render_contract_version": "v1",
                "runtime_observability_version": "v1",
                "has_blocking_regression": False,
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    return path


def test_release_pipeline_summary_returns_failed_payload_on_blocking_diff(tmp_path):
    baseline_path = _write_blocking_baseline(tmp_path / "baseline.json")
    release_gates_path = _write_existing_release_gates(tmp_path / "existing_release_gates.json")

    payload = pipeline.build_stock_release_pipeline_summary(
        tmp_path / "run-with-baseline",
        baseline_report=str(baseline_path),
        release_gates_json=str(release_gates_path),
        require_active_pointer=False,
        artifact_registry_path=str(tmp_path / "artifact_registry.jsonl"),
    )

    assert payload["status"] == "failed"
    assert payload["benchmark_diff"]["has_blocking_regression"] is True
    assert payload["release_gates"]["source"] == "existing_json"


def test_release_pipeline_summary_can_promote_baseline_without_cli_roundtrip(tmp_path):
    project_root = Path(__file__).resolve().parents[1]
    baselines_dir = tmp_path / "artifacts" / "baselines"
    release_decision_path = write_release_decision(tmp_path / "release_decision.json")
    release_gates_path = _write_existing_release_gates(tmp_path / "existing_release_gates.json")

    payload = pipeline.build_stock_release_pipeline_summary(
        tmp_path / "release",
        promote_baseline=True,
        baseline_id="stock-baseline-pipeline001",
        baselines_dir=str(baselines_dir),
        baseline_policy_path=str(project_root / "STOCK_PRIMARY_RESULT_BASELINE_POLICY.md"),
        artifact_registry_path=str(tmp_path / "artifact_registry.jsonl"),
        release_gates_json=str(release_gates_path),
        release_decision_json=str(release_decision_path),
        require_active_pointer=False,
    )

    evidence_payload = json.loads(
        (tmp_path / "release" / "release_evidence_bundle" / "release_evidence_bundle.json").read_text(encoding="utf-8")
    )
    current_payload = json.loads((baselines_dir / "current.json").read_text(encoding="utf-8"))
    snapshot_payload = json.loads((baselines_dir / "history" / "stock-baseline-pipeline001.json").read_text(encoding="utf-8"))

    assert payload["status"] == "passed"
    assert payload["baseline_promotion"]["status"] == "promoted"
    assert payload["baseline_promotion"]["baseline_id"] == "stock-baseline-pipeline001"
    assert payload["baseline_promotion"]["release_decision_hash"]
    assert payload["artifact_registry"]["registered_total"] == 11
    assert evidence_payload["baseline_promotion"]["status"] == "promoted"
    assert current_payload["baseline_id"] == "stock-baseline-pipeline001"
    assert snapshot_payload["run_id"] == payload["run_id"]
    assert snapshot_payload["source_evidence_bundle_path"].endswith("release_evidence_bundle.json")
    assert snapshot_payload["release_decision_hash"]


def test_release_pipeline_summary_requires_release_decision_for_promotion(tmp_path):
    project_root = Path(__file__).resolve().parents[1]
    release_gates_path = _write_existing_release_gates(tmp_path / "existing_release_gates.json")

    payload = pipeline.build_stock_release_pipeline_summary(
        tmp_path / "release",
        promote_baseline=True,
        baseline_id="stock-baseline-missing-decision",
        baselines_dir=str(tmp_path / "artifacts" / "baselines"),
        baseline_policy_path=str(project_root / "STOCK_PRIMARY_RESULT_BASELINE_POLICY.md"),
        artifact_registry_path=str(tmp_path / "artifact_registry.jsonl"),
        release_gates_json=str(release_gates_path),
        require_active_pointer=False,
    )

    assert payload["status"] == "failed"
    assert payload["baseline_promotion"]["status"] == "failed"
    assert payload["baseline_promotion"]["error"] == "baseline promotion requires approved release decision"
    assert not (tmp_path / "artifacts" / "baselines" / "history" / "stock-baseline-missing-decision.json").exists()


def test_release_pipeline_summary_skips_promotion_on_blocking_failure(tmp_path):
    project_root = Path(__file__).resolve().parents[1]
    baseline_path = _write_blocking_baseline(tmp_path / "baseline.json")
    release_gates_path = _write_existing_release_gates(tmp_path / "existing_release_gates.json")

    payload = pipeline.build_stock_release_pipeline_summary(
        tmp_path / "run-with-baseline",
        baseline_report=str(baseline_path),
        promote_baseline=True,
        baseline_id="stock-baseline-should-not-promote",
        baselines_dir=str(tmp_path / "artifacts" / "baselines"),
        baseline_policy_path=str(project_root / "STOCK_PRIMARY_RESULT_BASELINE_POLICY.md"),
        artifact_registry_path=str(tmp_path / "artifact_registry.jsonl"),
        release_gates_json=str(release_gates_path),
        require_active_pointer=False,
    )

    evidence_payload = json.loads(
        (tmp_path / "run-with-baseline" / "release_evidence_bundle" / "release_evidence_bundle.json").read_text(
            encoding="utf-8"
        )
    )

    assert payload["status"] == "failed"
    assert payload["baseline_promotion"]["status"] == "skipped"
    assert payload["artifact_registry"]["registered_total"] == 7
    assert payload["baseline_promotion"]["error"] == "pipeline has blocking failures"
    assert evidence_payload["baseline_promotion"]["status"] == "skipped"
    assert not (tmp_path / "artifacts" / "baselines" / "history" / "stock-baseline-should-not-promote.json").exists()
