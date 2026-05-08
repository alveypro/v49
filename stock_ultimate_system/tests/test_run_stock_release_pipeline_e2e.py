from __future__ import annotations

import json
from pathlib import Path

import pytest

from tests.release_pipeline_cli_test_support import run_release_pipeline_cli


pytestmark = pytest.mark.e2e


def test_run_stock_release_pipeline_requires_active_pointer_by_default(tmp_path):
    project_root = Path(__file__).resolve().parents[1]

    completed = run_release_pipeline_cli(
        project_root=project_root,
        args=[
            "--output-dir",
            str(tmp_path / "release"),
            "--artifact-registry-path",
            str(tmp_path / "artifact_registry.jsonl"),
        ],
    )

    assert completed.returncode == 1
    payload = json.loads(completed.stdout)
    evidence_payload = json.loads(
        (tmp_path / "release" / "release_evidence_bundle" / "release_evidence_bundle.json").read_text(encoding="utf-8")
    )
    assert payload["status"] == "failed"
    assert payload["release_gates"]["require_active_pointer"] is True
    assert payload["blocking_failures"] == ["release_gates"]
    assert evidence_payload["release_gate_result"]["gate_policy"]["require_active_pointer"] is True
    assert evidence_payload["release_gate_result"]["pointer_integrity"]["status"] == "failed_missing_active_pointer"


def test_run_stock_release_pipeline_accepts_existing_release_gates_with_active_pointer(tmp_path):
    project_root = Path(__file__).resolve().parents[1]
    existing_gates_path = tmp_path / "existing_release_gates.json"
    existing_gates_path.write_text(
        json.dumps(
            {
                "status": "passed",
                "gate_total": 3,
                "passed_total": 3,
                "failed_total": 0,
                "gate_policy": {"require_active_pointer": True},
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

    completed = run_release_pipeline_cli(
        project_root=project_root,
        args=[
            "--output-dir",
            str(tmp_path / "release"),
            "--release-gates-json",
            str(existing_gates_path),
            "--artifact-registry-path",
            str(tmp_path / "artifact_registry.jsonl"),
        ],
    )

    assert completed.returncode == 0
    payload = json.loads(completed.stdout)
    evidence_payload = json.loads(
        (tmp_path / "release" / "release_evidence_bundle" / "release_evidence_bundle.json").read_text(encoding="utf-8")
    )
    assert payload["status"] == "passed"
    assert payload["release_gates"]["source"] == "existing_json"
    assert payload["release_gates"]["require_active_pointer"] is True
    assert evidence_payload["release_gate_result"]["pointer_integrity"]["status"] == "passed"
