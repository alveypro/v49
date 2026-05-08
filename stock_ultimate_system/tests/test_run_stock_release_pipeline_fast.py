from __future__ import annotations

import sys
from pathlib import Path

import pytest

import scripts.run_stock_release_pipeline as pipeline


pytestmark = pytest.mark.fast


def test_release_pipeline_main_requires_active_pointer_by_default(monkeypatch, tmp_path, capsys):
    captured: dict[str, object] = {}

    def fake_build_stock_release_pipeline_summary(
        output_dir,
        *,
        baseline_report=None,
        promote_baseline=False,
        baseline_id=None,
        baselines_dir="artifacts/baselines",
        baseline_policy_path="STOCK_PRIMARY_RESULT_BASELINE_POLICY.md",
        artifact_registry_path=None,
        release_gates_json=None,
        release_decision_json=None,
        require_active_pointer=False,
    ):
        captured["require_active_pointer"] = require_active_pointer
        return {
            "status": "passed",
            "output_dir": str(output_dir),
        }

    monkeypatch.setattr(pipeline, "build_stock_release_pipeline_summary", fake_build_stock_release_pipeline_summary)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_stock_release_pipeline.py",
            "--output-dir",
            str(tmp_path / "release"),
        ],
    )

    exit_code = pipeline.main()

    assert exit_code == 0
    assert captured["require_active_pointer"] is True
    assert capsys.readouterr().out


def test_release_pipeline_main_allows_missing_active_pointer_when_explicitly_requested(monkeypatch, tmp_path):
    captured: dict[str, object] = {}

    def fake_build_stock_release_pipeline_summary(
        output_dir,
        *,
        baseline_report=None,
        promote_baseline=False,
        baseline_id=None,
        baselines_dir="artifacts/baselines",
        baseline_policy_path="STOCK_PRIMARY_RESULT_BASELINE_POLICY.md",
        artifact_registry_path=None,
        release_gates_json=None,
        release_decision_json=None,
        require_active_pointer=False,
    ):
        captured["require_active_pointer"] = require_active_pointer
        return {
            "status": "passed",
            "output_dir": str(output_dir),
        }

    monkeypatch.setattr(pipeline, "build_stock_release_pipeline_summary", fake_build_stock_release_pipeline_summary)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_stock_release_pipeline.py",
            "--output-dir",
            str(tmp_path / "release"),
            "--allow-missing-active-pointer",
        ],
    )

    exit_code = pipeline.main()

    assert exit_code == 0
    assert captured["require_active_pointer"] is False
