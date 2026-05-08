import json
from pathlib import Path

from src.artifact_source_audit import audit_artifact_source_pollution


def test_artifact_source_audit_detects_pytest_pollution_in_latest_files(tmp_path):
    artifacts_dir = tmp_path / "artifacts"
    (artifacts_dir / "primary_result_performance").mkdir(parents=True, exist_ok=True)
    (artifacts_dir / "primary_result_candidate_baskets").mkdir(parents=True, exist_ok=True)

    (artifacts_dir / "primary_result_performance" / "summary.json").write_text(
        json.dumps(
            {
                "entry_total": 1,
                "source_observation_path": "/private/var/folders/x/pytest-of-mac/pytest-1/test_case/primary_result_observation_latest.json",
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    (artifacts_dir / "primary_result_candidate_baskets" / "feedback_latest.json").write_text(
        json.dumps(
            {
                "source_performance_summary_path": "/tmp/pytest-1/performance_summary.json",
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    payload = audit_artifact_source_pollution(artifacts_dir=artifacts_dir)

    assert payload["polluted_file_total"] == 2
    polluted_paths = {item["path"] for item in payload["polluted_files"]}
    assert str(artifacts_dir / "primary_result_performance" / "summary.json") in polluted_paths
    assert str(artifacts_dir / "primary_result_candidate_baskets" / "feedback_latest.json") in polluted_paths


def test_artifact_source_audit_ignores_quarantine_directory(tmp_path):
    artifacts_dir = tmp_path / "artifacts"
    quarantine_dir = artifacts_dir / "_quarantine" / "20260426T232129" / "primary_result_performance"
    quarantine_dir.mkdir(parents=True, exist_ok=True)

    (quarantine_dir / "summary.json").write_text(
        json.dumps(
            {
                "entry_total": 1,
                "source_observation_path": "/private/var/folders/x/pytest-of-mac/pytest-1/test_case/primary_result_observation_latest.json",
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    payload = audit_artifact_source_pollution(artifacts_dir=artifacts_dir)

    assert payload["polluted_file_total"] == 0
    assert payload["polluted_files"] == []
