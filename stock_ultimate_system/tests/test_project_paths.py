from pathlib import Path

from src.utils import project_paths


def test_runtime_paths_can_be_redirected_by_environment(monkeypatch, tmp_path):
    artifacts_dir = tmp_path / "canonical-artifacts"
    experiments_dir = tmp_path / "canonical-experiments"
    reports_dir = tmp_path / "canonical-reports"
    monkeypatch.setenv(project_paths.ARTIFACTS_DIR_ENV, str(artifacts_dir))
    monkeypatch.setenv(project_paths.EXPERIMENTS_DIR_ENV, str(experiments_dir))
    monkeypatch.setenv(project_paths.REPORTS_DIR_ENV, str(reports_dir))

    assert project_paths.resolve_artifacts_path() == artifacts_dir
    assert project_paths.resolve_artifacts_path("primary_result_performance_evidence_latest.json") == (
        artifacts_dir / "primary_result_performance_evidence_latest.json"
    )
    assert project_paths.resolve_experiments_path() == experiments_dir
    assert project_paths.resolve_experiments_path("primary_result_daily_closure_latest.json") == (
        experiments_dir / "primary_result_daily_closure_latest.json"
    )
    assert project_paths.resolve_reports_path() == reports_dir
    assert project_paths.resolve_reports_path("backtest_report_latest.md") == reports_dir / "backtest_report_latest.md"
