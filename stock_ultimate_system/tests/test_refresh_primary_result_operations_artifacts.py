import json

from scripts import refresh_primary_result_operations_artifacts as refresh


def test_refresh_operations_artifacts_runs_scoreboard_after_gap(tmp_path, monkeypatch):
    calls = []

    def fake_builder(name, status="passed", decision=None):
        def _inner(**kwargs):
            calls.append(name)
            output = kwargs.get("output_path")
            if output:
                output.parent.mkdir(parents=True, exist_ok=True)
                output.write_text(json.dumps({"status": status, "decision": decision}), encoding="utf-8")
            return 0, {"status": status, "decision": decision, "blocking_reasons": [], "next_actions": []}

        return _inner

    monkeypatch.setattr(refresh, "build_primary_result_candidate_handoff_gate", fake_builder("candidate_handoff_gate", "passed", "aligned"))
    monkeypatch.setattr(refresh, "build_primary_result_performance_evidence", fake_builder("performance_evidence", "accumulating"))
    monkeypatch.setattr(refresh, "build_primary_result_promotion_readiness_gate", fake_builder("promotion_readiness_gate", "blocked", "blocked"))
    monkeypatch.setattr(refresh, "build_primary_result_competitive_gap_assessment", fake_builder("competitive_gap_assessment", "completed"))
    monkeypatch.setattr(refresh, "build_primary_result_daily_operations_scoreboard", fake_builder("daily_operations_scoreboard", "yellow"))
    monkeypatch.setattr(refresh, "_candidate_quality_validation_history_archive_stage", lambda exp_dir: fake_builder("candidate_quality_validation_history_archive", "passed")())
    monkeypatch.setattr(refresh, "_candidate_quality_density_progress_stage", lambda exp_dir: fake_builder("candidate_quality_density_progress", "blocked")())
    monkeypatch.setattr(refresh, "build_primary_result_daily_planner", fake_builder("daily_planner", "passed"))
    monkeypatch.setattr(refresh, "build_primary_result_morning_operations_brief", fake_builder("morning_operations_brief", "passed"))

    exit_code, payload = refresh.refresh_primary_result_operations_artifacts(
        exp_dir=tmp_path / "data" / "experiments",
        artifacts_dir=tmp_path / "artifacts",
        output_path=tmp_path / "artifacts" / "refresh.json",
    )

    assert exit_code == 0
    assert payload["status"] == "completed"
    assert calls == [
        "candidate_handoff_gate",
        "performance_evidence",
        "promotion_readiness_gate",
        "daily_operations_scoreboard",
        "competitive_gap_assessment",
        "daily_operations_scoreboard",
        "candidate_quality_validation_history_archive",
        "candidate_quality_density_progress",
        "daily_planner",
        "morning_operations_brief",
    ]
    assert (tmp_path / "artifacts" / "refresh.json").exists()


def test_refresh_operations_artifacts_reports_failed_stage(tmp_path, monkeypatch):
    def fail_handoff(**kwargs):
        raise RuntimeError("handoff artifact unavailable")

    def ok_builder(**kwargs):
        return 0, {"status": "passed", "blocking_reasons": [], "next_actions": []}

    monkeypatch.setattr(refresh, "build_primary_result_candidate_handoff_gate", fail_handoff)
    monkeypatch.setattr(refresh, "build_primary_result_performance_evidence", ok_builder)
    monkeypatch.setattr(refresh, "build_primary_result_promotion_readiness_gate", ok_builder)
    monkeypatch.setattr(refresh, "build_primary_result_competitive_gap_assessment", ok_builder)
    monkeypatch.setattr(refresh, "build_primary_result_daily_operations_scoreboard", ok_builder)
    monkeypatch.setattr(refresh, "_candidate_quality_validation_history_archive_stage", lambda exp_dir: ok_builder())
    monkeypatch.setattr(refresh, "_candidate_quality_density_progress_stage", lambda exp_dir: ok_builder())
    monkeypatch.setattr(refresh, "build_primary_result_daily_planner", ok_builder)
    monkeypatch.setattr(refresh, "build_primary_result_morning_operations_brief", ok_builder)

    exit_code, payload = refresh.refresh_primary_result_operations_artifacts(
        exp_dir=tmp_path / "data" / "experiments",
        artifacts_dir=tmp_path / "artifacts",
        output_path=tmp_path / "artifacts" / "refresh.json",
    )

    assert exit_code == 1
    assert payload["status"] == "failed"
    assert payload["failed_stages"] == ["candidate_handoff_gate"]
    assert "handoff artifact unavailable" in payload["blocking_reasons"]
