from pathlib import Path

from tools.tool_boundary_audit import build_tool_boundary_manifest, classify_tool_name, main


def test_classify_tool_name_respects_stable_and_archive_boundaries():
    assert classify_tool_name("run_daily_v9_evidence_pipeline.py")[0] == "stable"
    assert classify_tool_name("review_strategy_competition_broker_response.py")[0] == "archive_candidate"
    assert classify_tool_name("all_strategy_evidence_run.py")[0] == "archive_candidate"
    assert classify_tool_name("extract_top5_rebuild_service.py")[0] == "archive_candidate"
    assert classify_tool_name("run_governance_gate_ci.sh")[0] == "archive_candidate"
    assert classify_tool_name("strategy_optimization_stage_audit.py")[0] == "archive_candidate"
    assert classify_tool_name("record_strategy_competition_shadow_feedback.py")[0] == "archive_candidate"
    assert classify_tool_name("build_current_strategy_competition_audit.py")[0] == "support_review"
    assert classify_tool_name("strategy_competition_portfolio_audit.py")[0] == "archive_candidate"
    assert classify_tool_name("check_strategy_competition_live_order_authority.py")[0] == "archive_candidate"
    assert classify_tool_name("deploy_auth_to_server.sh")[0] == "support_review"
    assert classify_tool_name("top5_audit_scheduler_manifest.py")[0] == "support_review"
    assert classify_tool_name("custom_lab.py")[0] == "manual_review"
    assert classify_tool_name("README.md")[0] == "ignore"


def test_build_tool_boundary_manifest_is_audit_only(tmp_path: Path):
    tools_dir = tmp_path / "tools"
    tools_dir.mkdir()
    (tools_dir / "run_daily_v9_evidence_pipeline.py").write_text("print('stable')\n", encoding="utf-8")
    (tools_dir / "build_strategy_competition_formal_rerun_plan.py").write_text("print('one off')\n", encoding="utf-8")
    (tools_dir / "experimental_probe.py").write_text("print('manual')\n", encoding="utf-8")
    archive_dir = tools_dir / "archive" / "strategy_competition"
    archive_dir.mkdir(parents=True)
    (archive_dir / "review_strategy_competition_old.py").write_text("print('archived')\n", encoding="utf-8")

    manifest = build_tool_boundary_manifest(tools_dir)

    assert manifest["artifact_version"] == "airivo_tool_boundary_audit.v1"
    assert manifest["summary"]["stable"] == 1
    assert manifest["summary"]["archive_candidate"] == 1
    assert manifest["summary"]["manual_review"] == 1
    assert manifest["archive_summary"]["strategy_competition_archived_count"] == 1
    assert manifest["archive_summary"]["archive_counts"]["strategy_competition"] == 1
    assert manifest["archive_summary"]["archive_executable_counts"]["strategy_competition"] == 1
    assert manifest["archived_entrypoints"] == ["review_strategy_competition_old.py"]
    assert "audit_only_no_file_moves" in manifest["hard_boundaries"]


def test_tool_boundary_main_can_fail_on_archive_candidates(tmp_path: Path, monkeypatch, capsys):
    tools_dir = tmp_path / "tools"
    tools_dir.mkdir()
    (tools_dir / "build_strategy_competition_formal_rerun_plan.py").write_text("print('one off')\n", encoding="utf-8")
    monkeypatch.setattr(
        "sys.argv",
        [
            "tool_boundary_audit.py",
            "--tools-dir",
            str(tools_dir),
            "--fail-on-archive-candidates",
            "--max-manual-review",
            "0",
        ],
    )

    rc = main()
    captured = capsys.readouterr()

    assert rc == 2
    assert "archive_candidates_present" in captured.out


def test_tool_boundary_main_allows_current_budget(tmp_path: Path, monkeypatch):
    tools_dir = tmp_path / "tools"
    tools_dir.mkdir()
    from tools import tool_boundary_audit

    for name in tool_boundary_audit.STABLE_ENTRYPOINTS:
        (tools_dir / name).write_text("print('stable')\n", encoding="utf-8")
    monkeypatch.setattr(
        "sys.argv",
        [
            "tool_boundary_audit.py",
            "--tools-dir",
            str(tools_dir),
            "--fail-on-archive-candidates",
            "--max-manual-review",
            "0",
        ],
    )

    assert main() == 0


def test_tool_boundary_main_fails_when_stable_entrypoints_missing(tmp_path: Path, monkeypatch, capsys):
    tools_dir = tmp_path / "tools"
    tools_dir.mkdir()
    monkeypatch.setattr(
        "sys.argv",
        [
            "tool_boundary_audit.py",
            "--tools-dir",
            str(tools_dir),
            "--max-manual-review",
            "0",
        ],
    )

    rc = main()
    captured = capsys.readouterr()

    assert rc == 2
    assert "stable_entrypoints_missing" in captured.out


def test_tool_boundary_main_can_fail_on_support_review_budget(tmp_path: Path, monkeypatch, capsys):
    tools_dir = tmp_path / "tools"
    tools_dir.mkdir()
    from tools import tool_boundary_audit

    for name in tool_boundary_audit.STABLE_ENTRYPOINTS:
        (tools_dir / name).write_text("print('stable')\n", encoding="utf-8")
    (tools_dir / "build_current_strategy_competition_audit.py").write_text("print('support')\n", encoding="utf-8")
    monkeypatch.setattr(
        "sys.argv",
        [
            "tool_boundary_audit.py",
            "--tools-dir",
            str(tools_dir),
            "--max-support-review",
            "0",
        ],
    )

    rc = main()
    captured = capsys.readouterr()

    assert rc == 2
    assert "support_review_budget_exceeded" in captured.out
