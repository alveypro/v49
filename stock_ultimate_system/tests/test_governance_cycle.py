import json

from run_governance_cycle import _build_cycle_markdown, build_governance_cycle_summary


def test_governance_cycle_summary_marks_release_ready(tmp_path):
    out_dir = tmp_path / "data" / "experiments"
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "governance_decision.json").write_text(
        json.dumps({"decision": "promote_to_staging"}, ensure_ascii=False),
        encoding="utf-8",
    )
    (out_dir / "governance_audit_latest.json").write_text(
        json.dumps({"summary": {"overall_status": "pass"}}, ensure_ascii=False),
        encoding="utf-8",
    )

    summary = build_governance_cycle_summary(
        started_at="2026-04-14T10:00:00",
        steps=[
            {"name": "daily_research", "ok": True, "detail": "ok"},
            {"name": "artifact_bundle", "ok": True, "detail": "ok"},
            {"name": "governance_audit", "ok": True, "detail": "ok"},
        ],
        outputs_dir=out_dir,
    )

    assert summary["cycle_state"] == "release_ready"
    assert summary["recommended_action"] == "run_release_pipeline"
    assert summary["release_readiness"]["ready_for_release"] is True
    assert summary["release_readiness"]["keep_previous_stable_release"] is False


def test_governance_cycle_summary_marks_observe_only(tmp_path):
    out_dir = tmp_path / "data" / "experiments"
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "governance_decision.json").write_text(
        json.dumps({"decision": "observe"}, ensure_ascii=False),
        encoding="utf-8",
    )
    (out_dir / "governance_audit_latest.json").write_text(
        json.dumps({"summary": {"overall_status": "warn"}}, ensure_ascii=False),
        encoding="utf-8",
    )

    summary = build_governance_cycle_summary(
        started_at="2026-04-14T10:00:00",
        steps=[
            {"name": "daily_research", "ok": True, "detail": "ok"},
            {"name": "artifact_bundle", "ok": True, "detail": "ok"},
            {"name": "governance_audit", "ok": True, "detail": "warn"},
        ],
        outputs_dir=out_dir,
    )

    assert summary["cycle_state"] == "observe_only"
    assert summary["recommended_action"] == "hold_observation"
    assert summary["release_readiness"]["ready_for_release"] is False
    assert summary["release_readiness"]["ready_for_observation"] is True


def test_governance_cycle_summary_blocks_on_audit_failure_and_formats_markdown(tmp_path):
    out_dir = tmp_path / "data" / "experiments"
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "governance_decision.json").write_text(
        json.dumps({"decision": "reject"}, ensure_ascii=False),
        encoding="utf-8",
    )
    (out_dir / "governance_audit_latest.json").write_text(
        json.dumps({"summary": {"overall_status": "fail"}}, ensure_ascii=False),
        encoding="utf-8",
    )

    summary = build_governance_cycle_summary(
        started_at="2026-04-14T10:00:00",
        steps=[
            {"name": "daily_research", "ok": True, "detail": "ok"},
            {"name": "artifact_bundle", "ok": True, "detail": "ok"},
            {"name": "governance_audit", "ok": True, "detail": "fail"},
        ],
        outputs_dir=out_dir,
    )
    markdown = _build_cycle_markdown(summary)

    assert summary["cycle_state"] == "audit_blocked"
    assert summary["recommended_action"] == "review_governance_audit"
    assert "治理审计未通过" in summary["operator_message"]
    assert "cycle_state: audit_blocked" in markdown
    assert "recommended_action: review_governance_audit" in markdown


def test_governance_cycle_summary_mentions_previous_stable_release(tmp_path):
    out_dir = tmp_path / "data" / "experiments"
    stable_dir = tmp_path / "artifacts" / "stock_release_pipeline"
    out_dir.mkdir(parents=True, exist_ok=True)
    stable_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "governance_decision.json").write_text(
        json.dumps({"decision": "reject"}, ensure_ascii=False),
        encoding="utf-8",
    )
    (out_dir / "governance_audit_latest.json").write_text(
        json.dumps({"summary": {"overall_status": "warn"}}, ensure_ascii=False),
        encoding="utf-8",
    )
    stable_path = stable_dir / "stable_release_reference.json"
    stable_path.write_text(
        json.dumps({"run_id": "stock-release-stable001", "status": "passed"}, ensure_ascii=False),
        encoding="utf-8",
    )

    summary = build_governance_cycle_summary(
        started_at="2026-04-14T10:00:00",
        steps=[
            {"name": "daily_research", "ok": True, "detail": "ok"},
            {"name": "artifact_bundle", "ok": True, "detail": "ok"},
            {"name": "governance_audit", "ok": True, "detail": "warn"},
        ],
        outputs_dir=out_dir,
        stable_release_reference_path=stable_path,
    )

    assert summary["cycle_state"] == "rejected"
    assert summary["recommended_action"] == "keep_previous_stable_release"
    assert summary["previous_stable_release"]["run_id"] == "stock-release-stable001"
    assert "stock-release-stable001" in summary["operator_message"]
