import json

from src.utils.governance_audit import run_governance_audit


def test_governance_audit_generates_outputs(tmp_path):
    config_dir = tmp_path / "config"
    out_dir = tmp_path / "data" / "experiments"
    config_dir.mkdir(parents=True, exist_ok=True)
    out_dir.mkdir(parents=True, exist_ok=True)

    framework = """
experiment_framework:
  artifacts:
    required_json:
      - experiment_manifest.json
    required_markdown:
      - experiment_summary.md
    required_tabular:
      - research_pool_snapshot.csv
"""
    (config_dir / "experiment_framework.example.yaml").write_text(framework, encoding="utf-8")

    (out_dir / "update_status_latest.json").write_text(
        json.dumps(
            {
                "status": "success",
                "post_candidates": {"ok": True, "detail": "ok"},
                "post_daily_research": {"ok": True, "detail": "ok"},
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    (out_dir / "daily_research_status_latest.json").write_text(
        json.dumps({"state": "completed"}, ensure_ascii=False),
        encoding="utf-8",
    )
    (out_dir / "candidates_top_latest.md").write_text("# candidates\n", encoding="utf-8")
    (out_dir / "daily_research_latest.md").write_text("# research\n", encoding="utf-8")
    (out_dir / "experiment_manifest.json").write_text("{}", encoding="utf-8")
    (out_dir / "experiment_summary.md").write_text("# summary\n", encoding="utf-8")
    (out_dir / "research_pool_snapshot.csv").write_text("ts_code\n000001.SZ\n", encoding="utf-8")

    payload = run_governance_audit(
        config_dir=str(config_dir),
        output_dir=str(out_dir),
        max_status_age_hours=36.0,
        max_candidate_age_hours=36.0,
        max_research_age_hours=36.0,
    )

    assert payload["summary"]["overall_status"] == "pass"
    assert (out_dir / "governance_audit_latest.json").exists()
    assert (out_dir / "governance_audit_latest.md").exists()


def test_governance_audit_flags_missing_artifacts(tmp_path):
    config_dir = tmp_path / "config"
    out_dir = tmp_path / "data" / "experiments"
    config_dir.mkdir(parents=True, exist_ok=True)
    out_dir.mkdir(parents=True, exist_ok=True)

    framework = """
experiment_framework:
  artifacts:
    required_json:
      - experiment_manifest.json
"""
    (config_dir / "experiment_framework.example.yaml").write_text(framework, encoding="utf-8")
    (out_dir / "update_status_latest.json").write_text(
        json.dumps({"status": "partial_success", "post_candidates": {"ok": True}, "post_daily_research": {"ok": False}}),
        encoding="utf-8",
    )

    payload = run_governance_audit(config_dir=str(config_dir), output_dir=str(out_dir))
    by_name = {item["name"]: item for item in payload["items"]}

    assert by_name["update_status_health"]["ok"] is False
    assert by_name["framework_required_artifacts"]["ok"] is False


def test_governance_audit_accepts_partial_success_with_core_ok(tmp_path):
    config_dir = tmp_path / "config"
    out_dir = tmp_path / "data" / "experiments"
    config_dir.mkdir(parents=True, exist_ok=True)
    out_dir.mkdir(parents=True, exist_ok=True)

    framework = """
experiment_framework:
  artifacts:
    required_json: []
    required_markdown: []
    required_tabular: []
"""
    (config_dir / "experiment_framework.example.yaml").write_text(framework, encoding="utf-8")
    (out_dir / "update_status_latest.json").write_text(
        json.dumps(
            {
                "status": "partial_success",
                "post_candidates": {"ok": True, "detail": "ok"},
                "post_daily_research": {"ok": True, "detail": "ok"},
            }
        ),
        encoding="utf-8",
    )
    (out_dir / "daily_research_status_latest.json").write_text('{"state":"completed"}', encoding="utf-8")
    (out_dir / "candidates_top_latest.md").write_text("# c\n", encoding="utf-8")
    (out_dir / "daily_research_latest.md").write_text("# d\n", encoding="utf-8")

    payload = run_governance_audit(config_dir=str(config_dir), output_dir=str(out_dir))
    by_name = {item["name"]: item for item in payload["items"]}
    assert by_name["update_status_health"]["ok"] is True
