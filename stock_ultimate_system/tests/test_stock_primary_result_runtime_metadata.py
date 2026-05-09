from src import dashboard_context
from src.dashboard_support import STOCK_PRIMARY_RESULT_RUNTIME_METADATA_KEYS


def _prepare_dashboard_artifacts(tmp_path):
    exp_dir = tmp_path / "data" / "experiments"
    rep_dir = tmp_path / "data" / "reports"
    exp_dir.mkdir(parents=True, exist_ok=True)
    rep_dir.mkdir(parents=True, exist_ok=True)
    (exp_dir / "daily_research_latest.md").write_text(
        "- score: 90.00/100\n- success_rate: 95.00%\n- failure_penalty: 1.0\n- category_penalty: 0.0\n",
        encoding="utf-8",
    )
    (exp_dir / "daily_health_trend_latest.csv").write_text(
        "generated_at,score\n2026-04-05 08:00:00,90\n",
        encoding="utf-8",
    )
    (exp_dir / "backtest_leaderboard.csv").write_text(
        "run_id,source_type,stock_pool,total_return,sharpe_ratio,max_drawdown,win_rate,total_trades\n"
        "r1,official_research,000001.SZ|600036.SH,0.05,1.0,-0.04,0.55,24\n",
        encoding="utf-8",
    )
    (exp_dir / "candidates_top_latest.csv").write_text(
        "ts_code,stock_name,industry,signal,risk_level,final_score\n"
        "000001.SZ,平安银行,银行,strong_buy,low,155\n",
        encoding="utf-8",
    )
    (exp_dir / "candidates_top_latest.md").write_text("# Candidates\n", encoding="utf-8")
    (exp_dir / "candidates_basket_summary_latest.json").write_text("{}", encoding="utf-8")
    (exp_dir / "candidates_basket_validation_latest.json").write_text("{}", encoding="utf-8")
    (exp_dir / "daily_research_status_latest.json").write_text('{"state":"completed"}', encoding="utf-8")
    (exp_dir / "buylist_latest.json").write_text('{"items":[{"ts_code":"000001.SZ"}]}', encoding="utf-8")
    (exp_dir / "governance_audit_latest.json").write_text("{}", encoding="utf-8")
    (exp_dir / "t1_execution_checklist_latest.json").write_text("{}", encoding="utf-8")
    (exp_dir / "t12_alert_center_latest.json").write_text('{"summary":{"overall_status":"pass"}}', encoding="utf-8")
    (exp_dir / "t12_review_checklist_latest.json").write_text('{"overall_status":"pass"}', encoding="utf-8")
    (exp_dir / "t12_threshold_control_latest.json").write_text(
        '{"applied": true, "recommendation": "tighten_thresholds"}',
        encoding="utf-8",
    )
    (exp_dir / "t12_rollback_drill_latest.json").write_text(
        '{"mode":"off","triggered":false,"would_rollback_apply":false}',
        encoding="utf-8",
    )
    (exp_dir / "t12_threshold_rollback_events.json").write_text('{"items":[]}', encoding="utf-8")
    (exp_dir / "t12_threshold_stable_snapshot.json").write_text(
        '{"thresholds":{"warn_hot_threshold":1}}',
        encoding="utf-8",
    )
    (rep_dir / "backtest_report_20260405_080000.md").write_text("# report\n", encoding="utf-8")
    return exp_dir, rep_dir


def test_stock_primary_result_runtime_metadata_is_single_track_canonical(tmp_path, monkeypatch):
    exp_dir, rep_dir = _prepare_dashboard_artifacts(tmp_path)
    monkeypatch.setattr(
        dashboard_context,
        "resolve_project_path",
        lambda rel: {"data/experiments": exp_dir, "data/reports": rep_dir}[rel],
    )
    context = dashboard_context.build_dashboard_context(tmp_path, candidate_index=0, base_path="/stock")
    metadata = context["primary_result_runtime_metadata"]
    assert tuple(metadata.keys()) == STOCK_PRIMARY_RESULT_RUNTIME_METADATA_KEYS
    assert metadata["stock_primary_result_source"] == "canonical"
    assert metadata["stock_primary_result_runtime_mode"] == "canonical"
    assert metadata["stock_primary_result_fallback_reason"] == "none"
    assert metadata["stock_primary_result_has_problem_fallback"] is False
    assert metadata["stock_primary_result_is_canonical"] is True
    assert metadata["stock_primary_result_render_contract_version"] == "v1"
