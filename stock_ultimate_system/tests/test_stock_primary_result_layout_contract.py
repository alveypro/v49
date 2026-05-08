from src import dashboard_context
from run_dashboard import _render_dashboard


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
    (exp_dir / "update_status_latest.json").write_text(
        (
            '{"stage":"running_daily_research","status":"running",'
            '"update_summary":{"db_latest_after":"20260422"},'
            '"post_candidates":{"ok":false}}'
        ),
        encoding="utf-8",
    )
    (exp_dir / "candidate_prefilter_universe_latest.json").write_text(
        (
            '{"generated_at":"2026-04-21 23:50:17","trade_date":"20260420",'
            '"row_count":2,"market_symbol_count":2,"top_candidates":[]}'
        ),
        encoding="utf-8",
    )
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
    artifacts_dir = tmp_path / "artifacts"
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    (artifacts_dir / "primary_result_observation_wait_status_latest.json").write_text(
        (
            '{"generated_at":"2026-04-18T03:32:19+00:00","status":"pending_window","current_date":"2026-04-18",'
            '"observation_window":{"start_date":"2026-04-20","started_at":"2026-04-20T01:30:00Z","has_started":false}}'
        ),
        encoding="utf-8",
    )
    (exp_dir / "primary_result_daily_closure_latest.json").write_text(
        (
            '{"status":"pending_window","generated_at":"2026-04-26T23:28:32+08:00",'
            '"window_start":"2026-04-27T01:30:00Z","window_end":"2026-04-26"}'
        ),
        encoding="utf-8",
    )
    return exp_dir, rep_dir


def test_stock_primary_result_layout_contract_is_stable_in_default_canonical_mode(tmp_path, monkeypatch):
    exp_dir, rep_dir = _prepare_dashboard_artifacts(tmp_path)
    monkeypatch.setattr(
        dashboard_context,
        "resolve_project_path",
        lambda rel: {"data/experiments": exp_dir, "data/reports": rep_dir}[rel],
    )
    import run_dashboard as dashboard_module

    monkeypatch.setattr(
        dashboard_module,
        "resolve_project_path",
        lambda rel: {"data/experiments": exp_dir, "data/reports": rep_dir, ".": tmp_path}[rel],
    )
    html_text = _render_dashboard(tmp_path, current_view="overview", candidate_index=0, current_report="research", base_path="/stock")

    conclusion_index = html_text.index('class="stock-primary-result__conclusion"')
    explanation_index = html_text.index('class="stock-primary-result__explanation"')
    boundary_index = html_text.index('class="stock-primary-result__boundary"')

    assert conclusion_index < explanation_index < boundary_index
    assert "主结果" in html_text
    assert "解释层" in html_text
    assert "边界说明" in html_text
    assert html_text.count('class="stock-primary-result__summary-item') == 3
    assert html_text.count('class="stock-primary-result__boundary-text"') == 3
    assert "治理主解释权位于 /T12，此处仅作边界说明。" in html_text
    assert "当前推进状态" not in html_text
    assert "当前主要阻断" not in html_text
    assert "当前治理备注" not in html_text
    assert 'id="t12-governance-summary"' not in html_text
    assert "Airivo 是统一母平台" not in html_text
    assert "面向未来" not in html_text


def test_dashboard_separates_data_candidate_and_observation_timestamps(tmp_path, monkeypatch):
    exp_dir, rep_dir = _prepare_dashboard_artifacts(tmp_path)
    monkeypatch.setattr(
        dashboard_context,
        "resolve_project_path",
        lambda rel: {"data/experiments": exp_dir, "data/reports": rep_dir}[rel],
    )
    import run_dashboard as dashboard_module

    monkeypatch.setattr(
        dashboard_module,
        "resolve_project_path",
        lambda rel: {"data/experiments": exp_dir, "data/reports": rep_dir, ".": tmp_path}[rel],
    )
    html_text = _render_dashboard(tmp_path, current_view="overview", candidate_index=0, current_report="research", base_path="/stock")

    assert "数据最新交易日 2026-04-22" in html_text
    assert "候选产物" in html_text
    assert "观察窗口 2026-04-27" in html_text
    assert "观察窗口 2026-04-20" not in html_text
    assert "候选未跟上最新数据" in html_text
    assert "时间一致性" in html_text
