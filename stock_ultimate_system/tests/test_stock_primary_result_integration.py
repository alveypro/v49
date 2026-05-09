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
    (exp_dir / "candidates_basket_summary_latest.json").write_text(
        '{"risk_pressure_score":18.0,"weighted_liquidity_score":0.52,"liquidity_capacity_weight":0.24}',
        encoding="utf-8",
    )
    (exp_dir / "candidates_run_status_latest.json").write_text(
        """
        {
          "status": "running",
          "stage": "batch_prediction_running",
          "stage_label": "批量预测运行中",
          "detail": "正在生成中间候选结果",
          "updated_at": "2026-04-23T03:53:48+08:00",
          "elapsed_sec": 10.69,
          "results_ready": 31,
          "skipped_count": 0
        }
        """,
        encoding="utf-8",
    )
    (exp_dir / "candidates_basket_validation_latest.json").write_text("{}", encoding="utf-8")
    (exp_dir / "evolution_registry_latest.json").write_text(
        """
        {
          "champion_version": "evo_20260419_120000",
          "champion_summary": {
            "walk_forward_score": 0.241,
            "trade_objective_stability": 0.701
          },
          "history": [
            {
              "version": "evo_20260419_120000",
              "action": "observe",
              "reason": "最近候选篮子反馈要求复核，候选版本先观察，不直接晋级。",
              "created_at": "2026-04-19T12:00:00",
              "gates": {
                "execution_feedback": {
                  "feedback_level": "review",
                  "window_label": "5D",
                  "summary_note": "recent basket observation requires review before trusting the current candidate profile",
                  "change_total": 2,
                  "review_only": true,
                  "passed": true
                },
                "capacity_pressure": {
                  "capacity_state": "stretched",
                  "recommended_scale_profile": "top1_only",
                  "worst_stress_score": 55.0,
                  "passed": false
                }
              }
            }
          ]
        }
        """,
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
    basket_dir = tmp_path / "artifacts" / "primary_result_candidate_baskets"
    basket_dir.mkdir(parents=True, exist_ok=True)
    (basket_dir / "current.json").write_text(
        '{"status":"approved","basket_id":"basket-approved-001","updated_at":"2026-04-19T14:33:06+00:00"}',
        encoding="utf-8",
    )
    (basket_dir / "latest_attempt.json").write_text(
        '{"status":"blocked","basket_id":"basket-blocked-002","blocking_reasons":["top industry weight must stay within policy"]}',
        encoding="utf-8",
    )
    (rep_dir / "backtest_report_20260405_080000.md").write_text("# report\n", encoding="utf-8")
    return exp_dir, rep_dir


def test_stock_primary_result_canonical_runtime_metadata_is_constructed_in_dashboard_context(tmp_path, monkeypatch):
    exp_dir, rep_dir = _prepare_dashboard_artifacts(tmp_path)
    monkeypatch.setattr(
        dashboard_context,
        "resolve_project_path",
        lambda rel: {"data/experiments": exp_dir, "data/reports": rep_dir}[rel],
    )
    context = dashboard_context.build_dashboard_context(tmp_path, candidate_index=0, base_path="/stock")
    metadata = context["primary_result_runtime_metadata"]
    assert metadata["stock_primary_result_runtime_mode"] == "canonical"
    assert metadata["stock_primary_result_source"] == "canonical"
    assert metadata["stock_primary_result_fallback_reason"] == "none"
    assert metadata["stock_primary_result_has_problem_fallback"] is False


def test_stock_primary_result_canonical_render_is_default_main_path(tmp_path, monkeypatch):
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
    assert "Governance Summary" not in html_text
    assert "当前推进状态" not in html_text
    assert "Airivo 是统一母平台" not in html_text
    assert 'id="t12-governance-summary"' not in html_text
    assert "当前生效篮子" in html_text
    assert "basket-approved-001" in html_text
    assert "最新篮子尝试" in html_text
    assert "top industry weight must stay within policy" in html_text
    assert "容量门禁" in html_text
    assert "容量阻断" in html_text
    assert "最差压力分" in html_text
    assert "放大量状态" in html_text
    assert "放大量受限" in html_text
    assert "承载受限权重" in html_text
    assert "候选主链阶段" in html_text
    assert "候选主链处理中" in html_text
    assert "已处理结果数" in html_text
    assert "31" in html_text
    assert "已耗时" in html_text
    assert "当前阻塞点" in html_text
    assert "正在生成中间候选结果" in html_text
    assert "当前唯一动作" in html_text
    assert "继续观察" in html_text
    assert "当前是否可执行" in html_text
    assert "仅研究可见" in html_text
    assert "门禁总览" in html_text
    assert "阻断后果" in html_text
    assert "为什么是它" in html_text
    assert "排序证据" in html_text
    assert "最近治理轨迹" in html_text


def test_stock_primary_result_canonical_path_keeps_empty_and_degraded_states_stable(tmp_path, monkeypatch):
    exp_dir, rep_dir = _prepare_dashboard_artifacts(tmp_path)
    (exp_dir / "candidates_top_latest.csv").write_text("ts_code,stock_name\n,\n", encoding="utf-8")
    monkeypatch.setattr(
        dashboard_context,
        "resolve_project_path",
        lambda rel: {"data/experiments": exp_dir, "data/reports": rep_dir}[rel],
    )
    context = dashboard_context.build_dashboard_context(tmp_path, candidate_index=0, base_path="/stock")
    html_text = str(context["primary_result_card_html"])
    assert 'class="stock-primary-result__conclusion"' in html_text
    assert "降级说明" in html_text or "待补充" in html_text
