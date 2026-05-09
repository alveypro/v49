import json
from pathlib import Path

from src.dashboard_support import (
    backtest_diagnosis,
    backtest_drawdown_area_html,
    backtest_equity_curve_html,
    backtest_return_drawdown_chart_html,
    build_candidate_actions_render_contract,
    candidate_action_rows,
    candidate_brief_cards,
    candidate_detail_panel_html,
    candidate_action_cards_html,
    candidate_market_snapshot,
    candidate_risk_reward_chart_html,
    download_href,
    extract_health_metrics,
    file_href,
    fmt_progress_pct,
    load_candidate_artifact_status,
    load_daily_research_runtime_status,
    load_evolution_status,
    load_csv_rows,
    load_grid_backtest_status,
    load_prefilter_artifact_status,
    load_research_batch_status,
    load_research_topology,
    load_update_status,
    markdown_to_html_basic,
    preferred_backtest_table_html,
    read_text,
    resolve_automation_status,
    status_by_score,
    stock_primary_combined_name,
    stock_primary_disabled_reason,
    build_primary_result_card_view_model,
    render_primary_result_card_template,
    stock_primary_history_record,
    stock_primary_history_source,
    stock_primary_history_view_model,
    stock_primary_invalid_explanation,
    summarize_backtest_scope,
    top_candidate_brief,
    translate_generation_mode,
    translate_guardrail_mode,
    translate_strategy_mode,
    translate_strategy_strictness,
    translate_weak_market_action,
    update_alerts_html,
    update_timeline_html,
)


def test_extract_health_metrics_and_status():
    text = "- score: 92.50/100\n- success_rate: 88.00%\n- failure_penalty: 5.0\n- category_penalty: 2.0\n"
    metrics = extract_health_metrics(text)
    assert metrics["score"] == "92.50/100"
    assert metrics["success_rate"] == "88.00%"
    assert status_by_score(metrics["score"]) == ("稳健", "tag-good")


def test_markdown_to_html_basic_renders_sections():
    html_text = markdown_to_html_basic("# Title\n\n- item\n\n| a | b |\n")
    assert "<h2>Title</h2>" in html_text
    assert "<li>item</li>" in html_text
    assert "<pre>| a | b |</pre>" in html_text


def test_dashboard_value_translations():
    assert translate_generation_mode("interim") == "临时结果"
    assert translate_generation_mode("final") == "正式结果"
    assert translate_strategy_mode("diversified") == "分散篮子"
    assert translate_strategy_mode("top1") == "只取第一名"
    assert translate_strategy_strictness("loose") == "宽松"
    assert translate_strategy_strictness("tight") == "严格"
    assert translate_weak_market_action("normal") == "正常执行"
    assert translate_weak_market_action("top1_only") == "只取第一名"
    assert translate_guardrail_mode("defensive") == "防守"


def test_file_href_and_progress_format(tmp_path):
    root = tmp_path
    target = root / "data" / "x.txt"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text("x", encoding="utf-8")
    assert file_href(root, target) == "/file/data/x.txt"
    assert download_href(root, target) == "/download/data/x.txt"
    assert fmt_progress_pct("12.34") == "12.3%"
    assert fmt_progress_pct("-") == "-"


def test_read_text_uses_cache_and_invalidates_on_update(tmp_path, monkeypatch):
    target = tmp_path / "daily.md"
    target.write_text("alpha", encoding="utf-8")

    original = Path.read_text
    calls = {"count": 0}

    def counting_read_text(self, *args, **kwargs):
        if self == target:
            calls["count"] += 1
        return original(self, *args, **kwargs)

    monkeypatch.setattr(Path, "read_text", counting_read_text)

    assert read_text(target) == "alpha"
    assert read_text(target) == "alpha"
    assert calls["count"] == 1

    target.write_text("beta", encoding="utf-8")
    assert read_text(target) == "beta"
    assert calls["count"] == 2


def test_load_csv_rows_uses_cache_and_invalidates_on_update(tmp_path, monkeypatch):
    target = tmp_path / "candidates.csv"
    target.write_text("ts_code,score\n000001.SZ,1\n", encoding="utf-8")

    original = Path.open
    calls = {"count": 0}

    def counting_open(self, *args, **kwargs):
        mode = kwargs.get("mode")
        if mode is None and args:
            mode = args[0]
        if self == target and "r" in str(mode or "r"):
            calls["count"] += 1
        return original(self, *args, **kwargs)

    monkeypatch.setattr(Path, "open", counting_open)

    assert load_csv_rows(target, limit=5)[0]["ts_code"] == "000001.SZ"
    assert load_csv_rows(target, limit=5)[0]["ts_code"] == "000001.SZ"
    assert calls["count"] == 1

    target.write_text("ts_code,score\n000002.SZ,2\n", encoding="utf-8")
    assert load_csv_rows(target, limit=5)[0]["ts_code"] == "000002.SZ"
    assert calls["count"] == 2


def test_backtest_diagnosis_and_candidate_actions(tmp_path):
    leaderboard = tmp_path / "leaderboard.csv"
    leaderboard.write_text(
        "total_return,sharpe_ratio,max_drawdown,win_rate,total_trades\n0.15,1.3,-0.05,0.6,25\n",
        encoding="utf-8",
    )
    diag = backtest_diagnosis(leaderboard)
    assert diag["结论"] == "样本不足，需降级"
    assert diag["总收益"] == "15.00%"
    assert diag["Calmar"] == "0.000"

    candidates = tmp_path / "candidates.csv"
    candidates.write_text(
        "ts_code,stock_name,industry,signal,risk_level,final_score,direction_prob_up,pred_return,confidence,position_pct,stop_loss,take_profit,reason\n"
        "000001.SZ,平安银行,银行,strong_buy,low,155,0.7,0.08,0.9,0.2,10.0,12.0,model_bullish factor_strong\n",
        encoding="utf-8",
    )
    rows = candidate_action_rows(candidates, top_n=1)
    assert rows[0]["建议动作"].startswith("优先关注")
    assert rows[0]["依据"] == "模型看多 因子强势"
    render_contract = build_candidate_actions_render_contract(
        candidates,
        card_top_n=1,
        table_top_n=1,
        card_hrefs=["/stock/?view=candidates&candidate=0"],
    )
    assert render_contract["card_rows"][0]["代码"] == "000001.SZ"
    assert render_contract["card_rows"][0]["href"] == "/stock/?view=candidates&candidate=0"
    assert render_contract["table_id"] == "candidate-actions-table"
    cards_html = candidate_action_cards_html(candidates, top_n=1)
    assert "action-card" in cards_html
    assert "优先关注" in cards_html

    detail_html = candidate_detail_panel_html(candidates)
    assert "000001.SZ" in detail_html
    assert "模型看多 factor_strong" not in detail_html
    assert "模型看多(91%)" not in detail_html
    assert "研究判断" in detail_html
    assert "执行提示" in detail_html
    assert "风险收益比" in detail_html
    assert "分数拆解" in detail_html
    assert "风险拆解" in detail_html
    assert "入选原因" in detail_html

    cards = candidate_brief_cards(candidates, top_n=1)
    assert cards[0]["ts_code"] == "000001.SZ"

    snapshot = candidate_market_snapshot(candidates, top_n=1)
    assert snapshot["dominant_regime"] == "未识别"
    assert snapshot["candidate_count"] == "1"

    top_brief = top_candidate_brief(candidates, index=0)
    assert top_brief["stock_name"] == "平安银行"

    scatter_html = candidate_risk_reward_chart_html(candidates)
    assert "候选股风险收益散点" in scatter_html
    assert "<circle" in scatter_html

    backtest_map_html = backtest_return_drawdown_chart_html(leaderboard)
    assert "回测收益 / 回撤分布" in backtest_map_html
    assert "<circle" in backtest_map_html

    equity_html = backtest_equity_curve_html(leaderboard)
    assert "研究净值曲线" in equity_html
    assert "<polyline" in equity_html

    drawdown_html = backtest_drawdown_area_html(leaderboard)
    assert "历史回撤带" in drawdown_html
    assert "<polygon" in drawdown_html

    scope = summarize_backtest_scope(leaderboard)
    assert "单票样本" in scope["label"]
    assert "需降级" in scope["label"]

    leaderboard_multi = tmp_path / "leaderboard_multi.csv"
    leaderboard_multi.write_text(
        "run_id,source_type,stock_pool,total_return,sharpe_ratio,max_drawdown,win_rate,total_trades\n"
        "r1,manual,000001.SZ,0.01,0.1,-0.01,0.5,10\n"
        "r2,official_research,000001.SZ|600036.SH,0.03,0.6,-0.02,0.6,18\n",
        encoding="utf-8",
    )
    scope_multi = summarize_backtest_scope(leaderboard_multi)
    assert "正式研究样本" in scope_multi["label"]
    assert "需降级" in scope_multi["label"]
    preferred_html = preferred_backtest_table_html(leaderboard_multi, max_rows=5)
    assert "000001.SZ|600036.SH" in preferred_html

    timeline_html = update_timeline_html([
        {"status": "completed", "update_type": "daily_trading_update", "created_at": "2026-03-18 17:30:20"},
        {"status": "partial_success", "update_type": "daily_trading_update", "created_at": "2026-03-18 07:46:12"},
    ])
    assert "最近任务时间线" not in timeline_html
    assert "完成" in timeline_html
    assert "partial_success" not in timeline_html

    alerts_html = update_alerts_html(
        {"status": "completed", "post_candidates": "成功", "post_daily_research": "失败"},
        [
            {"status": "partial_success", "update_type": "daily_trading_update", "created_at": "2026-03-18 07:46:12"},
            {"status": "partial_success", "update_type": "daily_trading_update", "created_at": "2026-03-18 07:41:11"},
        ],
    )
    assert "最近 2 次更新出现待补齐状态" in alerts_html
    assert "每日研究失败" in alerts_html


def test_backtest_scope_and_diagnosis_reach_research_gate(tmp_path):
    leaderboard = tmp_path / "leaderboard_research.csv"
    header = (
        "run_id,source_type,stock_pool,total_return,sharpe_ratio,max_drawdown,win_rate,total_trades\n"
    )
    rows = []
    for idx in range(180):
        rows.append(
            f"o{idx},official_research,000001.SZ|600036.SH,0.05,1.0,-0.04,0.55,24\n"
        )
    for idx in range(120):
        rows.append(
            f"s{idx},official_research,000001.SZ,0.04,0.9,-0.03,0.56,22\n"
        )
    leaderboard.write_text(header + "".join(rows), encoding="utf-8")

    scope = summarize_backtest_scope(leaderboard)
    assert "够研究" in scope["label"]
    assert "正式研究样本 300 条" in scope["label"]
    assert "有效交易样本 300 条" in scope["label"]

    diag = backtest_diagnosis(leaderboard)
    assert diag["结论"] == "可继续观察"


def test_load_candidate_artifact_status_reads_current_pointer_and_latest_attempt(tmp_path):
    exp_dir = tmp_path / "data" / "experiments"
    basket_dir = tmp_path / "artifacts" / "primary_result_candidate_baskets"
    exp_dir.mkdir(parents=True, exist_ok=True)
    basket_dir.mkdir(parents=True, exist_ok=True)
    (exp_dir / "candidates_top_latest.csv").write_text(
        "ts_code,stock_name\n600522.SH,中天科技\n",
        encoding="utf-8",
    )
    (basket_dir / "current.json").write_text(
        '{"status":"approved","basket_id":"basket-approved-001","updated_at":"2026-04-19T14:33:06+00:00"}',
        encoding="utf-8",
    )
    (basket_dir / "latest_attempt.json").write_text(
        '{"status":"blocked","basket_id":"basket-blocked-002","blocking_reasons":["top industry weight must stay within policy"]}',
        encoding="utf-8",
    )

    status = load_candidate_artifact_status(exp_dir)

    assert status["current_basket_pointer_status"] == "approved"
    assert status["current_basket_pointer_basket_id"] == "basket-approved-001"
    assert status["latest_basket_attempt_status"] == "blocked"
    assert status["latest_basket_attempt_basket_id"] == "basket-blocked-002"
    assert status["latest_basket_attempt_blocking_reason"] == "top industry weight must stay within policy"


def test_backtest_scope_and_diagnosis_reach_launch_gate(tmp_path):
    leaderboard = tmp_path / "leaderboard_launch.csv"
    header = (
        "run_id,source_type,stock_pool,total_return,sharpe_ratio,max_drawdown,win_rate,total_trades\n"
    )
    rows = []
    for idx in range(500):
        rows.append(
            f"m{idx},official_research,000001.SZ|600036.SH,0.08,1.1,-0.04,0.58,28\n"
        )
    for idx in range(500):
        rows.append(
            f"n{idx},official_research,000001.SZ|600036.SH|600519.SH,0.07,1.0,-0.05,0.57,26\n"
        )
    for idx in range(100):
        rows.append(
            f"q{idx},official_research,000001.SZ,0.09,0.95,-0.03,0.6,24\n"
        )
    leaderboard.write_text(header + "".join(rows), encoding="utf-8")

    scope = summarize_backtest_scope(leaderboard)
    assert "可上线" in scope["label"]
    assert "正式研究样本 1100 条" in scope["label"]
    assert "多股票池样本 1000 条" in scope["label"]
    assert "单票样本 100 条" in scope["label"]

    diag = backtest_diagnosis(leaderboard)
    assert diag["结论"] == "可上线观察"


def test_load_evolution_status_reads_champion_models(tmp_path):
    exp_dir = tmp_path
    (exp_dir / "evolution_registry_latest.json").write_text(
        """
        {
          "champion_version": "evo_20260401_173000",
          "champion_summary": {
            "walk_forward_score": 0.2345,
            "trade_objective_stability": 0.6789
          },
          "champion_payload": {
            "model_evolution": {
              "selected_models": ["lightgbm", "xgboost"]
            }
          },
          "history": [
            {
              "version": "evo_20260401_173000",
              "action": "promote",
              "reason": "walk-forward improved",
              "created_at": "2026-04-01T17:30:00",
              "gates": {
                "execution_feedback": {
                  "feedback_level": "reinforce",
                  "window_label": "10D",
                  "summary_note": "recent basket observation is strong enough to reinforce the current selection profile",
                  "change_total": 0,
                  "passed": true
                },
                "capacity_pressure": {
                  "capacity_state": "scalable",
                  "recommended_scale_profile": "normal",
                  "worst_stress_score": 12.0,
                  "passed": true
                }
              }
            }
          ]
        }
        """,
        encoding="utf-8",
    )
    status = load_evolution_status(exp_dir)
    assert status["champion_version"] == "evo_20260401_173000"
    assert status["champion_models"] == "lightgbm / xgboost"
    assert status["latest_action"] == "promote"
    assert status["latest_feedback_level"] == "reinforce"
    assert status["latest_feedback_gate_status"] == "通过"
    assert status["latest_capacity_gate_status"] == "通过"
    assert status["latest_capacity_state"] == "scalable"


def test_candidate_market_snapshot_detects_regime_and_bias(tmp_path):
    candidates = tmp_path / "candidates.csv"
    candidates.write_text(
        "ts_code,stock_name,industry,signal,risk_level,final_score,direction_prob_up,pred_return,confidence,position_pct,stop_loss,take_profit,reason,regime,style_volatility,style_relative_strength,basket_risk_pressure_score,basket_guardrail_mode,basket_guardrail_reason,basket_risk_flag\n"
        "300750.SZ,宁德时代,电气设备,strong_buy,medium,180,0.58,0.02,0.8,0.10,380,438,factor_strong,trend,0.03,0.04,58,defensive,negative_validation_excess,ok\n"
        "000988.SZ,华工科技,专用机械,strong_buy,medium,156,0.52,0.01,0.7,0.09,38,43,env_favorable,trend,0.028,0.032,56,defensive,negative_validation_excess,high_vol_exposure\n",
        encoding="utf-8",
    )
    snapshot = candidate_market_snapshot(candidates, top_n=2)
    assert snapshot["dominant_regime"] == "趋势市"
    assert snapshot["risk_preference"] == "防守优先"
    assert snapshot["style_bias"] == "高弹性趋势"
    assert snapshot["guardrail_mode"] == "防守"
    assert snapshot["risk_flag_count"] == "1"


def test_load_research_topology(tmp_path):
    project_root = tmp_path / "server_app"
    config_dir = project_root / "config"
    deploy_dir = project_root / "deploy" / "aliyun"
    config_dir.mkdir(parents=True, exist_ok=True)
    deploy_dir.mkdir(parents=True, exist_ok=True)

    (config_dir / "settings.yaml").write_text(
        """
data:
  research_pool:
    enabled: true
    size: 50
    min_total_mv_yi: 100
    max_total_mv_yi: 15000
""".strip(),
        encoding="utf-8",
    )
    (deploy_dir / "stock-ultimate-update.service").write_text(
        "ExecStart=python run_update_database.py --post-universe-size 120 --post-top-n 20\n",
        encoding="utf-8",
    )
    (deploy_dir / "stock-ultimate-nightly-research.service").write_text(
        "ExecStart=python run_research_batch.py --candidate-universe-size 800 --research-pool-size 50\n",
        encoding="utf-8",
    )
    (deploy_dir / "stock-ultimate-weekly-long.service").write_text(
        "ExecStart=python run_grid_backtest.py --research-pool-size 150\n",
        encoding="utf-8",
    )

    topology = load_research_topology(project_root)
    assert topology["candidate_scan_scope"] == "120只"
    assert topology["candidate_top_n"] == "20"
    assert topology["formal_research_pool_rule"] == "100亿-15000亿市值"
    assert topology["formal_research_pool_size"] == "50"
    assert topology["nightly_universe_size"] == "800"
    assert topology["weekly_long_pool_size"] == "150"


def test_load_research_batch_status_prefers_explicit_pool_size(tmp_path):
    exp_dir = tmp_path / "data" / "experiments"
    exp_dir.mkdir(parents=True, exist_ok=True)
    (exp_dir / "research_batch_latest.json").write_text(
        """
{
  "status": "completed",
  "ended_at": "2026-03-20T20:00:00",
  "candidate_universe_size": 800,
  "candidate_top_n": 20,
  "daily_profiles": ["short", "medium"],
  "backtest_profile": "medium",
  "search_mode": "focused",
  "experiment": "direction_a_medium",
  "research_pool_size": 50,
  "research_pool_meta": {
    "liquidity_min_turnover": 1000000,
    "effective_liquidity_min_turnover": 650000,
    "liquidity_filtered_out": 18
  },
  "stock_pool": ["000001.SZ"],
  "steps": {}
}
""".strip(),
        encoding="utf-8",
    )
    status = load_research_batch_status(exp_dir)
    assert status["stock_pool_size"] == "50"
    assert status["search_mode"] == "focused"
    assert status["experiment"] == "direction_a_medium"
    assert status["liquidity_min_turnover"] == "650000"
    assert status["liquidity_filtered_out"] == "18"


def test_load_daily_research_runtime_status(tmp_path):
    exp_dir = tmp_path / "data" / "experiments"
    exp_dir.mkdir(parents=True, exist_ok=True)
    (exp_dir / "daily_research_status_latest.json").write_text(
        """
{
  "state": "running",
  "stage": "replaying_failed_profiles",
  "started_at": "2026-03-22T07:05:22",
  "ended_at": null,
  "completed_profiles": ["short"],
  "failed_profiles": ["medium"],
  "alert_count": 2,
  "health_score": {"score": 88.5},
  "active_profile": "medium",
  "active_progress": {"phase": "grid_run_done", "executed_runs": 3, "planned_runs": 24},
  "search_mode": "focused",
  "experiment": "direction_c_medium",
  "research_pool_meta": {"liquidity_filtered_out": 12}
}
""".strip(),
        encoding="utf-8",
    )
    status = load_daily_research_runtime_status(exp_dir)
    assert status["state"] == "running"
    assert status["stage"] == "replaying_failed_profiles"
    assert status["completed_profiles"] == "short"
    assert status["failed_profiles"] == "medium"
    assert status["alert_count"] == "2"
    assert status["health_score"] == "88.50"
    assert status["active_profile"] == "medium"
    assert status["active_progress"] == "grid_run_done 3/24"
    assert status["search_mode"] == "focused"
    assert status["experiment"] == "direction_c_medium"
    assert status["liquidity_filtered_out"] == "12"


def test_primary_stage_display_l3_l4_not_overridden_by_status_fields():
    l3_name = stock_primary_combined_name(
        {
            "result_lifecycle_stage": "L3",
            "audit_status": "in_review",
            "execution_status": None,
        }
    )
    l4_name = stock_primary_combined_name(
        {
            "result_lifecycle_stage": "L4",
            "audit_status": "passed",
            "execution_status": "running",
        }
    )
    assert l3_name.startswith("审核阶段结果")
    assert "审核中" in l3_name
    assert l3_name != "审核中"
    assert l4_name.startswith("执行阶段结果")
    assert "运行中" in l4_name
    assert l4_name != "执行中"


def test_load_evolution_status(tmp_path):
    exp_dir = tmp_path / "data" / "experiments"
    exp_dir.mkdir(parents=True, exist_ok=True)
    (exp_dir / "evolution_registry_latest.json").write_text(
        """
{
  "champion_version": "evo_20260401_223000",
  "champion_summary": {
    "walk_forward_score": 0.2345,
    "trade_objective_stability": 0.7123
  },
  "history": [
    {
      "version": "evo_20260401_221500",
      "created_at": "2026-04-01T22:15:00",
      "action": "observe",
      "reason": "候选版本未达到晋级阈值，先保留观察。",
      "gates": {
        "execution_feedback": {
          "feedback_level": "review",
          "window_label": "5D",
          "summary_note": "recent basket observation requires review before trusting the current candidate profile",
          "change_total": 2,
          "review_only": true,
          "passed": true
        }
      }
    },
    {
      "version": "evo_20260401_223000",
      "created_at": "2026-04-01T22:30:00",
      "action": "promote",
      "reason": "walk-forward 分数与稳定性均优于当前冠军。",
      "gates": {
        "execution_feedback": {
          "feedback_level": "tighten",
          "window_label": "5D",
          "summary_note": "recent basket observation requires tighter selection and basket risk controls",
          "change_total": 3,
          "passed": false
        },
        "capacity_pressure": {
          "capacity_state": "stretched",
          "recommended_scale_profile": "top1_only",
          "worst_stress_score": 58.0,
          "passed": false
        }
      }
    }
  ]
}
""".strip(),
        encoding="utf-8",
    )
    status = load_evolution_status(exp_dir)
    assert status["champion_version"] == "evo_20260401_223000"
    assert status["champion_walk_forward_score"] == "0.2345"
    assert status["champion_stability"] == "0.7123"
    assert status["latest_action"] == "promote"
    assert status["latest_feedback_level"] == "tighten"
    assert status["latest_feedback_gate_status"] == "收紧阻断"
    assert status["latest_feedback_change_total"] == "3"
    assert status["latest_capacity_gate_status"] == "容量阻断"
    assert status["latest_capacity_state"] == "stretched"
    assert status["latest_capacity_profile"] == "top1_only"
    assert len(status["history"]) == 2
    assert status["history"][0]["feedback_gate_status"] == "收紧阻断"
    assert status["history"][0]["capacity_gate_status"] == "容量阻断"


def test_load_update_status_upgrades_partial_success_when_post_steps_succeed(tmp_path):
    exp_dir = tmp_path / "data" / "experiments"
    exp_dir.mkdir(parents=True, exist_ok=True)
    (exp_dir / "update_status_latest.json").write_text(
        """
{
  "status": "partial_success",
  "stage": "done",
  "started_at": "2026-03-22T05:00:00",
  "ended_at": "2026-03-22T05:45:35+08:00",
  "update_summary": {
    "db_latest_after": "20260320",
    "written_rows": 0,
    "progress_pct": 0
  },
  "post_candidates": {"ok": true},
  "post_daily_research": {"ok": true}
}
""".strip(),
        encoding="utf-8",
    )
    status = load_update_status(exp_dir)
    assert status["status"] == "completed"
    assert status["post_candidates"] == "成功"
    assert status["post_daily_research"] == "成功"


def test_load_update_status_prefers_manual_rerun_timestamps(tmp_path):
    exp_dir = tmp_path / "data" / "experiments"
    exp_dir.mkdir(parents=True, exist_ok=True)
    (exp_dir / "update_status_latest.json").write_text(
        """
{
  "status": "completed",
  "stage": "done",
  "started_at": "2026-04-02T07:00:00",
  "ended_at": "2026-04-02T07:10:00",
  "update_summary": {
    "db_latest_after": "20260401",
    "written_rows": 0,
    "progress_pct": 100
  },
  "manual_runs": {
    "candidates": {
      "ok": true,
      "ended_at": "2026-04-02T09:57:55",
      "meta": {
        "effective_universe_size": 300
      }
    },
    "evolution": {
      "ok": true,
      "ended_at": "2026-04-02T09:40:00",
      "meta": {
        "action": "promote",
        "champion_version": "evo_20260402_073726"
      }
    }
  }
}
""".strip(),
        encoding="utf-8",
    )
    status = load_update_status(exp_dir)
    assert status["last_run"] == "2026-04-02T09:57:55"
    assert status["manual_candidates_last_run"] == "2026-04-02T09:57:55"
    assert status["manual_evolution_last_run"] == "2026-04-02T09:40:00"
    assert status["manual_evolution_action"] == "promote"
    assert status["manual_evolution_version"] == "evo_20260402_073726"


def test_load_candidate_artifact_status(tmp_path):
    exp_dir = tmp_path / "data" / "experiments"
    exp_dir.mkdir(parents=True, exist_ok=True)
    latest_attempt = tmp_path / "artifacts" / "primary_result_candidate_baskets" / "latest_attempt.json"
    (exp_dir / "candidates_top_latest.csv").write_text(
        "rank,ts_code,stock_name,final_score\n1,000001.SZ,平安银行,123.4\n2,600036.SH,招商银行,120.0\n",
        encoding="utf-8",
    )
    (exp_dir / "candidates_basket_summary_latest.json").write_text(
        '{"generation_degraded": true, "generation_reason": "interim_partial_generation", "strategy_mode": "top1", "strategy_strictness": "tight", "strategy_weak_market_action": "top1_only"}',
        encoding="utf-8",
    )
    latest_attempt.parent.mkdir(parents=True, exist_ok=True)
    latest_attempt.write_text(
        '{"status": "blocked", "basket_id": "basket-20260421", "blocking_reasons": ["top industry weight must stay within policy"]}',
        encoding="utf-8",
    )
    status = load_candidate_artifact_status(exp_dir)
    assert status["rows"] == "2"
    assert status["top1"] == "000001.SZ"
    assert status["generated_at"] != "-"
    assert status["basket_generated_at"] != "-"
    assert status["generation_mode"] == "interim"
    assert status["generation_reason"] == "interim_partial_generation"
    assert status["strategy_mode"] == "top1"
    assert status["strategy_strictness"] == "tight"
    assert status["strategy_weak_market_action"] == "top1_only"
    assert status["latest_basket_attempt_generated_at"] != "-"
    assert status["latest_basket_attempt_status"] == "blocked"
    assert status["latest_basket_attempt_basket_id"] == "basket-20260421"
    assert status["latest_basket_attempt_blocking_reason"] == "top industry weight must stay within policy"


def test_load_candidate_artifact_status_reads_runtime_stage(tmp_path):
    exp_dir = tmp_path / "data" / "experiments"
    exp_dir.mkdir(parents=True, exist_ok=True)
    (exp_dir / "candidates_top_latest.csv").write_text(
        "rank,ts_code,stock_name,final_score\n1,000001.SZ,平安银行,123.4\n",
        encoding="utf-8",
    )
    (exp_dir / "candidates_run_status_latest.json").write_text(
        json.dumps(
            {
                "status": "running",
                "stage": "validation_running",
                "stage_label": "历史验证运行中",
                "detail": "开始执行 validation 和 guardrail 回放",
                "updated_at": "2026-04-23T03:30:00+08:00",
                "elapsed_sec": 91.5,
                "results_ready": 45,
                "skipped_count": 1,
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    status = load_candidate_artifact_status(exp_dir)

    assert status["runtime_status"] == "running"
    assert status["runtime_stage"] == "validation_running"
    assert status["runtime_stage_label"] == "历史验证运行中"
    assert status["runtime_detail"] == "开始执行 validation 和 guardrail 回放"
    assert status["runtime_updated_at"] == "2026-04-23T03:30:00+08:00"
    assert status["runtime_elapsed_sec"] == "91.5"
    assert status["runtime_results_ready"] == "45"
    assert status["runtime_skipped_count"] == "1"


def test_load_prefilter_artifact_status(tmp_path):
    exp_dir = tmp_path / "data" / "experiments"
    exp_dir.mkdir(parents=True, exist_ok=True)
    (exp_dir / "candidate_prefilter_universe_latest.json").write_text(
        """
{
  "generated_at": "2026-03-27 10:00:00",
  "trade_date": "20260327",
  "row_count": 3200,
  "market_symbol_count": 4100,
  "excluded_count": 900,
  "configured_liquidity_min_turnover": 10000000,
  "effective_liquidity_min_turnover": 850000,
  "top_candidates": [
    {"ts_code": "300750.SZ", "prefilter_score": 0.8123, "prefilter_reason": "流动性质量高，趋势结构较强"},
    {"ts_code": "600519.SH", "prefilter_score": 0.8011, "prefilter_reason": "流动性稳定，趋势结构健康"}
  ],
  "exclusion_summary": [
    {"reason": "最近流动性不足", "count": 640},
    {"reason": "预筛排序未进入当前计算预算", "count": 180}
  ],
  "top_exclusions": [
    {"ts_code": "000001.SZ", "exclusion_reason": "rank_below_cutoff", "exclusion_reason_zh": "预筛排序未进入当前计算预算"}
  ]
}
        """.strip(),
        encoding="utf-8",
    )
    status = load_prefilter_artifact_status(exp_dir)
    assert status["trade_date"] == "20260327"
    assert status["row_count"] == "3200"
    assert status["market_symbol_count"] == "4100"
    assert status["excluded_count"] == "900"
    assert status["pass_rate_pct"] == "78.0%"
    assert status["excluded_rate_pct"] == "22.0%"
    assert status["configured_liquidity_min_turnover"] == "10000000"
    assert status["effective_liquidity_min_turnover"] == "850000"
    assert status["top1"] == "300750.SZ"
    assert status["top1_reason"] == "流动性质量高，趋势结构较强"
    assert status["top10_count"] == "2"
    assert status["top_exclusion_reason"] == "最近流动性不足"
    assert status["top_exclusion_reason_count"] == "640"
    assert status["exclusion_summary"][0]["reason"] == "最近流动性不足"
    assert status["exclusion_summary"][0]["share_pct"] == "71.1%"


def test_load_prefilter_artifact_status_marks_stale_against_update_status(tmp_path):
    exp_dir = tmp_path / "data" / "experiments"
    exp_dir.mkdir(parents=True, exist_ok=True)
    (exp_dir / "candidate_prefilter_universe_latest.json").write_text(
        """
{
  "generated_at": "2026-04-17 17:30:54",
  "trade_date": "20260417",
  "row_count": 5344,
  "market_symbol_count": 5377,
  "excluded_count": 33,
  "top_candidates": [
    {"ts_code": "300548.SZ", "prefilter_score": 0.88, "prefilter_reason": "流动性质量高"}
  ],
  "top_exclusions": []
}
        """.strip(),
        encoding="utf-8",
    )

    status = load_prefilter_artifact_status(
        exp_dir,
        {
            "status": "partial_success",
            "db_latest": "20260420",
            "post_candidates": "失败",
        },
    )

    assert status["trade_date"] == "20260417"
    assert status["expected_trade_date"] == "20260420"
    assert status["freshness_status"] == "stale"
    assert "不得按今日结果使用" in status["freshness_note"]


def test_resolve_automation_status_prefers_completed_daily_research():
    update_status = {
        "status": "running",
        "stage": "running_daily_research",
        "last_run": "2026-03-21T22:24:01",
        "duration": "-",
        "db_latest": "2026-03-21",
        "written_rows": "1000",
        "progress_pct": "100",
        "post_candidates": "成功",
        "post_daily_research": "-",
    }
    daily_status = {
        "state": "completed",
        "stage": "done",
        "ended_at": "2026-03-22T14:38:41",
        "duration": "3470.0秒",
    }

    resolved = resolve_automation_status(update_status, daily_status)
    assert resolved["status"] == "completed"
    assert resolved["stage"] == "done"
    assert resolved["last_run"] == "2026-03-22T14:38:41"
    assert resolved["duration"] == "3470.0秒"
    assert resolved["post_daily_research"] == "成功"
    assert "修正展示" in resolved["status_note"]


def test_update_alerts_html_uses_effective_status():
    alerts_html = update_alerts_html(
        {
            "status": "running",
            "post_candidates": "成功",
            "post_daily_research": "-",
        },
        [],
        {
            "state": "completed",
            "stage": "done",
            "ended_at": "2026-03-22T14:38:41",
            "duration": "3470.0秒",
        },
    )
    assert "当前更新状态异常" not in alerts_html
    assert "修正展示" in alerts_html


def test_resolve_automation_status_upgrades_partial_success_when_decoupled_daily_research_completed():
    update_status = {
        "status": "partial_success",
        "stage": "done",
        "last_run": "2026-03-22T05:45:35+08:00",
        "duration": "-",
        "db_latest": "20260320",
        "written_rows": "0",
        "progress_pct": "0",
        "post_candidates": "成功",
        "post_daily_research": "成功",
    }
    daily_status = {
        "state": "completed",
        "stage": "done",
        "ended_at": "2026-03-22T14:38:41",
        "duration": "3462.5秒",
        "completed_profiles": "short, medium",
        "failed_profiles": "-",
    }

    resolved = resolve_automation_status(update_status, daily_status)
    assert resolved["status"] == "completed"
    assert resolved["stage"] == "done"
    assert resolved["last_run"] == "2026-03-22T14:38:41"
    assert resolved["duration"] == "3462.5秒"
    assert "待补齐" in resolved["status_note"]


def test_load_grid_backtest_status_reads_governance_payload(tmp_path):
    exp_dir = tmp_path / "data" / "experiments"
    grid_dir = exp_dir / "grid_search"
    grid_dir.mkdir(parents=True, exist_ok=True)
    (grid_dir / "grid_backtest_latest.csv").write_text("run_id\nr1\nr2\n", encoding="utf-8")
    (grid_dir / "grid_backtest_governance_latest.json").write_text(
        '{"validation_window":{"start_date":"2025-01-01","end_date":"2025-06-30"},'
        '"replay_runs":3,"regime_coverage_score":0.75,"parameter_sensitivity_score":0.24,'
        '"observed_regimes":["trend","range"],"sampling_mode":"stratified"}',
        encoding="utf-8",
    )

    status = load_grid_backtest_status(exp_dir)

    assert status["rows"] == "2"
    assert status["validation_window"] == "2025-01-01 ~ 2025-06-30"
    assert status["replay_runs"] == "3"
    assert status["regime_coverage_score"] == "0.7500"
    assert status["parameter_sensitivity_score"] == "0.2400"
    assert status["observed_regimes"] == "trend, range"
    assert status["sampling_mode"] == "stratified"


def test_primary_invalid_explanation_only_follows_terminal_outcome():
    fact = {
        "result_lifecycle_stage": "L2",
        "terminal_outcome": "expired",
        "invalid_reason": None,
    }
    assert stock_primary_invalid_explanation(fact) == "该结果已过有效窗口，当前视为失效。"
    assert stock_primary_combined_name(
        {
            "result_lifecycle_stage": "L2",
            "candidate_status": "candidate",
            "terminal_outcome": "expired",
        }
    ).startswith("候选结果")


def test_disabled_reason_prefers_backend_explanation_fact():
    fact = {
        "disabled_reason": "后端解释：当前进入人工冻结窗口。",
        "audit_status": "failed",
        "risk_level": "critical",
        "terminal_outcome": "rejected",
    }
    assert stock_primary_disabled_reason(fact) == "后端解释：当前进入人工冻结窗口。"


def test_history_summary_remains_reference_not_current_pass_conclusion():
    fact = {
        "result_lifecycle_stage": "L3",
        "history_summary": "历史审核记录：in_review。当前主阶段仍以 审核阶段结果 为准。",
        "audit_status": "in_review",
    }
    record = stock_primary_history_record(fact)
    assert record is not None
    assert "历史审核记录" in record or "审核记录" in record
    vm = stock_primary_history_view_model(fact)
    assert vm["slot_c"] == "仅供参考，当前主阶段仍以制度主字段为准。"
    assert "当前已通过" not in (vm["slot_c"] or "")


def test_explanation_sections_degrade_safely_when_facts_missing():
    fact = {
        "result_lifecycle_stage": "L1",
        "history_summary": None,
        "disabled_reason": None,
        "invalid_reason": None,
        "terminal_outcome": None,
        "source_timestamps": {},
    }
    assert stock_primary_history_record(fact) is None
    assert stock_primary_disabled_reason(fact) is None
    assert stock_primary_invalid_explanation(fact) is None


def test_history_view_model_uses_fixed_three_slots():
    fact = {
        "result_lifecycle_stage": "L2",
        "history_summary": "研究记录 completed；候选记录 shortlisted",
        "history_source_file": "buylist_latest.json",
        "history_source_timestamp": "2026-04-12 09:11:19",
        "history_generation_mode": "degraded",
    }
    vm = stock_primary_history_view_model(fact)
    assert set(vm.keys()) == {"visible", "slot_a", "slot_b", "slot_c"}
    assert vm["slot_a"] is not None
    assert vm["slot_b"] is not None
    assert vm["slot_c"] is not None


def test_history_source_selection_is_stable():
    fact = {
        "candidate_status": "shortlisted",
        "research_status": "completed",
        "source_timestamps": {
            "daily_research_status_latest.json": "2026-04-12 09:10:00",
            "buylist_latest.json": "2026-04-12 09:11:19",
            "candidates_top_latest.csv": "2026-04-12 09:11:18",
        },
    }
    source_file, source_timestamp, generation_mode = stock_primary_history_source(fact)
    assert source_file == "buylist_latest.json"
    assert source_timestamp == "2026-04-12 09:11:19"
    assert generation_mode == "degraded"


def test_history_view_model_keeps_source_transparency_lightweight():
    fact = {
        "history_summary": "候选记录 shortlisted",
        "history_source_file": "buylist_latest.json",
        "history_source_timestamp": "2026-04-12 09:11:19",
        "history_generation_mode": "degraded",
    }
    vm = stock_primary_history_view_model(fact)
    assert vm["slot_b"] == "来源 buylist_latest.json · 同步 2026-04-12 09:11:19 · 降级生成"


def test_history_view_model_safe_degrade_rules():
    with_source_only = {
        "source_timestamps": {"candidates_top_latest.csv": "2026-04-12 09:11:19"},
    }
    vm = stock_primary_history_view_model(with_source_only)
    assert vm["visible"] is True
    assert vm["slot_a"] == "历史记录暂缺"
    assert vm["slot_b"] is not None

    no_source = {"source_timestamps": {}}
    vm_hidden = stock_primary_history_view_model(no_source)
    assert vm_hidden["visible"] is False
    assert vm_hidden["slot_a"] is None


def test_primary_result_card_view_model_structure():
    fact = {
        "ts_code": "000001.SZ",
        "stock_name": "平安银行",
        "result_lifecycle_stage": "L3",
        "result_type": "audit",
        "audit_status": "in_review",
        "risk_level": "medium",
        "data_sync_note": "制度字段已对齐。",
        "history_summary": "审核记录 in_review",
        "history_source_file": "governance_audit_latest.json",
        "history_source_timestamp": "2026-04-12 10:00:00",
        "history_generation_mode": "degraded",
    }
    vm = build_primary_result_card_view_model(fact)
    data = vm.as_dict()
    assert set(data.keys()) == {
        "ts_code",
        "stock_name",
        "stage_label",
        "stage_combined_label",
        "result_type_label",
        "risk_label",
        "sync_note",
        "source_timestamp",
        "history_visible",
        "history_slot_a",
        "history_slot_b",
        "history_slot_c",
        "disabled_visible",
        "disabled_text",
        "invalid_visible",
        "invalid_text",
    }
    assert data["stage_label"] == "审核阶段结果"
    assert data["stage_combined_label"] == "审核阶段结果（审核中）"


def test_primary_result_card_template_uses_fixed_slots():
    fact = {
        "ts_code": "000001.SZ",
        "stock_name": "平安银行",
        "result_lifecycle_stage": "L2",
        "result_type": "candidate",
        "candidate_status": "shortlisted",
        "risk_level": "low",
        "history_source_file": "buylist_latest.json",
        "history_source_timestamp": "2026-04-12 09:11:19",
        "history_generation_mode": "degraded",
    }
    html = render_primary_result_card_template(build_primary_result_card_view_model(fact))
    assert 'class="card primary-result-card"' in html
    assert 'id="primary-result-stage"' in html
    assert 'id="primary-result-history-slot-a"' in html
    assert 'id="primary-result-history-slot-b"' in html
    assert 'id="primary-result-history-slot-c"' in html
    assert 'id="primary-result-copy-summary"' in html
    assert 'id="primary-result-copy-json"' in html
    assert 'id="primary-result-history-toggle"' in html
    assert "来源 buylist_latest.json · 同步 2026-04-12 09:11:19 · 降级生成" in html
    assert 'id="primary-result-live-status"' in html
    assert 'aria-live="polite"' in html
    assert 'aria-atomic="true"' in html
    assert 'type="button"' in html
    assert 'aria-controls="primary-result-history-body"' in html
    assert 'aria-controls="primary-result-disabled-body"' in html
    assert 'aria-controls="primary-result-invalid-body"' in html
    assert 'aria-labelledby="primary-result-history-toggle"' in html
    assert 'aria-labelledby="primary-result-disabled-toggle"' in html
    assert 'aria-labelledby="primary-result-invalid-toggle"' in html
    assert 'id="primary-result-disabled-text"' in html
    assert 'id="primary-result-invalid-text"' in html


def test_primary_result_card_template_accessibility_ids_are_unique():
    fact = {
        "ts_code": "000001.SZ",
        "stock_name": "平安银行",
        "result_lifecycle_stage": "L2",
        "result_type": "candidate",
        "candidate_status": "shortlisted",
        "risk_level": "low",
    }
    html = render_primary_result_card_template(build_primary_result_card_view_model(fact))
    assert html.count('id="primary-result-history-toggle"') == 1
    assert html.count('id="primary-result-history-body"') == 1
    assert html.count('id="primary-result-disabled-toggle"') == 1
    assert html.count('id="primary-result-disabled-body"') == 1
    assert html.count('id="primary-result-invalid-toggle"') == 1
    assert html.count('id="primary-result-invalid-body"') == 1
    assert html.count('id="primary-result-live-status"') == 1


def test_primary_result_card_template_copy_buttons_have_accessible_labels():
    html = render_primary_result_card_template(
        build_primary_result_card_view_model(
            {
                "result_lifecycle_stage": "L1",
                "ts_code": "000001.SZ",
                "stock_name": "平安银行",
            }
        )
    )
    assert 'aria-label="复制制度事实摘要"' in html
    assert 'aria-label="复制当前已生效的事实 JSON"' in html


def test_primary_result_card_template_uses_unified_temp_copy():
    fact = {
        "result_lifecycle_stage": "L1",
        "ts_code": None,
        "stock_name": None,
        "risk_level": None,
        "source_timestamps": {},
        "data_sync_note": None,
    }
    html = render_primary_result_card_template(build_primary_result_card_view_model(fact))
    assert "对象信息暂缺" in html
    assert "名称信息暂缺" in html
    assert "风险信息暂缺" in html
    assert "同步信息暂缺。" in html
