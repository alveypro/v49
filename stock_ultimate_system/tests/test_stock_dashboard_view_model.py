from src.stock_dashboard_view_model import (
    build_stock_actions_view_model,
    build_stock_candidate_compare_view_model,
    build_stock_candidate_diagnostics_view_model,
    build_stock_candidate_focus_view_model,
    build_stock_charts_view_model,
    build_stock_guide_view_model,
    build_stock_home_view_model,
    build_stock_links_view_model,
    build_stock_overview_chrome_view_model,
    build_stock_overview_disclosure_view_model,
    build_stock_overview_kpi_view_model,
    build_stock_opportunities_view_model,
    build_stock_prefilter_view_model,
    build_stock_runtime_view_model,
    build_stock_selection_funnel_view_model,
    build_stock_summary_view_model,
    build_stock_top1_view_model,
    build_stock_validation_view_model,
)


def test_build_stock_runtime_view_model_marks_stale_observation_window():
    view_model = build_stock_runtime_view_model(
        effective_update_status={"status": "up_to_date", "stage": "completed", "db_latest": "2026-04-28"},
        prefilter_artifact_status={"freshness_status": "stale"},
        automation_health={"label": "稳健"},
        governance_recommended_action="hold_observation",
        cockpit_model={"promotion_decision": "blocked"},
        candidate_artifact_status={"generated_at": "2026-04-27T08:00:00", "basket_generated_at": "2026-04-27T09:00:00"},
        current_basket_pointer_status="approved",
        current_basket_pointer_updated_at="2026-04-28T10:00:00",
        current_basket_pointer_basket_id="basket-001",
        latest_basket_attempt_status="blocked",
        latest_basket_attempt_generated_at="2026-04-28T11:00:00",
        latest_basket_attempt_blocking_reason="gate blocked",
        observation_wait_status={"observation_window": {"start_date": "2026-04-25"}, "current_date": "2026-04-25"},
        daily_closure_latest={"generated_at": "2026-04-28T12:00:00"},
    )

    assert view_model["promotion_decision_label"] == "晋级锁定"
    assert view_model["observation_timeline_stale"] is True
    assert "旧观察窗口" in str(view_model["observation_timeline_label"])
    assert "候选链路未与最新交易日对齐" in str(view_model["timeline_consistency_note"])


def test_build_stock_runtime_view_model_softens_internal_failed_status_for_home_surface():
    view_model = build_stock_runtime_view_model(
        effective_update_status={"status": "partial_success", "stage": "completed", "db_latest": "2026-04-28"},
        prefilter_artifact_status={"freshness_status": "blocked"},
        automation_health={"label": "failed"},
        governance_recommended_action="hold_observation",
        cockpit_model={"promotion_decision": "blocked"},
        candidate_artifact_status={"generated_at": "2026-04-27T08:00:00", "basket_generated_at": "2026-04-27T09:00:00"},
        current_basket_pointer_status="approved",
        current_basket_pointer_updated_at="2026-04-28T10:00:00",
        current_basket_pointer_basket_id="basket-001",
        latest_basket_attempt_status="blocked",
        latest_basket_attempt_generated_at="2026-04-28T11:00:00",
        latest_basket_attempt_blocking_reason="gate blocked",
        observation_wait_status={"observation_window": {"start_date": "2026-04-25"}, "current_date": "2026-04-25"},
        daily_closure_latest={"generated_at": "2026-04-28T12:00:00"},
    )

    assert view_model["update_status_label"] == "待补齐"
    assert view_model["prefilter_freshness_label"] == "预筛待补齐"
    assert view_model["automation_health_label"] == "自动链路 待复核"
    assert "gate blocked" not in view_model["latest_basket_attempt_label"]
    assert "门禁待复核" in view_model["latest_basket_attempt_label"]


def test_build_stock_home_view_model_keeps_pointer_chain_as_primary_home_subject():
    view_model = build_stock_home_view_model(
        headline_tone="继续观察",
        headline_detail="主结果保持锁定",
        current_basket_pointer_label="当前生效篮子 已通过 · 2026-04-28T10:00:00 · basket-001",
        latest_basket_attempt_label="最新篮子尝试 已阻断 · 2026-04-28T11:00:00 · gate blocked",
        current_basket_pointer_status="approved",
        current_basket_pointer_basket_id="basket-001",
        latest_basket_attempt_status="blocked",
        latest_basket_attempt_blocking_reason="gate blocked",
        health_status="稳健",
        health_score="92",
        top_code="000001.SZ",
        candidate_name="平安银行",
        candidate_generated_at="2026-04-28T08:00:00",
        generation_mode_label="主链结果",
        update_status_label="已更新",
        update_stage_label="已完成",
        candidate_count=12,
        candidate_score="92.5",
        candidate_timeline_label="2026-04-28 08:00:00",
        run_freshness="fresh",
        db_latest_trade_date="2026-04-28",
        observation_timeline_label="2026-04-23",
        prefilter_freshness_label="预筛 fresh",
        backtest_scope_label="近 60 日",
        governance_cycle_state="observe_only",
        governance_recommended_action_label="继续观察",
        governance_ready_for_release=False,
        governance_fully_release_ready=False,
        result_id="primary:000001.SZ",
        stage_label="L4",
        result_subject="000001.SZ 平安银行",
        dominant_regime="震荡",
        risk_preference="中性",
        backtest_conclusion="继续观察",
        avg_risk_pressure="0.3",
        disabled_reason="",
        invalid_reason="",
        blocker_semantics={"has_blocker": True, "blocker_title": "等待观察", "blocker_detail": "样本不足"},
        cockpit_model={"decision_status": "观察", "primary_progress": "6/20", "primary_needed": "14"},
        promotion_decision_label="晋级锁定",
    )

    assert view_model["home_hero_facts"]["top_code"] == "000001.SZ"
    assert view_model["primary_result_home_facts"]["result_id"] == "primary:000001.SZ"
    assert view_model["primary_result_home_facts"]["blocker_title"] == "等待观察"
    assert view_model["external_decision_spine"]["primary_progress"] == "6/20"
    assert view_model["control_strip_cards"][2]["sub_lines"][1].startswith("当前生效篮子")
    assert view_model["validation_basket_kpis"][0]["value"] == "已对齐"
    assert view_model["validation_basket_kpis"][1]["value"] == "待复核"
    assert view_model["validation_basket_kpis"][1]["sub"] == "门禁待复核"


def test_build_stock_home_view_model_softens_zero_progress_as_sample_accumulating():
    view_model = build_stock_home_view_model(
        headline_tone="继续观察",
        headline_detail="主结果保持锁定",
        current_basket_pointer_label="",
        latest_basket_attempt_label="",
        current_basket_pointer_status="approved",
        current_basket_pointer_basket_id="basket-001",
        latest_basket_attempt_status="blocked",
        latest_basket_attempt_blocking_reason="gate blocked",
        health_status="告警",
        health_score="68",
        top_code="300750.SZ",
        candidate_name="",
        candidate_generated_at="2026-04-28T08:00:00",
        generation_mode_label="主链结果",
        update_status_label="待补齐",
        update_stage_label="已完成",
        candidate_count=1,
        candidate_score="",
        candidate_timeline_label="2026-04-28 08:00:00",
        run_freshness="fresh",
        db_latest_trade_date="2026-04-28",
        observation_timeline_label="2026-04-23",
        prefilter_freshness_label="预筛待补齐",
        backtest_scope_label="近 60 日",
        governance_cycle_state="observe_only",
        governance_recommended_action_label="继续观察",
        governance_ready_for_release=False,
        governance_fully_release_ready=False,
        result_id="primary:300750.SZ",
        stage_label="L4",
        result_subject="300750.SZ",
        dominant_regime="震荡",
        risk_preference="中性",
        backtest_conclusion="继续观察",
        avg_risk_pressure="0.3",
        disabled_reason="",
        invalid_reason="",
        blocker_semantics={},
        cockpit_model={"decision_status": "状态待复核", "primary_progress": "0/20", "primary_needed": "20", "basket_progress": "0/20", "basket_needed": "20"},
        promotion_decision_label="晋级锁定",
    )

    assert view_model["external_decision_spine"]["decision_status"] == "继续观察"
    assert view_model["external_decision_spine"]["decision_reason"] == "证据还不够"
    assert view_model["external_decision_spine"]["promotion_decision_label"] == "未开放行动"
    assert "首批 20 个正式样本仍在积累" == view_model["external_decision_spine"]["primary_needed"]


def test_build_stock_validation_view_model_keeps_basket_guard_and_runtime_facts():
    view_model = build_stock_validation_view_model(
        rebalance_dates=6,
        basket_validation_summary={},
        expected_basket_return=0.032,
        risk_pressure_score=0.4,
        liquidity_capacity_state="受限",
        weighted_liquidity_score=0.57,
        liquidity_capacity_weight=0.22,
        candidate_feedback_window="5D",
        candidate_feedback_level="review",
        candidate_feedback_summary="样本不足",
        candidate_feedback_changes=2,
        candidate_runtime_stage_label="L4",
        candidate_runtime_status_label="运行中",
        candidate_runtime_detail="等待补样本",
        candidate_runtime_results_ready="12",
        candidate_runtime_skipped="3",
        candidate_runtime_updated_label="2026-04-28 10:00:00",
        candidate_runtime_elapsed="180",
        guardrail_mode_label="防守",
        guardrail_reasons_label="liquidity pressure",
        generation_mode_label="主链结果",
        generation_reason="pointer verified",
        validation_basket_kpis=[{"label": "当前生效篮子", "value": "approved", "sub": "basket-001"}],
        skipped_count=5,
        best_variant_label="diversified",
        strategy_mode_label="均衡",
        strategy_strictness_label="收紧",
        strategy_weak_market_action_label="降级观察",
        diversified_avg_excess_return_5d_label="+1.2%",
        raw_avg_excess_return_5d_label="+0.8%",
        top1_avg_excess_return_5d_label="+0.5%",
        avg_basket_return_5d_label="+1.5%",
        avg_excess_return_5d_label="+0.9%",
        basket_win_rate_5d_label="66.0%",
        avg_top1_return_5d_label="+0.7%",
    )

    assert view_model["display_contract"]["title"] == "历史篮子验证"
    assert view_model["kpi_rows"][0][0]["value"] == "6"
    assert view_model["kpi_rows"][5][2]["value"] == "approved"
    assert view_model["kpi_rows"][7][0]["value"] == "+1.2%"
    assert view_model["kpi_rows"][4][0]["sub"] == "运行中 ｜ 等待补样本"


def test_build_stock_summary_view_model_preserves_execution_and_governance_chain():
    view_model = build_stock_summary_view_model(
        health_status="稳健",
        backtest_conclusion="继续观察",
        top_code="000001.SZ",
        candidate_name="平安银行",
        health_score="92",
        candidate_generated_at="2026-04-28 08:00:00",
        backtest_scope_label="近60日",
        basket_dual_track_rows=[{"tone": "pointer", "text": "当前生效篮子 approved"}],
        execution_semantics={
            "decision_action": "进入复核",
            "decision_action_reason": "等待样本闭合",
            "execution_eligibility": "否",
            "execution_eligibility_reason": "证据不足",
            "decision_validity_label": "T+1",
            "candidate_generated_at": "2026-04-28",
            "db_latest_trade_date": "2026-04-28",
        },
        blocker_semantics={"blocker_title": "等待观察", "blocker_detail": "样本不足"},
        governance_semantics={
            "gate_overall_status": "通过",
            "gate_overall_reason": "治理通过",
            "governance_block_effect": "禁止发布",
            "governance_block_effect_reason": "门禁锁定",
            "governance_recent_timeline": [{"result": "blocked", "reason": "等待闭合"}],
        },
        evidence_semantics={
            "score_gap": "12.0",
            "top_candidate_advantage_reason": "领先第二名",
            "evidence_confidence_label": "中",
            "top_candidate_audit_summary": "审计通过",
        },
        evolution_capacity_gate_status="yellow",
        evolution_capacity_state="受限",
        evolution_capacity_profile="small",
        evolution_capacity_stress_score="0.42",
        liquidity_capacity_state="受限",
        governance_cycle_state_label="观察中",
        governance_decision_label="锁定",
        governance_recommended_action_label="继续观察",
        governance_audit_status_label="待审",
        governance_ready_for_release=False,
        governance_fully_release_ready=False,
        previous_stable_run_id="run-prev",
        governance_operator_message="先补样本",
        summary_lines=["只读观察", "禁止发布"],
        ai_explainer={"visible": True, "summary_text": "AI explanation"},
    )

    assert view_model["display_contract"]["next_title"] == "进入复核"
    assert view_model["kpi_rows"][2][2]["value"] == "待复核"
    assert view_model["governance_operator_message"] == "先补样本"
    assert view_model["summary_lines"][1] == "禁止发布"
    assert view_model["ai_explainer"]["visible"] is True


def test_build_stock_candidate_diagnostics_view_model_derives_runtime_and_capacity_facts():
    view_model = build_stock_candidate_diagnostics_view_model(
        basket_validation_summary={
            "rebalance_dates": 6,
            "avg_basket_return_5d": 0.015,
            "avg_excess_return_5d": 0.009,
            "basket_win_rate_5d": 0.66,
            "avg_top1_return_5d": 0.007,
        },
        basket_validation_variants={
            "diversified": {"avg_excess_return_5d": 0.012},
            "raw": {"avg_excess_return_5d": 0.008},
            "top1": {"avg_excess_return_5d": 0.005},
        },
        candidate_basket_feedback={
            "feedback_level": "review",
            "window_label": "5D",
            "summary_note": "样本不足",
            "change_total": 2,
        },
        evolution_status={
            "latest_capacity_state": "受限",
            "latest_capacity_profile": "small",
            "latest_capacity_gate_status": "yellow",
            "latest_capacity_stress_score": "0.42",
        },
        basket_summary={
            "weighted_liquidity_score": 0.57,
            "liquidity_capacity_weight": 0.22,
        },
        candidate_artifact_status={
            "runtime_status": "running",
            "runtime_stage_label": "L4",
            "runtime_detail": "等待补样本",
            "runtime_updated_at": "2026-04-28T10:00:00",
            "runtime_elapsed_sec": 180,
            "runtime_results_ready": 12,
            "runtime_skipped_count": 3,
        },
        best_variant_label="均衡",
    )

    assert view_model["rebalance_dates"] == 6
    assert view_model["liquidity_capacity_state"] == "放大量受限"
    assert view_model["candidate_runtime_status_label"] == "运行中"
    assert view_model["candidate_runtime_updated_label"] == "2026-04-28 10:00:00"
    assert view_model["diversified_avg_excess_return_5d_label"] == "1.20%"
    assert view_model["avg_excess_return_5d_label"] == "0.90%"
    assert view_model["evolution_capacity_gate_status"] == "yellow"


def test_build_stock_actions_view_model_collects_section_metadata():
    view_model = build_stock_actions_view_model(
        candidate_generated_at="2026-04-28 08:00:00",
        basket_generated_at="2026-04-28 09:00:00",
        candidate_source_label="正式候选",
        generation_mode_label="主链结果",
        dominant_regime="震荡",
        risk_preference="中性",
        avg_risk_pressure="0.3",
        basket_dual_track_rows=[{"tone": "pointer", "text": "当前生效篮子 approved"}],
    )

    assert "来源 正式候选" in view_model["detail"]
    assert "更新" in view_model["detail"]
    assert view_model["metric_chips"][0] == "来源 正式候选"
    assert view_model["metric_chips"][1] == "市场状态 震荡"
    assert view_model["basket_dual_track_rows"][0]["tone"] == "pointer"


def test_build_stock_overview_chrome_view_model_collects_story_facts():
    view_model = build_stock_overview_chrome_view_model(
        home_hero_facts={
            "health_status": "稳健",
            "health_score": "92",
            "top_code": "000001.SZ",
            "candidate_name": "平安银行",
            "candidate_generated_at": "2026-04-28 08:00:00",
            "generation_mode_label": "主链结果",
            "update_status_label": "已更新",
            "update_stage_label": "已完成",
        },
        health_tag="ok",
        top1_label="000001.SZ 平安银行",
        top1_signal="strong_buy",
        top1_risk="low",
        candidate_score="95",
        run_freshness="fresh",
        update_stage_label="已完成",
        update_status_label="已更新",
        candidate_name="平安银行",
        db_latest_trade_date="2026-04-28",
        timeline_consistency_note="数据库与候选时间一致",
        view_title="总览",
        view_subtitle="首屏查看核心证据",
        command_pointer_sentence=" 当前生效篮子 approved。",
        command_attempt_sentence=" 最新篮子尝试 待复核。",
    )

    assert view_model["hero_side"]["top_code"] == "000001.SZ"
    assert view_model["command_focus"]["title"] == "000001.SZ 平安银行"
    assert "当前生效篮子 approved" in view_model["command_focus"]["detail"]
    assert view_model["view_banner"]["timeline_title"] == "2026-04-28"
    assert view_model["spotlight"]["meta_chips"][2] == "Score 95"


def test_build_stock_overview_chrome_view_model_omits_empty_name_and_score_placeholders():
    view_model = build_stock_overview_chrome_view_model(
        home_hero_facts={
            "health_status": "告警",
            "health_score": "68",
            "top_code": "300750.SZ",
            "candidate_name": "",
            "candidate_generated_at": "2026-04-28 08:00:00",
            "generation_mode_label": "主链结果",
            "update_status_label": "待补齐",
            "update_stage_label": "已完成",
        },
        health_tag="warn",
        top1_label="300750.SZ",
        top1_signal="观察",
        top1_risk="中",
        candidate_score="",
        run_freshness="fresh",
        update_stage_label="已完成",
        update_status_label="待补齐",
        candidate_name="",
        db_latest_trade_date="2026-04-28",
        timeline_consistency_note="数据库与候选时间一致",
        view_title="总览",
        view_subtitle="首屏查看核心证据",
        command_pointer_sentence="",
        command_attempt_sentence="",
    )

    assert view_model["view_banner"]["focus_subtitle"] == "观察 · 中"
    assert view_model["spotlight"]["meta_chips"] == ["观察", "中", "主链结果"]


def test_build_stock_overview_kpi_view_model_collects_core_dashboard_kpis():
    view_model = build_stock_overview_kpi_view_model(
        health_status="稳健",
        health_score="92",
        update_status_label="已更新",
        execution_semantics={"decision_action": "进入复核", "decision_action_reason": "等待样本闭合"},
        cockpit_model={"primary_progress": "6/20", "primary_needed": "14", "basket_progress": "4/20"},
        governance_semantics={"gate_overall_status": "通过", "gate_overall_reason": "治理通过"},
    )

    assert view_model["kpi_rows"][0][0]["value"] == "稳健"
    assert view_model["kpi_rows"][0][1]["value"] == "进入复核"
    assert view_model["kpi_rows"][0][2]["value"] == "6/20"
    assert view_model["kpi_rows"][0][3]["sub"] == "治理通过"


def test_build_stock_overview_disclosure_view_model_collects_folded_summary_copy():
    view_model = build_stock_overview_disclosure_view_model(
        update_status_label="已更新",
        update_stage_label="已完成",
    )

    assert view_model["system_summary_title"] == "运行、候选、预筛、治理指标已下沉"
    assert "当前更新 已更新，阶段 已完成。" == view_model["operations_detail"]


def test_build_stock_top1_view_model_preserves_candidate_summary():
    view_model = build_stock_top1_view_model(
        top1={"ts_code": "000001.SZ", "stock_name": "平安银行", "final_score": "95"},
        top1_signal="strong_buy",
        top1_risk="low",
    )

    assert view_model["top_code"] == "000001.SZ"
    assert view_model["signal"] == "strong_buy"
    assert view_model["final_score"] == "95"


def test_build_stock_candidate_focus_view_model_formats_position_and_nav():
    view_model = build_stock_candidate_focus_view_model(
        top1={"ts_code": "000001.SZ", "final_score": "95", "position_pct": "0.2"},
        top1_signal="strong_buy",
        top1_risk="low",
        candidate_cards=[
            {"ts_code": "000001.SZ"},
            {"ts_code": "000002.SZ"},
            {"ts_code": "000004.SZ"},
        ],
        candidate_index=1,
    )

    assert view_model["candidate_position_label"] == "20.0%"
    assert view_model["current_position_label"] == "候选 2 / 3"
    assert view_model["prev_index"] == 0
    assert view_model["next_index"] == 2
    assert view_model["quick_links"][1]["active"] is True


def test_build_stock_candidate_compare_view_model_keeps_top2_gap():
    view_model = build_stock_candidate_compare_view_model(
        candidate_cards=[
            {"ts_code": "000001.SZ", "stock_name": "平安银行", "signal": "strong_buy", "risk_level": "low", "final_score": "95", "pred_return": "8%"},
            {"ts_code": "000002.SZ", "stock_name": "万科A", "signal": "watch", "risk_level": "medium", "final_score": "88", "pred_return": "5%"},
        ]
    )

    assert view_model["available"] is True
    assert view_model["top_card"]["ts_code"] == "000001.SZ"
    assert view_model["next_card"]["ts_code"] == "000002.SZ"
    assert view_model["score_gap"] == "7.0"


def test_build_stock_links_view_model_collects_download_entry_contracts():
    view_model = build_stock_links_view_model(
        daily_md_href="/file/daily.md",
        daily_md_download_href="/download/daily.md",
        health_csv_href="/file/health.csv",
        health_csv_download_href="/download/health.csv",
        leaderboard_href="/file/leaderboard.csv",
        leaderboard_download_href="/download/leaderboard.csv",
        candidates_md_href="/file/candidates.md",
        candidates_md_download_href="/download/candidates.md",
        candidates_csv_href="/file/candidates.csv",
        candidates_csv_download_href="/download/candidates.csv",
        latest_report_href="/file/report.md",
        latest_report_download_href="/download/report.md",
        evolution_report_href="/stock?view=reports&report=evolution",
    )
    assert view_model["title"] == "快速入口"
    assert view_model["entries"][0]["actions"][0]["href"] == "/file/daily.md"
    assert view_model["entries"][-1]["title"] == "机制迭代摘要"


def test_build_stock_guide_view_model_collects_usage_copy():
    view_model = build_stock_guide_view_model()
    assert view_model["title"] == "指标释义与使用建议"
    assert "健康评分" in view_model["items"][0]


def test_build_stock_charts_view_model_collects_chart_blocks():
    view_model = build_stock_charts_view_model(
        health_chart_html="<div>h</div>",
        backtest_equity_html="<div>e</div>",
        backtest_drawdown_html="<div>d</div>",
        backtest_chart_html="<div>b</div>",
        backtest_map_chart_html="<div>m</div>",
        candidate_map_chart_html="<div>cm</div>",
        candidate_chart_html="<div>cc</div>",
    )
    assert view_model["title"] == "图形监控"
    assert view_model["chart_html_blocks"][4] == "<div>m</div>"


def test_build_stock_opportunities_view_model_collects_top_cards_and_contract():
    view_model = build_stock_opportunities_view_model(
        top_code="000001.SZ",
        candidate_name="平安银行",
        top1_signal="strong_buy",
        top1_risk="low",
        candidate_count=8,
        candidate_generated_at="2026-04-28 08:00:00",
        generation_mode_label="主链结果",
        basket_dual_track_rows=[{"tone": "pointer", "text": "当前生效篮子 approved"}],
        candidate_cards=[
            {"ts_code": "000001.SZ", "stock_name": "平安银行", "signal": "strong_buy", "risk_level": "low", "final_score": "95"},
            {"ts_code": "000002.SZ", "stock_name": "万科A", "signal": "watch", "risk_level": "medium", "final_score": "88"},
        ],
        candidate_hrefs=["/stock/?view=candidates&candidate=0", "/stock/?view=candidates&candidate=1"],
    )

    assert view_model["display_contract"]["conclusion_title"] == "000001.SZ"
    assert view_model["display_contract"]["evidence_title"] == "8 个候选"
    assert len(view_model["cards"]) == 2
    assert view_model["cards"][0]["href"] == "/stock/?view=candidates&candidate=0"


def test_build_stock_prefilter_view_model_marks_nonstandard_rows_as_diagnostic():
    view_model = build_stock_prefilter_view_model(
        prefilter_artifact_status={
            "row_count": "12",
            "trade_date": "2026-04-28",
            "freshness_status": "blocked",
            "freshness_note": "数据未闭合",
            "top1": "000001.SZ",
            "top1_reason": "强势入池",
            "top_exclusion_reason": "流动性不足",
            "market_symbol_count": "5200",
            "pass_rate_pct": "0.2%",
            "excluded_count": "5188",
            "excluded_rate_pct": "99.8%",
            "top_exclusion_reason_count": "600",
            "generated_at": "2026-04-28 08:00:00",
            "top10_count": "10",
        },
        db_latest_trade_date="2026-04-28",
        top_candidates=[
            {"ts_code": "diag-001", "stock_name": "", "prefilter_score": "-", "prefilter_reason": "-"},
            {"ts_code": "000001.SZ", "stock_name": "平安银行", "prefilter_score": "0.88", "prefilter_reason": "量价共振"},
        ],
        exclusion_summary_rows=[{"reason": "流动性不足", "count": 600, "share_pct": "12.0%"}],
        top_exclusion_rows=[{"ts_code": "000002.SZ", "stock_name": "万科A", "exclusion_reason_zh": "波动过大"}],
    )

    assert view_model["quality_label"] == "诊断样本，不作为对外结论"
    assert "过期" in view_model["trade_date_label"]
    assert view_model["top10_rows"][0]["is_standard_code"] == ""
    assert view_model["kpi_rows"][0][2]["value"] == "000001.SZ"


def test_build_stock_selection_funnel_view_model_preserves_ratios_and_widths():
    view_model = build_stock_selection_funnel_view_model(
        market_sample_count=5200,
        prefilter_pass_count=80,
        final_candidate_count=12,
        top_code="000001.SZ",
        pass_rate_pct="1.5%",
        top_exclusion_reason="流动性不足",
        top_exclusion_reason_count="600",
        configured_liquidity_min_turnover="10000000",
        effective_liquidity_min_turnover="25000000",
    )

    assert view_model["display_contract"]["conclusion_title"] == "80 进预筛，12 进候选"
    assert view_model["kpi_rows"][0][2]["value"] == "1.5%"
    assert view_model["funnel_rows"][0]["width_pct"] == "100.00"
    assert view_model["metric_chips"][3] == "主要出池原因 流动性不足 · 600 只"
