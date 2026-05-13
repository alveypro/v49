from src.stock_dashboard_shell_sections import build_stock_dashboard_shell_sections, compose_stock_top_story_html


def _base_kwargs(current_view: str = "overview") -> dict[str, object]:
    return {
        "current_view": current_view,
        "candidate_index": 0,
        "base_path": "/stock",
        "view_labels": {
            "overview": "总览",
            "research": "研究",
            "candidates": "候选",
            "operations": "运维",
            "reports": "报告",
        },
        "view_subtitle": "首屏查看核心证据",
        "report_state": {"current_report": "research"},
        "context": {"server_sync_preflight": {"status": "pass"}},
        "headline_tone": "primary",
        "headline_detail": "候选链路等待观察",
        "current_basket_pointer_label": "当前生效篮子 approved",
        "latest_basket_attempt_label": "最新篮子尝试 blocked",
        "current_basket_pointer_status": "approved",
        "current_basket_pointer_basket_id": "basket-001",
        "latest_basket_attempt_status": "blocked",
        "latest_basket_attempt_blocking_reason": "样本不足",
        "health_status": "稳健",
        "health_score": "92",
        "health_tag": "ok",
        "top1": {"ts_code": "000001.SZ"},
        "top1_label": "000001.SZ 平安银行",
        "top1_signal": "strong_buy",
        "top1_risk": "low",
        "candidate_name": "平安银行",
        "candidate_artifact_status": {"generated_at": "2026-05-13"},
        "generation_mode_label": "主链结果",
        "update_status_label": "已更新",
        "update_stage_label": "已完成",
        "candidate_count": 5,
        "candidate_score": "95",
        "candidate_timeline_label": "候选 2026-05-13",
        "run_freshness": "fresh",
        "db_latest_trade_date": "2026-05-13",
        "observation_timeline_label": "观察 6/20",
        "prefilter_freshness_label": "预筛已闭合",
        "backtest_scope": {"label": "近60日"},
        "governance_cycle_state": "observe_only",
        "governance_recommended_action_label": "继续观察",
        "governance_release_readiness": {"ready_for_release": False},
        "governance_fully_release_ready": False,
        "primary_conclusion": {
            "result_id": "primary-001",
            "stage_label": "执行准备",
            "ts_code": "000001.SZ",
            "stock_name": "平安银行",
        },
        "decision_semantics": {"market_regime": "震荡", "risk_preference": "中性", "strategy_conclusion": "继续观察"},
        "market_snapshot": {"dominant_regime": "震荡", "risk_preference": "中性", "avg_risk_pressure": "0.3"},
        "bt_diag": {"结论": "继续观察"},
        "blocker_semantics": {"blocker_title": "等待观察", "blocker_detail": "样本不足"},
        "cockpit_model": {"primary_progress": "6/20", "primary_needed": "14", "basket_progress": "4/20"},
        "promotion_decision_label": "禁止发布",
        "timeline_consistency_note": "数据库与候选时间一致",
        "automation_health_label": "自动链路正常",
    }


def test_build_stock_dashboard_shell_sections_collects_home_and_chrome_html():
    sections = build_stock_dashboard_shell_sections(**_base_kwargs())

    assert sections.home_view_model["home_hero_facts"]["top_code"] == "000001.SZ"
    assert sections.validation_basket_kpis
    assert "内部复核" in sections.external_system_summary_html
    assert 'id="external-decision-spine"' in sections.external_decision_spine_html
    assert "当前生效篮子 approved" in sections.hero_side_html
    assert "000001.SZ" in sections.spotlight_html
    assert "Score 95" in sections.spotlight_html
    assert "总览" in sections.nav_html
    assert sections.page_shell_context["nav_items"][0]["active"] is True


def test_compose_stock_top_story_html_delegates_overview_and_research_contracts():
    overview_sections = build_stock_dashboard_shell_sections(**_base_kwargs("overview"))
    research_sections = build_stock_dashboard_shell_sections(**_base_kwargs("research"))

    overview_html = compose_stock_top_story_html(
        current_view="overview",
        shell_sections=overview_sections,
        today_brief_html="<section>brief</section>",
        top1_label="000001.SZ 平安银行",
        top1_signal="strong_buy",
        top1_risk="low",
    )
    research_html = compose_stock_top_story_html(
        current_view="research",
        shell_sections=research_sections,
        today_brief_html="<section>brief</section>",
        top1_label="000001.SZ 平安银行",
        top1_signal="strong_buy",
        top1_risk="low",
    )

    assert overview_html == overview_sections.external_decision_spine_html
    assert "view-banner" in research_html
    assert "jump-strip" in research_html
