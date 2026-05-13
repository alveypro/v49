from src.stock_dashboard_overview_sections import build_stock_overview_sections


def _base_kwargs(visible_names: set[str], candidate_cards: list[dict[str, object]] | None = None) -> dict[str, object]:
    return {
        "visible": lambda name: name in visible_names,
        "health_status": "稳健",
        "health_score": "92",
        "update_status_label": "已更新",
        "execution_semantics": {
            "decision_action": "进入复核",
            "decision_action_reason": "等待样本闭合",
            "execution_eligibility": "否",
            "execution_eligibility_reason": "证据不足",
            "decision_validity_label": "T+1",
            "candidate_generated_at": "2026-05-13",
            "db_latest_trade_date": "2026-05-13",
        },
        "cockpit_model": {"primary_progress": "6/20", "primary_needed": "14", "basket_progress": "4/20"},
        "governance_semantics": {
            "gate_overall_status": "通过",
            "gate_overall_reason": "治理通过",
            "governance_block_effect": "禁止发布",
            "governance_block_effect_reason": "门禁锁定",
            "governance_recent_timeline": [{"result": "blocked", "reason": "等待闭合"}],
        },
        "bt_diag": {"结论": "继续观察"},
        "top1": {"ts_code": "000001.SZ"},
        "candidate_name": "平安银行",
        "candidate_artifact_status": {"generated_at": "2026-05-13"},
        "backtest_scope": {"label": "近60日"},
        "home_view_model": {"basket_dual_track_rows": [{"tone": "pointer", "text": "当前生效篮子 approved"}]},
        "blocker_semantics": {"blocker_title": "等待观察", "blocker_detail": "样本不足"},
        "evidence_semantics": {
            "score_gap": "12.0",
            "top_candidate_advantage_reason": "领先第二名",
            "evidence_confidence_label": "中",
            "top_candidate_audit_summary": "审计通过",
        },
        "diagnostics_view_model": {
            "evolution_capacity_gate_status": "yellow",
            "evolution_capacity_state": "受限",
            "evolution_capacity_profile": "small",
            "evolution_capacity_stress_score": "0.42",
            "liquidity_capacity_state": "受限",
        },
        "governance_cycle_state": "observe_only",
        "governance_cycle": {"governance_inputs": {"governance_decision": "blocked", "governance_audit_status": "pass"}},
        "governance_recommended_action_label": "继续观察",
        "governance_release_readiness": {"ready_for_release": False},
        "governance_fully_release_ready": False,
        "previous_stable_run_id": "run-prev",
        "governance_operator_message": "先补样本",
        "summary_lines": ["只读观察", "禁止发布"],
        "stock_ai_explainer": {"visible": False},
        "top1_signal": "strong_buy",
        "top1_risk": "low",
        "candidate_count": 2,
        "generation_mode_label": "主链结果",
        "candidate_cards": candidate_cards or [],
        "base_path": "/stock",
        "health_chart_html": '<div id="health-chart"></div>',
        "backtest_equity_html": '<div id="equity-chart"></div>',
        "backtest_drawdown_html": '<div id="drawdown-chart"></div>',
        "backtest_chart_html": '<div id="backtest-chart"></div>',
        "operations_section": '<div id="operations">ops</div>',
        "external_system_summary_html": '<details id="external-system-summary"></details>',
        "overview_disclosure_view_model": {"operations_detail": "只读运维证据"},
        "status_label": lambda value: f"label:{value}",
        "view_href": lambda view, idx, base_path: f"{base_path}/?view={view}&candidate={idx}",
    }


def test_build_stock_overview_sections_respects_visibility_without_candidates():
    sections = build_stock_overview_sections(**_base_kwargs(set()))

    assert sections.kpi_html == ""
    assert sections.summary_section == ""
    assert sections.opportunity_cards_html == ""
    assert 'id="overview-visuals-disclosure"' in sections.overview_visuals_disclosure_section
    assert 'id="overview-operations-disclosure"' in sections.overview_operations_disclosure_section
    assert "只读运维证据" in sections.overview_operations_disclosure_section


def test_build_stock_overview_sections_builds_summary_kpi_and_opportunities():
    sections = build_stock_overview_sections(
        **_base_kwargs(
            {"kpi", "summary"},
            candidate_cards=[
                {"ts_code": "000001.SZ", "stock_name": "平安银行", "signal": "strong_buy", "risk_level": "low", "final_score": "95"},
                {"ts_code": "000002.SZ", "stock_name": "万科A", "signal": "watch", "risk_level": "medium", "final_score": "88"},
            ],
        )
    )

    assert "系统状态" in sections.kpi_html
    assert 'data-display-contract="summary"' in sections.summary_section
    assert "禁止发布" in sections.summary_section
    assert 'id="opportunities"' in sections.opportunity_cards_html
    assert "/stock/?view=candidates&amp;candidate=1" in sections.opportunity_cards_html
    assert 'id="health-chart"' in sections.overview_visuals_disclosure_section
    assert 'id="operations"' in sections.overview_operations_disclosure_section
