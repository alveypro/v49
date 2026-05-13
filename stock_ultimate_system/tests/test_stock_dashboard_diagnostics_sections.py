from src.stock_dashboard_diagnostics_sections import build_stock_diagnostics_sections


def _base_kwargs(visible_names: set[str], current_view: str = "overview") -> dict[str, object]:
    return {
        "visible": lambda name: name in visible_names,
        "current_view": current_view,
        "basket_validation": {
            "summary": {
                "rebalance_dates": 6,
                "avg_basket_return_5d": 0.015,
                "avg_excess_return_5d": 0.009,
                "basket_win_rate_5d": 0.66,
                "avg_top1_return_5d": 0.007,
            },
            "variants": {
                "diversified": {"avg_excess_return_5d": 0.012},
                "raw": {"avg_excess_return_5d": 0.008},
            },
        },
        "candidate_basket_feedback": {
            "feedback_level": "review",
            "window_label": "5D",
            "summary_note": "样本不足",
            "change_total": 2,
        },
        "evolution_status": {
            "latest_capacity_state": "受限",
            "latest_capacity_profile": "small",
            "latest_capacity_gate_status": "yellow",
            "latest_capacity_stress_score": "0.42",
        },
        "basket_summary": {
            "expected_basket_return": 0.032,
            "risk_pressure_score": 0.4,
            "weighted_liquidity_score": 0.57,
            "liquidity_capacity_weight": 0.22,
            "guardrail_mode": "defensive",
            "guardrail_reasons": ["liquidity pressure"],
            "skipped_count": 5,
        },
        "candidate_artifact_status": {
            "runtime_status": "running",
            "runtime_stage_label": "L4",
            "runtime_detail": "等待补样本",
            "runtime_updated_at": "2026-04-28T10:00:00",
            "runtime_elapsed_sec": 180,
            "runtime_results_ready": 12,
            "runtime_skipped_count": 3,
            "generation_reason": "pointer verified",
            "strategy_mode": "diversified",
            "strategy_strictness": "tight",
            "strategy_weak_market_action": "observe",
        },
        "validation_basket_kpis": [{"label": "当前生效篮子", "value": "approved", "sub": "basket-001"}],
        "generation_mode_label": "主链结果",
        "candidate_count": 12,
        "top1": {"ts_code": "000001.SZ"},
        "prefilter_artifact_status": {
            "row_count": "80",
            "trade_date": "2026-04-28",
            "freshness_status": "fresh",
            "freshness_note": "数据闭合",
            "top1": "000001.SZ",
            "top1_reason": "强势入池",
            "top_exclusion_reason": "流动性不足",
            "market_symbol_count": "5200",
            "pass_rate_pct": "1.5%",
            "excluded_count": "5120",
            "excluded_rate_pct": "98.5%",
            "top_exclusion_reason_count": "600",
            "generated_at": "2026-04-28 08:00:00",
            "top10_count": "10",
            "top_candidates": [{"ts_code": "000001.SZ", "stock_name": "平安银行", "prefilter_score": "0.88", "prefilter_reason": "量价共振"}],
            "exclusion_summary": [{"reason": "流动性不足", "count": 600, "share_pct": "12.0%"}],
            "top_exclusions": [{"ts_code": "000002.SZ", "stock_name": "万科A", "exclusion_reason_zh": "波动过大"}],
        },
        "db_latest_trade_date": "2026-04-28",
        "diagnosis_section": '<div id="diagnosis">diag</div>',
    }


def test_build_stock_diagnostics_sections_builds_validation_prefilter_funnel_and_appendix():
    sections = build_stock_diagnostics_sections(**_base_kwargs({"prefilter"}))

    assert sections.diagnostics_view_model["rebalance_dates"] == 6
    assert 'id="validation"' in sections.validation_section
    assert 'id="prefilter"' in sections.prefilter_section
    assert 'id="selection-funnel"' in sections.selection_funnel_section
    assert 'id="diagnostics-appendix"' in sections.diagnostics_appendix_section
    assert "流动性不足" in sections.selection_funnel_section


def test_build_stock_diagnostics_sections_respects_prefilter_visibility_and_overview_appendix_scope():
    sections = build_stock_diagnostics_sections(**_base_kwargs(set(), current_view="reports"))

    assert sections.prefilter_section == ""
    assert sections.diagnostics_appendix_section == ""
    assert 'id="validation"' in sections.validation_section
    assert 'id="selection-funnel"' in sections.selection_funnel_section
