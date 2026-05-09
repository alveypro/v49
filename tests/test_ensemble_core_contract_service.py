from __future__ import annotations

from openclaw.services.ensemble_core_contract_service import build_ensemble_core_contract_review


def test_ensemble_core_contract_starts_research_only_without_full_portfolio_contract():
    review = build_ensemble_core_contract_review({})

    assert review["strategy"] == "ensemble_core"
    assert review["research_only"] is True
    assert review["eligible_for_formal_ranking"] is False
    assert "missing_alpha_sleeves:money_flow" in review["blocking_reasons"]
    assert "missing_portfolio_controls:risk_budget" in review["blocking_reasons"]
    assert "missing_attribution:alpha_ic" in review["blocking_reasons"]
    assert "runtime_backtest_handler" in review["blocking_reasons"]
    assert review["donor_strategy_policy"]["v6"].startswith("money_flow_event_alpha_sleeve_only")
    assert review["donor_strategy_policy"]["v7"].startswith("v8_filter_rebase_shadow_only")


def test_ensemble_core_contract_can_only_pass_when_all_required_parts_exist():
    review = build_ensemble_core_contract_review(
        {
            "alpha_sleeves": [
                "momentum",
                "reversal",
                "money_flow",
                "sector_rotation",
                "quality_low_vol",
                "event_risk",
            ],
            "pit_inputs": [
                "price_volume",
                "money_flow",
                "sector_heat",
                "suspension_limit",
                "volume_capacity",
            ],
            "portfolio_controls": [
                "risk_budget",
                "industry_caps",
                "single_name_caps",
                "turnover_budget",
                "drawdown_budget",
                "capacity_constraint",
            ],
            "attribution": [
                "alpha_ic",
                "alpha_decay",
                "alpha_correlation",
                "risk_contribution",
                "cost_slippage",
                "regime_split",
            ],
            "runtime_scan_handler": True,
            "runtime_backtest_handler": True,
            "walk_forward_backtest": True,
            "formal_pool_shadow_benchmark": True,
        }
    )

    assert review["research_only"] is False
    assert review["eligible_for_formal_ranking"] is True
    assert review["blocking_reasons"] == []
