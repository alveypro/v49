from __future__ import annotations

from openclaw.services.strategy_maturity_service import build_strategy_maturity_plan


def test_strategy_maturity_plan_keeps_observation_and_research_honest():
    plan = build_strategy_maturity_plan(
        {
            "all_strategy_reviews": [
                {
                    "strategy": "stable",
                    "eligible_for_daily_top3": False,
                    "blocking_reasons": ["backtest_quality_floor_not_passed"],
                    "backtest_component": {
                        "passed": True,
                        "quality_floor_passed": False,
                        "eligible_for_formal_ranking": False,
                        "source_run_id": "sweep_stable",
                    },
                },
                {
                    "strategy": "v6",
                    "eligible_for_daily_top3": False,
                    "blocking_reasons": ["backtest_credibility_not_passed"],
                    "backtest_component": {
                        "passed": False,
                        "quality_floor_passed": False,
                        "eligible_for_formal_ranking": False,
                        "source_run_id": "sweep_v6",
                        "blocking_reasons": ["missing_positive_signal_density", "missing_successful_test_windows"],
                    },
                },
                {
                    "strategy": "ai",
                    "eligible_for_daily_top3": False,
                    "blocking_reasons": ["missing_signal_run"],
                    "backtest_component": {"passed": False},
                },
                {
                    "strategy": "ensemble_core",
                    "eligible_for_daily_top3": False,
                    "blocking_reasons": ["missing_signal_run"],
                    "backtest_component": {"passed": False},
                },
            ],
            "observation_pool": [{"strategy": "stable"}],
            "diagnostic_pool": [{"strategy": "v6"}],
            "research_only_pool": [{"strategy": "ai"}, {"strategy": "ensemble_core"}],
        }
    )

    by_strategy = {item["strategy"]: item for item in plan["items"]}
    assert by_strategy["stable"]["truth"] == "credible_backtest_but_not_alpha_quality"
    assert by_strategy["stable"]["can_compete_for_formal_top"] is False
    assert "raise_out_of_sample_win_rate_to_at_least_0_45_with_positive_signal_density_and_drawdown_le_0_25" in by_strategy["stable"]["promotion_requirements"]
    assert by_strategy["stable"]["next_experiment"]["type"] == "defensive_allocator_rebuild"
    assert "portfolio_overlay_backtest_against_formal_strategy_pool" in by_strategy["stable"]["next_experiment"]["required_contract"]
    assert "formal_pool_benchmark_return_series" in by_strategy["stable"]["next_experiment"]["required_contract"]
    assert by_strategy["stable"]["next_experiment"]["success_metric"] == "portfolio_drawdown_reduction_with_non_negative_excess_return_and_no_hidden_turnover_cost"
    assert by_strategy["v6"]["next_experiment"]["type"] == "signal_generation_and_rolling_window_repair"
    assert "rolling_window_success_rate_report" in by_strategy["v6"]["next_experiment"]["required_contract"]
    assert "produce_nonzero_signal_density_under_current_formal_gate" in by_strategy["v6"]["promotion_requirements"]
    assert by_strategy["ai"]["truth"] == "research_concept_without_runtime_backtest_contract"
    assert "implement_real_runtime_scan_and_backtest_handler" in by_strategy["ai"]["promotion_requirements"]
    assert "runtime_backtest_handler" in by_strategy["ai"]["next_experiment"]["required_contract"]
    assert by_strategy["ensemble_core"]["truth"] == "research_concept_without_runtime_backtest_contract"
    assert by_strategy["ensemble_core"]["next_experiment"]["type"] == "multi_alpha_portfolio_research_contract"
    assert "missing_alpha_sleeves:momentum" in by_strategy["ensemble_core"]["promotion_requirements"]
    assert "missing_portfolio_controls:risk_budget" in by_strategy["ensemble_core"]["next_experiment"]["required_contract"]
    assert by_strategy["ensemble_core"]["next_experiment"]["donor_strategy_policy"]["v7"].startswith("v8_filter_rebase")
