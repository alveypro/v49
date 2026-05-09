from openclaw.services.strategy_backtest_diagnostic_service import build_strategy_backtest_diagnostics


def test_combo_zero_density_requires_component_diagnostic():
    diagnostics = build_strategy_backtest_diagnostics(
        strategy="combo",
        rows=[
            {
                "status": "failed",
                "win_rate": 0.0,
                "max_drawdown": 1.0,
                "signal_density": 0.0,
                "objective": -9999.0,
            }
        ],
        errors=[{"error": "rolling backtest produced 0 successful test windows"}],
        backtest_credibility={
            "passed": False,
            "blocking_reasons": [
                "missing_successful_test_windows",
                "missing_positive_signal_density",
            ],
        },
    )

    assert diagnostics["credible_evidence_present"] is False
    assert diagnostics["eligible_for_formal_ranking"] is False
    assert "combo_component_consensus_diagnostic_required" in diagnostics["failure_classes"]
    assert "no_successful_rolling_test_window" in diagnostics["failure_classes"]
    assert "compare_v5_v8_v9_component_pass_rates_and_consensus_dropoff_by_window" in diagnostics["next_actions"]


def test_v7_credible_but_weak_quality_floor_goes_to_observation_pool():
    diagnostics = build_strategy_backtest_diagnostics(
        strategy="v7",
        rows=[
            {
                "status": "success",
                "win_rate": 0.0,
                "max_drawdown": 0.07,
                "signal_density": 0.025,
                "objective": 2.0,
                "rolling_test_windows": 3,
            }
        ],
        errors=[],
        backtest_credibility={"passed": True, "blocking_reasons": []},
    )

    assert diagnostics["credible_evidence_present"] is True
    assert diagnostics["quality_floor_passed"] is False
    assert diagnostics["eligible_for_formal_ranking"] is False
    assert "weak_out_of_sample_win_rate" in diagnostics["failure_classes"]
    assert "move_strategy_to_observation_pool_until_oos_quality_improves" in diagnostics["next_actions"]


def test_v7_near_threshold_failure_stays_signal_generation_repair_before_quality_floor():
    diagnostics = build_strategy_backtest_diagnostics(
        strategy="v7",
        rows=[
            {
                "status": "failed",
                "failure_diagnostics": [
                    {
                        "type": "score_distribution",
                        "strategy": "v7",
                        "threshold": 60.0,
                        "evaluated": 20,
                        "passed_threshold": 0,
                        "score_count": 20,
	                        "max_score": 59.2,
	                        "avg_score": 44.0,
	                        "near_threshold": {"within_2": 2, "within_5": 3, "within_10": 7},
	                        "reason_counts": {"below_threshold": 20},
	                        "top_near_threshold_samples": [
	                            {
	                                "ts_code": "300059.SZ",
	                                "near_threshold_conversion": {
	                                    "blocking_reasons": ["industry_heat_negative"],
	                                    "confirmation_count": 3,
	                                },
	                            }
	                        ],
	                    }
                ],
            }
        ],
        errors=[{"error": "rolling backtest produced 0 successful test windows"}],
        backtest_credibility={
            "passed": False,
            "blocking_reasons": ["missing_successful_test_windows", "missing_positive_signal_density"],
        },
    )

    assert diagnostics["eligible_for_formal_ranking"] is False
    assert diagnostics["window_diagnostics"]["near_threshold"]["within_10"] == 7
    assert "v7_near_threshold_signal_generation_gap" in diagnostics["failure_classes"]
    assert "repair_v7_signal_generation_or_rebase_on_v8_filters_before_any_quality_floor_review" in diagnostics["next_actions"]
    plan = diagnostics["repair_experiment_plan"]
    assert plan["priority"] == "near_threshold_signal_generation_or_rebase_decision"
    assert plan["eligible_for_quality_floor_tuning"] is False
    assert plan["execution_contract"]["entrypoint"] == "tools/all_strategy_evidence_run.py"
    assert plan["rebase_or_retire_decision"]["recommended_action"] == "rebase_on_v8_market_and_sector_filters_before_any_v7_gate_repair"
    assert plan["experiments"][0]["type"] == "v7_near_threshold_conversion_probe"
    assert plan["experiments"][0]["input_evidence"]["decision"]["reason"] == "near_threshold_samples_blocked_by_negative_industry_heat"
    assert plan["experiments"][0]["runtime_params"]["score_threshold"] == 60
    assert plan["experiments"][0]["runtime_params"]["strategy_arg"] == "v7"
    assert "lower_threshold_to_create_signal_density" in plan["forbidden_shortcuts"]


def test_v6_pit_data_unavailable_is_not_treated_as_true_alpha_filter_failure():
    diagnostics = build_strategy_backtest_diagnostics(
        strategy="v6",
        rows=[
            {
                "status": "failed",
                "failure_diagnostics": [
                    {
                        "type": "score_distribution",
                        "strategy": "v6",
                        "threshold": 75.0,
                        "evaluated": 10,
                        "passed_threshold": 0,
                        "score_count": 0,
                        "reason_counts": {"mandatory_filter:pit_data_unavailable": 10},
                    }
                ],
            }
        ],
        errors=[{"error": "rolling backtest produced 0 successful test windows"}],
        backtest_credibility={
            "passed": False,
            "blocking_reasons": ["missing_successful_test_windows", "missing_positive_signal_density"],
        },
    )

    assert "v6_pit_data_availability_blocks_mandatory_filter_evidence" in diagnostics["failure_classes"]
    assert "separate_v6_missing_pit_money_sector_data_from_true_mandatory_filter_failures" in diagnostics["next_actions"]
    plan = diagnostics["repair_experiment_plan"]
    assert any(item["type"] == "v6_pit_money_sector_data_contract_probe" for item in plan["experiments"])


def test_v9_passes_formal_ranking_quality_floor():
    diagnostics = build_strategy_backtest_diagnostics(
        strategy="v9",
        rows=[
            {
                "status": "success",
                "win_rate": 0.72,
                "max_drawdown": 0.12,
                "signal_density": 0.12,
                "objective": 60.0,
                "rolling_test_windows": 3,
            }
        ],
        errors=[],
        backtest_credibility={"passed": True, "blocking_reasons": []},
    )

    assert diagnostics["credible_evidence_present"] is True
    assert diagnostics["quality_floor_passed"] is True
    assert diagnostics["eligible_for_formal_ranking"] is True
    assert diagnostics["failure_classes"] == []


def test_diagnostics_preserve_zero_drawdown_quality_floor():
    diagnostics = build_strategy_backtest_diagnostics(
        strategy="v9",
        rows=[
            {
                "status": "success",
                "win_rate": 1.0,
                "max_drawdown": 0.0,
                "signal_density": 0.1,
                "objective": 100.0,
                "rolling_test_windows": 2,
            }
        ],
        errors=[],
        backtest_credibility={"passed": True, "blocking_reasons": []},
    )

    assert diagnostics["quality_floor_passed"] is True
    assert "drawdown_above_quality_floor" not in diagnostics["failure_classes"]


def test_raw_backtest_credibility_audit_is_evaluated_for_diagnostics():
    diagnostics = build_strategy_backtest_diagnostics(
        strategy="v8",
        rows=[
            {
                "status": "success",
                "win_rate": 0.65,
                "max_drawdown": 0.15,
                "signal_density": 1.0,
                "objective": 70.0,
                "rolling_test_windows": 3,
            }
        ],
        errors=[],
        backtest_credibility={
            "point_in_time_data": True,
            "suspension_and_limit_handling": True,
            "volume_constraint": True,
            "cost_model": True,
            "slippage_model": True,
            "in_sample_out_of_sample_split": True,
            "parameter_sensitivity": True,
            "failed_backtests_recorded": True,
            "sample": {"in_sample": "train_windows:3", "out_of_sample": "test_windows:3"},
            "metrics": {"signal_density": 1.0, "test_windows": 3},
        },
    )

    assert diagnostics["credible_evidence_present"] is True
    assert diagnostics["quality_floor_passed"] is True
    assert diagnostics["eligible_for_formal_ranking"] is True


def test_legacy_return_only_strategy_is_not_promoted_without_constraints():
    diagnostics = build_strategy_backtest_diagnostics(
        strategy="v5",
        rows=[
            {
                "status": "success",
                "win_rate": 0.62,
                "max_drawdown": 0.10,
                "signal_density": 0.08,
                "objective": 50.0,
                "rolling_test_windows": 3,
            }
        ],
        errors=[],
        backtest_credibility={
            "passed": False,
            "blocking_reasons": ["missing_tradeability_filter", "missing_volume_constraint"],
        },
    )

    assert diagnostics["eligible_for_formal_ranking"] is False
    assert "legacy_return_sample_not_credible_without_constraints" in diagnostics["failure_classes"]
    assert "missing_execution_constraint_evidence" in diagnostics["failure_classes"]
    assert "keep_legacy_returns_as_reference_only_until_runtime_replay_constraints_exist" in diagnostics["next_actions"]


def test_v4_runtime_replay_with_constraints_is_not_labeled_legacy_return_only():
    diagnostics = build_strategy_backtest_diagnostics(
        strategy="v4",
        rows=[
            {
                "status": "success",
                "win_rate": 0.40,
                "max_drawdown": 0.18,
                "signal_density": 0.10,
                "objective": 12.0,
                "rolling_test_windows": 3,
                "tradeability_filter_enabled": True,
                "volume_constraint_enabled": True,
            }
        ],
        errors=[],
        backtest_credibility={"passed": True, "blocking_reasons": []},
    )

    assert "legacy_return_sample_not_credible_without_constraints" not in diagnostics["failure_classes"]
    assert "weak_out_of_sample_win_rate" in diagnostics["failure_classes"]


def test_runtime_credibility_missing_flags_are_classified():
    diagnostics = build_strategy_backtest_diagnostics(
        strategy="v8",
        rows=[
            {
                "status": "success",
                "win_rate": 0.62,
                "max_drawdown": 0.10,
                "signal_density": 0.08,
                "objective": 50.0,
                "rolling_test_windows": 3,
            }
        ],
        errors=[],
        backtest_credibility={
            "passed": False,
            "blocking_reasons": [
                "missing_or_failed:suspension_and_limit_handling",
                "missing_or_failed:volume_constraint",
                "missing_or_failed:cost_model",
                "missing_or_failed:slippage_model",
            ],
        },
    )

    assert diagnostics["eligible_for_formal_ranking"] is False
    assert "missing_execution_constraint_evidence" in diagnostics["failure_classes"]
    assert "missing_cost_or_slippage_evidence" in diagnostics["failure_classes"]


def test_ai_requires_real_runtime_backtest_handler_before_competition():
    diagnostics = build_strategy_backtest_diagnostics(
        strategy="ai",
        rows=[
            {
                "status": "failed",
                "win_rate": 0.0,
                "max_drawdown": 1.0,
                "signal_density": 0.0,
                "objective": -9999.0,
            }
        ],
        errors=[{"error": "rolling backtest produced 0 successful test windows"}],
        backtest_credibility={
            "passed": False,
            "blocking_reasons": ["missing_successful_test_windows"],
        },
    )

    assert "backtest_handler_missing_or_not_credible" in diagnostics["failure_classes"]
    assert "implement_real_runtime_backtest_handler_before_strategy_competition" in diagnostics["next_actions"]


def test_v6_passed_threshold_without_rolling_success_traces_signal_conversion_pipeline():
    diagnostics = build_strategy_backtest_diagnostics(
        strategy="v6",
        rows=[
            {
                "status": "failed",
                "failure_diagnostics": [
                    {
                        "type": "score_distribution",
                        "strategy": "v6",
                        "threshold": 65.0,
                        "evaluated": 12,
                        "passed_threshold": 1,
                        "score_count": 12,
                        "max_score": 67.0,
                        "avg_score": 41.0,
                        "near_threshold": {"within_2": 1, "within_5": 2, "within_10": 4},
                        "reason_counts": {"below_threshold": 11, "pass_threshold": 1},
                    }
                ],
            }
        ],
        errors=[{"error": "rolling backtest produced 0 successful test windows"}],
        backtest_credibility={
            "passed": False,
            "blocking_reasons": ["missing_successful_test_windows", "missing_positive_signal_density"],
        },
    )

    assert "v6_threshold_passes_do_not_convert_to_successful_windows" in diagnostics["failure_classes"]
    assert "trace_v6_passed_threshold_samples_through_tradeability_exit_and_rolling_window_pipeline" in diagnostics["next_actions"]
    plan = diagnostics["repair_experiment_plan"]
    assert plan["priority"] == "signal_generation_before_quality_floor"
    assert plan["eligible_for_quality_floor_tuning"] is False
    assert plan["execution_contract"]["must_preserve_pool_status_until_passed"] is True
    assert plan["experiments"][0]["type"] == "v6_signal_conversion_trace"
    assert plan["experiments"][0]["runtime_params"]["score_threshold"] == 75
    assert plan["experiments"][0]["runtime_params"]["strategy_arg"] == "v6"
    assert plan["experiments"][0]["input_evidence"]["passed_threshold"] == 1


def test_window_score_distribution_diagnostics_are_aggregated():
    diagnostics = build_strategy_backtest_diagnostics(
        strategy="v8",
        rows=[
            {
                "status": "failed",
                "failure_diagnostics": [
                    {
                        "type": "score_distribution",
                        "strategy": "v8",
                        "evaluated": 10,
                        "passed_threshold": 1,
                        "missing_score": 2,
                        "max_score": 48.0,
                        "avg_score": 32.0,
                        "score_count": 8,
                        "reason_counts": {"below_threshold": 7, "pass_threshold": 1},
                        "stage_timing_ms": {
                            "evaluate_stock_v6": {"count": 8, "avg": 300.0, "max": 450.0},
                            "point_in_time_context": {"count": 8, "avg": 20.0, "max": 35.0},
                        },
                        "score_breakdown": {
                            "advanced_score": {"count": 8, "avg": 31.0, "min": 20.0, "max": 44.0},
                            "market_penalty": {"count": 8, "avg": 0.8, "min": 0.5, "max": 1.0},
                            "risk_penalty": {"count": 8, "avg": 2.0, "min": 0.0, "max": 8.0},
                        },
                    }
                ],
            }
        ],
        errors=[
            {
                "error": "rolling backtest produced 0 successful test windows",
                "failure_diagnostics": [
                    {
                        "type": "score_distribution",
                        "strategy": "v8",
                        "evaluated": 5,
                        "passed_threshold": 0,
                        "missing_score": 1,
                        "max_score": 35.0,
                        "avg_score": 28.0,
                        "score_count": 4,
                        "reason_counts": {"below_threshold": 4, "evaluator_unsuccessful": 1},
                        "stage_timing_ms": {
                            "evaluate_stock_v6": {"count": 4, "avg": 500.0, "max": 800.0},
                        },
                        "score_breakdown": {
                            "advanced_score": {"count": 4, "avg": 25.0, "min": 19.0, "max": 33.0},
                            "factor:smart_money": {"count": 4, "avg": 4.0, "min": 1.0, "max": 6.0},
                        },
                    }
                ],
            }
        ],
        backtest_credibility={"passed": False, "blocking_reasons": ["missing_successful_test_windows"]},
    )

    window_diag = diagnostics["window_diagnostics"]
    assert window_diag["available"] is True
    assert window_diag["evaluated"] == 15
    assert window_diag["passed_threshold"] == 1
    assert abs(window_diag["pass_rate"] - (1 / 15)) < 1e-9
    assert window_diag["max_score"] == 48.0
    assert window_diag["reason_counts"]["below_threshold"] == 11
    assert abs(window_diag["score_breakdown"]["advanced_score"]["avg"] - 29.0) < 1e-9
    assert window_diag["score_breakdown"]["market_penalty"]["min"] == 0.5
    assert window_diag["score_breakdown"]["risk_penalty"]["min"] == 0.0
    assert window_diag["score_breakdown"]["factor:smart_money"]["avg"] == 4.0
    assert abs(window_diag["stage_timing_ms"]["evaluate_stock_v6"]["avg"] - (4400.0 / 12.0)) < 1e-9
    assert window_diag["stage_timing_ms"]["evaluate_stock_v6"]["max"] == 800.0
    assert window_diag["stage_timing_ms"]["point_in_time_context"]["avg"] == 20.0


def test_v6_score_distribution_evidence_requires_calibration_not_formal_ranking():
    diagnostics = build_strategy_backtest_diagnostics(
        strategy="v6",
        rows=[
            {
                "status": "failed",
                "win_rate": 0.0,
                "max_drawdown": 1.0,
                "signal_density": 0.0,
                "objective": -9999.0,
                "failure_diagnostics": [
                    {
                        "type": "score_distribution",
                        "strategy": "v6",
                        "threshold": 65.0,
                        "evaluated": 144,
                        "passed_threshold": 0,
                        "missing_score": 0,
                        "reason_counts": {"candidate_filter_relaxed": 91, "below_threshold": 144},
                        "candidate_filter_mode": "diagnostic_advisory",
                        "score_breakdown": {
                            "base_score": {"count": 144, "avg": 21.875, "min": 2.0, "max": 66.0},
                            "synergy_bonus": {"count": 144, "avg": 0.5, "min": 0.0, "max": 13.0},
                            "risk_penalty": {"count": 144, "avg": 12.5, "min": 0.0, "max": 60.0},
                            "dim:资金流向": {"count": 144, "avg": 3.3, "min": 0.0, "max": 15.0},
                        },
                        "near_threshold": {"within_2": 0, "within_5": 0, "within_10": 0},
                        "top_near_threshold_samples": [
                            {
                                "ts_code": "000001.SZ",
                                "as_of_date": "20260101",
                                "final_score": 63.0,
                                "gap_to_threshold": 2.0,
                                "dimension_scores": {"资金流向": 20.0, "短期动量": 13.0},
                                "entry_gate": {
                                    "passed": False,
                                    "mode": "wait_for_pullback_reconfirmation",
                                    "reason": "entry_gate_overheated_wait_for_pullback",
                                },
                            }
                        ],
                        "entry_gate_review": {
                            "observed": 144,
                            "passed": 3,
                            "blocked": 141,
                            "mode_counts": {
                                "pullback_reconfirmed": 3,
                                "wait_for_pullback_reconfirmation": 141,
                            },
                            "reason_counts": {
                                "entry_gate_pullback_reconfirmed": 3,
                                "entry_gate_overheated_wait_for_pullback": 141,
                            },
                            "overheat_flag_counts": {"price_position_extreme": 80},
                            "passed_score_max": 54.0,
                            "blocked_score_max": 63.0,
                        },
                        "entry_gate_passed_samples": [
                            {
                                "ts_code": "000002.SZ",
                                "as_of_date": "20260102",
                                "final_score": 54.0,
                                "gap_to_threshold": 11.0,
                                "entry_gate": {
                                    "passed": True,
                                    "mode": "pullback_reconfirmed",
                                    "reason": "entry_gate_pullback_reconfirmed",
                                },
                            }
                        ],
                        "entry_gate_quality_by_mode": {
                            "pullback_reconfirmed": {
                                "count": 3,
                                "avg_score": 51.0,
                                "max_score": 54.0,
                                "avg_gap_to_threshold": 14.0,
                                "min_gap_to_threshold": 11.0,
                                "base_score": {"count": 3, "avg": 45.0, "min": 39.0, "max": 62.0},
                                "synergy_bonus": {"count": 3, "avg": 11.0, "min": 10.0, "max": 13.0},
                                "risk_penalty": {"count": 3, "avg": 5.0, "min": 0.0, "max": 15.0},
                                "dimension_scores": {
                                    "资金流向": {"count": 3, "avg": 11.5, "min": 8.0, "max": 15.0},
                                    "技术突破": {"count": 3, "avg": 0.0, "min": 0.0, "max": 0.0},
                                },
                                "dimension_shortfall_to_reference": {
                                    "资金流向": {"count": 3, "avg": 10.5, "min": 7.0, "max": 14.0},
                                    "技术突破": {"count": 3, "avg": 5.0, "min": 5.0, "max": 5.0},
                                },
                                "secondary_confirmation_counts": {
                                    "technical_breakout_reconfirmed": 3,
                                    "quality_ready": 2,
                                    "money_flow_returned": 2,
                                    "sector_reheated": 2,
                                    "momentum_alive": 3,
                                    "confirmed_count:3": 2,
                                    "confirmed_count:1": 1,
                                },
                                "secondary_confirmation_metrics": {
                                    "technical_breakthrough_candidate_score": {"count": 3, "avg": 2.0, "min": 1.5, "max": 3.0},
                                    "technical_breakthrough_current_score": {"count": 3, "avg": 0.0, "min": 0.0, "max": 0.0},
                                    "technical_breakthrough_candidate_delta": {"count": 3, "avg": 2.0, "min": 1.5, "max": 3.0},
                                },
                            }
                        },
                        "score_count": 144,
                        "max_score": 38.0,
                        "avg_score": 11.37,
                        "v6_runtime_diagnostics": {
                            "type": "v6_runtime_diagnostics",
                            "point_in_time_context": True,
                            "production_candidate_allowed": False,
                            "candidate_filter_mode": "diagnostic_advisory",
                            "candidate_filter_relaxed_count": 91,
                            "replay_step": 60,
                            "short_cycle_noise_review": {
                                "enabled": True,
                                "replay_step": 60,
                                "coarse_step": True,
                            },
                        },
                    }
                ],
            }
        ],
        errors=[{"error": "rolling backtest produced 0 successful test windows"}],
        backtest_credibility={
            "passed": False,
            "blocking_reasons": [
                "missing_successful_test_windows",
                "missing_positive_signal_density",
            ],
        },
    )

    assert diagnostics["eligible_for_formal_ranking"] is False
    assert "factor_score_distribution_diagnostic_required" not in diagnostics["failure_classes"]
    assert "v6_score_model_calibration_required" in diagnostics["failure_classes"]
    assert "v6_no_near_threshold_samples" in diagnostics["failure_classes"]
    assert "v6_dense_replay_review_required" in diagnostics["failure_classes"]
    assert "v6_entry_gate_reconfirmation_samples_below_candidate_band" in diagnostics["failure_classes"]
    assert "v6_pullback_secondary_confirmation_missing_technical_breakout" not in diagnostics["failure_classes"]
    assert "v6_pullback_technical_confirmation_not_reflected_in_breakthrough_score" in diagnostics["failure_classes"]
    assert diagnostics["window_diagnostics"]["min_threshold"] == 65.0
    assert diagnostics["window_diagnostics"]["near_threshold"]["within_10"] == 0
    assert diagnostics["window_diagnostics"]["top_near_threshold_samples"][0]["ts_code"] == "000001.SZ"
    assert diagnostics["window_diagnostics"]["entry_gate_review"]["mode_counts"]["pullback_reconfirmed"] == 3
    assert diagnostics["window_diagnostics"]["entry_gate_passed_samples"][0]["ts_code"] == "000002.SZ"
    assert diagnostics["window_diagnostics"]["entry_gate_quality_by_mode"]["pullback_reconfirmed"]["avg_gap_to_threshold"] == 14.0
    assert diagnostics["window_diagnostics"]["entry_gate_quality_by_mode"]["pullback_reconfirmed"]["dimension_shortfall_to_reference"]["技术突破"]["avg"] == 5.0
    assert diagnostics["window_diagnostics"]["entry_gate_quality_by_mode"]["pullback_reconfirmed"]["secondary_confirmation_counts"]["momentum_alive"] == 3
    assert diagnostics["window_diagnostics"]["entry_gate_quality_by_mode"]["pullback_reconfirmed"]["secondary_confirmation_metrics"]["technical_breakthrough_candidate_delta"]["avg"] == 2.0
    assert diagnostics["window_diagnostics"]["v6_runtime_review"]["candidate_filter_relaxed_count"] == 91
    assert "calibrate_v6_score_model_against_point_in_time_runtime_distribution_without_lowering_gate" in diagnostics["next_actions"]
    assert "review_v6_pullback_reconfirmed_samples_and_calibrate_quality_dimensions_without_lowering_gate" in diagnostics["next_actions"]
    assert "map_pullback_platform_or_ma_reclaim_confirmation_into_breakthrough_diagnostics_before_score_lift" in diagnostics["next_actions"]


def test_v6_entry_gate_reconfirmation_gap_replaces_generic_distribution_action():
    diagnostics = build_strategy_backtest_diagnostics(
        strategy="v6",
        rows=[],
        errors=[
            {
                "error": "rolling backtest produced 0 successful test windows",
                "failure_diagnostics": [
                    {
                        "type": "score_distribution",
                        "strategy": "v6",
                        "threshold": 65.0,
                        "evaluated": 300,
                        "passed_threshold": 0,
                        "missing_score": 0,
                        "reason_counts": {"below_threshold": 300},
                        "near_threshold": {"within_2": 1, "within_5": 3, "within_10": 6},
                        "score_count": 300,
                        "max_score": 63.0,
                        "avg_score": 20.0,
                        "entry_gate_review": {
                            "observed": 300,
                            "passed": 130,
                            "blocked": 170,
                            "mode_counts": {
                                "pullback_reconfirmed": 23,
                                "direct_strength_entry": 107,
                                "wait_for_pullback_reconfirmation": 83,
                                "no_tradeable_entry": 87,
                            },
                            "reason_counts": {
                                "entry_gate_pullback_reconfirmed": 23,
                                "entry_gate_overheated_wait_for_pullback": 83,
                            },
                            "overheat_flag_counts": {"price_position_extreme": 49},
                            "passed_score_max": 53.5,
                            "blocked_score_max": 63.0,
                        },
                        "entry_gate_passed_samples": [
                            {
                                "ts_code": "300274.SZ",
                                "as_of_date": "20251226",
                                "final_score": 53.5,
                                "entry_gate": {"passed": True, "mode": "pullback_reconfirmed"},
                            }
                        ],
                        "entry_gate_quality_by_mode": {
                            "pullback_reconfirmed": {
                                "count": 23,
                                "avg_score": 42.0,
                                "max_score": 53.5,
                                "avg_gap_to_threshold": 23.0,
                                "min_gap_to_threshold": 11.5,
                                "dimension_scores": {
                                    "资金流向": {"count": 23, "avg": 9.0, "min": 0.0, "max": 15.0},
                                    "技术突破": {"count": 23, "avg": 0.5, "min": 0.0, "max": 3.0},
                                },
                                "dimension_shortfall_to_reference": {
                                    "资金流向": {"count": 23, "avg": 13.0, "min": 7.0, "max": 22.0},
                                    "技术突破": {"count": 23, "avg": 4.5, "min": 2.0, "max": 5.0},
                                },
                            }
                        },
                    }
                ],
            }
        ],
        backtest_credibility={
            "passed": False,
            "blocking_reasons": ["missing_successful_test_windows", "missing_positive_signal_density"],
        },
    )

    assert "v6_score_model_calibration_required" not in diagnostics["failure_classes"]
    assert "factor_score_distribution_diagnostic_required" not in diagnostics["failure_classes"]
    assert "v6_entry_gate_reconfirmation_samples_below_candidate_band" in diagnostics["failure_classes"]
    assert diagnostics["window_diagnostics"]["entry_gate_review"]["passed_score_max"] == 53.5
    assert diagnostics["window_diagnostics"]["entry_gate_passed_samples"][0]["ts_code"] == "300274.SZ"
    assert diagnostics["window_diagnostics"]["entry_gate_quality_by_mode"]["pullback_reconfirmed"]["max_score"] == 53.5
    assert diagnostics["window_diagnostics"]["entry_gate_quality_by_mode"]["pullback_reconfirmed"]["dimension_shortfall_to_reference"]["资金流向"]["avg"] == 13.0
    assert "review_v6_pullback_reconfirmed_samples_and_calibrate_quality_dimensions_without_lowering_gate" in diagnostics["next_actions"]


def test_v6_pullback_secondary_confirmation_missing_breakout_is_classified():
    diagnostics = build_strategy_backtest_diagnostics(
        strategy="v6",
        rows=[],
        errors=[
            {
                "error": "rolling backtest produced 0 successful test windows",
                "failure_diagnostics": [
                    {
                        "type": "score_distribution",
                        "strategy": "v6",
                        "threshold": 65.0,
                        "evaluated": 20,
                        "passed_threshold": 0,
                        "reason_counts": {"below_threshold": 20},
                        "near_threshold": {"within_2": 0, "within_5": 0, "within_10": 2},
                        "score_count": 20,
                        "max_score": 58.0,
                        "entry_gate_review": {
                            "observed": 20,
                            "passed": 5,
                            "blocked": 15,
                            "mode_counts": {"pullback_reconfirmed": 5, "wait_for_pullback_reconfirmation": 3},
                            "passed_score_max": 52.0,
                            "blocked_score_max": 58.0,
                        },
                        "entry_gate_quality_by_mode": {
                            "pullback_reconfirmed": {
                                "count": 5,
                                "max_score": 52.0,
                                "dimension_scores": {"技术突破": {"count": 5, "avg": 0.0, "min": 0.0, "max": 0.0}},
                                "secondary_confirmation_counts": {"money_flow_returned": 2, "confirmed_count:2": 5},
                            }
                        },
                    }
                ],
            }
        ],
        backtest_credibility={"passed": False, "blocking_reasons": ["missing_positive_signal_density"]},
    )

    assert "v6_pullback_secondary_confirmation_missing_technical_breakout" in diagnostics["failure_classes"]
    assert "v6_pullback_technical_confirmation_not_reflected_in_breakthrough_score" not in diagnostics["failure_classes"]
    assert "require_pullback_reconfirmed_samples_to_show_technical_breakout_before_any_score_lift" in diagnostics["next_actions"]


def test_combo_component_score_distribution_is_aggregated():
    diagnostics = build_strategy_backtest_diagnostics(
        strategy="combo",
        rows=[
            {
                "status": "failed",
                "failure_diagnostics": [
                    {
                        "type": "combo_consensus",
                        "strategy": "combo",
                        "evaluated": 10,
                        "component_available": {"v5": 10, "v8": 10, "v9": 10},
                        "component_pass": {"v5": 2, "v8": 0, "v9": 4},
                        "component_score_stats": {
                            "v8": {"count": 10, "avg": 51.0, "min": 30.0, "max": 64.0, "p50": 52.0},
                            "v9": {"count": 10, "avg": 63.0, "min": 40.0, "max": 88.0, "p50": 65.0},
                        },
                        "component_timing_ms": {
                            "v8": {"count": 10, "avg": 120.0, "max": 500.0},
                            "v9": {"count": 10, "avg": 12.0, "max": 30.0},
                        },
                        "component_score_cache": {
                            "v8": {"hit": 3, "miss": 7, "hit_rate": 0.3},
                        },
                        "v8_stage_timing_ms": {
                            "base_evaluator": {"count": 10, "avg": 80.0, "max": 200.0},
                            "advanced_factors": {"count": 10, "avg": 15.0, "max": 40.0},
                        },
                        "v7_stage_timing_ms": {
                            "v4_score": {"count": 10, "avg": 70.0, "max": 190.0},
                        },
                        "v5_score_breakdown": {
                            "final_score": {"count": 10, "avg": 48.0, "min": 30.0, "max": 59.0},
                            "base_score": {"count": 10, "avg": 44.0, "min": 28.0, "max": 55.0},
                            "synergy_bonus": {"count": 10, "avg": 3.0, "min": 0.0, "max": 8.0},
                            "risk_penalty": {"count": 10, "avg": 5.0, "min": 0.0, "max": 18.0},
                            "dim:启动确认": {"count": 10, "avg": 6.0, "min": 0.0, "max": 15.0},
                        },
                        "v5_synergy_combo_counts": {"无协同": 7, "量价启动": 3},
                        "v5_risk_reason_counts": {"放量过快": 4},
                        "v5_candidate_filter": {
                            "total": 10,
                            "applicable": 2,
                            "filtered_out": 8,
                            "applicable_rate": 0.2,
                            "reason_counts": {"launch_confirmation_below_candidate_floor": 8},
                        },
                        "component_near_threshold": {
                            "v8": {"pass": 0, "within_5": 1, "within_10": 2, "far_below": 7},
                        },
                        "base_weights": {"v5": 0.45, "v8": 0.05, "v9": 0.50},
                        "health_multipliers": {"v5": 0.8, "v8": 0.6, "v9": 0.2},
                        "pre_normalized_weights": {"v5": 0.36, "v8": 0.03, "v9": 0.10},
                        "weights": {"v5": 0.7346938775510206, "v8": 0.061224489795918366, "v9": 0.20408163265306123},
                        "pair_agreement": {"v5+v9": 1},
                        "weighted_consensus_candidates": {
                            "count": 1,
                            "avg": 58.0,
                            "min": 58.0,
                            "max": 58.0,
                            "below_combo_threshold": 1,
                            "avg_gap": 2.0,
                            "max_gap": 2.0,
                        },
                        "agree_count_histogram": {"0": 6, "1": 4},
                        "drop_reasons": {"not_enough_component_agreement": 10},
                    }
                ],
            }
        ],
        errors=[],
        backtest_credibility={"passed": False, "blocking_reasons": ["missing_successful_test_windows"]},
    )

    window_diag = diagnostics["window_diagnostics"]
    assert window_diag["component_pass_rate"]["v8"] == 0.0
    assert window_diag["component_score_stats"]["v8"]["max"] == 64.0
    assert window_diag["component_score_stats"]["v9"]["avg"] == 63.0
    assert window_diag["component_timing_ms"]["v8"]["avg"] == 120.0
    assert window_diag["component_timing_ms"]["v8"]["max"] == 500.0
    assert window_diag["component_score_cache"]["v8"]["hit_rate"] == 0.3
    assert window_diag["v8_stage_timing_ms"]["base_evaluator"]["avg"] == 80.0
    assert window_diag["v7_stage_timing_ms"]["v4_score"]["avg"] == 70.0
    assert window_diag["v5_score_breakdown"]["final_score"]["avg"] == 48.0
    assert window_diag["v5_score_breakdown"]["dim:启动确认"]["max"] == 15.0
    assert window_diag["v5_synergy_combo_counts"]["无协同"] == 7
    assert window_diag["v5_risk_reason_counts"]["放量过快"] == 4
    assert window_diag["v5_candidate_filter"]["applicable_rate"] == 0.2
    assert window_diag["v5_candidate_filter"]["reason_counts"]["launch_confirmation_below_candidate_floor"] == 8
    assert window_diag["component_near_threshold"]["v8"]["within_10"] == 2
    assert window_diag["weight_context"]["health_multipliers"]["v9"] == 0.2
    assert window_diag["pair_agreement"]["v5+v9"] == 1
    assert window_diag["weighted_consensus_candidates"]["avg_gap"] == 2.0


def test_v5_runtime_constraints_remove_legacy_failure_class():
    diagnostics = build_strategy_backtest_diagnostics(
        strategy="v5",
        rows=[
            {
                "status": "success",
                "win_rate": 0.34,
                "max_drawdown": 0.29,
                "signal_density": 0.3,
                "tradeability_filter_enabled": True,
                "volume_constraint_enabled": True,
            }
        ],
        errors=[],
        backtest_credibility={"passed": False, "blocking_reasons": ["missing_parameter_sensitivity"]},
    )

    assert "legacy_return_sample_not_credible_without_constraints" not in diagnostics["failure_classes"]
    assert "weak_out_of_sample_win_rate" in diagnostics["failure_classes"]
    assert "drawdown_above_quality_floor" in diagnostics["failure_classes"]
