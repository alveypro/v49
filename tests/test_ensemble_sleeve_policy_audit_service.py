from __future__ import annotations

from openclaw.services.ensemble_sleeve_policy_audit_service import (
    build_ensemble_sleeve_policy_audit,
    build_rebuilt_alpha_candidate_sleeve_policy_audit,
)


def test_ensemble_sleeve_policy_audit_separates_alpha_filters_and_blocked_sleeves():
    review = build_ensemble_sleeve_policy_audit(
        {
            "sleeve_use_policy": {
                "momentum": "positive_alpha_candidate",
                "money_flow": "research_blocked_negative_ic",
                "quality_low_vol": "risk_filter_candidate",
                "sector_rotation": "risk_filter_candidate",
            },
            "sleeves": {
                "momentum": {
                    "active_signal_count": 12,
                    "ic": 0.08,
                    "rank_ic": 0.04,
                    "multi_horizon_attribution": {
                        "positive_horizon_count": 3,
                        "positive_rank_horizon_count": 2,
                    },
                },
                "money_flow": {
                    "active_signal_count": 12,
                    "ic": -0.02,
                    "rank_ic": -0.01,
                    "multi_horizon_attribution": {
                        "positive_horizon_count": 1,
                        "positive_rank_horizon_count": 0,
                    },
                },
                "quality_low_vol": {
                    "active_signal_count": 12,
                    "ic": 0.2,
                    "rank_ic": 0.2,
                    "multi_horizon_attribution": {
                        "positive_horizon_count": 5,
                        "positive_rank_horizon_count": 5,
                    },
                },
                "sector_rotation": {
                    "active_signal_count": 12,
                    "ic": -0.2,
                    "rank_ic": -0.1,
                    "multi_horizon_attribution": {
                        "positive_horizon_count": 0,
                        "positive_rank_horizon_count": 0,
                    },
                },
            },
        }
    )

    assert review["passed"] is True
    assert review["alpha_candidate_sleeves"] == ["momentum"]
    assert review["risk_filter_sleeves"] == ["quality_low_vol", "sector_rotation"]
    assert "money_flow" in review["blocked_sleeves"]
    assert set(review["excluded_sleeves"]) == {"money_flow", "quality_low_vol", "sector_rotation"}
    assert review["policy_violations"] == []


def test_ensemble_sleeve_policy_audit_blocks_promoted_negative_or_filter_sleeves():
    review = build_ensemble_sleeve_policy_audit(
        {
            "sleeve_use_policy": {
                "money_flow": "positive_alpha_candidate",
                "quality_low_vol": "positive_alpha_candidate",
            },
            "sleeves": {
                "money_flow": {
                    "active_signal_count": 10,
                    "ic": -0.01,
                    "rank_ic": 0.02,
                    "multi_horizon_attribution": {
                        "positive_horizon_count": 2,
                        "positive_rank_horizon_count": 2,
                    },
                },
                "quality_low_vol": {
                    "active_signal_count": 10,
                    "ic": 0.3,
                    "rank_ic": 0.3,
                    "multi_horizon_attribution": {
                        "positive_horizon_count": 5,
                        "positive_rank_horizon_count": 5,
                    },
                },
            },
        }
    )

    assert review["passed"] is False
    assert "money_flow:non_positive_5d_ic" in review["policy_violations"]
    assert "quality_low_vol:risk_filter_only_sleeve" in review["policy_violations"]
    assert "quality_low_vol:risk_filter_only_sleeve_promoted_as_alpha" in review["policy_violations"]
    assert review["alpha_candidate_sleeves"] == []


def test_rebuilt_alpha_candidate_policy_audit_allows_discussion_but_not_approval():
    review = build_rebuilt_alpha_candidate_sleeve_policy_audit(
        {
            "walk_forward": {
                "research_only": True,
                "candidate": "hard_event_alpha_candidate",
                "passed_predeclared_walk_forward_gate": True,
                "blocking_reasons": [],
                "predeclared_gate": {
                    "declared_before_validation": True,
                    "source_strategy_filter_allowed": False,
                    "scenario_selection_allowed": False,
                },
                "validation_review": {
                    "retained_window_count": 4,
                    "positive_retained_window_count": 4,
                    "excluded_window_count": 2,
                    "sample_count": 200,
                    "raw_sample_count": 300,
                    "sample_retention": 0.666667,
                    "ic": 0.36,
                    "rank_ic": 0.31,
                    "retained_window_reviews": [
                        {"as_of_date": "20260224", "ic": 0.1, "rank_ic": 0.1},
                        {"as_of_date": "20260226", "ic": 0.1, "rank_ic": 0.1},
                        {"as_of_date": "20260305", "ic": 0.1, "rank_ic": 0.1},
                        {"as_of_date": "20260306", "ic": 0.1, "rank_ic": 0.1},
                    ],
                },
            }
        }
    )

    assert review["candidate_discussion_eligible"] is True
    assert review["sleeve_policy_approved"] is False
    assert review["observation_pool_eligible"] is False
    assert review["formal_pool_eligible"] is False
    assert review["blocking_reasons"] == []
    assert "after_cost_shadow_benchmark" in review["required_next_evidence"]
    assert "candidate_discussion_eligibility_is_not_sleeve_approval" in review["hard_boundaries"]


def test_rebuilt_alpha_candidate_policy_audit_blocks_failed_or_posthoc_walk_forward():
    review = build_rebuilt_alpha_candidate_sleeve_policy_audit(
        {
            "walk_forward": {
                "research_only": True,
                "candidate": "hard_event_alpha_candidate",
                "passed_predeclared_walk_forward_gate": False,
                "blocking_reasons": ["not_all_retained_windows_positive_ic_and_rank_ic"],
                "predeclared_gate": {
                    "declared_before_validation": False,
                    "source_strategy_filter_allowed": True,
                    "scenario_selection_allowed": True,
                },
                "validation_review": {
                    "retained_window_count": 3,
                    "positive_retained_window_count": 2,
                    "excluded_window_count": 0,
                    "sample_count": 150,
                    "raw_sample_count": 200,
                    "sample_retention": 0.75,
                    "ic": -0.01,
                    "rank_ic": 0.02,
                },
            }
        }
    )

    assert review["candidate_discussion_eligible"] is False
    assert "predeclared_walk_forward_not_passed" in review["blocking_reasons"]
    assert "walk_forward_has_blocking_reasons" in review["blocking_reasons"]
    assert "gate_not_declared_before_validation" in review["blocking_reasons"]
    assert "source_strategy_filter_was_allowed" in review["blocking_reasons"]
    assert "scenario_selection_was_allowed" in review["blocking_reasons"]
    assert "retained_population_ic_not_positive" in review["blocking_reasons"]
