from __future__ import annotations

from openclaw.services.ensemble_rebuilt_candidate_rule_freeze_service import build_rebuilt_candidate_rule_freeze


def test_rebuilt_candidate_rule_freeze_requires_discussion_eligible_policy_audit():
    review = build_rebuilt_candidate_rule_freeze(
        {
            "candidate": "hard_event_alpha_candidate",
            "candidate_discussion_eligible": True,
            "sleeve_policy_approved": False,
            "observation_pool_eligible": False,
            "formal_pool_eligible": False,
        }
    )

    assert review["research_only"] is True
    assert review["frozen"] is True
    assert review["rule_version"] == "hard_event_alpha_candidate.neutral_consensus_turnover_guard.v5"
    assert review["rule_hash"]
    assert "risk_off_exhaustion_veto" in review["rule_spec"]
    assert "neutral_noise_turnover_guard" in review["rule_spec"]
    assert "neutral_consensus_turnover_guard" in review["rule_spec"]
    assert "source_strategy != combo and hard_event_alpha>=30 and capacity_liquidity>=35 and money_flow_persistence>=50" in review["rule_spec"]["risk_off_exhaustion_veto"]["veto_rules"]
    assert "hard_event_alpha<40" in review["rule_spec"]["neutral_noise_turnover_guard"]["veto_rules"]
    assert "source_strategy != combo and cross_strategy_source_count<2 and construction_score<45" in review["rule_spec"]["neutral_consensus_turnover_guard"]["veto_rules"]
    assert review["sleeve_policy_approved"] is False
    assert review["observation_pool_eligible"] is False
    assert review["formal_pool_eligible"] is False
    assert review["blocking_reasons"] == []


def test_rebuilt_candidate_rule_freeze_supports_predeclared_v6_candidate():
    review = build_rebuilt_candidate_rule_freeze(
        {
            "candidate": "hard_event_alpha_candidate",
            "candidate_discussion_eligible": True,
            "sleeve_policy_approved": False,
            "observation_pool_eligible": False,
            "formal_pool_eligible": False,
        },
        rule_version="hard_event_alpha_candidate.neutral_over_veto_rebalance_guard.v6",
    )

    assert review["frozen"] is True
    assert review["rule_version"] == "hard_event_alpha_candidate.neutral_over_veto_rebalance_guard.v6"
    assert review["rule_hash"]
    assert "neutral_over_veto_rebalance_guard" in review["rule_spec"]
    assert "predeclared_failure_targets" in review["rule_spec"]
    assert review["sleeve_policy_approved"] is False
    assert review["formal_pool_eligible"] is False


def test_rebuilt_candidate_rule_freeze_blocks_noneligible_policy_audit():
    review = build_rebuilt_candidate_rule_freeze(
        {
            "candidate": "hard_event_alpha_candidate",
            "candidate_discussion_eligible": False,
            "sleeve_policy_approved": False,
        }
    )

    assert review["frozen"] is False
    assert "candidate_policy_discussion_not_eligible" in review["blocking_reasons"]
