from __future__ import annotations

from openclaw.services.ensemble_shadow_portfolio_service import (
    build_ensemble_shadow_portfolio,
    build_rebuilt_candidate_shadow_portfolio,
)


def test_ensemble_shadow_portfolio_blocks_without_alpha_candidates():
    review = build_ensemble_shadow_portfolio(
        {
            "sleeve_policy_audit": {
                "passed": True,
                "alpha_candidate_sleeves": [],
                "risk_filter_sleeves": ["quality_low_vol"],
                "excluded_sleeves": ["momentum", "quality_low_vol"],
            },
            "sample_facts": [
                {
                    "ts_code": "000001.SZ",
                    "sleeve_scores": {"quality_low_vol": {"score": 90.0}},
                }
            ],
        }
    )

    assert review["research_only"] is True
    assert review["not_for_production"] is True
    assert review["shadow_weights"] == []
    assert review["cash_weight"] == 1.0
    assert "missing_positive_alpha_candidate_sleeves" in review["blocking_reasons"]


def test_ensemble_shadow_portfolio_builds_constrained_research_weights_from_alpha_budget():
    review = build_ensemble_shadow_portfolio(
        {
            "sleeve_policy_audit": {
                "passed": True,
                "alpha_candidate_sleeves": ["momentum", "reversal"],
                "risk_filter_sleeves": ["quality_low_vol"],
                "excluded_sleeves": ["money_flow", "quality_low_vol"],
            },
            "sample_facts": [
                _fact("000001.SZ", "电子", 90.0, 40.0, 80.0, 1_000_000.0),
                _fact("000002.SZ", "电子", 75.0, 55.0, 70.0, 1_000_000.0),
                _fact("000003.SZ", "医药", 60.0, 70.0, 50.0, 1_000_000.0),
            ],
        },
        max_positions=3,
        portfolio_value=1_000_000.0,
        single_name_cap=0.2,
        industry_cap=0.3,
        capacity_participation=0.1,
        alpha_risk_budget={"momentum": 0.7, "reversal": 0.3},
    )

    assert review["research_only"] is True
    assert review["not_for_production"] is True
    assert review["blocking_reasons"] == []
    assert review["risk_budget"] == {"momentum": 0.7, "reversal": 0.3}
    assert review["shadow_weights"]
    assert sum(item["weight"] for item in review["shadow_weights"]) <= 1.0
    assert review["industry_exposure"]["电子"] <= 0.3
    assert "money_flow" in review["excluded_sleeves"]
    assert "quality_low_vol" in review["excluded_sleeves"]
    assert "do_not_bypass_after_cost_benchmark_before_observation" in review["hard_boundaries"]


def test_rebuilt_candidate_shadow_portfolio_requires_frozen_discussion_eligible_candidate():
    blocked = build_rebuilt_candidate_shadow_portfolio(
        {"research_only": True, "sample_facts": [_hard_event_fact("000001.SZ", "电子", 60.0, 1_000_000.0)]},
        candidate_policy_audit={"candidate": "hard_event_alpha_candidate", "candidate_discussion_eligible": False},
        rule_freeze={"candidate": "hard_event_alpha_candidate", "frozen": False},
    )

    assert blocked["shadow_weights"] == []
    assert "candidate_policy_discussion_not_eligible" in blocked["blocking_reasons"]
    assert "candidate_rule_not_frozen" in blocked["blocking_reasons"]


def test_rebuilt_candidate_shadow_portfolio_builds_research_only_weights():
    review = build_rebuilt_candidate_shadow_portfolio(
        {
            "research_only": True,
            "sample_facts": [
                _hard_event_fact("000001.SZ", "电子", 70.0, 1_000_000.0),
                _hard_event_fact("000001.SZ", "电子", 69.0, 1_000_000.0),
                _hard_event_fact("000002.SZ", "电子", 65.0, 1_000_000.0),
                _hard_event_fact("000003.SZ", "医药", 55.0, 1_000_000.0),
            ],
        },
        candidate_policy_audit={
            "candidate": "hard_event_alpha_candidate",
            "candidate_discussion_eligible": True,
            "sleeve_policy_approved": False,
            "observation_pool_eligible": False,
            "formal_pool_eligible": False,
        },
        rule_freeze={
            "candidate": "hard_event_alpha_candidate",
            "frozen": True,
            "rule_version": "hard_event_alpha_candidate.exhaustion_penalty.v1",
            "rule_hash": "abc123",
            "sleeve_policy_approved": False,
        },
        max_positions=3,
        portfolio_value=1_000_000.0,
        single_name_cap=0.2,
        industry_cap=0.3,
        capacity_participation=0.1,
    )

    assert review["research_only"] is True
    assert review["not_for_production"] is True
    assert review["candidate_discussion_eligible"] is True
    assert review["sleeve_policy_approved"] is False
    assert review["shadow_weights"]
    assert len({item["ts_code"] for item in review["shadow_weights"]}) == len(review["shadow_weights"])
    assert review["industry_exposure"]["电子"] <= 0.3
    assert sum(item["weight"] for item in review["shadow_weights"]) <= 1.0
    assert "do_not_promote_without_after_cost_shadow_benchmark" in review["hard_boundaries"]


def test_rebuilt_candidate_shadow_portfolio_throttles_neutral_regime_exposure():
    review = build_rebuilt_candidate_shadow_portfolio(
        {
            "research_only": True,
            "sample_facts": [
                _hard_event_fact("000001.SZ", "银行", 80.0, 1_000_000.0),
                _hard_event_fact("000002.SZ", "银行", 75.0, 1_000_000.0),
                _hard_event_fact("000003.SZ", "银行", 70.0, 1_000_000.0),
                _hard_event_fact("000004.SZ", "医药", 68.0, 1_000_000.0),
                _hard_event_fact("000005.SZ", "电子", 64.0, 1_000_000.0),
            ],
        },
        candidate_policy_audit={
            "candidate": "hard_event_alpha_candidate",
            "candidate_discussion_eligible": True,
            "sleeve_policy_approved": False,
            "observation_pool_eligible": False,
            "formal_pool_eligible": False,
        },
        rule_freeze={
            "candidate": "hard_event_alpha_candidate",
            "frozen": True,
            "rule_version": "hard_event_alpha_candidate.exhaustion_penalty.v1",
            "rule_hash": "abc123",
            "sleeve_policy_approved": False,
        },
        market_regime_label="neutral",
        max_positions=5,
        portfolio_value=1_000_000.0,
        single_name_cap=0.2,
        industry_cap=0.28,
        capacity_participation=0.2,
        target_gross_exposure=0.75,
        neutral_gross_exposure=0.45,
    )

    assert review["blocking_reasons"] == []
    assert review["allocator_controls"]["market_regime_label"] == "neutral"
    assert review["allocator_controls"]["target_gross_exposure"] == 0.45
    assert review["turnover_estimate"] <= 0.45
    assert max(review["industry_exposure"].values()) < 0.3
    assert any(item["constraint"] == "gross_exposure_throttle" for item in review["constraint_hits"])
    assert "allocator_throttle_is_not_alpha_improvement" in review["hard_boundaries"]


def test_rebuilt_candidate_shadow_portfolio_vetoes_risk_off_exhaustion_structures():
    review = build_rebuilt_candidate_shadow_portfolio(
        {
            "research_only": True,
            "sample_facts": [
                _hard_event_fact("000001.SZ", "银行", 80.0, 1_000_000.0, strategy="v8", capacity=75, flow=68, seat=75, limit_structure=55),
                _hard_event_fact("000002.SZ", "医药", 55.0, 1_000_000.0, strategy="combo", capacity=55, flow=50, seat=45, limit_structure=45),
            ],
        },
        candidate_policy_audit={
            "candidate": "hard_event_alpha_candidate",
            "candidate_discussion_eligible": True,
            "sleeve_policy_approved": False,
            "observation_pool_eligible": False,
            "formal_pool_eligible": False,
        },
        rule_freeze={
            "candidate": "hard_event_alpha_candidate",
            "frozen": True,
            "rule_version": "hard_event_alpha_candidate.risk_off_exhaustion_veto.v3",
            "rule_hash": "abc123",
            "sleeve_policy_approved": False,
        },
        market_regime_label="risk_off",
        max_positions=2,
        portfolio_value=1_000_000.0,
        single_name_cap=0.2,
        industry_cap=0.3,
        capacity_participation=0.2,
    )

    assert [item["ts_code"] for item in review["shadow_weights"]] == ["000002.SZ"]
    assert review["risk_off_vetoed_signals"][0]["ts_code"] == "000001.SZ"
    assert "risk_off" in review["risk_off_vetoed_signals"][0]["reason"]


def test_rebuilt_candidate_shadow_portfolio_vetoes_risk_off_non_consensus_hard_event():
    review = build_rebuilt_candidate_shadow_portfolio(
        {
            "research_only": True,
            "sample_facts": [
                _hard_event_fact("000001.SZ", "银行", 52.0, 1_000_000.0, strategy="v4", capacity=45, flow=55, seat=45, limit_structure=45),
                _hard_event_fact("000002.SZ", "医药", 52.0, 1_000_000.0, strategy="combo", capacity=45, flow=55, seat=45, limit_structure=45),
            ],
        },
        candidate_policy_audit={
            "candidate": "hard_event_alpha_candidate",
            "candidate_discussion_eligible": True,
            "sleeve_policy_approved": False,
            "observation_pool_eligible": False,
            "formal_pool_eligible": False,
        },
        rule_freeze={
            "candidate": "hard_event_alpha_candidate",
            "frozen": True,
            "rule_version": "hard_event_alpha_candidate.risk_off_exhaustion_veto.v3",
            "rule_hash": "abc123",
            "sleeve_policy_approved": False,
        },
        market_regime_label="risk_off",
        max_positions=2,
        portfolio_value=1_000_000.0,
        single_name_cap=0.2,
        industry_cap=0.3,
        capacity_participation=0.2,
    )

    assert [item["ts_code"] for item in review["shadow_weights"]] == ["000002.SZ"]
    assert review["risk_off_vetoed_signals"][0]["ts_code"] == "000001.SZ"
    assert "risk_off_hard_event_without_cross_strategy_consensus" in review["risk_off_vetoed_signals"][0]["reason"]


def test_rebuilt_candidate_shadow_portfolio_v4_vetoes_neutral_low_confirmation_noise():
    review = build_rebuilt_candidate_shadow_portfolio(
        {
            "research_only": True,
            "sample_facts": [
                _hard_event_fact("000001.SZ", "银行", 39.0, 1_000_000.0, strategy="v4", capacity=55, flow=60, seat=60, limit_structure=60),
                _hard_event_fact("000002.SZ", "医药", 45.0, 1_000_000.0, strategy="v8", capacity=55, flow=55, seat=60, limit_structure=60),
                _hard_event_fact("000003.SZ", "电子", 50.0, 1_000_000.0, strategy="v4", capacity=55, flow=56, seat=50, limit_structure=50),
            ],
        },
        candidate_policy_audit={
            "candidate": "hard_event_alpha_candidate",
            "candidate_discussion_eligible": True,
            "sleeve_policy_approved": False,
            "observation_pool_eligible": False,
            "formal_pool_eligible": False,
        },
        rule_freeze={
            "candidate": "hard_event_alpha_candidate",
            "frozen": True,
            "rule_version": "hard_event_alpha_candidate.neutral_noise_turnover_guard.v4",
            "rule_hash": "abc123",
            "sleeve_policy_approved": False,
        },
        market_regime_label="neutral",
        max_positions=3,
        portfolio_value=1_000_000.0,
        single_name_cap=0.2,
        industry_cap=0.3,
        capacity_participation=0.2,
        target_gross_exposure=1.0,
        neutral_gross_exposure=1.0,
    )

    assert [item["ts_code"] for item in review["shadow_weights"]] == ["000003.SZ"]
    assert [item["ts_code"] for item in review["neutral_vetoed_signals"]] == ["000001.SZ", "000002.SZ"]
    assert "neutral_hard_event_below_conviction_floor" in review["neutral_vetoed_signals"][0]["reason"]
    assert "neutral_high_churn_source_without_flow_confirmation:v8" in review["neutral_vetoed_signals"][1]["reason"]
    assert review["turnover_estimate"] == 0.2


def test_rebuilt_candidate_shadow_portfolio_v4_keeps_risk_off_exhaustion_veto():
    review = build_rebuilt_candidate_shadow_portfolio(
        {
            "research_only": True,
            "sample_facts": [
                _hard_event_fact("000001.SZ", "银行", 80.0, 1_000_000.0, strategy="v8", capacity=75, flow=68, seat=75, limit_structure=55),
                _hard_event_fact("000002.SZ", "医药", 55.0, 1_000_000.0, strategy="combo", capacity=55, flow=50, seat=45, limit_structure=45),
            ],
        },
        candidate_policy_audit={
            "candidate": "hard_event_alpha_candidate",
            "candidate_discussion_eligible": True,
            "sleeve_policy_approved": False,
            "observation_pool_eligible": False,
            "formal_pool_eligible": False,
        },
        rule_freeze={
            "candidate": "hard_event_alpha_candidate",
            "frozen": True,
            "rule_version": "hard_event_alpha_candidate.neutral_noise_turnover_guard.v4",
            "rule_hash": "abc123",
            "sleeve_policy_approved": False,
        },
        market_regime_label="risk_off",
        max_positions=2,
        portfolio_value=1_000_000.0,
        single_name_cap=0.2,
        industry_cap=0.3,
        capacity_participation=0.2,
    )

    assert [item["ts_code"] for item in review["shadow_weights"]] == ["000002.SZ"]
    assert review["risk_off_vetoed_signals"][0]["ts_code"] == "000001.SZ"
    assert review["neutral_vetoed_signals"] == []


def test_rebuilt_candidate_shadow_portfolio_v5_requires_neutral_consensus_or_conviction():
    review = build_rebuilt_candidate_shadow_portfolio(
        {
            "research_only": True,
            "sample_facts": [
                _hard_event_fact("000001.SZ", "银行", 48.0, 1_000_000.0, strategy="v4", capacity=55, flow=56, seat=50, limit_structure=50),
                _hard_event_fact("000002.SZ", "医药", 50.0, 1_000_000.0, strategy="v4", capacity=55, flow=56, seat=50, limit_structure=50),
                _hard_event_fact("000002.SZ", "医药", 50.0, 1_000_000.0, strategy="v8", capacity=55, flow=56, seat=50, limit_structure=50),
                _hard_event_fact("000003.SZ", "电子", 66.0, 1_000_000.0, strategy="v4", capacity=55, flow=58, seat=58, limit_structure=58),
            ],
        },
        candidate_policy_audit={
            "candidate": "hard_event_alpha_candidate",
            "candidate_discussion_eligible": True,
            "sleeve_policy_approved": False,
            "observation_pool_eligible": False,
            "formal_pool_eligible": False,
        },
        rule_freeze={
            "candidate": "hard_event_alpha_candidate",
            "frozen": True,
            "rule_version": "hard_event_alpha_candidate.neutral_consensus_turnover_guard.v5",
            "rule_hash": "abc123",
            "sleeve_policy_approved": False,
        },
        market_regime_label="neutral",
        max_positions=3,
        portfolio_value=1_000_000.0,
        single_name_cap=0.2,
        industry_cap=0.3,
        capacity_participation=0.2,
        target_gross_exposure=1.0,
        neutral_gross_exposure=1.0,
    )

    assert [item["ts_code"] for item in review["shadow_weights"]] == ["000003.SZ", "000002.SZ"]
    assert [item["ts_code"] for item in review["neutral_vetoed_signals"]] == ["000001.SZ"]
    assert "neutral_single_source_low_conviction_turnover_churn" in review["neutral_vetoed_signals"][0]["reason"]
    assert review["shadow_weights"][1]["cross_strategy_source_count"] == 2


def test_rebuilt_candidate_shadow_portfolio_v6_reduces_neutral_over_veto_without_disabling_guard():
    review = build_rebuilt_candidate_shadow_portfolio(
        {
            "research_only": True,
            "sample_facts": [
                _hard_event_fact("000001.SZ", "银行", 48.0, 1_000_000.0, strategy="v4", capacity=55, flow=56, seat=50, limit_structure=50),
                _hard_event_fact("000002.SZ", "医药", 37.0, 1_000_000.0, strategy="v4", capacity=55, flow=56, seat=50, limit_structure=50),
            ],
        },
        candidate_policy_audit={
            "candidate": "hard_event_alpha_candidate",
            "candidate_discussion_eligible": True,
            "sleeve_policy_approved": False,
            "observation_pool_eligible": False,
            "formal_pool_eligible": False,
        },
        rule_freeze={
            "candidate": "hard_event_alpha_candidate",
            "frozen": True,
            "rule_version": "hard_event_alpha_candidate.neutral_over_veto_rebalance_guard.v6",
            "rule_hash": "abc123",
            "sleeve_policy_approved": False,
        },
        market_regime_label="neutral",
        max_positions=3,
        portfolio_value=1_000_000.0,
        single_name_cap=0.2,
        industry_cap=0.3,
        capacity_participation=0.2,
        target_gross_exposure=1.0,
        neutral_gross_exposure=1.0,
    )

    assert [item["ts_code"] for item in review["shadow_weights"]] == ["000001.SZ"]
    assert [item["ts_code"] for item in review["neutral_vetoed_signals"]] == ["000002.SZ"]
    assert "neutral_v6_hard_event_below_conviction_floor" in review["neutral_vetoed_signals"][0]["reason"]


def _fact(
    ts_code: str,
    industry: str,
    momentum: float,
    reversal: float,
    quality_low_vol: float,
    amount: float,
) -> dict:
    return {
        "ts_code": ts_code,
        "strategy": "v5",
        "sleeve_scores": {
            "momentum": {"score": momentum},
            "reversal": {"score": reversal},
            "quality_low_vol": {"score": quality_low_vol},
            "money_flow": {"score": 100.0},
        },
        "tushare_pro_alpha_features": {
            "evidence": {
                "industry": industry,
                "latest_amount": amount,
            }
        },
    }


def _hard_event_fact(
    ts_code: str,
    industry: str,
    hard_event: float,
    amount: float,
    *,
    strategy: str = "v4",
    capacity: float = 55.0,
    flow: float = 50.0,
    seat: float = 45.0,
    limit_structure: float = 45.0,
) -> dict:
    return {
        "ts_code": ts_code,
        "strategy": strategy,
        "sleeve_scores": {
            "hard_event_alpha": {"score": hard_event},
        },
        "tushare_pro_alpha_features": {
            "evidence": {
                "industry": industry,
                "latest_amount": amount,
                "hard_alpha": {
                    "capacity_liquidity": {"score": capacity},
                    "money_flow_persistence": {"score": flow},
                    "dragon_tiger_seat_quality": {"score": seat},
                    "limit_break_structure": {"score": limit_structure},
                    "industry_crowding": {"score": 20.0},
                    "margin_pressure": {"score": 20.0},
                },
            }
        },
    }
