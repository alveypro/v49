from __future__ import annotations

from openclaw.services.ensemble_observation_gate_service import build_ensemble_observation_gate


def _payload(*, window_count: int = 8, neutral_windows: int = 3, turnover: float = 0.5, industry: float = 0.24):
    risk_on_windows = window_count - neutral_windows
    return {
        "research_only": True,
        "candidate": "hard_event_alpha_candidate",
        "rule_freeze": {
            "frozen": True,
            "rule_hash": "abc123",
        },
        "windows": [{"as_of_date": f"202603{idx + 1:02d}"} for idx in range(window_count)],
        "benchmark": {
            "research_only": True,
            "not_for_production": True,
            "passed": True,
            "valid_window_count": window_count,
            "after_cost_excess_return": 0.42,
            "hit_rate": 0.75,
            "turnover": turnover,
            "capacity_utilization": 0.04,
            "industry_concentration": industry,
            "blocking_reasons": [],
            "regime_split": {
                "risk_on": {
                    "window_count": risk_on_windows,
                    "avg_after_cost_excess_return": 0.55,
                    "hit_rate": 0.75,
                },
                "neutral": {
                    "window_count": neutral_windows,
                    "avg_after_cost_excess_return": 0.18,
                    "hit_rate": 0.667,
                },
            },
        },
    }


def test_ensemble_observation_gate_blocks_thin_five_window_candidate():
    payload = _payload(window_count=5, neutral_windows=1, turnover=0.899914, industry=0.3)
    payload["benchmark"]["regime_split"]["neutral"] = {
        "window_count": 1,
        "avg_after_cost_excess_return": -1.383549,
        "hit_rate": 0.0,
    }

    review = build_ensemble_observation_gate(payload)

    assert review["research_only"] is True
    assert review["observation_gate_passed"] is False
    assert review["observation_pool_eligible"] is False
    assert review["formal_pool_eligible"] is False
    assert "insufficient_fresh_windows:5/8" in review["blocking_reasons"]
    assert "insufficient_neutral_windows:1/2" in review["blocking_reasons"]
    assert "regime_excess_not_positive:neutral:-1.383549" in review["blocking_reasons"]
    assert "regime_hit_rate_below_floor:neutral:0.0/0.5" in review["blocking_reasons"]
    assert "turnover_above_cap:0.899914/0.75" in review["blocking_reasons"]
    assert "industry_concentration_at_or_above_cap:0.3/0.3" in review["blocking_reasons"]
    assert "prove_regime_split_with_multiple_positive_neutral_and_risk_on_windows" in review["required_next_evidence"]


def test_ensemble_observation_gate_allows_only_review_after_hard_criteria_pass():
    review = build_ensemble_observation_gate(_payload(window_count=9, neutral_windows=3))

    assert review["observation_gate_passed"] is True
    assert review["observation_review_eligible"] is True
    assert review["observation_pool_eligible"] is False
    assert review["formal_pool_eligible"] is False
    assert review["blocking_reasons"] == []
    assert review["evidence_summary"]["unique_as_of_window_count"] == 9
    assert "observation_gate_does_not_mutate_strategy_pool" in review["hard_boundaries"]
