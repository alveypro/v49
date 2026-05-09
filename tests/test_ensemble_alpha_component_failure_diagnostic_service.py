from __future__ import annotations

from openclaw.services.ensemble_alpha_component_failure_diagnostic_service import (
    build_ensemble_alpha_component_failure_diagnostic,
)


def test_component_failure_diagnostic_flags_high_quality_top_bucket_loss():
    fact_chain = {
        "as_of_date": "20260305",
        "sample_facts": [
            _item("000001.SZ", "v9", 90.0, -5.0, capacity=80.0, seat=90.0),
            _item("000002.SZ", "v8", 80.0, -4.0, capacity=75.0, seat=80.0),
            _item("000003.SZ", "v4", 20.0, 2.0, capacity=30.0, seat=20.0),
            _item("000004.SZ", "combo", 10.0, 3.0, capacity=25.0, seat=10.0),
            _item("000005.SZ", "v5", 5.0, 1.0, capacity=20.0, seat=5.0),
        ],
    }

    review = build_ensemble_alpha_component_failure_diagnostic(fact_chain)

    assert review["research_only"] is True
    assert review["candidate_ic"] < 0.0
    assert review["top_score_bucket"]["avg_return_pct"] < review["bottom_score_bucket"]["avg_return_pct"]
    assert "negative_component_ic:capacity_liquidity" in review["failure_hypotheses"]
    assert "high_seat_quality_top_bucket_still_lost_money" in review["failure_hypotheses"]
    assert "do_not_use_component_failure_diagnostic_as_promotion_evidence" in review["hard_boundaries"]


def _item(
    ts_code: str,
    strategy: str,
    score: float,
    forward_return: float,
    *,
    capacity: float,
    seat: float,
) -> dict:
    return {
        "ts_code": ts_code,
        "strategy": strategy,
        "sleeve_scores": {"hard_event_alpha": {"score": score}},
        "tushare_pro_alpha_features": {
            "evidence": {
                "hard_alpha": {
                    "money_flow_persistence": {"score": score},
                    "dragon_tiger_seat_quality": {"score": seat},
                    "limit_break_structure": {"score": score},
                    "industry_crowding": {"score": 0.0},
                    "capacity_liquidity": {"score": capacity},
                    "margin_pressure": {"score": 50.0},
                }
            }
        },
        "forward_returns": {"5": {"available": True, "return_pct": forward_return}},
    }
