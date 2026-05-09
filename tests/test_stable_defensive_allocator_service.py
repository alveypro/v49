from __future__ import annotations

import pandas as pd

from openclaw.services.stable_defensive_allocator_service import build_stable_defensive_allocator_review


def test_stable_defensive_allocator_review_is_overlay_contract_not_promotion():
    review = build_stable_defensive_allocator_review(
        pd.DataFrame(
            [
                {"future_return": -8.0, "signal_strength": 60.0},
                {"future_return": 3.0, "signal_strength": 80.0},
                {"future_return": -4.0, "signal_strength": 70.0},
            ]
        ),
        params={"score_threshold": 60, "stable_allocator_min_weight": 0.10, "stable_allocator_max_weight": 0.45},
    )

    assert review["available"] is True
    assert review["contract"]["role"] == "defensive_allocator_overlay"
    assert review["contract"]["not_standalone_alpha"] is True
    assert review["promotion_eligible"] is False
    assert review["allocator_candidate_eligible"] is False
    assert review["overlay_max_drawdown"] < review["full_exposure_max_drawdown"]
    assert "missing_formal_pool_benchmark_return_series" in review["blocking_reasons"]


def test_stable_defensive_allocator_review_blocks_missing_trade_returns():
    review = build_stable_defensive_allocator_review([], params={})

    assert review["available"] is False
    assert review["promotion_eligible"] is False
    assert review["blocking_reasons"] == ["missing_stable_trade_return_series"]


def test_stable_defensive_allocator_review_uses_formal_pool_benchmark_contract():
    review = build_stable_defensive_allocator_review(
        pd.DataFrame(
            [
                {"future_return": 4.0, "signal_strength": 90.0, "formal_pool_return": 2.0},
                {"future_return": -2.0, "signal_strength": 70.0, "formal_pool_return": -12.0},
                {"future_return": 5.0, "signal_strength": 85.0, "formal_pool_return": 1.0},
            ]
        ),
        params={"score_threshold": 60, "stable_allocator_min_weight": 0.20, "stable_allocator_max_weight": 0.60},
    )

    assert review["available"] is True
    assert review["promotion_eligible"] is False
    assert review["allocator_candidate_eligible"] is True
    assert review["success_metric_passed"] is True
    assert review["blocking_reasons"] == []
    assert review["benchmark_drawdown_reduction"] > 0.0
    assert review["benchmark_excess_return_pct"] >= 0.0
