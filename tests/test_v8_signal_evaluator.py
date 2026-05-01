from __future__ import annotations

from openclaw.runtime.v8_signal_evaluator import (
    build_v8_evaluation_result,
    calculate_v8_final_score,
    calculate_v8_star_rating,
)


def test_calculate_v8_final_score_freezes_weighting_and_market_penalty():
    result = calculate_v8_final_score(
        v7_score=50,
        advanced_total_score=80,
        advanced_max_score=100,
        market_penalty=0.5,
    )

    assert result["advanced_score"] == 80.0
    assert result["v7_score"] == 50.0
    assert result["final_score"] == 38.5
    assert result["v7_weight"] == 0.1
    assert result["advanced_weight"] == 0.9


def test_calculate_v8_star_rating_uses_production_thresholds():
    assert calculate_v8_star_rating(75) == (5, 0.25)
    assert calculate_v8_star_rating(65) == (4, 0.20)
    assert calculate_v8_star_rating(55) == (3, 0.15)
    assert calculate_v8_star_rating(45) == (2, 0.10)
    assert calculate_v8_star_rating(44.99) == (1, 0.05)


def test_build_v8_evaluation_result_freezes_public_payload():
    result = build_v8_evaluation_result(
        version="8.0",
        v7_result={"success": True, "final_score": 70},
        advanced_result={"total_score": 90, "max_score": 100, "factors": {"smart_money": {"score": 10}}},
        market_status={"can_trade": True, "reason": "ok"},
        market_penalty=1.0,
        atr_stops={"stop_loss": 9.5},
        timestamp="2026-05-01 09:30:00",
    )

    assert result["success"] is True
    assert result["final_score"] == 88.0
    assert result["grade"] == "SSS"
    assert result["star_rating"] == 5
    assert result["position_suggestion"] == 0.25
    assert result["advanced_factors"]["factors"]["smart_money"]["score"] == 10
    assert result["timestamp"] == "2026-05-01 09:30:00"
