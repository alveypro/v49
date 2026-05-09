from __future__ import annotations

from strategies.evaluators.comprehensive_stock_evaluator_v7_ultimate import (
    ComprehensiveStockEvaluatorV7Ultimate,
)


def test_v7_near_threshold_conversion_requires_broad_factor_confirmation():
    evaluator = ComprehensiveStockEvaluatorV7Ultimate.__new__(ComprehensiveStockEvaluatorV7Ultimate)

    review = evaluator._apply_near_threshold_conversion_v7(
        final_score=59.2,
        dimension_scores={
            "潜伏价值": 12.0,
            "底部特征": 11.0,
            "量价配合": 8.0,
            "MACD趋势": 8.0,
            "均线多头": 3.0,
            "主力行为": 5.0,
            "启动确认": 2.5,
        },
        adaptive_weights={
            "潜伏价值": 20.0,
            "底部特征": 20.0,
            "量价配合": 15.0,
            "MACD趋势": 15.0,
            "均线多头": 10.0,
            "主力行为": 10.0,
            "启动确认": 5.0,
        },
        industry_heat=0.1,
        filter_penalty=0.0,
        filter_warnings=[],
    )

    assert review["eligible"] is True
    assert review["applied"] is True
    assert review["bonus"] == 0.8
    assert review["final_score"] == 60.0
    assert review["confirmation_count"] >= 4


def test_v7_near_threshold_conversion_blocks_weak_or_filtered_samples():
    evaluator = ComprehensiveStockEvaluatorV7Ultimate.__new__(ComprehensiveStockEvaluatorV7Ultimate)

    review = evaluator._apply_near_threshold_conversion_v7(
        final_score=59.2,
        dimension_scores={"潜伏价值": 4.0, "底部特征": 3.0, "量价配合": 2.0},
        adaptive_weights={"潜伏价值": 20.0, "底部特征": 20.0, "量价配合": 15.0},
        industry_heat=-0.2,
        filter_penalty=7.0,
        filter_warnings=["成交量萎缩，需要75分以上"],
    )

    assert review["eligible"] is False
    assert review["applied"] is False
    assert "filter_penalty_present" in review["blocking_reasons"]
    assert "industry_heat_negative" in review["blocking_reasons"]
    assert "insufficient_broad_factor_confirmation" in review["blocking_reasons"]
