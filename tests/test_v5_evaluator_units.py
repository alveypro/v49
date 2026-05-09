import numpy as np

from strategies.evaluators.comprehensive_stock_evaluator_v5 import ComprehensiveStockEvaluatorV5


def test_v5_uses_decimal_return_units_for_volume_price_and_launch_scores():
    evaluator = ComprehensiveStockEvaluatorV5()
    ind = {
        "vol_ratio": 1.6,
        "price_chg_5d": 0.04,
        "ma20": 10.0,
        "ma60": 9.5,
    }
    close = np.array([10.0] * 64 + [10.6])
    volume = np.array([100.0] * 60 + [140.0, 150.0, 160.0, 170.0, 180.0])
    pct_chg = np.array([-1.0, 1.0, 0.5, 1.2, 0.8] * 13)

    assert evaluator._score_volume_price_v5(ind, close, volume) >= 16
    assert evaluator._score_launch_confirm_v5(ind, close, pct_chg) >= 16


def test_v5_uses_decimal_return_units_for_lurking_and_bottom_scores():
    evaluator = ComprehensiveStockEvaluatorV5()

    lurking_score = evaluator._score_lurking_value_v5(
        {"vol_ratio": 0.8, "price_chg_5d": 0.04},
        np.array([10.0] * 65),
        np.array([100.0] * 65),
        np.array([0.0] * 65),
    )
    bottom_score = evaluator._score_bottom_feature_v5({"price_position": 0.4, "price_chg_20d": -0.15})

    assert lurking_score == 6
    assert bottom_score == 8
