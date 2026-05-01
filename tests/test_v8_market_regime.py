from __future__ import annotations

import pandas as pd

from openclaw.runtime.v8_market_regime import (
    calculate_v8_market_penalty,
    calculate_v8_market_regime,
    calculate_v8_market_sentiment,
    check_v8_volume_confirmation,
    detect_v8_market_trend,
)


def test_detect_v8_market_trend_freezes_bull_multiplier():
    close = pd.Series(range(1, 71))

    result = detect_v8_market_trend(close)

    assert result["trend"] == "bull"
    assert result["signal_quality_multiplier"] == 1.0
    assert result["current_price"] == 70


def test_calculate_v8_market_sentiment_freezes_greedy_signal():
    returns = pd.Series([0.0] * 20 + [0.001] * 19 + [0.002])

    result = calculate_v8_market_sentiment(returns)

    assert result["sentiment"] == "greedy"
    assert result["sentiment_score"] == 0.6
    assert result["trade_signal"] == "caution"


def test_check_v8_volume_confirmation_freezes_weak_volume():
    volume = pd.Series([100.0] * 15 + [50.0] * 5)

    result = check_v8_volume_confirmation(volume)

    assert result == {"volume_status": "weak", "volume_ratio": 0.57, "volume_score": -5}


def test_calculate_v8_market_regime_freezes_short_data_fallback():
    result = calculate_v8_market_regime(pd.DataFrame({"close": [1.0] * 30, "volume": [100.0] * 30}))

    assert result == {"can_trade": True, "reason": "数据不足，默认可交易", "position_multiplier": 1.0}


def test_calculate_v8_market_regime_freezes_reason_and_multiplier():
    close = [100.0]
    for _ in range(39):
        close.append(close[-1] * 0.995)
    for change in [-0.3] + [0.01] * 19:
        close.append(close[-1] * (1 + change))
    index_data = pd.DataFrame(
        {
            "close": close[:60],
            "volume": [100.0] * 55 + [50.0] * 5,
        }
    )

    result = calculate_v8_market_regime(index_data)

    assert result["can_trade"] is False
    assert result["position_multiplier"] == 0.048
    assert result["reason"] == "趋势bear + 情绪恐慌 + 成交萎缩 + 市场环境极差，暂停交易"


def test_calculate_v8_market_penalty_freezes_soft_filter_bands():
    assert calculate_v8_market_penalty({"can_trade": False, "position_multiplier": 0.1}) == 0.3
    assert calculate_v8_market_penalty({"can_trade": True, "position_multiplier": 0.4}) == 0.5
    assert calculate_v8_market_penalty({"can_trade": True, "position_multiplier": 0.7}) == 0.8
    assert calculate_v8_market_penalty({"can_trade": True, "position_multiplier": 1.0}) == 1.0
