from __future__ import annotations

import pandas as pd

from openclaw.runtime.v8_atr_risk import (
    calculate_v8_atr,
    calculate_v8_atr_stops,
    calculate_v8_dynamic_stops,
)


def test_calculate_v8_dynamic_stops_freezes_payload():
    result = calculate_v8_dynamic_stops(price=10.0, atr=0.5)

    assert result == {
        "stop_loss": 9.0,
        "take_profit": 11.5,
        "trailing_stop": 9.25,
        "atr_value": 0.5,
        "stop_loss_pct": 10.0,
        "take_profit_pct": 15.0,
    }


def test_calculate_v8_atr_freezes_true_range_average():
    close = pd.Series([10.0, 11.0, 12.0])
    high = pd.Series([10.5, 11.5, 12.5])
    low = pd.Series([9.5, 10.5, 11.5])

    result = calculate_v8_atr(high, low, close, period=2)

    assert pd.isna(result.iloc[0])
    assert result.iloc[1] == 1.25
    assert result.iloc[2] == 1.5


def test_calculate_v8_atr_stops_freezes_stock_payload_with_price_aliases():
    rows = 15
    stock_data = pd.DataFrame(
        {
            "close_price": [10.0 + i for i in range(rows)],
            "high_price": [10.5 + i for i in range(rows)],
            "low_price": [9.5 + i for i in range(rows)],
        }
    )

    result = calculate_v8_atr_stops(stock_data)

    assert result == {
        "stop_loss": 21.0,
        "take_profit": 28.5,
        "trailing_stop": 21.75,
        "atr_value": 1.5,
        "stop_loss_pct": 12.5,
        "take_profit_pct": 18.75,
    }


def test_calculate_v8_atr_stops_returns_empty_payload_when_atr_not_ready():
    stock_data = pd.DataFrame(
        {
            "close": [10.0, 11.0],
            "high": [10.5, 11.5],
            "low": [9.5, 10.5],
        }
    )

    assert calculate_v8_atr_stops(stock_data) == {}
