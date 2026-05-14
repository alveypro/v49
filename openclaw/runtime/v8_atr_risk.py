from __future__ import annotations

from typing import Dict

import pandas as pd


def calculate_v8_atr(high: pd.Series, low: pd.Series, close: pd.Series, period: int = 14) -> pd.Series:
    tr1 = high - low
    tr2 = abs(high - close.shift(1))
    tr3 = abs(low - close.shift(1))
    true_range = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    return true_range.rolling(window=period).mean()


def calculate_v8_dynamic_stops(
    price: float,
    atr: float,
    stop_loss_multiplier: float = 2.0,
    take_profit_multiplier: float = 3.0,
) -> Dict:
    stop_loss = price - (stop_loss_multiplier * atr)
    take_profit = price + (take_profit_multiplier * atr)
    trailing_stop = price - (1.5 * atr)

    return {
        "stop_loss": round(stop_loss, 2),
        "take_profit": round(take_profit, 2),
        "trailing_stop": round(trailing_stop, 2),
        "atr_value": round(atr, 2),
        "stop_loss_pct": round((price - stop_loss) / price * 100, 2),
        "take_profit_pct": round((take_profit - price) / price * 100, 2),
    }


def calculate_v8_atr_stops(stock_data: pd.DataFrame) -> Dict:
    close = stock_data["close_price"] if "close_price" in stock_data.columns else stock_data["close"]
    high = stock_data["high_price"] if "high_price" in stock_data.columns else close
    low = stock_data["low_price"] if "low_price" in stock_data.columns else close

    atr = calculate_v8_atr(high, low, close)
    current_atr = atr.iloc[-1]
    if pd.isna(current_atr):
        return {}

    current_price = close.iloc[-1]
    return calculate_v8_dynamic_stops(current_price, current_atr)
