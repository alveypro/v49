from __future__ import annotations

from typing import Dict, Optional

import numpy as np
import pandas as pd


def normalize_v8_market_index_data(index_data: Optional[pd.DataFrame]) -> Optional[pd.DataFrame]:
    if index_data is None or not hasattr(index_data, "columns"):
        return index_data

    normalized = index_data
    copy_made = False
    aliases = (
        ("close", "close_price"),
        ("volume", "vol"),
        ("high", "high_price"),
        ("low", "low_price"),
        ("open", "open_price"),
    )
    for canonical, alias in aliases:
        if canonical not in normalized.columns and alias in normalized.columns:
            if not copy_made:
                normalized = normalized.copy()
                copy_made = True
            normalized[canonical] = normalized[alias]
    return normalized


def detect_v8_market_trend(close: pd.Series, ma_short: int = 20, ma_long: int = 60) -> Dict:
    ma_short_val = close.rolling(window=ma_short).mean().iloc[-1]
    ma_long_val = close.rolling(window=ma_long).mean().iloc[-1]
    current_price = close.iloc[-1]

    if current_price > ma_long_val and ma_short_val > ma_long_val:
        trend = "bull"
        signal_quality = 1.0
    elif current_price < ma_long_val and ma_short_val < ma_long_val:
        trend = "bear"
        signal_quality = 0.2
    else:
        trend = "sideways"
        signal_quality = 0.5

    return {
        "trend": trend,
        "signal_quality_multiplier": signal_quality,
        "ma_short": ma_short_val,
        "ma_long": ma_long_val,
        "current_price": current_price,
    }


def calculate_v8_market_sentiment(returns: pd.Series, window: int = 20) -> Dict:
    volatility = returns.rolling(window=window).std().iloc[-1] * np.sqrt(252)
    skewness = returns.rolling(window=window).skew().iloc[-1]

    sentiment_score = 0.0
    if volatility < 0.15:
        sentiment_score += 0.3
    elif volatility > 0.35:
        sentiment_score -= 0.4

    if skewness > 0:
        sentiment_score += 0.3
    else:
        sentiment_score -= 0.3

    if sentiment_score > 0.3:
        sentiment = "greedy"
        trade_signal = "caution"
    elif sentiment_score < -0.3:
        sentiment = "fear"
        trade_signal = "pause"
    else:
        sentiment = "neutral"
        trade_signal = "normal"

    return {
        "sentiment": sentiment,
        "sentiment_score": round(sentiment_score, 2),
        "volatility": round(volatility, 4),
        "trade_signal": trade_signal,
    }


def check_v8_volume_confirmation(volume: pd.Series, window: int = 20) -> Dict:
    recent_volume = volume.iloc[-5:].mean()
    avg_volume = volume.rolling(window=window).mean().iloc[-1]
    volume_ratio = recent_volume / avg_volume if avg_volume > 0 else 1.0

    if volume_ratio > 1.3:
        volume_status = "active"
        volume_score = 10
    elif volume_ratio > 1.0:
        volume_status = "normal"
        volume_score = 5
    else:
        volume_status = "weak"
        volume_score = -5

    return {
        "volume_status": volume_status,
        "volume_ratio": round(volume_ratio, 2),
        "volume_score": volume_score,
    }


def calculate_v8_market_regime(index_data: pd.DataFrame) -> Dict:
    index_data = normalize_v8_market_index_data(index_data)
    if len(index_data) < 60:
        return {"can_trade": True, "reason": "数据不足，默认可交易", "position_multiplier": 1.0}

    trend_result = detect_v8_market_trend(index_data["close"])
    returns = index_data["close"].pct_change()
    sentiment_result = calculate_v8_market_sentiment(returns)
    volume_result = check_v8_volume_confirmation(index_data["volume"])

    can_trade = True
    position_multiplier = 1.0
    reasons = []

    position_multiplier *= trend_result["signal_quality_multiplier"]
    reasons.append(f"趋势{trend_result['trend']}")

    if sentiment_result["trade_signal"] == "pause":
        position_multiplier *= 0.3
        reasons.append("情绪恐慌")
    elif sentiment_result["trade_signal"] == "caution":
        position_multiplier *= 0.7
        reasons.append("情绪贪婪")

    if volume_result["volume_status"] == "weak":
        position_multiplier *= 0.8
        reasons.append("成交萎缩")

    if position_multiplier < 0.15:
        can_trade = False
        reasons.append("市场环境极差，暂停交易")

    return {
        "can_trade": can_trade,
        "position_multiplier": position_multiplier,
        "reason": " + ".join(reasons),
        "trend": trend_result,
        "sentiment": sentiment_result,
        "volume": volume_result,
    }


def calculate_v8_market_penalty(market_status: Dict) -> float:
    if not market_status.get("can_trade", True):
        return 0.3
    if market_status.get("position_multiplier", 1.0) < 0.5:
        return 0.5
    if market_status.get("position_multiplier", 1.0) < 0.8:
        return 0.8
    return 1.0
