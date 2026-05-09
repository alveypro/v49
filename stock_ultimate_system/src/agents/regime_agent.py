from __future__ import annotations

import numpy as np
import pandas as pd


class RegimeAgent:
    """Detect market regime using technical indicators."""

    def __init__(self, config: dict) -> None:
        self.config = config

    def detect_market_regime(self, df: pd.DataFrame) -> dict:
        latest = df.iloc[-1]
        signals = []

        ma_20 = latest.get('ma_20', 0)
        ma_60 = latest.get('ma_60', 0)
        if ma_20 and ma_60:
            signals.append('trend' if ma_20 > ma_60 else 'range')

        rsi = latest.get('rsi', 50)
        if rsi > 70:
            signals.append('overbought')
        elif rsi < 30:
            signals.append('oversold')
        else:
            signals.append('neutral')

        vol = latest.get('hist_vol_20', 0)
        high_vol = vol > 0.03 if vol else False

        trend_count = sum(1 for s in signals if s == 'trend')
        range_count = sum(1 for s in signals if s in ('range', 'neutral'))

        if trend_count > range_count:
            regime = 'trend'
            score = 0.8
        elif 'overbought' in signals or 'oversold' in signals:
            regime = 'extreme'
            score = 0.3
        else:
            regime = 'range'
            score = 0.5

        if high_vol:
            score *= 0.8
            regime = f'{regime}_volatile'

        market_trend = int(latest.get('market_trend', 0))

        return {
            'regime': regime,
            'environment_score': round(score, 2),
            'volatility_state': 'high' if high_vol else 'normal',
            'market_trend': 'bullish' if market_trend else 'bearish',
            'rsi': float(rsi),
        }
