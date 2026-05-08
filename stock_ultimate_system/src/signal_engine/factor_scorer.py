from __future__ import annotations

import pandas as pd


class FactorScorer:
    """Score individual factors from the latest row of data."""

    def score_trend_factors(self, df: pd.DataFrame) -> dict[str, float]:
        latest = df.iloc[-1]
        scores = {}
        if latest.get('ma_5', 0) > latest.get('ma_20', 0):
            scores['short_trend'] = 15
        else:
            scores['short_trend'] = 0
        if latest.get('ma_20', 0) > latest.get('ma_60', 0):
            scores['medium_trend'] = 10
        else:
            scores['medium_trend'] = 0
        if latest.get('ema_5', 0) > latest.get('ema_20', 0):
            scores['ema_trend'] = 10
        else:
            scores['ema_trend'] = 0
        return scores

    def score_momentum_factors(self, df: pd.DataFrame) -> dict[str, float]:
        latest = df.iloc[-1]
        scores = {}
        rsi = latest.get('rsi', 50)
        if 40 < rsi < 70:
            scores['rsi'] = 10
        elif rsi >= 70:
            scores['rsi'] = -5
        else:
            scores['rsi'] = 0
        macd = latest.get('macd', 0)
        scores['macd'] = 10 if macd > 0 else 0
        return scores

    def score_volume_factors(self, df: pd.DataFrame) -> dict[str, float]:
        latest = df.iloc[-1]
        scores = {}
        vr = latest.get('volume_ratio', 1)
        if vr > 1.5:
            scores['volume_surge'] = 10
        elif vr > 1.0:
            scores['volume_surge'] = 5
        else:
            scores['volume_surge'] = 0
        return scores

    def score_research_factors(self, df: pd.DataFrame) -> dict[str, float]:
        latest = df.iloc[-1]
        scores = {}

        close = float(latest.get('close', 0) or 0)
        ma_20 = float(latest.get('ma_20', 0) or 0)
        ma_60 = float(latest.get('ma_60', 0) or 0)
        macd = float(latest.get('macd', 0) or 0)
        volume_ratio = float(latest.get('volume_ratio', 0) or 0)

        scores['trend_transition'] = 0
        scores['quiet_breakout'] = 0

        if close > 0 and ma_20 > 0 and ma_60 > 0:
            ma20_vs_ma60 = ma_20 / ma_60
            if close > ma_20 and 0.98 <= ma20_vs_ma60 <= 1.02 and macd > 0:
                scores['trend_transition'] = 8
            if close > ma_20 and macd > 0 and 0.85 <= volume_ratio <= 1.1:
                scores['quiet_breakout'] = 4

        return scores

    def aggregate_factor_score(self, scores: dict[str, float]) -> dict:
        return {'total': sum(scores.values()), 'components': scores}
