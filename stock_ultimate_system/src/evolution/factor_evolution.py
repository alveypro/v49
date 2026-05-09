from __future__ import annotations

import logging
from typing import Any

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


class FactorEvolution:
    """Monitor factor performance decay and manage factor lifecycle."""

    def __init__(self, decay_threshold: float = 0.3) -> None:
        self.decay_threshold = decay_threshold
        self._factor_scores: dict[str, list[float]] = {}

    def evaluate_factor_ic(self, df: pd.DataFrame, factor_col: str, return_col: str = 'label_return_5') -> float:
        """Compute Information Coefficient (Spearman rank correlation)."""
        if factor_col not in df.columns or return_col not in df.columns:
            return 0.0
        valid = df[[factor_col, return_col]].dropna()
        if len(valid) < 30:
            return 0.0
        return float(valid[factor_col].corr(valid[return_col], method='spearman'))

    def evaluate_all_factors(self, df: pd.DataFrame, factor_cols: list[str], return_col: str = 'label_return_5') -> dict[str, float]:
        results = {}
        for col in factor_cols:
            ic = self.evaluate_factor_ic(df, col, return_col)
            results[col] = ic
            if col not in self._factor_scores:
                self._factor_scores[col] = []
            self._factor_scores[col].append(ic)
        return results

    def detect_decay(self, factor_col: str, window: int = 10) -> bool:
        scores = self._factor_scores.get(factor_col, [])
        if len(scores) < window:
            return False
        recent = scores[-window:]
        earlier = scores[-window * 2:-window] if len(scores) >= window * 2 else scores[:window]
        if not earlier:
            return False
        recent_mean = np.mean(np.abs(recent))
        earlier_mean = np.mean(np.abs(earlier))
        if earlier_mean == 0:
            return False
        decay_ratio = 1 - recent_mean / earlier_mean
        return decay_ratio > self.decay_threshold

    def get_active_factors(self, factor_cols: list[str]) -> list[str]:
        active = []
        for col in factor_cols:
            if not self.detect_decay(col):
                active.append(col)
            else:
                logger.info('Factor %s shows decay, removing from active set', col)
        return active

    def rank_factors(self) -> list[tuple[str, float]]:
        ranked = []
        for col, scores in self._factor_scores.items():
            if scores:
                ranked.append((col, float(np.mean(np.abs(scores[-10:])))))
        ranked.sort(key=lambda x: x[1], reverse=True)
        return ranked

    def suggest_new_factors(self, df: pd.DataFrame) -> list[str]:
        """Suggest potential new factor columns based on column naming patterns."""
        existing = set(self._factor_scores.keys())
        candidates = []
        for col in df.columns:
            if col not in existing and df[col].dtype in ('float64', 'float32', 'int64'):
                if col.startswith(('ma_', 'ema_', 'rsi', 'macd', 'atr', 'vol_', 'drawdown', 'slope', 'hist_vol')):
                    candidates.append(col)
        return candidates
