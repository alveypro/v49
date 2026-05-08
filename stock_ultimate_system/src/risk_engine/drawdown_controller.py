from __future__ import annotations

import numpy as np
import pandas as pd


class DrawdownController:
    """Monitor portfolio drawdown and trigger protection mode."""

    def __init__(
        self,
        max_drawdown: float = 0.10,
        protection_scale: float = 0.30,
        recovery_ratio: float = 0.50,
    ) -> None:
        self.max_drawdown = max_drawdown
        self.protection_scale = max(0.0, min(float(protection_scale), 1.0))
        self.recovery_ratio = max(0.1, min(float(recovery_ratio), 1.0))
        self._peak_equity: float = 0.0
        self._in_protection: bool = False

    def update(self, equity: float) -> None:
        if equity > self._peak_equity:
            self._peak_equity = equity
        current_dd = self._calc_drawdown(equity)
        if current_dd <= -self.max_drawdown:
            self._in_protection = True
        elif current_dd > -self.max_drawdown * self.recovery_ratio:
            self._in_protection = False

    def _calc_drawdown(self, equity: float) -> float:
        if self._peak_equity <= 0:
            return 0.0
        return (equity - self._peak_equity) / self._peak_equity

    def should_enter_protection_mode(self, equity_curve: list[float] | pd.Series | None = None, threshold: float | None = None) -> bool:
        if equity_curve is not None:
            arr = np.array(equity_curve)
            if len(arr) < 2:
                return False
            peak = np.maximum.accumulate(arr)
            dd = (arr - peak) / np.where(peak > 0, peak, 1)
            thr = threshold if threshold is not None else self.max_drawdown
            return float(dd.min()) <= -thr
        return self._in_protection

    @property
    def in_protection(self) -> bool:
        return self._in_protection

    def get_position_scale(self) -> float:
        if self._in_protection:
            return self.protection_scale
        return 1.0

    def reset(self) -> None:
        self._peak_equity = 0.0
        self._in_protection = False
