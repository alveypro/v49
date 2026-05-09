from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class ForecastResult:
    direction_prob: float = 0.5
    expected_return: float = 0.0
    confidence: float = 0.0
    components: dict[str, Any] = field(default_factory=dict)


@dataclass
class SignalResult:
    signal: str = 'watch'
    score: float = 0.0
    explanation: str = ''
    entry_plan: dict[str, Any] = field(default_factory=dict)


@dataclass
class RiskResult:
    allow_trade: bool = True
    risk_level: str = 'medium'
    stop_loss: float = 0.0
    take_profit: float = 0.0
    position_scale: float = 1.0


@dataclass
class PositionResult:
    position_pct: float = 0.0
    position_amount: float = 0.0


@dataclass
class TradeRecord:
    ts_code: str = ''
    date: str = ''
    side: str = ''
    price: float = 0.0
    qty: int = 0
    commission: float = 0.0
    stamp_tax: float = 0.0
    pnl: float = 0.0
    holding_days: int = 0
