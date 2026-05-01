from __future__ import annotations

import logging
from typing import Any, Dict, Optional

import pandas as pd

from openclaw.runtime.v8_core_factors import (
    calculate_capital_flow,
    calculate_chip_concentration,
    calculate_momentum_acceleration,
    calculate_momentum_persistence,
    calculate_obv_energy,
    calculate_profit_quality,
    calculate_relative_strength_momentum,
    calculate_sector_resonance,
    calculate_smart_money,
    calculate_turnover_momentum,
    calculate_valuation_repair,
)

logger = logging.getLogger(__name__)


def normalize_v8_index_data(index_data: Optional[pd.DataFrame]) -> Optional[pd.DataFrame]:
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


def _add_factor(
    *,
    factors: Dict[str, Dict[str, Any]],
    key: str,
    result: Dict[str, Any],
    max_points: int,
) -> tuple[Any, int]:
    factors[key] = result
    return result.get("score", 0) or 0, max_points


class RuntimeV8FactorApi:
    @staticmethod
    def relative_strength_momentum(stock_returns: pd.Series, index_returns: pd.Series, window: int = 60) -> Dict:
        return calculate_relative_strength_momentum(stock_returns, index_returns, window=window)

    @staticmethod
    def momentum_acceleration(returns: pd.Series) -> Dict:
        return calculate_momentum_acceleration(returns)

    @staticmethod
    def momentum_persistence(close: pd.Series, window: int = 60) -> Dict:
        return calculate_momentum_persistence(close, window=window)

    @staticmethod
    def obv_energy(close: pd.Series, volume: pd.Series) -> Dict:
        return calculate_obv_energy(close, volume)

    @staticmethod
    def chip_concentration(
        high: pd.Series,
        low: pd.Series,
        close: pd.Series,
        volume: pd.Series,
        window: int = 20,
    ) -> Dict:
        return calculate_chip_concentration(high, low, close, volume, window=window)

    @staticmethod
    def _turnover_momentum(volume: pd.Series, turnover_rate: pd.Series, window: int = 20) -> Dict:
        return calculate_turnover_momentum(volume, turnover_rate, window=window)

    @staticmethod
    def _evaluate_valuation_repair(close: pd.Series, volume: pd.Series) -> Dict:
        return calculate_valuation_repair(close, volume)

    @staticmethod
    def _evaluate_profit_quality(close: pd.Series, volume: pd.Series, pct_chg: pd.Series) -> Dict:
        return calculate_profit_quality(close, volume, pct_chg)

    @staticmethod
    def _evaluate_capital_flow(close: pd.Series, volume: pd.Series, pct_chg: pd.Series) -> Dict:
        return calculate_capital_flow(close, volume, pct_chg)

    @staticmethod
    def _evaluate_sector_resonance(
        stock_returns: pd.Series,
        index_data: Optional[pd.DataFrame],
        index_returns: pd.Series,
    ) -> Dict:
        return calculate_sector_resonance(stock_returns, index_data, index_returns)

    @staticmethod
    def _evaluate_smart_money(close: pd.Series, volume: pd.Series, pct_chg: pd.Series) -> Dict:
        return calculate_smart_money(close, volume, pct_chg)


RUNTIME_V8_FACTOR_API = RuntimeV8FactorApi()


def calculate_v8_advanced_factors(
    stock_data: pd.DataFrame,
    *,
    index_data: Optional[pd.DataFrame] = None,
    factor_api: Any = None,
) -> Dict[str, Any]:
    factors: Dict[str, Dict[str, Any]] = {}
    total_score = 0
    max_score = 0

    try:
        factor_api = factor_api or RUNTIME_V8_FACTOR_API
        normalized_index = normalize_v8_index_data(index_data)
        close = stock_data["close_price"] if "close_price" in stock_data.columns else stock_data["close"]
        volume = stock_data["vol"]
        if "float_share" in stock_data.columns:
            turnover_rate = volume / stock_data["float_share"]
        else:
            turnover_rate = volume / volume.rolling(60).mean()
        returns = close.pct_change()

        if normalized_index is not None and len(normalized_index) > 0:
            index_returns = normalized_index["close"].pct_change()
            score, points = _add_factor(
                factors=factors,
                key="relative_strength",
                result=factor_api.relative_strength_momentum(returns, index_returns),
                max_points=15,
            )
            total_score += score
            max_score += points

        for key, result, points in (
            ("acceleration", factor_api.momentum_acceleration(returns), 12),
            ("persistence", factor_api.momentum_persistence(close), 12),
            ("obv", factor_api.obv_energy(close, volume), 13),
        ):
            score, max_points = _add_factor(factors=factors, key=key, result=result, max_points=points)
            total_score += score
            max_score += max_points

        if "high_price" in stock_data.columns:
            high = stock_data["high_price"]
            low = stock_data["low_price"]
        else:
            high = stock_data.get("high", close)
            low = stock_data.get("low", close)

        for key, result, points in (
            ("chip_concentration", factor_api.chip_concentration(high, low, close, volume), 15),
            ("turnover_momentum", factor_api._turnover_momentum(volume, turnover_rate), 12),
            ("valuation_repair", factor_api._evaluate_valuation_repair(close, volume), 12),
            ("roe_trend", factor_api._evaluate_profit_quality(close, volume, returns), 10),
            ("capital_flow", factor_api._evaluate_capital_flow(close, volume, returns), 12),
        ):
            score, max_points = _add_factor(factors=factors, key=key, result=result, max_points=points)
            total_score += score
            max_score += max_points

        if normalized_index is not None and len(normalized_index) > 0:
            index_returns = normalized_index["close"].pct_change()
            sector_result = factor_api._evaluate_sector_resonance(returns, normalized_index, index_returns)
        else:
            sector_result = {"score": 6, "grade": "无大盘对比"}
        score, max_points = _add_factor(
            factors=factors,
            key="sector_resonance",
            result=sector_result,
            max_points=12,
        )
        total_score += score
        max_score += max_points

        score, max_points = _add_factor(
            factors=factors,
            key="smart_money",
            result=factor_api._evaluate_smart_money(close, volume, returns),
            max_points=15,
        )
        total_score += score
        max_score += max_points
    except Exception as exc:
        logger.error("V8 advanced factor aggregation failed: %s", exc)
        return {"total_score": 0, "factors": {}, "max_score": max_score if max_score > 0 else 100}

    return {
        "total_score": total_score,
        "max_score": max_score if max_score > 0 else 100,
        "factors": factors,
    }
