from __future__ import annotations

from typing import Any, Callable, Dict, Optional

import pandas as pd

from openclaw.runtime.v8_advanced_factor_aggregator import calculate_v8_advanced_factors
from openclaw.runtime.v8_atr_risk import calculate_v8_atr_stops
from openclaw.runtime.v8_market_regime import calculate_v8_market_penalty, calculate_v8_market_regime
from openclaw.runtime.v8_signal_evaluator import build_v8_evaluation_result


def build_v8_empty_result(version: str) -> Dict[str, Any]:
    return {
        "success": False,
        "final_score": 0,
        "grade": "D",
        "star_rating": 0,
        "description": "数据不足或不符合标准",
        "version": version,
    }


def _sort_by_trade_date(data: Optional[pd.DataFrame]) -> Optional[pd.DataFrame]:
    if data is not None and "trade_date" in data.columns:
        return data.sort_values("trade_date").reset_index(drop=True)
    return data


def evaluate_v8_signal(
    *,
    stock_data: pd.DataFrame,
    version: str,
    base_evaluator: Any,
    ts_code: str | None = None,
    index_data: Optional[pd.DataFrame] = None,
    industry: str | None = None,
    industry_resolver: Optional[Callable[[str], str]] = None,
    logger: Any = None,
    timestamp: str | None = None,
) -> Dict[str, Any]:
    if len(stock_data) < 60:
        return build_v8_empty_result(version)

    stock_data = _sort_by_trade_date(stock_data)
    index_data = _sort_by_trade_date(index_data)

    market_status = {"can_trade": True, "position_multiplier": 1.0, "reason": "未启用市场过滤"}
    market_penalty = 1.0

    if index_data is not None and len(index_data) >= 60:
        market_status = calculate_v8_market_regime(index_data)
        market_penalty = calculate_v8_market_penalty(market_status)
        if market_penalty == 0.3 and logger is not None:
            logger.warning(f"⚠️ 市场环境极差（{market_status['reason']}），评分将降至30%")
    else:
        market_status["reason"] = "大盘数据不足，未降分"

    if hasattr(base_evaluator, "evaluate_stock_v7"):
        industry_value = industry or (industry_resolver(ts_code) if ts_code and industry_resolver else "未知行业")
        v7_result = base_evaluator.evaluate_stock_v7(stock_data, ts_code, industry_value)
    else:
        v7_result = base_evaluator.evaluate_stock_v4(stock_data)

    if not v7_result["success"]:
        return v7_result

    advanced_result = calculate_v8_advanced_factors(stock_data, index_data=index_data)

    try:
        atr_stops = calculate_v8_atr_stops(stock_data)
    except Exception as exc:
        if logger is not None:
            logger.warning(f"ATR计算失败: {exc}")
        atr_stops = {}

    return build_v8_evaluation_result(
        version=version,
        v7_result=v7_result,
        advanced_result=advanced_result,
        market_status=market_status,
        market_penalty=market_penalty,
        atr_stops=atr_stops,
        timestamp=timestamp,
    )
