"""Bridge for root-level legacy modules still used by the mainline.

This module is the only place where mainline code should directly resolve
root-level legacy modules during the transition period.
"""

from __future__ import annotations

import importlib
from types import ModuleType
from typing import Any, Callable, Tuple


def _import_module(module_name: str) -> ModuleType:
    return importlib.import_module(module_name)


def _load_attr(module_name: str, attr_name: str) -> Any:
    module = _import_module(module_name)
    return getattr(module, attr_name)


def load_notification_service_class() -> type:
    return _load_attr("openclaw.services.notification_service", "NotificationService")


def load_v3_evaluator_class() -> type:
    return _load_attr("strategies.evaluators.comprehensive_stock_evaluator_v3", "ComprehensiveStockEvaluatorV3")


def load_v4_evaluator_class() -> type:
    return _load_attr("strategies.evaluators.comprehensive_stock_evaluator_v4", "ComprehensiveStockEvaluatorV4")


def load_v5_evaluator_class() -> type:
    try:
        return _load_attr("strategies.evaluators.comprehensive_stock_evaluator_v5", "ComprehensiveStockEvaluatorV5")
    except Exception:
        return load_v4_evaluator_class()


def load_v6_evaluator_class() -> type:
    return _load_attr("strategies.evaluators.comprehensive_stock_evaluator_v6", "ComprehensiveStockEvaluatorV6")


def load_v6_ultimate_evaluator_class() -> type:
    return _load_attr("strategies.evaluators.comprehensive_stock_evaluator_v6_ultimate", "ComprehensiveStockEvaluatorV6Ultimate")


def load_v7_evaluator_class() -> type:
    return _load_attr("strategies.evaluators.comprehensive_stock_evaluator_v7_ultimate", "ComprehensiveStockEvaluatorV7Ultimate")


def load_v8_evaluator_class() -> type:
    return _load_attr("strategies.evaluators.comprehensive_stock_evaluator_v8_ultimate", "ComprehensiveStockEvaluatorV8Ultimate")


def load_kelly_position_manager_class() -> type:
    return _load_attr("strategies.support.kelly_position_manager", "KellyPositionManager")


def load_dynamic_rebalance_manager_class() -> type:
    return _load_attr("strategies.support.dynamic_rebalance_manager", "DynamicRebalanceManager")


def load_backtest_with_dynamic_strategy() -> Callable[..., Any]:
    return _load_attr("optimized_backtest_strategy_v49", "backtest_with_dynamic_strategy")


def load_v6_ultra_short_backtest_module() -> ModuleType:
    return _import_module("backtest_v6_ultra_short")


def load_stable_uptrend_renderer() -> Callable[..., Any]:
    return _load_attr("strategies.support.stable_uptrend_strategy", "render_stable_uptrend_strategy")


def load_stable_uptrend_module() -> ModuleType:
    return _import_module("strategies.support.stable_uptrend_strategy")


def load_risk_params_helpers() -> Tuple[Callable[..., Any], Callable[..., Any]]:
    module = _import_module("strategies.support.risk_params")
    return module.get_strategy_risk_params, module.normalize_strategy_name
