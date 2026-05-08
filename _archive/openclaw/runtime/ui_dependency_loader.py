"""Load UI-facing strategy/runtime dependencies for the legacy v49 app."""

from __future__ import annotations

from typing import Any, Dict

from openclaw.runtime.root_dependency_bridge import (
    load_dynamic_rebalance_manager_class,
    load_kelly_position_manager_class,
    load_notification_service_class,
    load_stable_uptrend_renderer,
    load_v3_evaluator_class,
    load_v4_evaluator_class,
    load_v6_ultimate_evaluator_class,
    load_v7_evaluator_class,
    load_v8_evaluator_class,
)


def load_ui_dependencies(logger) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "ComprehensiveStockEvaluatorV3": None,
        "ComprehensiveStockEvaluatorV4": None,
        "ComprehensiveStockEvaluatorV5": None,
        "ComprehensiveStockEvaluatorV6": None,
        "ComprehensiveStockEvaluatorV7Ultimate": None,
        "ComprehensiveStockEvaluatorV8Ultimate": None,
        "KellyPositionManager": None,
        "DynamicRebalanceManager": None,
        "NotificationService": None,
        "render_stable_uptrend_strategy": None,
        "V3_EVALUATOR_AVAILABLE": False,
        "V4_EVALUATOR_AVAILABLE": False,
        "V5_EVALUATOR_AVAILABLE": False,
        "V6_EVALUATOR_AVAILABLE": False,
        "V7_EVALUATOR_AVAILABLE": False,
        "V8_EVALUATOR_AVAILABLE": False,
        "STABLE_UPTREND_AVAILABLE": False,
    }

    try:
        out["ComprehensiveStockEvaluatorV4"] = load_v4_evaluator_class()
        out["V4_EVALUATOR_AVAILABLE"] = True
        logger.info("v4.0综合优选评分器（潜伏策略版）加载成功！")
    except ImportError as e:
        logger.warning(f"v4.0评分器未找到，将使用v3.0版本: {e}")
        try:
            out["ComprehensiveStockEvaluatorV3"] = load_v3_evaluator_class()
            out["V3_EVALUATOR_AVAILABLE"] = True
            logger.info("v3.0综合优选评分器加载成功（备用）！")
        except ImportError:
            pass

    try:
        out["ComprehensiveStockEvaluatorV5"] = out["ComprehensiveStockEvaluatorV4"]
        out["V5_EVALUATOR_AVAILABLE"] = out["ComprehensiveStockEvaluatorV5"] is not None
        if out["V5_EVALUATOR_AVAILABLE"]:
            logger.info("v5.0启动确认型评分器加载成功（基于v4.0八维体系）！")
    except ImportError as e:
        logger.warning(f"v5.0评分器未找到: {e}")

    try:
        out["ComprehensiveStockEvaluatorV6"] = load_v6_ultimate_evaluator_class()
        out["V6_EVALUATOR_AVAILABLE"] = True
        logger.info("v6.0超短线狙击评分器·专业版加载成功！")
    except ImportError as e:
        logger.warning(f"v6.0评分器未找到: {e}")

    try:
        out["ComprehensiveStockEvaluatorV7Ultimate"] = load_v7_evaluator_class()
        out["V7_EVALUATOR_AVAILABLE"] = True
        logger.info("v7.0智能选股系统加载成功！")
    except ImportError as e:
        logger.warning(f"v7.0评分器未找到: {e}")

    try:
        out["ComprehensiveStockEvaluatorV8Ultimate"] = load_v8_evaluator_class()
        out["KellyPositionManager"] = load_kelly_position_manager_class()
        out["DynamicRebalanceManager"] = load_dynamic_rebalance_manager_class()
        out["V8_EVALUATOR_AVAILABLE"] = True
        logger.info("v8.0进阶版加载成功！ATR风控+市场过滤+凯利仓位+动态再平衡")
    except ImportError as e:
        logger.warning(f"v8.0评分器未找到: {e}")

    try:
        out["render_stable_uptrend_strategy"] = load_stable_uptrend_renderer()
        out["STABLE_UPTREND_AVAILABLE"] = True
        logger.info("稳定上涨策略模块加载成功！")
    except ImportError as e:
        logger.warning(f"稳定上涨策略模块未找到: {e}")

    try:
        out["NotificationService"] = load_notification_service_class()
    except Exception:
        pass

    return out
