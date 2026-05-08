from __future__ import annotations

from typing import Any, Callable, Dict

from openclaw.runtime.v49_backtest_entry import (
    is_v49_backtest_route,
    render_v49_backtest_entry,
)
from openclaw.runtime.v49_data_ops_entry import (
    is_v49_data_ops_route,
    render_v49_data_ops_entry,
)
from openclaw.runtime.v49_research_entries import (
    is_v49_ai_signal_route,
    is_v49_sector_flow_route,
    is_v49_stock_pool_route,
    render_v49_ai_signal_entry,
    render_v49_research_light_entries,
    render_v49_sector_flow_entry,
    render_v49_stock_pool_entry,
)
from openclaw.runtime.v49_strategy_evolution_entry import (
    is_v49_strategy_evolution_route,
    render_v49_strategy_evolution_entry,
)
from openclaw.runtime.v49_today_decision_entry import (
    is_v49_today_decision_route,
    render_v49_today_decision_entry,
)
from openclaw.runtime.v49_trading_assistant_entry import render_v49_trading_assistant_entry


def is_v49_task_logs_route(routes: Dict[str, str]) -> bool:
    return routes.get("root") == "运维后台" and routes.get("ops") == "任务与日志"


def is_v49_execution_center_route(routes: Dict[str, str]) -> bool:
    return routes.get("root") == "生产后台" and routes.get("production") == "执行中心"


def render_v49_task_logs_entry(
    *,
    routes: Dict[str, str],
    render_async_task_dashboard: Callable[..., Any],
    limit: int = 12,
) -> None:
    if is_v49_task_logs_route(routes):
        render_async_task_dashboard(limit=limit)


def render_v49_execution_center_entry(
    *,
    routes: Dict[str, str],
    render_airivo_execution_center: Callable[[str], Any],
    permanent_db_path: str,
) -> None:
    if is_v49_execution_center_route(routes):
        render_airivo_execution_center(permanent_db_path)


def render_v49_task_guide_entry(
    *,
    routes: Dict[str, str],
    st: Any,
    render_page_header: Callable[..., Any],
) -> None:
    if not is_v49_task_logs_route(routes):
        return
    st.markdown("---")
    render_page_header(
        " 实战操作指南",
        "系统用法 · 风险提示 · 实战流程",
        tag="Guide",
    )
