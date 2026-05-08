from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Dict


@dataclass(frozen=True)
class DataOpsEntryDependencies:
    render_data_ops_core_page: Callable[..., Any]
    render_data_ops_status_page: Callable[..., Any]
    render_data_ops_update_page: Callable[..., Any]
    render_page_header: Callable[..., Any]
    get_auto_evolve_status: Callable[..., Any]
    load_production_report_by_strategy: Callable[..., Any]
    safe_parse_dt: Callable[..., Any]
    airivo_has_role: Callable[..., Any]
    airivo_guard_action: Callable[..., Any]
    airivo_append_action_audit: Callable[..., Any]
    trigger_auto_evolve_optimize: Callable[..., Any]
    load_portfolio_risk_budget: Callable[..., Any]
    evaluate_production_rollback_trigger: Callable[..., Any]
    load_production_rollback_state: Callable[..., Any]
    execute_production_auto_rollback: Callable[..., Any]
    compute_production_allocation_plan: Callable[..., Any]
    write_production_allocation_report: Callable[..., Any]
    build_production_rebalance_orders: Callable[..., Any]
    write_production_rebalance_report: Callable[..., Any]
    precheck_production_rebalance_orders: Callable[..., Any]
    execute_production_rebalance_orders: Callable[..., Any]
    load_latest_production_rebalance_audit: Callable[..., Any]
    build_weekly_rebalance_quality_dashboard: Callable[..., Any]
    load_latest_auto_rebalance_log: Callable[..., Any]
    db_manager: Any
    connect_permanent_db: Callable[..., Any]
    rollback_latest_promoted_params: Callable[..., Any]
    fund_bonus_enabled: Callable[..., Any]
    get_last_trade_date_from_tushare: Callable[..., Any]
    compute_health_report: Callable[..., Any]
    run_funding_repair: Callable[..., Any]
    permanent_db_path: str


def is_v49_data_ops_route(routes: Dict[str, str]) -> bool:
    return routes.get("root") == "运维后台" and routes.get("ops") == "数据与系统"


def render_v49_data_ops_entry(
    *,
    routes: Dict[str, str],
    dependencies: DataOpsEntryDependencies,
) -> None:
    if not is_v49_data_ops_route(routes):
        return
    deps = dependencies
    deps.render_data_ops_core_page(
        render_page_header=deps.render_page_header,
        get_auto_evolve_status=deps.get_auto_evolve_status,
        load_production_report_by_strategy=deps.load_production_report_by_strategy,
        safe_parse_dt=deps.safe_parse_dt,
        airivo_has_role=deps.airivo_has_role,
        airivo_guard_action=deps.airivo_guard_action,
        airivo_append_action_audit=deps.airivo_append_action_audit,
        trigger_auto_evolve_optimize=deps.trigger_auto_evolve_optimize,
        load_portfolio_risk_budget=deps.load_portfolio_risk_budget,
        evaluate_production_rollback_trigger=deps.evaluate_production_rollback_trigger,
        load_production_rollback_state=deps.load_production_rollback_state,
        execute_production_auto_rollback=deps.execute_production_auto_rollback,
        compute_production_allocation_plan=deps.compute_production_allocation_plan,
        write_production_allocation_report=deps.write_production_allocation_report,
        build_production_rebalance_orders=deps.build_production_rebalance_orders,
        write_production_rebalance_report=deps.write_production_rebalance_report,
        precheck_production_rebalance_orders=deps.precheck_production_rebalance_orders,
        execute_production_rebalance_orders=deps.execute_production_rebalance_orders,
        load_latest_production_rebalance_audit=deps.load_latest_production_rebalance_audit,
        build_weekly_rebalance_quality_dashboard=deps.build_weekly_rebalance_quality_dashboard,
        load_latest_auto_rebalance_log=deps.load_latest_auto_rebalance_log,
        db_manager=deps.db_manager,
    )
    deps.render_data_ops_status_page(
        connect_permanent_db=deps.connect_permanent_db,
        rollback_latest_promoted_params=deps.rollback_latest_promoted_params,
        fund_bonus_enabled=deps.fund_bonus_enabled,
        get_last_trade_date_from_tushare=deps.get_last_trade_date_from_tushare,
        compute_health_report=deps.compute_health_report,
        run_funding_repair=deps.run_funding_repair,
        airivo_has_role=deps.airivo_has_role,
        airivo_guard_action=deps.airivo_guard_action,
        airivo_append_action_audit=deps.airivo_append_action_audit,
        db_manager=deps.db_manager,
        permanent_db_path=deps.permanent_db_path,
    )
    deps.render_data_ops_update_page(
        airivo_has_role=deps.airivo_has_role,
        airivo_guard_action=deps.airivo_guard_action,
        airivo_append_action_audit=deps.airivo_append_action_audit,
        db_manager=deps.db_manager,
    )
