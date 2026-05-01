from __future__ import annotations

from openclaw.runtime.v49_data_ops_entry import (
    DataOpsEntryDependencies,
    is_v49_data_ops_route,
    render_v49_data_ops_entry,
)


def _recorder(calls, name, return_value=None):
    def _inner(*args, **kwargs):
        calls.append((name, args, kwargs))
        return return_value

    return _inner


def _data_ops_kwargs(calls, routes):
    deps = DataOpsEntryDependencies(
        render_data_ops_core_page=_recorder(calls, "core"),
        render_data_ops_status_page=_recorder(calls, "status"),
        render_data_ops_update_page=_recorder(calls, "update"),
        render_page_header=_recorder(calls, "header"),
        db_manager=object(),
        permanent_db_path="/tmp/db.sqlite",
        **{name: _recorder(calls, name) for name in [
            "get_auto_evolve_status", "load_production_report_by_strategy", "safe_parse_dt",
            "airivo_has_role", "airivo_guard_action", "airivo_append_action_audit",
            "trigger_auto_evolve_optimize", "load_portfolio_risk_budget", "evaluate_production_rollback_trigger",
            "load_production_rollback_state", "execute_production_auto_rollback", "compute_production_allocation_plan",
            "write_production_allocation_report", "build_production_rebalance_orders", "write_production_rebalance_report",
            "precheck_production_rebalance_orders", "execute_production_rebalance_orders",
            "load_latest_production_rebalance_audit", "build_weekly_rebalance_quality_dashboard",
            "load_latest_auto_rebalance_log", "connect_permanent_db", "rollback_latest_promoted_params",
            "fund_bonus_enabled", "get_last_trade_date_from_tushare", "compute_health_report", "run_funding_repair",
        ]}
    )
    return {
        "routes": routes,
        "dependencies": deps,
    }


def test_v49_data_ops_route_predicate_freezes_scope():
    assert is_v49_data_ops_route({"root": "运维后台", "ops": "数据与系统"})
    assert not is_v49_data_ops_route({"root": "运维后台", "ops": "任务与日志"})


def test_render_v49_data_ops_entry_freezes_page_call_order():
    calls = []
    render_v49_data_ops_entry(**_data_ops_kwargs(calls, {"root": "运维后台", "ops": "数据与系统"}))

    assert [call[0] for call in calls[:3]] == ["core", "status", "update"]


def test_render_v49_data_ops_entry_skips_non_matching_route():
    calls = []
    render_v49_data_ops_entry(**_data_ops_kwargs(calls, {"root": "生产后台", "production": "今日决策"}))

    assert calls == []
