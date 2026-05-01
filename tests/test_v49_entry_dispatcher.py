from __future__ import annotations

from openclaw.runtime.v49_backtest_entry import BacktestEntryDependencies
from openclaw.runtime.v49_entry_dispatcher import (
    is_v49_ai_signal_route,
    is_v49_backtest_route,
    is_v49_data_ops_route,
    is_v49_execution_center_route,
    is_v49_sector_flow_route,
    is_v49_stock_pool_route,
    is_v49_strategy_evolution_route,
    is_v49_task_logs_route,
    is_v49_today_decision_route,
    render_v49_ai_signal_entry,
    render_v49_backtest_entry,
    render_v49_execution_center_entry,
    render_v49_sector_flow_entry,
    render_v49_stock_pool_entry,
    render_v49_strategy_evolution_entry,
    render_v49_task_guide_entry,
    render_v49_task_logs_entry,
)


def _recorder(calls, name, return_value=None):
    def _inner(*args, **kwargs):
        calls.append((name, args, kwargs))
        return return_value

    return _inner


class _FakeSt:
    def __init__(self, mode="单策略深度回测"):
        self.mode = mode
        self.session_state = {}

    def markdown(self, text):
        return None

    def radio(self, *args, **kwargs):
        return self.mode

    def tabs(self, labels):
        return [_FakeTab() for _ in labels]

    def error(self, text):
        return None

    def info(self, text):
        return None


class _FakeTab:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


def test_v49_entry_route_predicates_freeze_entry_scope():
    assert is_v49_task_logs_route({"root": "运维后台", "ops": "任务与日志"})
    assert is_v49_execution_center_route({"root": "生产后台", "production": "执行中心"})
    assert is_v49_today_decision_route({"root": "生产后台", "production": "今日决策"})
    assert is_v49_strategy_evolution_route({"root": "生产后台", "production": "策略演进"})
    assert is_v49_stock_pool_route({"root": "研究后台", "research": "股票池分析"})
    assert is_v49_sector_flow_route({"root": "研究后台", "research": "板块与热点"})
    assert is_v49_backtest_route({"root": "研究后台", "research": "回测与参数"})
    assert is_v49_ai_signal_route({"root": "研究后台", "research": "智能助手"})
    assert is_v49_data_ops_route({"root": "运维后台", "ops": "数据与系统"})
    assert not is_v49_today_decision_route({"root": "研究后台", "research": "智能助手"})


def test_render_v49_task_logs_entry_dispatches_only_matching_route():
    calls = []
    render_v49_task_logs_entry(
        routes={"root": "运维后台", "ops": "任务与日志"},
        render_async_task_dashboard=_recorder(calls, "tasks"),
        limit=12,
    )
    render_v49_task_logs_entry(
        routes={"root": "生产后台", "production": "今日决策"},
        render_async_task_dashboard=_recorder(calls, "skip"),
        limit=12,
    )

    assert calls == [("tasks", (), {"limit": 12})]


def test_render_v49_execution_center_entry_dispatches_only_matching_route():
    calls = []
    render_v49_execution_center_entry(
        routes={"root": "生产后台", "production": "执行中心"},
        render_airivo_execution_center=_recorder(calls, "execution"),
        permanent_db_path="/tmp/db.sqlite",
    )

    assert calls == [("execution", ("/tmp/db.sqlite",), {})]


def test_render_v49_research_and_ops_entries_dispatch_only_route_shells():
    calls = []
    render_v49_stock_pool_entry(
        routes={"root": "研究后台", "research": "股票池分析"},
        render_page_header=_recorder(calls, "pool_header"),
        render_stock_pool_workspace=_recorder(calls, "pool"),
    )
    render_v49_sector_flow_entry(
        routes={"root": "研究后台", "research": "板块与热点"},
        render_sector_flow_page=_recorder(calls, "sector"),
        render_page_header=_recorder(calls, "sector_header"),
        market_scanner_cls=object,
    )
    render_v49_ai_signal_entry(
        routes={"root": "研究后台", "research": "智能助手"},
        show_ai_signal_panel=True,
        render_ai_signal_page=_recorder(calls, "ai"),
        render_page_header=_recorder(calls, "ai_header"),
        load_evolve_params=_recorder(calls, "evolve"),
        vp_analyzer=object(),
        connect_permanent_db=_recorder(calls, "connect"),
        apply_filter_mode=_recorder(calls, "filter"),
        apply_multi_period_filter=_recorder(calls, "multi_filter"),
        permanent_db_path="/tmp/db.sqlite",
        add_reason_summary=_recorder(calls, "reason"),
        get_sim_account=_recorder(calls, "sim"),
        auto_buy_ai_stocks=_recorder(calls, "buy"),
        render_result_overview=_recorder(calls, "overview"),
        signal_density_hint=_recorder(calls, "density"),
        standardize_result_df=_recorder(calls, "standardize"),
        df_to_csv_bytes=_recorder(calls, "csv"),
    )
    render_v49_task_guide_entry(
        routes={"root": "运维后台", "ops": "任务与日志"},
        st=type("St", (), {"markdown": _recorder(calls, "markdown")})(),
        render_page_header=_recorder(calls, "guide_header"),
    )

    assert [call[0] for call in calls] == ["pool_header", "pool", "sector", "ai", "markdown", "guide_header"]


def test_render_v49_strategy_evolution_entry_returns_snapshot():
    calls = []
    result = render_v49_strategy_evolution_entry(
        routes={"root": "生产后台", "production": "策略演进"},
        permanent_db_path="/tmp/db.sqlite",
        airivo_snapshot={},
        render_airivo_production_dashboard=_recorder(calls, "dashboard", {"snapshot": True}),
        render_airivo_strategy_evolution=_recorder(calls, "evolution"),
    )

    assert result == {"snapshot": True}
    assert [call[0] for call in calls] == ["dashboard", "evolution"]


def test_render_v49_backtest_entry_dispatches_selected_mode():
    calls = []
    kwargs = {
        "routes": {"root": "研究后台", "research": "回测与参数"},
        "st": _FakeSt(mode="策略横向对比（辅助）"),
        "dependencies": BacktestEntryDependencies(
            render_page_header=_recorder(calls, "header"),
            render_production_backtest_audit_panel=_recorder(calls, "audit"),
            render_strategy_comparison_page=_recorder(calls, "comparison"),
            render_single_backtest_page=_recorder(calls, "single"),
            render_parameter_optimization_page=_recorder(calls, "optimize"),
            vp_analyzer=object(),
            v7_evaluator_available=True,
            v8_evaluator_available=True,
            **{name: _recorder(calls, name) for name in [
                "get_production_compare_params", "start_async_backtest_job", "connect_permanent_db",
                "get_async_backtest_job", "is_pid_alive", "merge_async_backtest_job", "now_ts",
                "load_evolve_params", "airivo_has_role", "airivo_guard_action", "airivo_append_action_audit",
                "set_sim_meta", "auto_backtest_scheduler_tick", "ensure_price_aliases", "now_text",
                "pick_tradable_segment_from_strength", "apply_tradable_segment_to_strategy_session",
                "build_calibrated_strength_df", "production_baseline_params",
                "apply_production_baseline_to_session", "save_production_unified_profile",
            ]}
        ),
    }

    render_v49_backtest_entry(**kwargs)

    assert [call[0] for call in calls[:3]] == ["header", "audit", "comparison"]
