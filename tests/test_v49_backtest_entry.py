from __future__ import annotations

from openclaw.runtime.v49_backtest_entry import (
    BacktestEntryDependencies,
    BACKTEST_MODE_COMPARISON,
    BACKTEST_MODE_OPTIMIZE,
    BACKTEST_MODE_SINGLE,
    is_v49_backtest_route,
    render_v49_backtest_mode_entry,
    render_v49_backtest_mode_selector,
)


def _recorder(calls, name, return_value=None):
    def _inner(*args, **kwargs):
        calls.append((name, args, kwargs))
        return return_value

    return _inner


def _mode_kwargs(calls, mode):
    deps = BacktestEntryDependencies(
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
    )
    return {"backtest_mode": mode, "dependencies": deps}


def test_v49_backtest_route_predicate_freezes_scope():
    assert is_v49_backtest_route({"root": "研究后台", "research": "回测与参数"})
    assert not is_v49_backtest_route({"root": "生产后台", "production": "今日决策"})


def test_render_v49_backtest_mode_selector_freezes_options():
    calls = []

    class St:
        def radio(self, *args, **kwargs):
            calls.append((args, kwargs))
            return BACKTEST_MODE_SINGLE

    assert render_v49_backtest_mode_selector(St()) == BACKTEST_MODE_SINGLE
    assert calls[0][0][1] == [BACKTEST_MODE_SINGLE, BACKTEST_MODE_OPTIMIZE, BACKTEST_MODE_COMPARISON]


def test_render_v49_backtest_mode_entry_dispatches_comparison():
    calls = []
    render_v49_backtest_mode_entry(**_mode_kwargs(calls, BACKTEST_MODE_COMPARISON))
    assert [call[0] for call in calls[:1]] == ["comparison"]


def test_render_v49_backtest_mode_entry_dispatches_single():
    calls = []
    render_v49_backtest_mode_entry(**_mode_kwargs(calls, BACKTEST_MODE_SINGLE))
    assert [call[0] for call in calls[:1]] == ["single"]


def test_render_v49_backtest_mode_entry_dispatches_optimization_by_default():
    calls = []
    render_v49_backtest_mode_entry(**_mode_kwargs(calls, BACKTEST_MODE_OPTIMIZE))
    assert [call[0] for call in calls[:1]] == ["optimize"]
