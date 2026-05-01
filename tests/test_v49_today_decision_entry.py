from __future__ import annotations

from openclaw.runtime.v49_today_decision_entry import (
    TodayDecisionDependencies,
    is_v49_today_decision_route,
    render_v49_today_decision_entry,
)


def _recorder(calls, name, return_value=None):
    def _inner(*args, **kwargs):
        calls.append((name, args, kwargs))
        return return_value

    return _inner


def _today_dependencies(calls):
    def _selector(**kwargs):
        calls.append(("selector", (), kwargs))
        return "v9", True

    return TodayDecisionDependencies(
        permanent_db_path="/tmp/db.sqlite",
        render_airivo_production_dashboard=_recorder(calls, "dashboard", {"ok": True}),
        render_today_console_panel=_recorder(calls, "console"),
        render_today_advanced_ops_panel=_recorder(calls, "advanced"),
        render_today_strategy_selector_panel=_selector,
        render_today_strategy_dispatcher=_recorder(calls, "dispatcher", False),
        production_strategies=["v9"],
        experimental_strategies=[],
        vp_analyzer=object(),
        status={"ok": True},
        set_focus_once=_recorder(calls, "focus"),
        render_today_execution_queues=_recorder(calls, "queues"),
        airivo_has_role=_recorder(calls, "has_role"),
        airivo_guard_action=_recorder(calls, "guard"),
        airivo_append_action_audit=_recorder(calls, "audit"),
        render_airivo_batch_manager=_recorder(calls, "batch"),
        publish_manual_scan_to_execution_queue=_recorder(calls, "publish"),
        production_baseline_params=_recorder(calls, "baseline"),
        apply_production_baseline_to_session=_recorder(calls, "apply_baseline"),
        save_production_unified_profile=_recorder(calls, "save_profile"),
        build_unified_from_latest_evolve=_recorder(calls, "unified"),
        get_production_compare_params=_recorder(calls, "compare"),
        logger=object(),
        db_manager=object(),
        bulk_history_limit=100,
        strict_full_market_mode=False,
        v4_evaluator_available=True,
        v5_evaluator_available=True,
        v6_evaluator_available=True,
        v7_evaluator_available=True,
        v8_evaluator_available=True,
        stable_uptrend_available=True,
        stable_uptrend_context_cls=object,
        render_stable_uptrend_strategy=_recorder(calls, "stable"),
        load_evolve_params=_recorder(calls, "evolve"),
        load_strategy_center_scan_defaults=_recorder(calls, "defaults"),
        sync_scan_task_with_params=_recorder(calls, "sync"),
        render_scan_param_hint=_recorder(calls, "hint"),
        render_front_scan_summary=_recorder(calls, "summary"),
        detect_heavy_background_job=_recorder(calls, "heavy"),
        start_async_scan_task=_recorder(calls, "start"),
        mark_scan_submitted=_recorder(calls, "submitted"),
        run_front_scan_via_offline_pipeline=_recorder(calls, "front"),
        mark_front_scan_completed=_recorder(calls, "completed"),
        get_db_last_trade_date=_recorder(calls, "last_date"),
        load_scan_cache=_recorder(calls, "load_cache"),
        save_scan_cache=_recorder(calls, "save_cache"),
        load_v7_cache=_recorder(calls, "load_v7"),
        save_v7_cache=_recorder(calls, "save_v7"),
        connect_permanent_db=_recorder(calls, "connect"),
        load_candidate_stocks=_recorder(calls, "candidates"),
        load_external_bonus_maps=_recorder(calls, "bonus_maps"),
        load_stock_history=_recorder(calls, "history"),
        load_stock_history_bulk=_recorder(calls, "history_bulk"),
        load_stock_history_fallback=_recorder(calls, "history_fallback"),
        load_history_range_bulk=_recorder(calls, "range_bulk"),
        batch_load_stock_histories=_recorder(calls, "batch_history"),
        calc_external_bonus=_recorder(calls, "bonus"),
        update_scan_progress_ui=_recorder(calls, "progress"),
        apply_filter_mode=_recorder(calls, "filter"),
        apply_multi_period_filter=_recorder(calls, "multi_filter"),
        add_reason_summary=_recorder(calls, "reason"),
        render_result_overview=_recorder(calls, "overview"),
        render_v7_results=_recorder(calls, "v7_results"),
        signal_density_hint=_recorder(calls, "density"),
        set_stock_pool_candidate=_recorder(calls, "candidate"),
        append_reason_col=_recorder(calls, "append_reason"),
        standardize_result_df=_recorder(calls, "standardize"),
        normalize_stock_df=_recorder(calls, "normalize"),
        df_to_csv_bytes=_recorder(calls, "csv"),
        render_cached_scan_results=_recorder(calls, "cached"),
        render_async_scan_status=_recorder(calls, "async_status"),
    )


def test_v49_today_decision_route_predicate_freezes_scope():
    assert is_v49_today_decision_route({"root": "生产后台", "production": "今日决策"})
    assert not is_v49_today_decision_route({"root": "生产后台", "production": "执行中心"})


def test_render_v49_today_decision_entry_freezes_call_order_and_return_state():
    calls = []
    result = render_v49_today_decision_entry(
        routes={"root": "生产后台", "production": "今日决策"},
        dependencies=_today_dependencies(calls),
        airivo_snapshot={},
        show_ai_signal_panel=False,
    )

    assert [call[0] for call in calls[:5]] == ["dashboard", "console", "advanced", "selector", "dispatcher"]
    assert result == {"airivo_snapshot": {"ok": True}, "show_ai_signal_panel": False}


def test_render_v49_today_decision_entry_skips_non_matching_route():
    calls = []
    result = render_v49_today_decision_entry(
        routes={"root": "研究后台", "research": "智能助手"},
        dependencies=_today_dependencies(calls),
        airivo_snapshot={"old": True},
        show_ai_signal_panel=True,
    )

    assert result == {"airivo_snapshot": {"old": True}, "show_ai_signal_panel": True}
    assert calls == []
