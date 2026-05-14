from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Dict


@dataclass(frozen=True)
class TodayDecisionDependencies:
    permanent_db_path: str
    render_airivo_production_dashboard: Callable[[str], Any]
    render_today_console_panel: Callable[..., Any]
    render_today_advanced_ops_panel: Callable[..., Any]
    render_today_strategy_selector_panel: Callable[..., Any]
    render_today_strategy_dispatcher: Callable[..., Any]
    production_strategies: Any
    experimental_strategies: Any
    vp_analyzer: Any
    status: Any
    set_focus_once: Callable[..., Any]
    render_today_execution_queues: Callable[..., Any]
    airivo_has_role: Callable[..., Any]
    airivo_guard_action: Callable[..., Any]
    airivo_append_action_audit: Callable[..., Any]
    render_airivo_batch_manager: Callable[..., Any]
    publish_manual_scan_to_execution_queue: Callable[..., Any]
    production_baseline_params: Callable[..., Any]
    apply_production_baseline_to_session: Callable[..., Any]
    save_production_unified_profile: Callable[..., Any]
    build_unified_from_latest_evolve: Callable[..., Any]
    get_production_compare_params: Callable[..., Any]
    logger: Any
    db_manager: Any
    bulk_history_limit: int
    strict_full_market_mode: bool
    v4_evaluator_available: bool
    v5_evaluator_available: bool
    v6_evaluator_available: bool
    v7_evaluator_available: bool
    v8_evaluator_available: bool
    stable_uptrend_available: bool
    stable_uptrend_context_cls: Any
    render_stable_uptrend_strategy: Callable[..., Any]
    load_evolve_params: Callable[..., Any]
    load_strategy_center_scan_defaults: Callable[..., Any]
    sync_scan_task_with_params: Callable[..., Any]
    render_scan_param_hint: Callable[..., Any]
    render_front_scan_summary: Callable[..., Any]
    detect_heavy_background_job: Callable[..., Any]
    start_async_scan_task: Callable[..., Any]
    mark_scan_submitted: Callable[..., Any]
    run_front_scan_via_offline_pipeline: Callable[..., Any]
    mark_front_scan_completed: Callable[..., Any]
    get_db_last_trade_date: Callable[..., Any]
    load_scan_cache: Callable[..., Any]
    save_scan_cache: Callable[..., Any]
    load_v7_cache: Callable[..., Any]
    save_v7_cache: Callable[..., Any]
    connect_permanent_db: Callable[..., Any]
    load_candidate_stocks: Callable[..., Any]
    load_external_bonus_maps: Callable[..., Any]
    load_stock_history: Callable[..., Any]
    load_stock_history_bulk: Callable[..., Any]
    load_stock_history_fallback: Callable[..., Any]
    load_history_range_bulk: Callable[..., Any]
    batch_load_stock_histories: Callable[..., Any]
    calc_external_bonus: Callable[..., Any]
    update_scan_progress_ui: Callable[..., Any]
    apply_filter_mode: Callable[..., Any]
    apply_multi_period_filter: Callable[..., Any]
    add_reason_summary: Callable[..., Any]
    render_result_overview: Callable[..., Any]
    render_v7_results: Callable[..., Any]
    signal_density_hint: Callable[..., Any]
    set_stock_pool_candidate: Callable[..., Any]
    append_reason_col: Callable[..., Any]
    standardize_result_df: Callable[..., Any]
    normalize_stock_df: Callable[..., Any]
    df_to_csv_bytes: Callable[..., Any]
    render_cached_scan_results: Callable[..., Any]
    render_async_scan_status: Callable[..., Any]


def is_v49_today_decision_route(routes: Dict[str, str]) -> bool:
    return routes.get("root") == "生产后台" and routes.get("production") == "今日决策"


def render_v49_today_decision_entry(
    *,
    routes: Dict[str, str],
    dependencies: TodayDecisionDependencies,
    airivo_snapshot: Dict[str, Any],
    show_ai_signal_panel: bool,
) -> Dict[str, Any]:
    if not is_v49_today_decision_route(routes):
        return {"airivo_snapshot": airivo_snapshot, "show_ai_signal_panel": show_ai_signal_panel}

    deps = dependencies
    airivo_snapshot = deps.render_airivo_production_dashboard(deps.permanent_db_path)
    snapshot = airivo_snapshot if isinstance(airivo_snapshot, dict) else {}
    deps.render_today_console_panel(
        permanent_db_path=deps.permanent_db_path,
        airivo_snapshot=snapshot,
        vp_analyzer=deps.vp_analyzer,
        status=deps.status if isinstance(deps.status, dict) else None,
        set_focus_once=deps.set_focus_once,
        render_today_execution_queues=deps.render_today_execution_queues,
    )

    deps.render_today_advanced_ops_panel(
        permanent_db_path=deps.permanent_db_path,
        airivo_snapshot=snapshot,
        airivo_has_role=deps.airivo_has_role,
        airivo_guard_action=deps.airivo_guard_action,
        airivo_append_action_audit=deps.airivo_append_action_audit,
        render_airivo_batch_manager=deps.render_airivo_batch_manager,
        publish_manual_scan_to_execution_queue=deps.publish_manual_scan_to_execution_queue,
        production_baseline_params=deps.production_baseline_params,
        apply_production_baseline_to_session=deps.apply_production_baseline_to_session,
        save_production_unified_profile=deps.save_production_unified_profile,
        build_unified_from_latest_evolve=deps.build_unified_from_latest_evolve,
        get_production_compare_params=deps.get_production_compare_params,
    )

    strategy_mode, show_ai_signal_panel = deps.render_today_strategy_selector_panel(
        production_strategies=deps.production_strategies,
        experimental_strategies=deps.experimental_strategies,
        show_ai_signal_panel=show_ai_signal_panel,
    )

    show_ai_signal_panel = deps.render_today_strategy_dispatcher(
        strategy_mode=strategy_mode,
        show_ai_signal_panel=show_ai_signal_panel,
        vp_analyzer=deps.vp_analyzer,
        logger=deps.logger,
        permanent_db_path=deps.permanent_db_path,
        db_manager=deps.db_manager,
        bulk_history_limit=deps.bulk_history_limit,
        strict_full_market_mode=deps.strict_full_market_mode,
        v4_evaluator_available=deps.v4_evaluator_available,
        v5_evaluator_available=deps.v5_evaluator_available,
        v6_evaluator_available=deps.v6_evaluator_available,
        v7_evaluator_available=deps.v7_evaluator_available,
        v8_evaluator_available=deps.v8_evaluator_available,
        stable_uptrend_available=deps.stable_uptrend_available,
        stable_uptrend_context_cls=deps.stable_uptrend_context_cls,
        render_stable_uptrend_strategy=deps.render_stable_uptrend_strategy,
        load_evolve_params=deps.load_evolve_params,
        load_strategy_center_scan_defaults=deps.load_strategy_center_scan_defaults,
        sync_scan_task_with_params=deps.sync_scan_task_with_params,
        render_scan_param_hint=deps.render_scan_param_hint,
        render_front_scan_summary=deps.render_front_scan_summary,
        has_role=deps.airivo_has_role,
        guard_action=deps.airivo_guard_action,
        append_action_audit=deps.airivo_append_action_audit,
        detect_heavy_background_job=deps.detect_heavy_background_job,
        start_async_scan_task=deps.start_async_scan_task,
        mark_scan_submitted=deps.mark_scan_submitted,
        run_front_scan_via_offline_pipeline=deps.run_front_scan_via_offline_pipeline,
        mark_front_scan_completed=deps.mark_front_scan_completed,
        get_db_last_trade_date=deps.get_db_last_trade_date,
        load_scan_cache=deps.load_scan_cache,
        save_scan_cache=deps.save_scan_cache,
        load_v7_cache=deps.load_v7_cache,
        save_v7_cache=deps.save_v7_cache,
        connect_permanent_db=deps.connect_permanent_db,
        load_candidate_stocks=deps.load_candidate_stocks,
        load_external_bonus_maps=deps.load_external_bonus_maps,
        load_stock_history=deps.load_stock_history,
        load_stock_history_bulk=deps.load_stock_history_bulk,
        load_stock_history_fallback=deps.load_stock_history_fallback,
        load_history_range_bulk=deps.load_history_range_bulk,
        batch_load_stock_histories=deps.batch_load_stock_histories,
        calc_external_bonus=deps.calc_external_bonus,
        update_scan_progress_ui=deps.update_scan_progress_ui,
        apply_filter_mode=deps.apply_filter_mode,
        apply_multi_period_filter=deps.apply_multi_period_filter,
        add_reason_summary=deps.add_reason_summary,
        render_result_overview=deps.render_result_overview,
        render_v7_results=deps.render_v7_results,
        signal_density_hint=deps.signal_density_hint,
        set_stock_pool_candidate=deps.set_stock_pool_candidate,
        append_reason_col=deps.append_reason_col,
        standardize_result_df=deps.standardize_result_df,
        normalize_stock_df=deps.normalize_stock_df,
        df_to_csv_bytes=deps.df_to_csv_bytes,
        render_cached_scan_results=deps.render_cached_scan_results,
        render_async_scan_status=deps.render_async_scan_status,
    )
    return {"airivo_snapshot": airivo_snapshot, "show_ai_signal_panel": show_ai_signal_panel}
