from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Dict


BACKTEST_MODE_SINGLE = "单策略深度回测"
BACKTEST_MODE_OPTIMIZE = "参数优化"
BACKTEST_MODE_COMPARISON = "策略横向对比（辅助）"


@dataclass(frozen=True)
class BacktestEntryDependencies:
    render_page_header: Callable[..., Any]
    render_production_backtest_audit_panel: Callable[..., Any]
    render_strategy_comparison_page: Callable[..., Any]
    render_single_backtest_page: Callable[..., Any]
    render_parameter_optimization_page: Callable[..., Any]
    vp_analyzer: Any
    get_production_compare_params: Callable[..., Any]
    start_async_backtest_job: Callable[..., Any]
    connect_permanent_db: Callable[..., Any]
    get_async_backtest_job: Callable[..., Any]
    is_pid_alive: Callable[..., Any]
    merge_async_backtest_job: Callable[..., Any]
    now_ts: Callable[..., Any]
    v7_evaluator_available: bool
    v8_evaluator_available: bool
    load_evolve_params: Callable[..., Any]
    airivo_has_role: Callable[..., Any]
    airivo_guard_action: Callable[..., Any]
    airivo_append_action_audit: Callable[..., Any]
    set_sim_meta: Callable[..., Any]
    auto_backtest_scheduler_tick: Callable[..., Any]
    ensure_price_aliases: Callable[..., Any]
    now_text: Callable[..., Any]
    pick_tradable_segment_from_strength: Callable[..., Any]
    apply_tradable_segment_to_strategy_session: Callable[..., Any]
    build_calibrated_strength_df: Callable[..., Any]
    production_baseline_params: Callable[..., Any]
    apply_production_baseline_to_session: Callable[..., Any]
    save_production_unified_profile: Callable[..., Any]


def is_v49_backtest_route(routes: Dict[str, str]) -> bool:
    return routes.get("root") == "研究后台" and routes.get("research") == "回测与参数"


def render_v49_backtest_mode_selector(st: Any) -> str:
    return st.radio(
        "选择回测模式",
        [BACKTEST_MODE_SINGLE, BACKTEST_MODE_OPTIMIZE, BACKTEST_MODE_COMPARISON],
        horizontal=True,
        help="优先做单策略深度回测；横向对比仅用于辅助校验。",
    )


def render_v49_backtest_mode_entry(
    *,
    backtest_mode: str,
    dependencies: BacktestEntryDependencies,
) -> None:
    deps = dependencies
    if backtest_mode == BACKTEST_MODE_COMPARISON:
        deps.render_strategy_comparison_page(
            vp_analyzer=deps.vp_analyzer,
            get_production_compare_params=deps.get_production_compare_params,
            start_async_backtest_job=deps.start_async_backtest_job,
            connect_permanent_db=deps.connect_permanent_db,
            get_async_backtest_job=deps.get_async_backtest_job,
            is_pid_alive=deps.is_pid_alive,
            merge_async_backtest_job=deps.merge_async_backtest_job,
            now_ts=deps.now_ts,
            v7_evaluator_available=deps.v7_evaluator_available,
            v8_evaluator_available=deps.v8_evaluator_available,
        )
    elif backtest_mode == BACKTEST_MODE_SINGLE:
        deps.render_single_backtest_page(
            vp_analyzer=deps.vp_analyzer,
            load_evolve_params=deps.load_evolve_params,
            airivo_has_role=deps.airivo_has_role,
            airivo_guard_action=deps.airivo_guard_action,
            airivo_append_action_audit=deps.airivo_append_action_audit,
            start_async_backtest_job=deps.start_async_backtest_job,
            set_sim_meta=deps.set_sim_meta,
            auto_backtest_scheduler_tick=deps.auto_backtest_scheduler_tick,
            connect_permanent_db=deps.connect_permanent_db,
            ensure_price_aliases=deps.ensure_price_aliases,
            get_async_backtest_job=deps.get_async_backtest_job,
            is_pid_alive=deps.is_pid_alive,
            merge_async_backtest_job=deps.merge_async_backtest_job,
            now_ts=deps.now_ts,
            now_text=deps.now_text,
            pick_tradable_segment_from_strength=deps.pick_tradable_segment_from_strength,
            apply_tradable_segment_to_strategy_session=deps.apply_tradable_segment_to_strategy_session,
            build_calibrated_strength_df=deps.build_calibrated_strength_df,
        )
    else:
        deps.render_parameter_optimization_page(
            vp_analyzer=deps.vp_analyzer,
            connect_permanent_db=deps.connect_permanent_db,
            ensure_price_aliases=deps.ensure_price_aliases,
            airivo_has_role=deps.airivo_has_role,
            airivo_guard_action=deps.airivo_guard_action,
            airivo_append_action_audit=deps.airivo_append_action_audit,
            production_baseline_params=deps.production_baseline_params,
            get_production_compare_params=deps.get_production_compare_params,
            apply_production_baseline_to_session=deps.apply_production_baseline_to_session,
            save_production_unified_profile=deps.save_production_unified_profile,
        )


def render_v49_backtest_entry(
    *,
    routes: Dict[str, str],
    st: Any,
    dependencies: BacktestEntryDependencies,
) -> None:
    if not is_v49_backtest_route(routes):
        return
    deps = dependencies
    deps.render_page_header(
        " 回测中心",
        "单策略深度回测 · 参数优化 · 横向对比（辅助）",
        tag="Backtest Lab",
    )
    deps.render_production_backtest_audit_panel()
    st.markdown("---")
    render_v49_backtest_mode_entry(
        backtest_mode=render_v49_backtest_mode_selector(st),
        dependencies=deps,
    )
