from __future__ import annotations

from typing import Any, Callable, Dict


def is_v49_stock_pool_route(routes: Dict[str, str]) -> bool:
    return routes.get("root") == "研究后台" and routes.get("research") == "股票池分析"


def is_v49_sector_flow_route(routes: Dict[str, str]) -> bool:
    return routes.get("root") == "研究后台" and routes.get("research") == "板块与热点"


def is_v49_ai_signal_route(routes: Dict[str, str]) -> bool:
    return routes.get("root") == "研究后台" and routes.get("research") == "智能助手"


def render_v49_stock_pool_entry(
    *,
    routes: Dict[str, str],
    render_page_header: Callable[..., Any],
    render_stock_pool_workspace: Callable[..., Any],
) -> None:
    if not is_v49_stock_pool_route(routes):
        return
    render_page_header(
        " 股票池分析",
        "多策略结果汇总 · 候选池分层 · 导出复盘",
        tag="Pool Workspace",
    )
    render_stock_pool_workspace()


def render_v49_sector_flow_entry(
    *,
    routes: Dict[str, str],
    render_sector_flow_page: Callable[..., Any],
    render_page_header: Callable[..., Any],
    market_scanner_cls: Any,
) -> None:
    if is_v49_sector_flow_route(routes):
        render_sector_flow_page(
            render_page_header=render_page_header,
            market_scanner_cls=market_scanner_cls,
        )


def render_v49_ai_signal_entry(
    *,
    routes: Dict[str, str],
    show_ai_signal_panel: bool,
    render_ai_signal_page: Callable[..., Any],
    render_page_header: Callable[..., Any],
    load_evolve_params: Callable[..., Any],
    vp_analyzer: Any,
    connect_permanent_db: Callable[..., Any],
    apply_filter_mode: Callable[..., Any],
    apply_multi_period_filter: Callable[..., Any],
    permanent_db_path: str,
    add_reason_summary: Callable[..., Any],
    get_sim_account: Callable[..., Any],
    auto_buy_ai_stocks: Callable[..., Any],
    render_result_overview: Callable[..., Any],
    signal_density_hint: Callable[..., Any],
    standardize_result_df: Callable[..., Any],
    df_to_csv_bytes: Callable[..., Any],
) -> None:
    if is_v49_ai_signal_route(routes):
        render_ai_signal_page(
            show_ai_signal_panel=show_ai_signal_panel,
            render_page_header=render_page_header,
            load_evolve_params=load_evolve_params,
            vp_analyzer=vp_analyzer,
            connect_permanent_db=connect_permanent_db,
            apply_filter_mode=apply_filter_mode,
            apply_multi_period_filter=apply_multi_period_filter,
            permanent_db_path=permanent_db_path,
            add_reason_summary=add_reason_summary,
            get_sim_account=get_sim_account,
            auto_buy_ai_stocks=auto_buy_ai_stocks,
            render_result_overview=render_result_overview,
            signal_density_hint=signal_density_hint,
            standardize_result_df=standardize_result_df,
            df_to_csv_bytes=df_to_csv_bytes,
        )


def render_v49_research_light_entries(
    *,
    routes: Dict[str, str],
    show_ai_signal_panel: bool,
    render_page_header: Callable[..., Any],
    render_stock_pool_workspace: Callable[..., Any],
    render_sector_flow_page: Callable[..., Any],
    market_scanner_cls: Any,
    render_ai_signal_page: Callable[..., Any],
    load_evolve_params: Callable[..., Any],
    vp_analyzer: Any,
    connect_permanent_db: Callable[..., Any],
    apply_filter_mode: Callable[..., Any],
    apply_multi_period_filter: Callable[..., Any],
    permanent_db_path: str,
    add_reason_summary: Callable[..., Any],
    get_sim_account: Callable[..., Any],
    auto_buy_ai_stocks: Callable[..., Any],
    render_result_overview: Callable[..., Any],
    signal_density_hint: Callable[..., Any],
    standardize_result_df: Callable[..., Any],
    df_to_csv_bytes: Callable[..., Any],
) -> None:
    render_v49_stock_pool_entry(
        routes=routes,
        render_page_header=render_page_header,
        render_stock_pool_workspace=render_stock_pool_workspace,
    )
    render_v49_sector_flow_entry(
        routes=routes,
        render_sector_flow_page=render_sector_flow_page,
        render_page_header=render_page_header,
        market_scanner_cls=market_scanner_cls,
    )
    render_v49_ai_signal_entry(
        routes=routes,
        show_ai_signal_panel=show_ai_signal_panel,
        render_ai_signal_page=render_ai_signal_page,
        render_page_header=render_page_header,
        load_evolve_params=load_evolve_params,
        vp_analyzer=vp_analyzer,
        connect_permanent_db=connect_permanent_db,
        apply_filter_mode=apply_filter_mode,
        apply_multi_period_filter=apply_multi_period_filter,
        permanent_db_path=permanent_db_path,
        add_reason_summary=add_reason_summary,
        get_sim_account=get_sim_account,
        auto_buy_ai_stocks=auto_buy_ai_stocks,
        render_result_overview=render_result_overview,
        signal_density_hint=signal_density_hint,
        standardize_result_df=standardize_result_df,
        df_to_csv_bytes=df_to_csv_bytes,
    )
