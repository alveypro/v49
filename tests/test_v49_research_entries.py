from __future__ import annotations

from openclaw.runtime.v49_research_entries import (
    is_v49_ai_signal_route,
    is_v49_sector_flow_route,
    is_v49_stock_pool_route,
    render_v49_research_light_entries,
)


def _recorder(calls, name, return_value=None):
    def _inner(*args, **kwargs):
        calls.append((name, args, kwargs))
        return return_value

    return _inner


def _research_kwargs(calls, routes):
    return {
        "routes": routes,
        "show_ai_signal_panel": True,
        "render_page_header": _recorder(calls, "header"),
        "render_stock_pool_workspace": _recorder(calls, "pool"),
        "render_sector_flow_page": _recorder(calls, "sector"),
        "market_scanner_cls": object,
        "render_ai_signal_page": _recorder(calls, "ai"),
        "load_evolve_params": _recorder(calls, "evolve"),
        "vp_analyzer": object(),
        "connect_permanent_db": _recorder(calls, "connect"),
        "apply_filter_mode": _recorder(calls, "filter"),
        "apply_multi_period_filter": _recorder(calls, "multi_filter"),
        "permanent_db_path": "/tmp/db.sqlite",
        "add_reason_summary": _recorder(calls, "reason"),
        "get_sim_account": _recorder(calls, "sim"),
        "auto_buy_ai_stocks": _recorder(calls, "buy"),
        "render_result_overview": _recorder(calls, "overview"),
        "signal_density_hint": _recorder(calls, "density"),
        "standardize_result_df": _recorder(calls, "standardize"),
        "df_to_csv_bytes": _recorder(calls, "csv"),
    }


def test_v49_research_route_predicates_freeze_scope():
    assert is_v49_stock_pool_route({"root": "研究后台", "research": "股票池分析"})
    assert is_v49_sector_flow_route({"root": "研究后台", "research": "板块与热点"})
    assert is_v49_ai_signal_route({"root": "研究后台", "research": "智能助手"})
    assert not is_v49_ai_signal_route({"root": "生产后台", "production": "今日决策"})


def test_render_v49_research_light_entries_dispatches_stock_pool():
    calls = []
    render_v49_research_light_entries(**_research_kwargs(calls, {"root": "研究后台", "research": "股票池分析"}))

    assert [call[0] for call in calls] == ["header", "pool"]


def test_render_v49_research_light_entries_dispatches_sector_flow():
    calls = []
    render_v49_research_light_entries(**_research_kwargs(calls, {"root": "研究后台", "research": "板块与热点"}))

    assert [call[0] for call in calls] == ["sector"]


def test_render_v49_research_light_entries_dispatches_ai_signal():
    calls = []
    render_v49_research_light_entries(**_research_kwargs(calls, {"root": "研究后台", "research": "智能助手"}))

    assert [call[0] for call in calls] == ["ai"]
