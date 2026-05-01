from __future__ import annotations

from openclaw.runtime.v49_app_router import (
    OPS_ROUTES,
    PRODUCTION_ROUTES,
    RESEARCH_ROUTES,
    ROOT_ROUTES,
    apply_v49_desired_routes,
    render_v49_route_selector,
    subroute_defaults_for_root,
)


class FakeStreamlit:
    def __init__(self, radio_returns):
        self.radio_returns = list(radio_returns)
        self.calls = []

    def radio(self, *args, **kwargs):
        self.calls.append(("radio", args, kwargs))
        return self.radio_returns.pop(0)

    def caption(self, *args, **kwargs):
        self.calls.append(("caption", args, kwargs))


def test_apply_v49_desired_routes_maps_legacy_main_tab_to_authoritative_route():
    session = {"desired_main_tab": "研究与回测"}

    result = apply_v49_desired_routes(session)

    assert session["airivo_root_route"] == "研究后台"
    assert session["airivo_research_route"] == "回测与参数"
    assert "desired_main_tab" not in session
    assert result["root"] == "研究后台"


def test_apply_v49_desired_routes_direct_subroute_overrides_main_mapping():
    session = {
        "desired_main_tab": "今日决策",
        "desired_research_tab": "股票池分析",
    }

    result = apply_v49_desired_routes(session)

    assert session["airivo_root_route"] == "研究后台"
    assert session["airivo_production_route"] == "今日决策"
    assert session["airivo_research_route"] == "股票池分析"
    assert result["research"] == "股票池分析"


def test_apply_v49_desired_routes_preserves_unknown_root_compatibility():
    session = {"desired_main_tab": "自定义页面"}

    apply_v49_desired_routes(session)

    assert session["airivo_root_route"] == "自定义页面"


def test_v49_route_option_constants_freeze_workbench_topology():
    assert ROOT_ROUTES == ["生产后台", "研究后台", "运维后台"]
    assert PRODUCTION_ROUTES == ["今日决策", "执行中心", "策略演进"]
    assert RESEARCH_ROUTES == ["回测与参数", "股票池分析", "板块与热点", "智能助手"]
    assert OPS_ROUTES == ["数据与系统", "任务与日志"]


def test_subroute_defaults_for_root_freezes_single_route_cleanup():
    assert subroute_defaults_for_root("生产后台") == {"research": "", "ops": ""}
    assert subroute_defaults_for_root("研究后台") == {"production": "", "ops": ""}
    assert subroute_defaults_for_root("运维后台") == {"production": "", "research": ""}


def test_render_v49_route_selector_freezes_production_route_cleanup():
    fake_st = FakeStreamlit(["生产后台", "执行中心"])

    result = render_v49_route_selector(fake_st)

    assert result == {"root": "生产后台", "production": "执行中心", "research": "", "ops": ""}
    assert fake_st.calls[0][1][0] == "主工作面"
    assert fake_st.calls[1][0] == "caption"
    assert fake_st.calls[2][1][0] == "生产工作面"


def test_render_v49_route_selector_freezes_research_route_cleanup():
    fake_st = FakeStreamlit(["研究后台", "股票池分析"])

    result = render_v49_route_selector(fake_st)

    assert result == {"root": "研究后台", "production": "", "research": "股票池分析", "ops": ""}


def test_render_v49_route_selector_freezes_ops_route_cleanup():
    fake_st = FakeStreamlit(["运维后台", "任务与日志"])

    result = render_v49_route_selector(fake_st)

    assert result == {"root": "运维后台", "production": "", "research": "", "ops": "任务与日志"}
