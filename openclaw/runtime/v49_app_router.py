from __future__ import annotations

from typing import Any, Dict, MutableMapping, Tuple


ROOT_ROUTES = ["生产后台", "研究后台", "运维后台"]
PRODUCTION_ROUTES = ["今日决策", "执行中心", "策略演进"]
RESEARCH_ROUTES = ["回测与参数", "股票池分析", "板块与热点", "智能助手"]
OPS_ROUTES = ["数据与系统", "任务与日志"]

DESIRED_MAIN_ROUTE_MAP: Dict[str, Tuple[str, str]] = {
    "今日决策": ("生产后台", "今日决策"),
    "执行中心": ("生产后台", "执行中心"),
    "策略演进": ("生产后台", "策略演进"),
    "研究与回测": ("研究后台", "回测与参数"),
    "股票池分析": ("研究后台", "股票池分析"),
    "板块与热点": ("研究后台", "板块与热点"),
    "智能交易助手": ("研究后台", "智能助手"),
    "数据与系统": ("运维后台", "数据与系统"),
    "任务与日志": ("运维后台", "任务与日志"),
}


def _set_root_subroute(session_state: MutableMapping[str, Any], root: str, subroute: str = "") -> None:
    session_state["airivo_root_route"] = root
    if root == "生产后台" and subroute:
        session_state["airivo_production_route"] = subroute
    elif root == "研究后台" and subroute:
        session_state["airivo_research_route"] = subroute
    elif root == "运维后台" and subroute:
        session_state["airivo_ops_route"] = subroute


def apply_v49_desired_routes(session_state: MutableMapping[str, Any]) -> Dict[str, str]:
    desired_main_tab = session_state.pop("desired_main_tab", "")
    if desired_main_tab:
        mapped_root, mapped_sub = DESIRED_MAIN_ROUTE_MAP.get(desired_main_tab, (desired_main_tab, ""))
        _set_root_subroute(session_state, mapped_root, mapped_sub)

    desired_production_tab = session_state.pop("desired_production_tab", "")
    if desired_production_tab:
        _set_root_subroute(session_state, "生产后台", desired_production_tab)

    desired_research_tab = session_state.pop("desired_research_tab", "")
    if desired_research_tab:
        _set_root_subroute(session_state, "研究后台", desired_research_tab)

    desired_ops_tab = session_state.pop("desired_ops_tab", "")
    if desired_ops_tab:
        _set_root_subroute(session_state, "运维后台", desired_ops_tab)

    return {
        "root": str(session_state.get("airivo_root_route", "")),
        "production": str(session_state.get("airivo_production_route", "")),
        "research": str(session_state.get("airivo_research_route", "")),
        "ops": str(session_state.get("airivo_ops_route", "")),
    }


def subroute_defaults_for_root(root: str) -> Dict[str, str]:
    if root == "生产后台":
        return {"research": "", "ops": ""}
    if root == "研究后台":
        return {"production": "", "ops": ""}
    return {"production": "", "research": ""}


def render_v49_route_selector(st: Any) -> Dict[str, str]:
    current_root = st.radio(
        "主工作面",
        ROOT_ROUTES,
        key="airivo_root_route",
        horizontal=True,
        label_visibility="collapsed",
    )

    if current_root == "生产后台":
        st.caption("只保留当天真正要做的事：今日决策、执行中心、策略演进。")
        current_production_route = st.radio(
            "生产工作面",
            PRODUCTION_ROUTES,
            key="airivo_production_route",
            horizontal=True,
            label_visibility="collapsed",
        )
        current_research_route = ""
        current_ops_route = ""
    elif current_root == "研究后台":
        st.caption("研究后台不参与当天生产决策，专门放回测、实验策略、辅助分析。")
        current_research_route = st.radio(
            "研究工作面",
            RESEARCH_ROUTES,
            key="airivo_research_route",
            horizontal=True,
            label_visibility="collapsed",
        )
        current_production_route = ""
        current_ops_route = ""
    else:
        st.caption("运维后台只放数据、健康门禁、任务与日志。")
        current_ops_route = st.radio(
            "运维工作面",
            OPS_ROUTES,
            key="airivo_ops_route",
            horizontal=True,
            label_visibility="collapsed",
        )
        current_production_route = ""
        current_research_route = ""

    return {
        "root": current_root,
        "production": current_production_route,
        "research": current_research_route,
        "ops": current_ops_route,
    }
