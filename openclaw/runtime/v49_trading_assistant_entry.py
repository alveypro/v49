from __future__ import annotations

from typing import Any, Callable, Dict


ASSISTANT_TAB_LABELS = [
    "OpenClaw问答",
    "交易工作台",
    "持仓管理",
    "交易记录",
    "每日报告",
    "配置设置",
    "单股评分",
]


def is_v49_trading_assistant_route(routes: Dict[str, str]) -> bool:
    return routes.get("root") == "研究后台" and routes.get("research") == "智能助手"


def render_v49_trading_assistant_shell(
    *,
    st: Any,
    permanent_db_path: str,
    assistant_cls: Any,
    qa_assistant_cls: Any,
    render_page_header: Callable[..., Any],
    focus_tab_by_text: Callable[..., Any],
    set_focus_once: Callable[..., Any],
    render_qa_chat_shell: Callable[..., Any],
    render_qa_self_learning_panel: Callable[..., Any],
    render_qa_submission_controller: Callable[..., Any],
    render_assistant_ops_tabs: Callable[..., Any],
    render_result_overview: Callable[..., Any],
    render_single_stock_eval_tab: Callable[..., Any],
    notification_service_cls: Any,
    airivo_has_role: Callable[..., Any],
    airivo_guard_action: Callable[..., Any],
    airivo_append_action_audit: Callable[..., Any],
) -> None:
    render_page_header(
        " 智能交易助手",
        "半自动化交易 · 每日选股 · 持仓管理 · 止盈止损提醒",
        tag="Execution",
    )
    if "trading_assistant" not in st.session_state:
        st.session_state.trading_assistant = assistant_cls(db_path=permanent_db_path)
    assistant = st.session_state.trading_assistant

    qa_tab, sub_tab1, sub_tab2, sub_tab3, sub_tab4, sub_tab5, sub_tab6 = st.tabs(ASSISTANT_TAB_LABELS)
    desired_assistant_tab = st.session_state.pop("desired_assistant_tab", "")
    if desired_assistant_tab:
        focus_tab_by_text(desired_assistant_tab)

    with qa_tab:
        render_qa_chat_shell(
            qa_assistant_cls=qa_assistant_cls,
            permanent_db_path=permanent_db_path,
            set_focus_once=set_focus_once,
        )
        render_qa_self_learning_panel(
            qa_assistant=st.session_state.openclaw_qa_assistant,
            airivo_has_role=airivo_has_role,
            airivo_guard_action=airivo_guard_action,
            airivo_append_action_audit=airivo_append_action_audit,
            set_focus_once=set_focus_once,
        )
        render_qa_submission_controller(
            qa_assistant=st.session_state.openclaw_qa_assistant,
            set_focus_once=set_focus_once,
        )

    render_assistant_ops_tabs(
        sub_tab1=sub_tab1,
        sub_tab2=sub_tab2,
        sub_tab3=sub_tab3,
        sub_tab4=sub_tab4,
        sub_tab5=sub_tab5,
        sub_tab6=sub_tab6,
        assistant=assistant,
        render_result_overview=render_result_overview,
        render_single_stock_eval_tab=render_single_stock_eval_tab,
        notification_service_cls=notification_service_cls,
        airivo_has_role=airivo_has_role,
        airivo_guard_action=airivo_guard_action,
        airivo_append_action_audit=airivo_append_action_audit,
    )


def render_v49_trading_assistant_entry(
    *,
    routes: Dict[str, str],
    st: Any,
    permanent_db_path: str,
    render_page_header: Callable[..., Any],
    focus_tab_by_text: Callable[..., Any],
    set_focus_once: Callable[..., Any],
    render_qa_chat_shell: Callable[..., Any],
    render_qa_self_learning_panel: Callable[..., Any],
    render_qa_submission_controller: Callable[..., Any],
    render_assistant_ops_tabs: Callable[..., Any],
    render_result_overview: Callable[..., Any],
    render_single_stock_eval_tab: Callable[..., Any],
    notification_service_cls: Any,
    airivo_has_role: Callable[..., Any],
    airivo_guard_action: Callable[..., Any],
    airivo_append_action_audit: Callable[..., Any],
) -> None:
    if not is_v49_trading_assistant_route(routes):
        return
    try:
        from openclaw.assistant import OpenClawStockAssistant
        from trading_assistant import TradingAssistant

        render_v49_trading_assistant_shell(
            st=st,
            permanent_db_path=permanent_db_path,
            assistant_cls=TradingAssistant,
            qa_assistant_cls=OpenClawStockAssistant,
            render_page_header=render_page_header,
            focus_tab_by_text=focus_tab_by_text,
            set_focus_once=set_focus_once,
            render_qa_chat_shell=render_qa_chat_shell,
            render_qa_self_learning_panel=render_qa_self_learning_panel,
            render_qa_submission_controller=render_qa_submission_controller,
            render_assistant_ops_tabs=render_assistant_ops_tabs,
            render_result_overview=render_result_overview,
            render_single_stock_eval_tab=render_single_stock_eval_tab,
            notification_service_cls=notification_service_cls,
            airivo_has_role=airivo_has_role,
            airivo_guard_action=airivo_guard_action,
            airivo_append_action_audit=airivo_append_action_audit,
        )
    except ImportError as exc:
        st.error(f"交易助手模块加载失败: {exc}")
        st.info("请确保 trading_assistant.py 文件存在")
