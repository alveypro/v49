from __future__ import annotations

from typing import Any, Callable

from openclaw.runtime.assistant_config_page import render_assistant_config_page
from openclaw.runtime.assistant_daily_report_page import render_assistant_daily_report_page
from openclaw.runtime.assistant_holdings_page import render_assistant_holdings_page
from openclaw.runtime.assistant_trade_history_page import render_assistant_trade_history_page
from openclaw.runtime.assistant_workbench_page import render_assistant_workbench_page


def render_assistant_ops_tabs(
    *,
    sub_tab1: Any,
    sub_tab2: Any,
    sub_tab3: Any,
    sub_tab4: Any,
    sub_tab5: Any,
    sub_tab6: Any,
    assistant: Any,
    render_result_overview: Callable[..., None],
    render_single_stock_eval_tab: Callable[[Any], None],
    notification_service_cls: Any,
    airivo_has_role: Callable[[str], bool],
    airivo_guard_action: Callable[[str, str, str, str], bool],
    airivo_append_action_audit: Callable[..., None],
) -> None:
    with sub_tab1:
        render_assistant_workbench_page(
            assistant=assistant,
            render_result_overview=render_result_overview,
        )

    with sub_tab2:
        render_assistant_holdings_page(assistant=assistant)

    with sub_tab3:
        render_assistant_trade_history_page(assistant_db=assistant.assistant_db)

    with sub_tab4:
        render_assistant_daily_report_page(assistant=assistant)

    with sub_tab5:
        render_assistant_config_page(
            assistant=assistant,
            notification_service_cls=notification_service_cls,
            airivo_has_role=airivo_has_role,
            airivo_guard_action=airivo_guard_action,
            airivo_append_action_audit=airivo_append_action_audit,
        )

    with sub_tab6:
        render_single_stock_eval_tab(assistant)
