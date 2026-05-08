from __future__ import annotations


def _surface_status(value: object) -> str:
    text = str(value or "").strip()
    if not text or text == "-":
        return "待确认"
    mapping = {
        "approved": "已对齐",
        "completed": "已完成",
        "up_to_date": "已更新",
        "partial_success": "待补齐",
        "blocked": "待复核",
        "failed": "待复核",
        "running": "运行中",
    }
    return mapping.get(text.lower(), text)


def _surface_observation_label(value: object) -> str:
    text = str(value or "").strip()
    if not text or text == "-":
        return "等待形成"
    return text


def _surface_sync_preflight_label(server_sync_preflight: dict[str, object] | None) -> str:
    payload = server_sync_preflight or {}
    decision = payload.get("sync_decision")
    if not isinstance(decision, dict):
        return "服务器同步 待预检"
    if decision.get("allowed_to_sync") is True:
        return "服务器同步 可同步"
    blocking = decision.get("blocking_checks") or []
    blocked_total = len(blocking) if isinstance(blocking, list) else 0
    suffix = f"{blocked_total}项阻断" if blocked_total else "阻断"
    return f"服务器同步 {suffix}"


def build_jump_strip_context(*, current_view: str) -> dict[str, object]:
    jump_links_by_view = {
        "overview": [
            {"label": "系统结论", "anchor": "summary"},
            {"label": "T12 镜像", "anchor": "t12-overview-card"},
            {"label": "候选机会", "anchor": "opportunities"},
            {"label": "选股漏斗", "anchor": "selection-funnel"},
            {"label": "诊断附录", "anchor": "diagnostics-appendix"},
            {"label": "快速入口", "anchor": "links"},
        ],
        "research": [
            {"label": "方法结构", "anchor": "architecture"},
            {"label": "策略诊断", "anchor": "diagnosis"},
            {"label": "研究图形", "anchor": "research-visuals"},
        ],
        "candidates": [
            {"label": "候选终端", "anchor": "candidate-focus"},
            {"label": "候选阵列", "anchor": "candidate-actions"},
            {"label": "候选图形", "anchor": "candidate-visuals"},
            {"label": "全A预筛", "anchor": "prefilter"},
        ],
        "operations": [
            {"label": "运维中心", "anchor": "ops"},
            {"label": "策略诊断", "anchor": "diagnosis"},
            {"label": "全A预筛", "anchor": "prefilter"},
        ],
        "reports": [
            {"label": "证据链", "anchor": "reports"},
        ],
        "t12": [
            {"label": "制度总览", "anchor": "t12-overview-card"},
            {"label": "治理摘要", "anchor": "t12-governance-summary"},
        ],
    }
    tool_button = None
    if current_view == "overview":
        tool_button = {"label": "聚焦主内容", "body_class": "overview-focus"}
    elif current_view == "reports":
        tool_button = {"label": "折叠目录", "body_class": "reports-focus"}
    return {
        "links": jump_links_by_view.get(current_view, []),
        "tool_button": tool_button,
    }


def build_section_visibility_context(*, current_view: str) -> dict[str, bool]:
    return {
        "overview": {
            "hero": True,
            "kpi": True,
            "architecture": False,
            "summary": True,
            "actions": True,
            "operations": True,
            "prefilter": True,
            "top1": True,
            "candidate_focus": False,
            "diagnosis": True,
            "links": True,
            "guide": False,
            "charts": False,
            "reports": False,
        },
        "research": {
            "hero": True,
            "kpi": True,
            "architecture": True,
            "summary": False,
            "actions": False,
            "operations": False,
            "prefilter": False,
            "top1": False,
            "candidate_focus": False,
            "diagnosis": True,
            "links": False,
            "guide": False,
            "charts": False,
            "reports": False,
        },
        "candidates": {
            "hero": True,
            "kpi": False,
            "architecture": False,
            "summary": False,
            "actions": True,
            "operations": False,
            "prefilter": True,
            "top1": False,
            "candidate_focus": True,
            "diagnosis": False,
            "links": False,
            "guide": False,
            "charts": False,
            "reports": False,
        },
        "operations": {
            "hero": True,
            "kpi": True,
            "architecture": False,
            "summary": False,
            "actions": False,
            "operations": True,
            "prefilter": True,
            "top1": False,
            "candidate_focus": False,
            "diagnosis": True,
            "links": True,
            "guide": False,
            "charts": False,
            "reports": False,
        },
        "reports": {
            "hero": True,
            "kpi": False,
            "architecture": False,
            "summary": False,
            "actions": False,
            "operations": False,
            "prefilter": False,
            "top1": False,
            "candidate_focus": False,
            "diagnosis": False,
            "links": True,
            "guide": False,
            "charts": False,
            "reports": True,
        },
        "t12": {
            "hero": True,
            "kpi": False,
            "architecture": False,
            "summary": False,
            "actions": False,
            "operations": False,
            "prefilter": False,
            "top1": False,
            "candidate_focus": False,
            "diagnosis": False,
            "links": False,
            "guide": False,
            "charts": False,
            "reports": False,
        },
    }.get(current_view, {})


def build_page_shell_context(
    *,
    current_view: str,
    candidate_index: int,
    base_path: str,
    view_labels: dict[str, str],
    health_status: str,
    update_status_label: str,
    update_stage_label: str,
    report_state: str,
    db_latest_trade_date: str,
    candidate_timeline_label: str,
    observation_timeline_label: str,
    prefilter_freshness_label: str,
    automation_health_label: str,
    server_sync_preflight: dict[str, object] | None = None,
) -> dict[str, object]:
    base = str(base_path or "").rstrip("/")
    prefix = f"{base}/" if base else "/"

    def _nav_href(view: str) -> str:
        if view == "candidates":
            return f"{prefix}?view={view}&candidate={candidate_index}"
        return f"{prefix}?view={view}"

    nav_items = [
        {
            "label": label,
            "href": _nav_href(view),
            "active": view == current_view,
        }
        for view, label in view_labels.items()
    ]
    sidebar_stats = [
        {"label": "健康", "value": health_status},
        {"label": "更新", "value": _surface_status(update_status_label)},
        {"label": "阶段", "value": _surface_status(update_stage_label)},
        {"label": "报告", "value": report_state},
    ]
    architecture_steps = [
        {"index": "01", "title": "数据层", "desc": "拉取 A 股日线/交易日历/基础资料，做清洗、合并和运行缓存。"},
        {"index": "02", "title": "特征层", "desc": "构建趋势、动量、波动率、量价、市场环境和风险特征。"},
        {"index": "03", "title": "预测层", "desc": "用 LightGBM / XGBoost / RF / Logistic 等模型输出上涨方向、收益区间和置信度。"},
        {"index": "04", "title": "信号层", "desc": "把模型预测、市场状态、风控过滤融合成买卖信号。"},
        {"index": "05", "title": "执行与回测", "desc": "按 A 股规则模拟成交、统计收益、夏普、回撤、胜率。"},
        {"index": "06", "title": "研究与进化", "desc": "批量跑 profile、生成日报、回放失败配置、持续优化参数。"},
    ]
    return {
        "nav_items": nav_items,
        "sidebar_stats": sidebar_stats,
        "architecture_steps": architecture_steps,
        "topbar_pills": [
            f"数据最新交易日 {db_latest_trade_date}",
            f"候选产物 {candidate_timeline_label}",
            f"观察窗口 {_surface_observation_label(observation_timeline_label)}",
            prefilter_freshness_label,
            automation_health_label,
            _surface_sync_preflight_label(server_sync_preflight),
            f"更新 {_surface_status(update_status_label)}",
        ],
    }


def build_primary_result_bridge_context(
    *,
    current_view: str,
    primary_result_bridge_enabled: bool,
    primary_result_api_url: str,
    primary_result_initial_json_html: str,
) -> dict[str, object]:
    enabled_for_view = current_view in {"overview", "candidates"}
    enabled = bool(primary_result_bridge_enabled and enabled_for_view)
    return {
        "enabled_for_view": enabled_for_view,
        "enabled": enabled,
        "api_url": primary_result_api_url,
        "initial_json_html": primary_result_initial_json_html if enabled_for_view else "",
    }


def build_page_interaction_context(
    *,
    current_view: str,
    candidate_index: int,
    candidate_count: int,
    candidate_base_href: str,
) -> dict[str, object]:
    return {
        "current_view": current_view,
        "candidate_index": candidate_index,
        "candidate_count": candidate_count,
        "candidate_base_href": candidate_base_href,
    }


def build_dashboard_page_shell_contract(
    *,
    nav_html: str,
    sidebar_status_html: str,
    topbar_pills_html: str,
    top_story_html: str,
    current_view: str,
    kpi_html: str,
    primary_result_bridge_json: str,
    main_content_html: str,
) -> dict[str, object]:
    return {
        "nav_html": nav_html,
        "sidebar_status_html": sidebar_status_html,
        "topbar_pills_html": topbar_pills_html,
        "top_story_html": top_story_html,
        "current_view": current_view,
        "kpi_html": kpi_html,
        "primary_result_bridge_json": primary_result_bridge_json,
        "main_content_html": main_content_html,
    }


def build_dashboard_asset_contract(
    *,
    dashboard_style_tag: str,
    dashboard_script_tag: str,
    document_title: str = "Airivo Alpha | 股票研究终端",
) -> dict[str, str]:
    return {
        "document_title": document_title,
        "dashboard_style_tag": dashboard_style_tag,
        "dashboard_script_tag": dashboard_script_tag,
    }


def build_candidate_focus_render_contract(
    *,
    candidate_focus_view_model: dict[str, object],
    candidate_cards: list[dict[str, object]],
    candidate_index: int,
    base_path: str,
) -> dict[str, object]:
    current_index = int(candidate_focus_view_model.get("current_index", candidate_index) or candidate_index)
    prev_index = int(candidate_focus_view_model.get("prev_index", max(0, candidate_index - 1)) or 0)
    next_index = int(candidate_focus_view_model.get("next_index", candidate_index) or candidate_index)
    total_candidates = max(
        int(candidate_focus_view_model.get("total_candidates", len(candidate_cards)) or len(candidate_cards)),
        1,
    )
    nav = {
        "prev_href": f"{base_path}/?view=candidates&candidate={prev_index}",
        "prev_disabled": current_index == 0,
        "next_href": f"{base_path}/?view=candidates&candidate={next_index}",
        "next_disabled": current_index >= len(candidate_cards) - 1,
        "position_label": str(
            candidate_focus_view_model.get("current_position_label", f"候选 {candidate_index + 1} / {total_candidates}")
        ),
    }
    quick_links = [
        {
            "label": str(link.get("label", "")),
            "ts_code": str(link.get("ts_code", "")),
            "href": f'{base_path}/?view=candidates&candidate={int(link.get("index", 0) or 0)}',
            "active": bool(link.get("active")),
        }
        for link in candidate_focus_view_model.get("quick_links", [])
    ]
    switcher_items = [
        {
            "rank": f"#{index + 1}",
            "href": f"{base_path}/?view=candidates&candidate={index}",
            "active": index == candidate_index,
            "ts_code": str(card.get("ts_code", "")),
            "stock_name": str(card.get("stock_name", "")),
            "signal": str(card.get("signal", "")),
            "final_score": str(card.get("final_score", "")),
        }
        for index, card in enumerate(candidate_cards)
    ]
    return {
        "nav": nav,
        "quick_links": quick_links,
        "switcher_items": switcher_items,
    }


def build_main_content_sections(
    *,
    first_place_evidence_cockpit_section: str,
    summary_section: str,
    primary_result_card_section: str,
    kpi_html: str,
    t12_overview_card_section: str,
    spotlight_html: str,
    overview_visuals_disclosure_section: str,
    overview_operations_disclosure_section: str,
    links_section: str,
    opportunity_cards_html: str,
    candidate_compare_section: str,
    market_snapshot_section: str,
    selection_funnel_section: str,
    diagnostics_appendix_section: str,
    validation_section: str,
    diagnosis_section: str,
    prefilter_section: str,
    architecture_section: str,
    research_visuals_section: str,
    candidate_focus_section: str,
    actions_section: str,
    candidate_visuals_section: str,
    operations_section: str,
    reports_section: str,
    t12_governance_summary_section: str,
    charts_section: str,
    top1_section: str,
    guide_section: str,
) -> dict[str, str]:
    return {
        "first_place_evidence_cockpit_section": first_place_evidence_cockpit_section,
        "summary_section": summary_section,
        "primary_result_card_section": primary_result_card_section,
        "kpi_html": kpi_html,
        "t12_overview_card_section": t12_overview_card_section,
        "spotlight_html": spotlight_html,
        "overview_visuals_disclosure_section": overview_visuals_disclosure_section,
        "overview_operations_disclosure_section": overview_operations_disclosure_section,
        "links_section": links_section,
        "opportunity_cards_html": opportunity_cards_html,
        "candidate_compare_section": candidate_compare_section,
        "market_snapshot_section": market_snapshot_section,
        "selection_funnel_section": selection_funnel_section,
        "diagnostics_appendix_section": diagnostics_appendix_section,
        "validation_section": validation_section,
        "diagnosis_section": diagnosis_section,
        "prefilter_section": prefilter_section,
        "architecture_section": architecture_section,
        "research_visuals_section": research_visuals_section,
        "candidate_focus_section": candidate_focus_section,
        "actions_section": actions_section,
        "candidate_visuals_section": candidate_visuals_section,
        "operations_section": operations_section,
        "reports_section": reports_section,
        "t12_governance_summary_section": t12_governance_summary_section,
        "charts_section": charts_section,
        "top1_section": top1_section,
        "guide_section": guide_section,
    }


def build_top_story_context(
    *,
    current_view: str,
    external_decision_spine_html: str,
    external_system_summary_html: str,
    jump_strip_html: str,
    view_banner_html: str,
    control_strip_html: str,
    headline_html: str,
    today_brief_html: str,
    hero_side_html: str,
    command_deck_html: str,
    top1_label: str,
    top1_signal: str,
    top1_risk: str,
) -> dict[str, str]:
    return {
        "current_view": current_view,
        "external_decision_spine_html": external_decision_spine_html,
        "external_system_summary_html": external_system_summary_html,
        "jump_strip_html": jump_strip_html,
        "view_banner_html": view_banner_html,
        "control_strip_html": control_strip_html,
        "headline_html": headline_html,
        "today_brief_html": today_brief_html,
        "hero_side_html": hero_side_html,
        "command_deck_html": command_deck_html,
        "top1_label": top1_label,
        "top1_signal": top1_signal,
        "top1_risk": top1_risk,
    }
