from __future__ import annotations

import html


def _surface_top_label(value: str) -> str:
    text = str(value or "").strip()
    if not text or text in {"-", "暂无"}:
        return "等待候选形成"
    return text


def compose_primary_result_shell_html(
    *,
    primary_result_card_html: str,
    primary_result_bridge_shell_html: str,
) -> str:
    return f"{primary_result_card_html}{primary_result_bridge_shell_html}"


def compose_main_content_html(
    *,
    current_view: str,
    sections: dict[str, str],
) -> str:
    if current_view == "overview":
        return (
            '<div class="layout-main layout-main-balanced overview-layout">'
            '<div class="stack">'
            f'{sections["actions_section"]}'
            '</div>'
            '<div class="stack decision-sidebar overview-secondary">'
            '<div class="decision-sidebar-head">'
            '<div class="eyebrow">当前重点</div>'
            '<h3>关注对象与材料</h3>'
            '<div class="muted">先看观察对象，再查研究材料。</div>'
            '</div>'
            f'{sections["spotlight_html"]}'
            f'{sections["links_section"]}'
            '</div>'
            '</div>'
            '<details class="overview-noise-disclosure professional-evidence-disclosure overview-deep-dive" id="professional-evidence">'
            '<summary>'
            '<span>专业依据</span>'
            '<strong>展开完整证据</strong>'
            '<small>主结果、对比、市场状态、漏斗和内部复核。</small>'
            '</summary>'
            '<div class="stack">'
            f'{sections["candidate_compare_section"]}'
            f'{sections["market_snapshot_section"]}'
            f'{sections["selection_funnel_section"]}'
            f'{sections["diagnostics_appendix_section"]}'
            f'{sections["overview_visuals_disclosure_section"]}'
            f'{sections["overview_operations_disclosure_section"]}'
            '</div>'
            '</details>'
        )
    if current_view == "research":
        return (
            '<div class="layout-main layout-main-balanced">'
            '<div class="stack">'
            f'{sections["architecture_section"]}'
            f'{sections["diagnosis_section"]}'
            '</div>'
            '<div class="stack">'
            f'{sections["research_visuals_section"]}'
            '</div>'
            '</div>'
        )
    if current_view == "candidates":
        return (
            '<div class="stack">'
            f'{sections["primary_result_card_section"]}'
            f'{sections["candidate_focus_section"]}'
            '<div class="layout-main layout-main-balanced">'
            '<div class="stack">'
            f'{sections["candidate_compare_section"]}'
            f'{sections["market_snapshot_section"]}'
            f'{sections["selection_funnel_section"]}'
            f'{sections["validation_section"]}'
            '</div>'
            '<div class="stack decision-sidebar">'
            '<div class="decision-sidebar-head">'
            '<div class="eyebrow">研究辅助</div>'
            '<h3>候选决策侧栏</h3>'
            '<div class="muted">把候选阵列、图形视图和辅助判断收在同一列，阅读路径更稳定。</div>'
            '</div>'
            f'{sections["actions_section"]}'
            f'{sections["candidate_visuals_section"]}'
            '</div>'
            '</div>'
            f'{sections["prefilter_section"]}'
            '</div>'
        )
    if current_view == "operations":
        return (
            '<div class="layout-main layout-main-balanced">'
            '<div class="stack">'
            f'{sections["operations_section"]}'
            '</div>'
            '<div class="stack">'
            f'{sections["diagnosis_section"]}'
            f'{sections["links_section"]}'
            '</div>'
            '</div>'
            f'{sections["prefilter_section"]}'
        )
    if current_view == "reports":
        return sections["reports_section"]
    if current_view == "t12":
        return (
            '<div class="layout-main layout-main-balanced">'
            '<div class="stack">'
            f'{sections["t12_overview_card_section"]}'
            f'{sections["t12_governance_summary_section"]}'
            '</div>'
            '<div class="stack">'
            '<div class="card t12-readonly-note" id="t12-readonly-note">'
            '<div class="section-title">'
            '<div><div class="eyebrow">T12 边界</div><h3>只读镜像范围</h3></div>'
            '<div class="muted">当前页面只保留最小制度镜像与治理摘要，不包含操作入口。</div>'
            '</div>'
            '<div class="t12-readonly-note__body">共享制度事实层只读、服务端渲染、无按钮、无 bridge、无动作回写。</div>'
            '</div>'
            '</div>'
            '</div>'
        )
    return (
        '<div class="layout-main">'
        '<div class="stack">'
        f'{sections["architecture_section"]}{sections["summary_section"]}{sections["actions_section"]}'
        '</div>'
        '<div class="stack">'
        f'{sections["spotlight_html"]}{sections["candidate_focus_section"]}{sections["operations_section"]}{sections["top1_section"]}{sections["diagnosis_section"]}{sections["links_section"]}{sections["guide_section"]}'
        '</div>'
        '</div>'
        f'{sections["charts_section"]}{sections["reports_section"]}'
    )


def compose_top_story_html(
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
) -> str:
    if current_view == "overview":
        return external_decision_spine_html
    if current_view in {"research", "candidates", "operations", "reports"}:
        return f'{view_banner_html}{jump_strip_html}'
    return (
        f'{view_banner_html}'
        f'{external_decision_spine_html}'
        f'{control_strip_html}'
        f'{jump_strip_html}'
        f'{headline_html}'
        f'{today_brief_html}'
        '<div class="hero hero-compact" id="overview">'
        '<div class="hero-top">'
        '<div>'
        '<div class="eyebrow" style="color: rgba(248, 251, 255, 0.72);">生产研究视图</div>'
        '<h1>服务器主结果、候选篮和证据链同屏对齐</h1>'
        '<p>展示以服务器 artifacts、当前 pointer 和 /stock/api/primary-result 为准；首屏先给出结论、证据、边界和下一步，再进入候选、预筛和历史验证。</p>'
        '</div>'
        f'{hero_side_html}'
        '</div>'
        '<div class="hero-bottom">'
        '<div class="hero-meta">'
        '<div class="meta-chip">服务器 artifacts</div>'
        '<div class="meta-chip">/stock API 对齐</div>'
        '<div class="meta-chip">候选篮 pointer</div>'
        '<div class="meta-chip">只读展示</div>'
        '</div>'
        f'<div class="muted" style="color: rgba(248, 251, 255, 0.76);">当前优先候选：{html.escape(_surface_top_label(top1_label))} · 信号 {html.escape(top1_signal)} · 风险 {html.escape(top1_risk)}</div>'
        '</div>'
        '</div>'
        f'{command_deck_html}'
    )


def compose_page_shell_html(
    *,
    nav_html: str,
    sidebar_status_html: str,
    topbar_pills_html: str,
    top_story_html: str,
    current_view: str,
    kpi_html: str,
    primary_result_bridge_json: str,
    main_content_html: str,
) -> str:
    return (
        '<body id="top">'
        '<div class="app-shell">'
        '<aside class="sidebar">'
        '<div class="sidebar-brand">'
        '<div class="brand-mark">Airivo Stock</div>'
        '<h2>股票研究观察台</h2>'
        '<p>当前判断、观察名单和风险边界。</p>'
        '</div>'
        '<div class="nav-list">'
        f'{nav_html}'
        '</div>'
        f'{sidebar_status_html}'
        '</aside>'
        '<div class="container">'
        '<div class="masthead-stack">'
        '<div class="topbar">'
        '<div class="topbar-title">'
        '<strong>Airivo Stock</strong>'
        '<span>先看判断，再看观察名单</span>'
        '</div>'
        '<div class="topbar-meta">'
        f'{topbar_pills_html}'
        '</div>'
        '</div>'
        f'{top_story_html}'
        f'{"" if current_view == "overview" else kpi_html}'
        f'{primary_result_bridge_json}'
        f'{main_content_html}'
        '</div>'
        '</div>'
    )
