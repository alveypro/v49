from __future__ import annotations

import html


def _attach_table_id(table_html: str, table_id: str) -> str:
    return table_html.replace("<table>", f'<table id="{table_id}">', 1)


def render_table_export_block(title: str, table_id: str, filename: str, table_html: str) -> str:
    if "<table" not in table_html:
        return table_html
    return (
        '<div class="table-export-block">'
        '<div class="table-export-bar">'
        f'<div class="table-export-title">{html.escape(title)}</div>'
        f'<button class="table-export-btn" type="button" onclick="exportTableToCsv(\'{table_id}\', \'{filename}\')">导出当前表格 CSV</button>'
        '</div>'
        f'{_attach_table_id(table_html, table_id)}'
        '</div>'
    )


def render_dual_track_rows(rows: list[dict[str, str]]) -> str:
    tone_colors = {"pointer": "#35566e", "attempt": "#9f2f2f"}
    rendered = []
    for row in rows:
        text = str(row.get("text", "")).strip()
        if not text:
            continue
        rendered.append(
            '<div class="muted" style="color:'
            f'{tone_colors.get(str(row.get("tone", "")), "#4d5c69")};">'
            f'{html.escape(text)}'
            '</div>'
        )
    return "".join(rendered)


def render_metric_ribbon(chips: list[str]) -> str:
    return '<div class="metric-ribbon">' + "".join(
        f'<div class="metric-chip">{html.escape(chip)}</div>' for chip in chips if str(chip).strip()
    ) + "</div>"


def render_kpi_grid_rows(rows: list[list[dict[str, str]]]) -> str:
    rendered_rows = []
    for idx, row in enumerate(rows):
        klass = "grid-kpi" if idx == 0 and len(row) == 4 else "grid3"
        style = "" if idx == 0 else ' style="margin-top:12px;"'
        rendered_rows.append(
            f'<div class="{klass}"{style}>'
            + "".join(
                '<div class="kpi">'
                f'<div class="label">{html.escape(str(item.get("label", "")))}</div>'
                f'<div class="value">{html.escape(str(item.get("value", "")))}</div>'
                f'<div class="sub">{html.escape(str(item.get("sub", "")))}</div>'
                '</div>'
                for item in row
            )
            + '</div>'
        )
    return "".join(rendered_rows)


def render_overview_kpi_section(overview_kpi_view_model: dict[str, object]) -> str:
    return render_kpi_grid_rows(overview_kpi_view_model["kpi_rows"])


def render_jump_strip(jump_strip_view_model: dict[str, object]) -> str:
    links_html = "".join(
        f'<a class="jump-link" href="#{html.escape(str(item["anchor"]))}">{html.escape(str(item["label"]))}</a>'
        for item in jump_strip_view_model.get("links", [])
    )
    tool_button = jump_strip_view_model.get("tool_button")
    tool_html = ""
    if isinstance(tool_button, dict):
        tool_html = (
            '<button class="tool-button" type="button" '
            f'data-body-class="{html.escape(str(tool_button.get("body_class", "")))}">'
            f'{html.escape(str(tool_button.get("label", "")))}'
            '</button>'
        )
    return f'<div class="jump-strip">{links_html}{tool_html}<a class="jump-link jump-link-ghost" href="#top">返回顶部</a></div>'


def render_nav_links(nav_items: list[dict[str, object]]) -> str:
    return "".join(
        f'<a class="nav-link{" nav-link-active" if bool(item.get("active")) else ""}" href="{html.escape(str(item.get("href", "#")))}">{html.escape(str(item.get("label", "")))}</a>'
        for item in nav_items
    )


def render_sidebar_status_section(sidebar_stats: list[dict[str, str]]) -> str:
    return (
        '<div class="sidebar-section">'
        '<div class="sidebar-caption">系统状态</div>'
        + "".join(
            f'<div class="sidebar-stat"><span>{html.escape(str(item.get("label", "")))}</span><strong>{html.escape(str(item.get("value", "")))}</strong></div>'
            for item in sidebar_stats
        )
        + '</div>'
    )


def render_topbar_pills_section(topbar_pills: list[str]) -> str:
    return "".join(
        f'<div class="topbar-pill">{html.escape(str(item))}</div>'
        for item in topbar_pills
        if str(item).strip()
    )


def render_home_headline_section(hero_facts: dict[str, str]) -> str:
    pointer_html = (
        f'<div class="headline-detail" style="margin-top:8px; color:#35566e;">{html.escape(hero_facts["current_basket_pointer_label"])}</div>'
        if hero_facts["current_basket_pointer_label"]
        else ""
    )
    attempt_html = (
        f'<div class="headline-detail" style="margin-top:8px; color:#9f2f2f;">{html.escape(hero_facts["latest_basket_attempt_label"])}</div>'
        if hero_facts["latest_basket_attempt_label"]
        else ""
    )
    return (
        '<div class="headline-bar">'
        '<div class="headline-kicker">今日判断</div>'
        f'<div class="headline-title">{hero_facts["headline_tone"]}</div>'
        f'<div class="headline-detail">{hero_facts["headline_detail"]}</div>'
        f"{pointer_html}"
        f"{attempt_html}"
        "</div>"
    )


def render_external_decision_spine_section(spine: dict[str, str]) -> str:
    return (
        '<section class="external-decision-spine external-decision-public" id="external-decision-spine" aria-label="当前判断">'
        '<div class="external-spine-lead">'
        '<div class="external-spine-kicker">对外决策脊柱</div>'
        '<div class="muted">先读这里</div>'
        '<div class="muted">数据门通过前保持制度锁定</div>'
        f'<h2>{html.escape(spine["decision_status"])}</h2>'
        f'<p>{html.escape(spine["decision_reason"])}。{html.escape(spine["decision_summary"])}。</p>'
        '</div>'
        '<div class="external-spine-grid">'
        '<div class="external-spine-fact">'
        '<span>下一步</span>'
        f'<strong>{html.escape(spine["next_check"])}</strong>'
        '<small>更新后再看</small>'
        '</div>'
        '<div class="external-spine-fact">'
        '<span>当前关注</span>'
        f'<strong>{html.escape(spine["primary_progress"])}</strong>'
        '<small>只作观察</small>'
        '</div>'
        '<div class="external-spine-fact">'
        '<span>观察名单</span>'
        f'<strong>{html.escape(spine["basket_progress"])}</strong>'
        '<small>不是买入建议</small>'
        '</div>'
        '<div class="external-spine-fact external-spine-lock">'
        '<span>行动状态</span>'
        f'<strong>{html.escape(spine["promotion_decision_label"])}</strong>'
        '<small>暂不行动</small>'
        '</div>'
        '</div>'
        f'<div class="external-spine-boundary">{html.escape(spine["boundary"])}</div>'
        '</section>'
    )


def render_control_strip_section(cards: list[dict[str, object]]) -> str:
    rendered_cards = []
    for card in cards:
        sub_lines = [
            f'<div class="control-sub">{html.escape(str(line))}</div>'
            for line in (card.get("sub_lines") or [])
            if str(line).strip()
        ]
        rendered_cards.append(
            '<div class="control-card">'
            f'<div class="control-label">{html.escape(str(card.get("label", "")))}</div>'
            f'<div class="control-value">{html.escape(str(card.get("value", "")))}</div>'
            f'{"".join(sub_lines)}'
            '</div>'
        )
    return f'<div class="control-strip">{"".join(rendered_cards)}</div>'


def render_primary_result_home_brief_section(primary_facts: dict[str, str]) -> str:
    blocker_html = ""
    if primary_facts.get("blocker_title"):
        blocker_html = (
            '<div class="today-brief-sub" style="margin-top:10px; color:#9f2f2f;">'
            f'当前阻塞点 {html.escape(primary_facts["blocker_title"])} ｜ '
            f'{html.escape(primary_facts.get("blocker_detail", "-"))}'
            "</div>"
        )
    return (
        '<div class="today-brief">'
        '<div class="today-brief-kicker">今日一句话结论</div>'
        f'<div class="today-brief-title">{html.escape(primary_facts["result_id"] or "primary:unavailable")} '
        f'是当前唯一主结果对象，主体 {html.escape(primary_facts["result_subject"] or "对象暂缺")}，'
        f'当前处于 {html.escape(primary_facts["stage_label"] or "阶段待确认")}。</div>'
        f'<div class="today-brief-sub">策略结论 {html.escape(primary_facts["backtest_conclusion"])} ｜ '
        f'市场环境 {html.escape(primary_facts["dominant_regime"])} ｜ '
        f'风险偏好 {html.escape(primary_facts["risk_preference"])} ｜ '
        f'数据最新交易日 {html.escape(primary_facts["db_latest_trade_date"])} ｜ '
        f'候选产物 {html.escape(primary_facts["candidate_generated_at"])} ｜ '
        f'观察窗口 {html.escape(primary_facts["observation_timeline_label"])} ｜ '
        f'平均风险压力 {html.escape(primary_facts["avg_risk_pressure"])}</div>'
        f'<div class="today-brief-sub">制度阻断 {html.escape(primary_facts["disabled_reason"] or "无")} ｜ '
        f'终局状态 {html.escape(primary_facts["invalid_reason"] or "无")}</div>'
        f"{blocker_html}"
        '</div>'
    )


def render_display_contract(contract: dict[str, str], *, data_contract: str, extra_evidence_html: str = "") -> str:
    return (
        f'<div class="display-contract" data-display-contract="{html.escape(data_contract)}">'
        '<div class="display-contract-cell display-contract-conclusion">'
        '<span>状态</span>'
        f'<strong>{html.escape(str(contract.get("conclusion_title", "")))}</strong>'
        f'<small>{html.escape(str(contract.get("conclusion_sub", "")))}</small>'
        '</div>'
        '<div class="display-contract-cell">'
        '<span>证据</span>'
        f'<strong>{html.escape(str(contract.get("evidence_title", "")))}</strong>'
        f'<small>{html.escape(str(contract.get("evidence_sub", "")))}</small>'
        f'{extra_evidence_html}'
        '</div>'
        '<div class="display-contract-cell display-contract-boundary">'
        '<span>边界</span>'
        f'<strong>{html.escape(str(contract.get("boundary_title", "")))}</strong>'
        f'<small>{html.escape(str(contract.get("boundary_sub", "")))}</small>'
        '</div>'
        '<div class="display-contract-cell display-contract-next">'
        '<span>下一步</span>'
        f'<strong>{html.escape(str(contract.get("next_title", "")))}</strong>'
        f'<small>{html.escape(str(contract.get("next_sub", "")))}</small>'
        '</div>'
        '</div>'
    )


def _format_prefilter_score(value: object) -> str:
    text = str(value or "").strip()
    if not text or text == "-":
        return "待生成"
    try:
        number = float(text)
    except Exception:
        return text
    if 0 <= number <= 1:
        return f"{number * 100.0:.1f}"
    return f"{number:.1f}"


def render_validation_section(validation_view_model: dict[str, object]) -> str:
    return (
        '<div class="card" id="validation">'
        '<div class="section-title">'
        '<div><div class="eyebrow">历史验证</div><h3>历史篮子验证</h3></div>'
        f'<div class="muted">{html.escape(str(validation_view_model["display_contract"]["detail"]))}</div>'
        '</div>'
        f'{render_kpi_grid_rows(validation_view_model["kpi_rows"])}'
        '</div>'
    )


def render_diagnostics_appendix_section(*, validation_section: str, diagnosis_section: str, prefilter_section: str) -> str:
    body_html = f"{validation_section}{diagnosis_section}{prefilter_section}"
    if not body_html.strip():
        return ""
    return (
        '<details class="overview-noise-disclosure diagnostics-appendix-disclosure" id="diagnostics-appendix">'
        '<summary>'
        '<span>诊断附录</span>'
        '<strong>历史验证、策略诊断与预筛噪声已下沉</strong>'
        '<small>总览首屏只保留状态、对象、证据、边界和下钻入口；需要排障或做研究复核时再展开。</small>'
        '</summary>'
        f'{body_html}'
        '</details>'
    )


def render_summary_section(summary_view_model: dict[str, object]) -> str:
    contract = summary_view_model["display_contract"]
    summary_items = "".join(f"<li>{html.escape(x)}</li>" for x in summary_view_model["summary_lines"])
    ai_explainer = summary_view_model.get("ai_explainer") or {}
    ai_explainer_html = ""
    if isinstance(ai_explainer, dict) and bool(ai_explainer.get("visible")) and str(ai_explainer.get("summary_text", "")).strip():
        risk_flags = [
            str(item).strip()
            for item in (ai_explainer.get("risk_flags") or [])
            if str(item).strip()
        ]
        supporting_facts = [
            str(item).strip()
            for item in (ai_explainer.get("supporting_facts") or [])
            if str(item).strip()
        ]
        risk_html = ""
        if risk_flags:
            risk_html = (
                '<div class="muted" style="margin-top:8px;">风险提示：'
                + "｜".join(html.escape(item) for item in risk_flags)
                + "</div>"
            )
        support_html = ""
        if supporting_facts:
            support_html = (
                '<ul class="hero-summary" style="margin-top:8px;">'
                + "".join(f"<li>{html.escape(item)}</li>" for item in supporting_facts)
                + "</ul>"
            )
        ai_explainer_html = (
            '<div class="callout" id="stock-ai-explainer">'
            f'<strong>{html.escape(str(ai_explainer.get("display_label", "AI 解释")))}</strong>'
            f'<div style="margin-top:6px;">{html.escape(str(ai_explainer.get("summary_title", "")))}</div>'
            f'<div class="muted" style="margin-top:6px;">{html.escape(str(ai_explainer.get("summary_text", "")))}</div>'
            f"{risk_html}"
            f"{support_html}"
            '</div>'
        )
    return (
        '<div class="card" id="candidates">'
        '<div id="summary"></div>'
        '<div class="section-title">'
        '<div>'
        '<div class="eyebrow">对外判断</div>'
        '<h3>现在怎么看</h3>'
        '</div>'
        '<div class="muted">先看判断，再看观察名单。</div>'
        '</div>'
        f'{render_display_contract(contract, data_contract="summary", extra_evidence_html=render_dual_track_rows(summary_view_model["basket_dual_track_rows"]))}'
        f'{render_metric_ribbon(summary_view_model.get("evidence_chips", []))}'
        f'{render_kpi_grid_rows(summary_view_model["kpi_rows"])}'
        f'<div class="callout" id="governance-cycle-callout"><strong>治理主链建议：</strong> {html.escape(str(summary_view_model["governance_operator_message"]))}</div>'
        f'{ai_explainer_html}'
        '<div class="callout"><ul class="hero-summary">'
        f"{summary_items}"
        '</ul></div>'
        '<div class="footer-note">说明：以下建议用于研究决策，不构成任何保证收益的投资承诺。</div>'
        '</div>'
    )


def render_actions_section(*, actions_view_model: dict[str, object], actions_render_contract: dict[str, object]) -> str:
    rendered_cards = []
    for idx, row in enumerate(actions_render_contract.get("card_rows", []), start=1):
        href = str(row.get("href", "") or "").strip()
        tag_open = f'<a class="action-card" href="{html.escape(href)}">' if href else '<div class="action-card">'
        tag_close = "</a>" if href else "</div>"
        rendered_cards.append(
            tag_open +
            f'<div class="action-card-rank">#{idx}</div>'
            '<div class="action-card-top">'
            f'<div><h4>{html.escape(str(row["代码"]))}</h4><div class="action-card-name">{html.escape(str(row["名称"]))} · {html.escape(str(row["行业"] or "未知行业"))}</div></div>'
            f'<div class="action-card-score">{html.escape(str(row["综合分"]))}</div>'
            '</div>'
            '<div class="action-chip-row">'
            f'<span class="signal-chip">{html.escape(str(row["信号"]))}</span>'
            f'<span class="risk-chip">{html.escape(str(row["风险"]))}</span>'
            f'<span class="action-chip">{html.escape(str(row["建议动作"]))}</span>'
            '</div>'
            '<div class="action-metric-grid">'
            f'<div><span>上涨概率</span><strong>{html.escape(str(row["上涨概率"]))}</strong></div>'
            f'<div><span>预测收益</span><strong>{html.escape(str(row["预测收益"]))}</strong></div>'
            f'<div><span>置信度</span><strong>{html.escape(str(row["置信度"]))}</strong></div>'
            f'<div><span>建议仓位</span><strong>{html.escape(str(row["建议仓位"]))}</strong></div>'
            '</div>'
            f'<div class="action-card-note">{html.escape(str(row["依据"] or "暂无依据拆解"))}</div>'
            f'{tag_close}'
        )
    cards_html = "".join(rendered_cards)
    if not cards_html:
        cards_html = '<div class="chart-empty">暂无候选建议，请先运行候选生成脚本。</div>'
    table_rows = actions_render_contract.get("table_rows", [])
    if table_rows:
        head = ["代码", "名称", "行业", "信号", "风险", "上涨概率", "预测收益", "置信度", "综合分", "建议仓位", "建议动作"]
        th = "".join(f"<th>{html.escape(header)}</th>" for header in head)
        body = []
        for row in table_rows:
            body.append(
                "<tr>"
                f"<td class='cell-code'>{html.escape(str(row['代码']))}</td>"
                f"<td class='cell-name'>{html.escape(str(row['名称']))}</td>"
                f"<td class='cell-industry'>{html.escape(str(row['行业']))}</td>"
                f"<td><span class='signal-chip'>{html.escape(str(row['信号']))}</span></td>"
                f"<td><span class='risk-chip'>{html.escape(str(row['风险']))}</span></td>"
                f"<td>{html.escape(str(row['上涨概率']))}</td>"
                f"<td>{html.escape(str(row['预测收益']))}</td>"
                f"<td>{html.escape(str(row['置信度']))}</td>"
                f"<td class='cell-score'>{html.escape(str(row['综合分']))}</td>"
                f"<td>{html.escape(str(row['建议仓位']))}</td>"
                f"<td class='cell-action'>{html.escape(str(row['建议动作']))}</td>"
                "</tr>"
            )
        table_html = (
            "<div class='table-shell'><table class='data-table data-table-candidates'><thead><tr>"
            + th
            + "</tr></thead><tbody>"
            + "".join(body)
            + "</tbody></table></div>"
        )
    else:
        table_html = "<p>暂无候选建议，请先运行候选生成脚本。</p>"
    actions_table_html = render_table_export_block(
        str(actions_render_contract.get("table_title", "候选股操作建议")),
        str(actions_render_contract.get("table_id", "candidate-actions-table")),
        str(actions_render_contract.get("export_filename", "candidate_actions_view.csv")),
        table_html,
    )
    cards_block_html = f'<div class="action-card-grid">{cards_html}</div>' if "action-card" in cards_html else cards_html
    return (
        '<div class="card card-compact actions-panel public-watchlist" id="candidate-actions">'
        '<div class="section-title">'
        '<div>'
        '<div class="eyebrow">观察名单</div>'
        '<h3>观察名单</h3>'
        '</div>'
        f'<div class="muted">{html.escape(str(actions_view_model["detail"]))}</div>'
        '</div>'
        f'{render_metric_ribbon(actions_view_model["metric_chips"])}'
        f'{render_dual_track_rows(actions_view_model["basket_dual_track_rows"])}'
        '<div class="action-list-head">'
        '<span>怎么读</span>'
        '<strong>仅供观察。点击查看依据。</strong>'
        '</div>'
        f'{cards_block_html}'
        '<details class="detail-disclosure" style="margin-top:14px;">'
        '<summary>查看专业明细</summary>'
        f'{actions_table_html}'
        '</details>'
        '</div>'
    )


def render_opportunities_section(*, opportunities_view_model: dict[str, object]) -> str:
    cards_html = "".join(
        '<a class="opportunity-card" href="'
        + html.escape(str(card.get("href", "#")))
        + '">'
        f'<div class="opportunity-rank">#{html.escape(str(card.get("rank", "-")))}</div>'
        f'<div class="opportunity-code">{html.escape(str(card.get("ts_code", "")))}</div>'
        f'<div class="opportunity-name">{html.escape(str(card.get("stock_name", "")))}</div>'
        '<div class="opportunity-meta">'
        f'<span>{html.escape(str(card.get("signal", "")))}</span>'
        f'<span>{html.escape(str(card.get("risk_level", "")))}</span>'
        '</div>'
        f'<div class="opportunity-score">{html.escape(str(card.get("final_score", "")))}</div>'
        '</a>'
        for card in opportunities_view_model.get("cards", [])
    )
    return (
        '<div class="card" id="opportunities">'
        '<div class="section-title">'
        '<div><div class="eyebrow">机会板</div><h3>今日候选机会</h3></div>'
        '<div class="muted">总览页只展示最值得继续下钻的候选，不把完整表格直接推到首页</div>'
        '</div>'
        f'{render_display_contract(opportunities_view_model["display_contract"], data_contract="opportunities", extra_evidence_html=render_dual_track_rows(opportunities_view_model["basket_dual_track_rows"]))}'
        f'<div class="opportunity-grid">{cards_html}</div>'
        '</div>'
    )


def render_prefilter_section(prefilter_view_model: dict[str, object]) -> str:
    prefilter_guard_html = "".join(f"<li>{html.escape(note)}</li>" for note in prefilter_view_model["guard_notes"])
    prefilter_table_rows = []
    for row in prefilter_view_model["top10_rows"]:
        code_badge = "" if row["is_standard_code"] else '<div class="table-stock-alert">诊断样本</div>'
        prefilter_table_rows.append(
            f"<tr><td>{html.escape(row['rank'])}</td><td>{html.escape(row['ts_code'])}{code_badge}<div class='table-stock-name'>{html.escape(row['name_display'])}</div></td>"
            f"<td>{html.escape(_format_prefilter_score(row['prefilter_score']))}</td>"
            f"<td>{html.escape(row['prefilter_reason'])}</td></tr>"
        )
    exclusion_summary_rows = [
        f"<tr><td>{html.escape(row['rank'])}</td><td>{html.escape(row['reason'])}</td><td>{html.escape(row['count'])}</td><td>{html.escape(row['share_pct'])}</td></tr>"
        for row in prefilter_view_model["exclusion_rows"]
    ]
    top_exclusion_rows = []
    for row in prefilter_view_model["top_exclusion_rows"]:
        code_badge = "" if row["is_standard_code"] else '<div class="table-stock-alert">诊断样本</div>'
        top_exclusion_rows.append(
            f"<tr><td>{html.escape(row['rank'])}</td><td>{html.escape(row['ts_code'])}{code_badge}<div class='table-stock-name'>{html.escape(row['name_display'])}</div></td>"
            f"<td>{html.escape(row['reason'])}</td></tr>"
        )
    return (
        '<div class="card prefilter-evidence-card" id="prefilter">'
        '<div class="section-title">'
        '<div>'
        '<div class="eyebrow">专家证据</div>'
        '<h3>全A预筛底表</h3>'
        '</div>'
        '<div class="muted">只作为筛选过程证据，不替代第一屏受控等待结论；当前阶段不得触发样本闭合或促晋级。</div>'
        '</div>'
        f'{render_display_contract(prefilter_view_model["display_contract"], data_contract="prefilter")}'
        '<div class="prefilter-guard">'
        '<div>'
        '<div class="prefilter-guard-label">展示层级</div>'
        '<strong>专家证据区</strong>'
        '<span>底表用于解释为什么进入或未进入计算预算。</span>'
        '</div>'
        '<div>'
        '<div class="prefilter-guard-label">数据口径</div>'
        f'<strong>{html.escape(str(prefilter_view_model["quality_label"]))}</strong>'
        f'<span>最新交易日 {html.escape(str(prefilter_view_model["trade_date_label"]))}，预筛生成时间 {html.escape(str(prefilter_view_model["generated_at"]))}</span>'
        '</div>'
        f'<ul>{prefilter_guard_html}</ul>'
        '</div>'
        f'{render_kpi_grid_rows(prefilter_view_model["kpi_rows"])}'
        '<div class="split-panel prefilter-tables-grid" style="margin-top:14px;">'
        '<div class="panel-subcard prefilter-top10-card">'
        '<div class="subsection-title">预筛入池 Top10</div>'
        '<div class="table-shell">'
        '<table class="prefilter-table prefilter-table-top10"><thead><tr><th>排名</th><th>代码 / 名称</th><th>预筛分</th><th>入池理由</th></tr></thead><tbody>'
        f'{"".join(prefilter_table_rows) if prefilter_table_rows else "<tr><td colspan=4>暂无预筛底表</td></tr>"}'
        '</tbody></table>'
        '</div>'
        '</div>'
        '<div class="panel-subcard prefilter-reason-card">'
        '<div class="subsection-title">主要出池原因 Top5</div>'
        '<div class="table-shell">'
        '<table class="prefilter-table prefilter-table-reasons"><thead><tr><th>排名</th><th>原因</th><th>数量</th><th>占出池比例</th></tr></thead><tbody>'
        f'{"".join(exclusion_summary_rows) if exclusion_summary_rows else "<tr><td colspan=4>暂无出池明细</td></tr>"}'
        '</tbody></table>'
        '</div>'
        '</div>'
        '</div>'
        '<div class="panel-subcard prefilter-sample-card" style="margin-top:14px;">'
        '<div class="subsection-title">未进池样本抽样</div>'
        '<div class="table-shell">'
        '<table class="prefilter-table prefilter-table-samples"><thead><tr><th>排名</th><th>代码 / 名称</th><th>出池原因</th></tr></thead><tbody>'
        f'{"".join(top_exclusion_rows) if top_exclusion_rows else "<tr><td colspan=3>暂无未进池样本</td></tr>"}'
        '</tbody></table>'
        '</div>'
        '</div>'
        f'<div class="muted" style="margin-top:10px;">预筛生成时间 {html.escape(str(prefilter_view_model["generated_at"]))}，当前展示 Top {html.escape(str(prefilter_view_model["top10_count"]))}。</div>'
        '</div>'
    )


def render_selection_funnel_section(funnel_view_model: dict[str, object]) -> str:
    funnel_row_html = "".join(
        f'<div class="funnel-row"><div class="funnel-head"><strong>{html.escape(str(row["label"]))}</strong><span>{html.escape(str(row["value"]))}</span></div>'
        f'<div class="funnel-track"><div class="funnel-fill" style="width:{html.escape(str(row["width_pct"]))}%"></div></div>'
        f'<div class="funnel-sub">{html.escape(str(row["sub"]))}</div></div>'
        for row in funnel_view_model["funnel_rows"]
    )
    return (
        '<div class="card" id="selection-funnel">'
        '<div class="section-title">'
        '<div><div class="eyebrow">选股漏斗</div><h3>今日选股漏斗</h3></div>'
        '<div class="muted">先看筛选收缩比例，再看流动性门槛与主要淘汰原因，避免误把参数问题当成市场问题</div>'
        '</div>'
        f'{render_display_contract(funnel_view_model["display_contract"], data_contract="selection-funnel")}'
        f'{render_kpi_grid_rows(funnel_view_model["kpi_rows"])}'
        f'{render_metric_ribbon(funnel_view_model["metric_chips"])}'
        f'<div class="funnel-card">{funnel_row_html}</div>'
        '</div>'
    )


def render_overview_system_summary_section(*, control_strip_html: str, disclosure_view_model: dict[str, str]) -> str:
    return (
        '<details class="overview-noise-disclosure authority-summary-disclosure" id="external-system-summary">'
        '<summary>'
        '<span>内部复核</span>'
        '<span>运行摘要</span>'
        f'<strong>{html.escape(str(disclosure_view_model.get("system_summary_title", "")))}</strong>'
        f'<small>{html.escape(str(disclosure_view_model.get("system_summary_detail", "")))} 仅在需要核对内部链路时展开。</small>'
        '</summary>'
        f'{control_strip_html}'
        '</details>'
    )


def render_overview_visuals_section(
    *,
    health_chart_html: str,
    backtest_equity_html: str,
    backtest_drawdown_html: str,
    backtest_chart_html: str,
) -> str:
    return (
        '<div class="card" id="overview-visuals">'
        '<div class="section-title">'
        '<div><div class="eyebrow">图形监控</div><h3>关键研究图形</h3></div>'
        '<div class="muted">总览只保留最重要的系统与策略轨迹</div>'
        '</div>'
        f'<div class="chart-grid chart-grid-compact">{health_chart_html}{backtest_equity_html}{backtest_drawdown_html}{backtest_chart_html}</div>'
        '</div>'
    )


def render_view_banner_section(view_banner_view_model: dict[str, str]) -> str:
    return (
        '<div class="view-banner">'
        '<div>'
        f'<h2>{html.escape(str(view_banner_view_model.get("view_title", "")))}</h2>'
        f'<p>{html.escape(str(view_banner_view_model.get("view_subtitle", "")))}</p>'
        '</div>'
        '<div class="view-banner-side">'
        '<div class="view-side-card">'
        '<span>今日焦点</span>'
        f'<strong>{html.escape(str(view_banner_view_model.get("focus_code", "")))}</strong>'
        f'<small>{html.escape(str(view_banner_view_model.get("focus_subtitle", "")))}</small>'
        '</div>'
        '<div class="view-side-card">'
        '<span>时间一致性</span>'
        f'<strong>{html.escape(str(view_banner_view_model.get("timeline_title", "")))}</strong>'
        f'<small>{html.escape(str(view_banner_view_model.get("timeline_detail", "")))}</small>'
        '</div>'
        '</div>'
        '</div>'
    )


def render_hero_side_section(*, hero_side_view_model: dict[str, str], basket_dual_track_html: str) -> str:
    candidate_summary_parts = [str(hero_side_view_model.get("candidate_name", "")).strip()]
    generated_at = str(hero_side_view_model.get("candidate_generated_at", "")).strip()
    generation_mode = str(hero_side_view_model.get("generation_mode_label", "")).strip()
    if generated_at:
        candidate_summary_parts.append(f"候选生成 {generated_at}")
    if generation_mode and generation_mode != "-":
        candidate_summary_parts.append(generation_mode)
    candidate_summary = " · ".join(part for part in candidate_summary_parts if part)
    return (
        '<div class="hero-side">'
        '<div class="hero-status">'
        '<div class="status-label">系统状态</div>'
        f'<div class="status-value">{html.escape(str(hero_side_view_model.get("health_status", "")))}</div>'
        f'<div class="tag {html.escape(str(hero_side_view_model.get("health_tag", "")))}">健康分 {html.escape(str(hero_side_view_model.get("health_score", "")))}</div>'
        '</div>'
        '<div class="hero-stack">'
        '<div class="hero-metric">'
        '<span>优先候选</span>'
        f'<strong>{html.escape(str(hero_side_view_model.get("top_code", "")))}</strong>'
        f'<small>{html.escape(candidate_summary)}</small>'
        f'{basket_dual_track_html}'
        '</div>'
        '<div class="hero-metric">'
        '<span>自动链路</span>'
        f'<strong>{html.escape(str(hero_side_view_model.get("update_status_label", "")))}</strong>'
        f'<small>阶段 {html.escape(str(hero_side_view_model.get("update_stage_label", "")))}</small>'
        '</div>'
        '</div>'
        '</div>'
    )


def render_spotlight_section(spotlight_view_model: dict[str, object]) -> str:
    chips_html = "".join(
        f"<span>{html.escape(str(chip))}</span>" for chip in spotlight_view_model.get("meta_chips", [])
    )
    subtitle = str(spotlight_view_model.get("candidate_name", "")).strip()
    subtitle_html = f'<div class="spotlight-subtitle">{html.escape(subtitle)}</div>' if subtitle else ""
    description = str(
        spotlight_view_model.get(
            "description",
            "优先看信号方向、风险等级、仓位建议和依据的一致性，不建议只按综合分决策。",
        )
    ).strip()
    eyebrow = str(spotlight_view_model.get("eyebrow", "优先标的"))
    klass = "spotlight-card spotlight-card-subdued" if eyebrow == "当前对象" else "spotlight-card"
    return (
        f'<div class="{klass}">'
        f'<div class="eyebrow">{html.escape(eyebrow)}</div>'
        f'<div class="spotlight-title">{html.escape(str(spotlight_view_model.get("top_code", "")))}</div>'
        f'{subtitle_html}'
        f'<div class="spotlight-meta">{chips_html}</div>'
        f'<p>{html.escape(description)}</p>'
        '</div>'
    )


def render_command_deck_section(
    *,
    command_focus_view_model: dict[str, str],
    command_runtime_view_model: dict[str, str],
) -> str:
    return (
        '<div class="command-deck">'
        '<div class="command-panel">'
        '<div class="eyebrow">研究闭环</div>'
        '<h3>一条链路跑完研究</h3>'
        '<p>数据、特征、预测、信号、风控、回测和日报在同一条流水线里闭环运行。</p>'
        '</div>'
        '<div class="command-panel">'
        '<div class="eyebrow">当前焦点</div>'
        f'<h3>{html.escape(str(command_focus_view_model.get("title", "")))}</h3>'
        f'<p>{html.escape(str(command_focus_view_model.get("detail", "")))}</p>'
        '</div>'
        '<div class="command-panel">'
        '<div class="eyebrow">运行新鲜度</div>'
        f'<h3>{html.escape(str(command_runtime_view_model.get("title", "")))}</h3>'
        f'<p>{html.escape(str(command_runtime_view_model.get("detail", "")))}</p>'
        '</div>'
        '</div>'
    )


def render_architecture_section(architecture_steps: list[dict[str, str]]) -> str:
    steps_html = "".join(
        f'<div class="flow-step"><div class="flow-index">{html.escape(str(step.get("index", "")))}</div><div class="flow-title">{html.escape(str(step.get("title", "")))}</div><div class="flow-desc">{html.escape(str(step.get("desc", "")))}</div></div>'
        for step in architecture_steps
    )
    return (
        '<div class="card" id="architecture">'
        '<div class="section-title">'
        '<h3>系统如何运作</h3>'
        '<div class="muted">专业版说明：你看到的每个候选和指标，都是以下流水线的输出结果</div>'
        '</div>'
        f'<div class="grid-flow">{steps_html}</div>'
        '</div>'
    )


def render_overview_visuals_disclosure_section(*, overview_visuals_section: str) -> str:
    return (
        '<details class="overview-noise-disclosure" id="overview-visuals-disclosure">'
        '<summary>'
        '<span>内部复核</span>'
        '<strong>关键研究图形已收起</strong>'
        '<small>只在需要核对健康、收益和回撤轨迹时展开，不参与首页主叙事。</small>'
        '</summary>'
        f'{overview_visuals_section}'
        '</details>'
    )


def render_overview_operations_disclosure_section(
    *,
    operations_section: str,
    external_system_summary_html: str,
    disclosure_view_model: dict[str, str],
) -> str:
    if not operations_section:
        return ""
    return (
        '<details class="overview-noise-disclosure" id="overview-operations-disclosure">'
        '<summary>'
        '<span>内部复核</span>'
        '<strong>运维控制台已收起</strong>'
        f'<small>{html.escape(str(disclosure_view_model.get("operations_detail", "")))}</small>'
        '</summary>'
        f'{external_system_summary_html}'
        f'{operations_section}'
        '</details>'
    )


def render_candidate_visuals_section(*, candidate_map_chart_html: str, candidate_chart_html: str) -> str:
    return (
        '<div class="card card-compact visuals-card" id="candidate-visuals">'
        '<div class="section-title">'
        '<div><div class="eyebrow">候选图形</div><h3>候选图形视图</h3></div>'
        '<div class="muted">用图而不是大宽表看候选的排序和风险收益</div>'
        '</div>'
        f'<div class="chart-grid chart-grid-compact candidate-visual-grid">{candidate_map_chart_html}{candidate_chart_html}</div>'
        '</div>'
    )


def render_candidate_compare_section(candidate_compare_view_model: dict[str, object]) -> str:
    if not candidate_compare_view_model.get("available"):
        return ""
    top_card = candidate_compare_view_model["top_card"]
    next_card = candidate_compare_view_model["next_card"]
    return (
        '<div class="card" id="candidate-compare">'
        '<div class="section-title">'
        '<div><div class="eyebrow">候选对比</div><h3>为什么是它，不是下一个</h3></div>'
        '<div class="muted">把当前第一名和第二名直接放在一张卡里比较，减少来回切换成本</div>'
        '</div>'
        '<div class="compare-grid">'
        '<div class="compare-card">'
        '<div class="compare-tag">当前第一名</div>'
        f'<h4>{html.escape(str(top_card["ts_code"]))}</h4>'
        f'<div class="compare-name">{html.escape(str(top_card["stock_name"]))}</div>'
        '<div class="compare-chip-row">'
        f'<span class="signal-chip">{html.escape(str(top_card["signal"]))}</span>'
        f'<span class="risk-chip">{html.escape(str(top_card["risk_level"]))}</span>'
        '</div>'
        f'<div class="compare-score">{html.escape(str(top_card["final_score"]))}</div>'
        f'<div class="compare-note">预测收益 {html.escape(str(top_card["pred_return"]))}</div>'
        '</div>'
        '<div class="compare-middle">'
        f'<div class="compare-gap">{html.escape(str(candidate_compare_view_model["score_gap"]))}</div>'
        '<div class="compare-gap-label">综合分差值</div>'
        '</div>'
        '<div class="compare-card">'
        '<div class="compare-tag">紧随其后</div>'
        f'<h4>{html.escape(str(next_card["ts_code"]))}</h4>'
        f'<div class="compare-name">{html.escape(str(next_card["stock_name"]))}</div>'
        '<div class="compare-chip-row">'
        f'<span class="signal-chip">{html.escape(str(next_card["signal"]))}</span>'
        f'<span class="risk-chip">{html.escape(str(next_card["risk_level"]))}</span>'
        '</div>'
        f'<div class="compare-score">{html.escape(str(next_card["final_score"]))}</div>'
        f'<div class="compare-note">预测收益 {html.escape(str(next_card["pred_return"]))}</div>'
        '</div>'
        '</div>'
        '</div>'
    )


def render_links_section(links_view_model: dict[str, object]) -> str:
    entries_html = []
    for entry in links_view_model.get("entries", []):
        rendered_actions = []
        for action in entry.get("actions", []):
            target_attr = ' target="_blank"' if bool(action.get("target_blank")) else ""
            rendered_actions.append(
                f'<a href="{html.escape(str(action.get("href", "#")))}"{target_attr}>{html.escape(str(action.get("label", "")))}</a>'
            )
        actions_html = "".join(rendered_actions)
        entries_html.append(
            '<div class="quick-link">'
            f'<strong>{html.escape(str(entry.get("title", "")))}</strong>'
            f'<span>{html.escape(str(entry.get("desc", "")))}</span>'
            f'<div class="quick-link-actions">{actions_html}</div>'
            '</div>'
        )
    return (
        '<div class="card" id="links">'
        '<div class="section-title">'
        '<div>'
        '<div class="eyebrow">资料入口</div>'
        f'<h3>{html.escape(str(links_view_model.get("title", "")))}</h3>'
        '</div>'
        f'<div class="muted">{html.escape(str(links_view_model.get("detail", "")))}</div>'
        '</div>'
        f'<div class="quick-links">{"".join(entries_html)}</div>'
        '</div>'
    )


def render_guide_section(guide_view_model: dict[str, object]) -> str:
    items_html = "".join(
        f"<li>{html.escape(str(item))}</li>" for item in guide_view_model.get("items", []) if str(item).strip()
    )
    return (
        '<div class="card">'
        '<div class="section-title">'
        '<div>'
        '<div class="eyebrow">使用说明</div>'
        f'<h3>{html.escape(str(guide_view_model.get("title", "")))}</h3>'
        '</div>'
        f'<div class="muted">{html.escape(str(guide_view_model.get("detail", "")))}</div>'
        '</div>'
        f'<ul class="guide-list">{items_html}</ul>'
        '</div>'
    )


def render_charts_section(charts_view_model: dict[str, object]) -> str:
    chart_html = "".join(str(block) for block in charts_view_model.get("chart_html_blocks", []) if str(block).strip())
    return (
        '<div class="card" id="charts">'
        '<div class="section-title">'
        '<div>'
        '<div class="eyebrow">图形监控</div>'
        f'<h3>{html.escape(str(charts_view_model.get("title", "")))}</h3>'
        '</div>'
        f'<div class="muted">{html.escape(str(charts_view_model.get("detail", "")))}</div>'
        '</div>'
        f'<div class="chart-grid">{chart_html}</div>'
        '</div>'
    )


def render_market_snapshot_section(*, market_snapshot: dict[str, object]) -> str:
    chips = [
        f"候选样本 {market_snapshot.get('candidate_count', '0')} 只",
        f"平均建议仓位 {market_snapshot.get('avg_position_pct', '0.0%')}",
        f"平均风险压力 {market_snapshot.get('avg_risk_pressure', '0.0')}",
        f"风险标记 {market_snapshot.get('risk_flag_count', '0')} 只",
    ]
    return (
        '<div class="card" id="market-snapshot">'
        '<div class="section-title">'
        '<div><div class="eyebrow">市场状态</div><h3>候选池市场状态</h3></div>'
        '<div class="muted">从当前候选池反推系统正在面对的市场状态、风格偏向和风险偏好，而不是只看单票排序</div>'
        '</div>'
        '<div class="grid-kpi">'
        f'<div class="kpi"><div class="label">主导状态</div><div class="value">{html.escape(str(market_snapshot.get("dominant_regime", "-")))}</div><div class="sub">当前候选池里出现最多的 regime 类型</div></div>'
        f'<div class="kpi"><div class="label">风险偏好</div><div class="value">{html.escape(str(market_snapshot.get("risk_preference", "-")))}</div><div class="sub">由 guardrail、平均仓位和风险压力共同推断</div></div>'
        f'<div class="kpi"><div class="label">风格暴露</div><div class="value">{html.escape(str(market_snapshot.get("style_bias", "-")))}</div><div class="sub">根据相对强弱与波动代理判断当前候选风格</div></div>'
        f'<div class="kpi"><div class="label">Guardrail</div><div class="value">{html.escape(str(market_snapshot.get("guardrail_mode", "-")))}</div><div class="sub">{html.escape(str(market_snapshot.get("guardrail_reason", "-")))}</div></div>'
        '</div>'
        f'{render_metric_ribbon(chips)}'
        '</div>'
    )


def render_research_visuals_section(*, health_chart_html: str, backtest_equity_html: str, backtest_drawdown_html: str, backtest_map_chart_html: str) -> str:
    return (
        '<div class="card" id="research-visuals">'
        '<div class="section-title">'
        '<div><div class="eyebrow">研究图形</div><h3>策略研究图形</h3></div>'
        '<div class="muted">聚焦健康、净值、回撤和收益/回撤关系</div>'
        '</div>'
        f'<div class="chart-grid chart-grid-compact">{health_chart_html}{backtest_equity_html}{backtest_drawdown_html}{backtest_map_chart_html}</div>'
        '</div>'
    )


def render_top1_section(*, top1_view_model: dict[str, object]) -> str:
    return (
        '<div class="card">'
        '<div class="section-title">'
        '<div>'
        '<div class="eyebrow">优先标的</div>'
        '<h3>当日优先候选</h3>'
        '</div>'
        '<div class="muted">当前自动研究优先级最高的标的</div>'
        '</div>'
        '<div class="grid3">'
        f'<div class="kpi"><div class="label">代码 / 名称</div><div class="value">{html.escape(str(top1_view_model.get("top_code", "暂无")))}</div><div class="sub">{html.escape(str(top1_view_model.get("stock_name", "")))}</div></div>'
        f'<div class="kpi"><div class="label">信号 / 风险</div><div class="value">{html.escape(str(top1_view_model.get("signal", "-")))}</div><div class="sub">{html.escape(str(top1_view_model.get("risk", "-")))}</div></div>'
        f'<div class="kpi"><div class="label">综合分</div><div class="value">{html.escape(str(top1_view_model.get("final_score", "-")))}</div><div class="sub">来自模型、收益、风险的综合排序</div></div>'
        '</div>'
        '</div>'
    )


def render_candidate_focus_section(
    *,
    candidate_focus_view_model: dict[str, object],
    candidate_focus_render_contract: dict[str, object],
    candidate_detail_html: str,
) -> str:
    nav = candidate_focus_render_contract["nav"]
    nav_controls = (
        '<div class="candidate-nav-controls">'
        f'<a class="candidate-nav-btn{" disabled" if bool(nav.get("prev_disabled")) else ""}" href="{html.escape(str(nav.get("prev_href", "#")))}">上一候选</a>'
        f'<div class="candidate-nav-hint">{html.escape(str(nav.get("position_label", "")))} · 可用键盘 ← / → 切换</div>'
        f'<a class="candidate-nav-btn{" disabled" if bool(nav.get("next_disabled")) else ""}" href="{html.escape(str(nav.get("next_href", "#")))}">下一候选</a>'
        '</div>'
    )
    top_quick_links = "".join(
        f'<a class="candidate-mini-link{" active" if bool(link.get("active")) else ""}" href="{html.escape(str(link.get("href", "#")))}">'
        f'<span>{html.escape(str(link.get("label", "")))}</span><strong>{html.escape(str(link.get("ts_code", "")))}</strong></a>'
        for link in candidate_focus_render_contract.get("quick_links", [])
    )
    switcher_html = "".join(
        f'<a class="candidate-switch{" candidate-switch-active" if bool(item.get("active")) else ""}" href="{html.escape(str(item.get("href", "#")))}">'
        f'<div class="candidate-switch-rank">{html.escape(str(item.get("rank", "")))}</div>'
        f'<div class="candidate-switch-main"><strong>{html.escape(str(item.get("ts_code", "")))}</strong><span>{html.escape(str(item.get("stock_name", "")))}</span></div>'
        f'<div class="candidate-switch-side"><span>{html.escape(str(item.get("signal", "")))}</span><b>{html.escape(str(item.get("final_score", "")))}</b></div>'
        '</a>'
        for item in candidate_focus_render_contract.get("switcher_items", [])
    )
    return (
        '<div class="candidate-terminal-shell" id="candidate-focus">'
        '<div class="section-title">'
        '<div>'
        '<div class="eyebrow">研究终端</div>'
        '<h3>候选股深度拆解</h3>'
        '</div>'
        '<div class="muted">把分数拆成可执行的信号、收益、风险和风控参数</div>'
        '</div>'
        f'{nav_controls}'
        '<div class="candidate-summary-bar">'
        f'<div class="candidate-summary-item"><span>当前候选</span><strong>{html.escape(str(candidate_focus_view_model.get("top_code", "暂无")))}</strong></div>'
        f'<div class="candidate-summary-item"><span>信号 / 风险</span><strong>{html.escape(str(candidate_focus_view_model.get("signal", "-")))} · {html.escape(str(candidate_focus_view_model.get("risk", "-")))}</strong></div>'
        f'<div class="candidate-summary-item"><span>综合分 / 仓位</span><strong>{html.escape(str(candidate_focus_view_model.get("final_score", "-")))} · {html.escape(str(candidate_focus_view_model.get("candidate_position_label", "-")))}</strong></div>'
        '</div>'
        f'<div class="candidate-mini-switch">{top_quick_links}</div>'
        '<div class="candidate-terminal-grid">'
        f'<div class="candidate-terminal-main">{candidate_detail_html}</div>'
        '<div class="candidate-terminal-side">'
        '<div class="terminal-side-head">'
        '<div class="eyebrow-inline">候选切换</div>'
        '<h4>今日候选序列</h4>'
        '<div class="muted">先看前五名之间的信号强度、综合分和行业分布，再决定重点跟踪谁。</div>'
        '</div>'
        f'<div class="candidate-switchboard">{switcher_html}</div>'
        '</div>'
        '</div>'
        '</div>'
    )


def render_diagnosis_section(*, bt_diag: dict[str, object], next_check: str) -> str:
    diagnosis_items = "".join(f"<li>{html.escape(str(t))}</li>" for t in bt_diag["诊断"])
    contract = {
        "conclusion_title": str(bt_diag["结论"]),
        "conclusion_sub": f"总收益 {bt_diag['总收益']} · 夏普 {bt_diag['夏普']}",
        "evidence_title": f"胜率 {bt_diag['胜率']}",
        "evidence_sub": f"最大回撤 {bt_diag['最大回撤']} · 交易次数 {bt_diag['交易次数']}",
        "boundary_title": "回测审计",
        "boundary_sub": "诊断只评估策略状态，不解除样本等待和行动锁。",
        "next_title": next_check,
        "next_sub": "等待真实数据门后再复核闭合证据。",
    }
    return (
        '<div class="card" id="diagnosis">'
        '<div class="section-title">'
        '<div>'
        '<div class="eyebrow">策略审计</div>'
        '<h3>策略诊断</h3>'
        '</div>'
        '<div class="muted">基于最新回测结果的策略状态判断</div>'
        '</div>'
        f'{render_display_contract(contract, data_contract="diagnosis")}'
        '<div class="grid3">'
        f'<div class="kpi"><div class="label">综合结论</div><div class="value">{html.escape(str(bt_diag["结论"]))}</div></div>'
        f'<div class="kpi"><div class="label">总收益 / 夏普</div><div class="value">{html.escape(str(bt_diag["总收益"]))} / {html.escape(str(bt_diag["夏普"]))}</div></div>'
        f'<div class="kpi"><div class="label">最大回撤 / 胜率</div><div class="value">{html.escape(str(bt_diag["最大回撤"]))} / {html.escape(str(bt_diag["胜率"]))}</div></div>'
        f'<div class="kpi"><div class="label">Calmar / 盈亏比</div><div class="value">{html.escape(str(bt_diag["Calmar"]))} / {html.escape(str(bt_diag["盈亏比"]))}</div></div>'
        f'<div class="kpi"><div class="label">佣金 / 滑点</div><div class="value">{html.escape(str(bt_diag["佣金"]))} / {html.escape(str(bt_diag["滑点"]))}</div></div>'
        f'<div class="kpi"><div class="label">低流动性拦截 / 交易次数</div><div class="value">{html.escape(str(bt_diag["低流动性拦截"]))} / {html.escape(str(bt_diag["交易次数"]))}</div></div>'
        '</div>'
        f'<ul class="diag-list">{diagnosis_items}</ul>'
        '</div>'
    )
