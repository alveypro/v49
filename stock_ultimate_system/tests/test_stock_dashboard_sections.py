from src.stock_dashboard_sections import (
    render_actions_section,
    render_architecture_section,
    render_charts_section,
    render_candidate_focus_section,
    render_candidate_compare_section,
    render_candidate_visuals_section,
    render_command_deck_section,
    render_control_strip_section,
    render_diagnostics_appendix_section,
    render_diagnosis_section,
    render_external_decision_spine_section,
    render_guide_section,
    render_hero_side_section,
    render_home_headline_section,
    render_jump_strip,
    render_links_section,
    render_market_snapshot_section,
    render_nav_links,
    render_overview_kpi_section,
    render_overview_operations_disclosure_section,
    render_overview_system_summary_section,
    render_overview_visuals_disclosure_section,
    render_overview_visuals_section,
    render_opportunities_section,
    render_prefilter_section,
    render_research_visuals_section,
    render_sidebar_status_section,
    render_selection_funnel_section,
    render_spotlight_section,
    render_summary_section,
    render_top1_section,
    render_topbar_pills_section,
    render_primary_result_home_brief_section,
    render_validation_section,
    render_view_banner_section,
)


def test_render_validation_section_renders_kpi_grid():
    html = render_validation_section(
        {
            "display_contract": {"detail": "验证摘要"},
            "kpi_rows": [
                [
                    {"label": "验证次数", "value": "6", "sub": "最近样本"},
                    {"label": "5日平均篮子收益", "value": "+1.5%", "sub": "说明"},
                    {"label": "5日平均超额", "value": "+0.9%", "sub": "说明"},
                    {"label": "5日篮子胜率", "value": "66%", "sub": "说明"},
                ]
            ],
        }
    )

    assert 'id="validation"' in html
    assert "验证摘要" in html
    assert "验证次数" in html


def test_render_diagnostics_appendix_section_wraps_legacy_diagnostics_under_disclosure():
    html = render_diagnostics_appendix_section(
        validation_section="<div id=\"validation\">validation</div>",
        diagnosis_section="<div id=\"diagnosis\">diagnosis</div>",
        prefilter_section="<div id=\"prefilter\">prefilter</div>",
    )

    assert 'id="diagnostics-appendix"' in html
    assert "历史验证、策略诊断与预筛噪声已下沉" in html
    assert 'id="validation"' in html
    assert 'id="diagnosis"' in html
    assert 'id="prefilter"' in html


def test_render_summary_section_renders_contract_and_summary_lines():
    html = render_summary_section(
        {
            "display_contract": {
                "conclusion_title": "稳健 · 继续观察",
                "conclusion_sub": "当前优先候选 000001.SZ 平安银行",
                "evidence_title": "健康分 92",
                "evidence_sub": "候选生成 2026-04-28",
                "boundary_title": "研究参考",
                "boundary_sub": "等待第一屏行动锁",
                "next_title": "进入复核",
                "next_sub": "等待样本闭合",
            },
            "basket_dual_track_rows": [{"tone": "pointer", "text": "当前生效篮子 approved"}],
            "kpi_rows": [[{"label": "当前唯一动作", "value": "进入复核", "sub": "等待样本闭合"}]],
            "governance_operator_message": "先补样本",
            "summary_lines": ["只读观察", "禁止发布"],
            "ai_explainer": {
                "visible": True,
                "display_label": "AI 解释",
                "summary_title": "只做说明增强",
                "summary_text": "页面仍以唯一 pointer 主结果为准。",
                "risk_flags": ["制度阻断"],
                "supporting_facts": ["继续观察"],
            },
        }
    )

    assert 'data-display-contract="summary"' in html
    assert "当前生效篮子 approved" in html
    assert "禁止发布" in html
    assert 'id="stock-ai-explainer"' in html
    assert "页面仍以唯一 pointer 主结果为准" in html


def test_render_home_headline_section_renders_pointer_and_attempt_lines():
    html = render_home_headline_section(
        {
            "headline_tone": "继续观察",
            "headline_detail": "先等数据门",
            "current_basket_pointer_label": "当前生效篮子 approved",
            "latest_basket_attempt_label": "最新篮子尝试 待复核",
        }
    )
    assert "继续观察" in html
    assert "当前生效篮子 approved" in html
    assert "最新篮子尝试 待复核" in html


def test_render_external_decision_spine_section_renders_decision_facts():
    html = render_external_decision_spine_section(
        {
            "decision_status": "受控等待",
            "decision_reason": "数据门未过",
            "decision_summary": "继续制度锁定",
            "next_check": "2026-04-30",
            "primary_progress": "12/20",
            "primary_needed": "8",
            "basket_progress": "9/20",
            "basket_needed": "11",
            "promotion_decision_label": "晋级锁定",
            "boundary": "只读证据驾驶舱。",
        }
    )
    assert "受控等待" in html
    assert "12/20" in html
    assert "只读证据驾驶舱" in html


def test_render_external_decision_spine_section_uses_note_verbatim_for_sample_accumulation():
    html = render_external_decision_spine_section(
        {
            "decision_status": "继续观察",
            "decision_reason": "证据还不够",
            "decision_summary": "可看观察名单，暂不行动",
            "next_check": "等下一批更新",
            "primary_progress": "观察中",
            "primary_needed": "内部证据说明",
            "basket_progress": "观察中",
            "basket_needed": "内部证据说明",
            "promotion_decision_label": "未开放行动",
            "boundary": "仅供观察，不构成买卖建议。",
        }
    )
    assert "继续观察" in html
    assert "观察名单" in html
    assert "未开放行动" in html
    assert "主结果证据" not in html
    assert "晋级门禁" not in html


def test_render_control_strip_section_renders_cards_and_sub_lines():
    html = render_control_strip_section(
        [
            {"label": "研究状态", "value": "稳健", "sub_lines": ["健康分 92"]},
            {"label": "自动链路", "value": "已更新", "sub_lines": ["已完成"]},
        ]
    )
    assert "研究状态" in html
    assert "健康分 92" in html
    assert "自动链路" in html


def test_render_primary_result_home_brief_section_renders_brief_and_blocker():
    html = render_primary_result_home_brief_section(
        {
            "result_id": "primary:1",
            "result_subject": "000001.SZ 平安银行",
            "stage_label": "观察中",
            "backtest_conclusion": "继续观察",
            "dominant_regime": "震荡",
            "risk_preference": "中性",
            "db_latest_trade_date": "2026-04-29",
            "candidate_generated_at": "2026-04-29 08:00:00",
            "observation_timeline_label": "2026-04-24",
            "avg_risk_pressure": "0.33",
            "disabled_reason": "无",
            "invalid_reason": "无",
            "blocker_title": "样本不足",
            "blocker_detail": "还差 8 个样本",
        }
    )
    assert "primary:1" in html
    assert "000001.SZ 平安银行" in html
    assert "样本不足" in html


def test_render_actions_section_renders_cards_and_table():
    html = render_actions_section(
        actions_view_model={
            "detail": "候选生成时间 2026-04-28",
            "metric_chips": ["候选类型 主链结果"],
            "basket_dual_track_rows": [{"tone": "pointer", "text": "当前生效篮子 approved"}],
        },
        actions_render_contract={
            "card_rows": [
                {
                    "代码": "000001.SZ",
                    "名称": "平安银行",
                    "行业": "银行",
                    "信号": "强买",
                    "风险": "低",
                    "综合分": "155.00",
                    "上涨概率": "70.00%",
                    "预测收益": "8.00%",
                    "置信度": "90.00%",
                    "建议仓位": "20.0%",
                    "建议动作": "优先关注（低风险高胜率）",
                    "依据": "模型看多 因子强势",
                    "href": "/stock/?view=candidates&candidate=0",
                }
            ],
            "table_rows": [
                {
                    "代码": "000001.SZ",
                    "名称": "平安银行",
                    "行业": "银行",
                    "信号": "强买",
                    "风险": "低",
                    "综合分": "155.00",
                    "上涨概率": "70.00%",
                    "预测收益": "8.00%",
                    "置信度": "90.00%",
                    "建议仓位": "20.0%",
                    "建议动作": "优先关注（低风险高胜率）",
                }
            ],
        },
    )

    assert 'id="candidate-actions"' in html
    assert "候选类型 主链结果" in html
    assert '<a class="action-card" href="/stock/?view=candidates&amp;candidate=0">' in html
    assert "candidate-actions-table" in html


def test_render_opportunities_section_renders_contract_and_cards():
    html = render_opportunities_section(
        opportunities_view_model={
            "display_contract": {
                "conclusion_title": "000001.SZ",
                "conclusion_sub": "平安银行 · strong_buy · low",
                "evidence_title": "8 个候选",
                "evidence_sub": "生成 2026-04-28 · 类型 主链结果",
                "boundary_title": "研究优先级",
                "boundary_sub": "只用于下钻顺序",
            "next_title": "下钻候选详情",
            "next_sub": "先核对风险",
            },
            "basket_dual_track_rows": [{"tone": "pointer", "text": "当前生效篮子 approved"}],
            "cards": [
                {
                    "rank": "1",
                    "href": "/stock/?view=candidates&candidate=0",
                    "ts_code": "000001.SZ",
                    "stock_name": "平安银行",
                    "signal": "strong_buy",
                    "risk_level": "low",
                    "final_score": "155.00",
                }
            ],
        },
    )

    assert 'id="opportunities"' in html
    assert "当前生效篮子 approved" in html
    assert "opportunity-card" in html


def test_render_prefilter_section_renders_diagnostic_rows():
    html = render_prefilter_section(
        {
            "display_contract": {
                "conclusion_title": "诊断样本，不作为对外结论",
                "conclusion_sub": "Top1 000001.SZ",
                "evidence_title": "12 / 5200",
                "evidence_sub": "预筛交易日 2026-04-28（过期）",
                "boundary_title": "专家证据区",
                "boundary_sub": "不替代主结论",
                "next_title": "回看证据驾驶舱",
                "next_sub": "先确认等待状态",
            },
            "quality_label": "诊断样本，不作为对外结论",
            "trade_date_label": "2026-04-28（过期）",
            "guard_notes": ["对外结论以第一屏受控等待驾驶舱为准"],
            "kpi_rows": [[{"label": "预筛总数", "value": "12", "sub": "覆盖率 0.2%"}]],
            "top10_rows": [
                {
                    "rank": "1",
                    "ts_code": "diag-001",
                    "name_display": "诊断样本，非正式A股代码",
                    "is_standard_code": "",
                    "prefilter_score": "-",
                    "prefilter_reason": "暂无入池理由，需回看 artifact",
                }
            ],
            "exclusion_rows": [],
            "top_exclusion_rows": [],
            "generated_at": "2026-04-28 08:00:00",
            "top10_count": "10",
        }
    )

    assert 'id="prefilter"' in html
    assert "诊断样本" in html
    assert "暂无预筛底表" not in html


def test_render_selection_funnel_section_renders_funnel_rows():
    html = render_selection_funnel_section(
        {
            "display_contract": {
                "conclusion_title": "80 进预筛，12 进候选",
                "conclusion_sub": "Top1 000001.SZ",
                "evidence_title": "5200 → 80 → 12",
                "evidence_sub": "通过率 1.5%",
                "boundary_title": "漏斗解释",
                "boundary_sub": "解释筛选收缩",
                "next_title": "检查异常收缩",
                "next_sub": "先查预筛口径",
            },
            "kpi_rows": [[{"label": "预筛通过率", "value": "1.5%", "sub": "说明"}]],
            "metric_chips": ["全市场样本 5200"],
            "funnel_rows": [{"label": "全市场样本", "value": "5200", "sub": "诊断基底", "width_pct": "100.00"}],
        }
    )

    assert 'id="selection-funnel"' in html
    assert "全市场样本 5200" in html
    assert "width:100.00%" in html


def test_render_overview_kpi_section_renders_core_kpi_grid():
    html = render_overview_kpi_section(
        {
            "kpi_rows": [[
                {"label": "系统状态", "value": "稳健", "sub": "健康分 92 ｜ 自动链路 已更新"},
                {"label": "推进决策", "value": "进入复核", "sub": "等待样本闭合"},
                {"label": "证据进度", "value": "6/20", "sub": "主结果 14 待补 ｜ 候选篮 4/20"},
                {"label": "治理门禁", "value": "通过", "sub": "治理通过"},
            ]]
        }
    )
    assert 'class="grid-kpi"' in html
    assert "进入复核" in html
    assert "治理通过" in html


def test_render_jump_strip_renders_links_and_tool_button():
    html = render_jump_strip(
        {
            "links": [{"label": "系统结论", "anchor": "summary"}],
            "tool_button": {"label": "聚焦主内容", "body_class": "overview-focus"},
        }
    )
    assert 'href="#summary"' in html
    assert 'data-body-class="overview-focus"' in html


def test_render_nav_links_marks_active_entry():
    html = render_nav_links(
        [
            {"label": "总览", "href": "/stock?view=overview", "active": False},
            {"label": "候选股", "href": "/stock?view=candidates&candidate=2", "active": True},
        ]
    )
    assert 'href="/stock?view=candidates&amp;candidate=2"' in html
    assert "nav-link-active" in html


def test_render_sidebar_status_section_renders_stats():
    html = render_sidebar_status_section(
        [
            {"label": "健康", "value": "稳健"},
            {"label": "更新", "value": "已更新"},
        ]
    )
    assert "sidebar-caption" in html
    assert "稳健" in html


def test_render_topbar_pills_section_renders_multiple_pills():
    html = render_topbar_pills_section(
        ["数据最新交易日 2026-04-28", "候选产物 2026-04-28 08:00:00"]
    )
    assert "topbar-pill" in html
    assert "数据最新交易日 2026-04-28" in html


def test_render_overview_visuals_section_wraps_core_charts():
    html = render_overview_visuals_section(
        health_chart_html="<div>h</div>",
        backtest_equity_html="<div>e</div>",
        backtest_drawdown_html="<div>d</div>",
        backtest_chart_html="<div>b</div>",
    )
    assert 'id="overview-visuals"' in html
    assert "<div>b</div>" in html


def test_render_view_banner_section_renders_focus_and_timeline():
    html = render_view_banner_section(
        {
            "view_title": "研究中心",
            "view_subtitle": "聚焦方法结构",
            "focus_code": "000001.SZ",
            "focus_subtitle": "平安银行 · strong_buy · low",
            "timeline_title": "2026-04-28",
            "timeline_detail": "数据库与候选时间一致",
        }
    )
    assert "研究中心" in html
    assert "000001.SZ" in html
    assert "2026-04-28" in html


def test_render_hero_side_section_renders_status_and_pointer_rows():
    html = render_hero_side_section(
        hero_side_view_model={
            "health_status": "稳健",
            "health_tag": "ok",
            "health_score": "92",
            "top_code": "000001.SZ",
            "candidate_name": "平安银行",
            "candidate_generated_at": "2026-04-28 08:00:00",
            "generation_mode_label": "主链结果",
            "update_status_label": "已更新",
            "update_stage_label": "已完成",
        },
        basket_dual_track_html="<div>pointer</div>",
    )
    assert "hero-side" in html
    assert "<div>pointer</div>" in html
    assert "000001.SZ" in html


def test_render_spotlight_section_renders_chips():
    html = render_spotlight_section(
        {
            "top_code": "000001.SZ",
            "candidate_name": "平安银行",
            "meta_chips": ["strong_buy", "low", "Score 95"],
        }
    )
    assert "spotlight-card" in html
    assert "Score 95" in html


def test_render_spotlight_section_omits_empty_candidate_name():
    html = render_spotlight_section(
        {
            "top_code": "300750.SZ",
            "candidate_name": "",
            "meta_chips": ["观察", "中", "主链结果"],
        }
    )
    assert "spotlight-card" in html
    assert "spotlight-subtitle" not in html


def test_render_command_deck_section_renders_focus_and_runtime():
    html = render_command_deck_section(
        command_focus_view_model={"title": "000001.SZ 平安银行", "detail": "当前优先候选"},
        command_runtime_view_model={"title": "fresh", "detail": "最近任务阶段 已完成"},
    )
    assert "command-deck" in html
    assert "当前优先候选" in html
    assert "fresh" in html


def test_render_overview_visuals_disclosure_section_wraps_visuals_card():
    html = render_overview_visuals_disclosure_section(overview_visuals_section="<div>visuals</div>")
    assert 'id="overview-visuals-disclosure"' in html
    assert "<div>visuals</div>" in html


def test_render_overview_system_summary_section_wraps_control_strip():
    html = render_overview_system_summary_section(
        control_strip_html="<div>control-strip</div>",
        disclosure_view_model={
            "system_summary_title": "运行、候选、预筛、治理指标已下沉",
            "system_summary_detail": "需要核对运营指标时再展开。",
        },
    )
    assert 'id="external-system-summary"' in html
    assert "control-strip" in html


def test_render_overview_operations_disclosure_section_nests_ops_panel():
    html = render_overview_operations_disclosure_section(
        operations_section="<div>ops</div>",
        external_system_summary_html="<div>summary</div>",
        disclosure_view_model={
            "operations_title": "内部复核面已收起",
            "operations_detail": "折叠区只保留内部复核入口，不参与首页正式判断。",
        },
    )
    assert 'id="overview-operations-disclosure"' in html
    assert "<div>summary</div>" in html
    assert "<div>ops</div>" in html
    assert html.count("默认不参与首页正式判断") == 0


def test_render_candidate_visuals_section_wraps_candidate_charts():
    html = render_candidate_visuals_section(
        candidate_map_chart_html="<div>map</div>",
        candidate_chart_html="<div>chart</div>",
    )
    assert 'id="candidate-visuals"' in html
    assert "<div>map</div>" in html


def test_render_links_section_renders_entries_and_actions():
    html = render_links_section(
        {
            "title": "快速入口",
            "detail": "报告、CSV、候选列表、回测文档",
            "entries": [
                {
                    "title": "每日研究原文",
                    "desc": "查看完整机器研究报告",
                    "actions": [
                        {"label": "查看", "href": "/file/research.md", "target_blank": True},
                        {"label": "下载", "href": "/download/research.md", "target_blank": False},
                    ],
                }
            ],
        }
    )
    assert 'id="links"' in html
    assert "每日研究原文" in html
    assert 'target="_blank"' in html


def test_render_guide_section_renders_usage_items():
    html = render_guide_section(
        {
            "title": "指标释义与使用建议",
            "detail": "第一次看这个系统，建议先看这块",
            "items": ["健康评分：系统运行稳定性", "综合分：合成分"],
        }
    )
    assert "指标释义与使用建议" in html
    assert "健康评分：系统运行稳定性" in html


def test_render_charts_section_renders_chart_grid_blocks():
    html = render_charts_section(
        {
            "title": "图形监控",
            "detail": "从趋势图和分布图快速判断系统状态，而不是只看文本",
            "chart_html_blocks": ["<div>h</div>", "<div>e</div>", "<div>c</div>"],
        }
    )
    assert 'id="charts"' in html
    assert "<div>c</div>" in html


def test_render_architecture_section_renders_flow_steps():
    html = render_architecture_section(
        [
            {"index": "01", "title": "数据层", "desc": "拉取日线"},
            {"index": "02", "title": "特征层", "desc": "构建特征"},
        ]
    )
    assert 'id="architecture"' in html
    assert "数据层" in html
    assert "构建特征" in html


def test_render_candidate_compare_section_renders_top2_gap():
    html = render_candidate_compare_section(
        {
            "available": True,
            "top_card": {
                "ts_code": "000001.SZ",
                "stock_name": "平安银行",
                "signal": "strong_buy",
                "risk_level": "low",
                "final_score": "95",
                "pred_return": "8%",
            },
            "next_card": {
                "ts_code": "000002.SZ",
                "stock_name": "万科A",
                "signal": "watch",
                "risk_level": "medium",
                "final_score": "88",
                "pred_return": "5%",
            },
            "score_gap": "7.0",
        }
    )
    assert 'id="candidate-compare"' in html
    assert "综合分差值" in html
    assert "7.0" in html


def test_render_market_snapshot_section_renders_metric_ribbon():
    html = render_market_snapshot_section(
        market_snapshot={
            "dominant_regime": "震荡",
            "risk_preference": "中性",
            "style_bias": "均衡",
            "guardrail_mode": "normal",
            "guardrail_reason": "未触发",
            "candidate_count": "12",
            "avg_position_pct": "18.0%",
            "avg_risk_pressure": "0.3",
            "risk_flag_count": "2",
        }
    )
    assert 'id="market-snapshot"' in html
    assert "候选样本 12 只" in html


def test_render_research_visuals_section_wraps_charts():
    html = render_research_visuals_section(
        health_chart_html="<div>h</div>",
        backtest_equity_html="<div>e</div>",
        backtest_drawdown_html="<div>d</div>",
        backtest_map_chart_html="<div>m</div>",
    )
    assert 'id="research-visuals"' in html
    assert "<div>m</div>" in html


def test_render_top1_section_renders_candidate_summary():
    html = render_top1_section(
        top1_view_model={
            "top_code": "000001.SZ",
            "stock_name": "平安银行",
            "final_score": "95",
            "signal": "strong_buy",
            "risk": "low",
        },
    )
    assert "000001.SZ" in html
    assert "strong_buy" in html


def test_render_candidate_focus_section_renders_terminal():
    html = render_candidate_focus_section(
        candidate_focus_view_model={
            "top_code": "000001.SZ",
            "signal": "strong_buy",
            "risk": "low",
            "final_score": "95",
            "candidate_position_label": "20.0%",
        },
        candidate_focus_render_contract={
            "nav": {
                "prev_href": "/stock/?view=candidates&candidate=0",
                "prev_disabled": True,
                "next_href": "/stock/?view=candidates&candidate=1",
                "next_disabled": False,
                "position_label": "候选 1 / 2",
            },
            "quick_links": [
                {"label": "Top1", "ts_code": "000001.SZ", "href": "/stock/?view=candidates&candidate=0", "active": True}
            ],
            "switcher_items": [
                {"rank": "#1", "href": "/stock/?view=candidates&candidate=0", "active": True, "ts_code": "000001.SZ", "stock_name": "平安银行", "signal": "strong_buy", "final_score": "95"}
            ],
        },
        candidate_detail_html="<div>detail</div>",
    )
    assert 'id="candidate-focus"' in html
    assert "候选 1 / 2" in html
    assert "Top1" in html
    assert "20.0%" in html


def test_render_diagnosis_section_renders_contract_and_list():
    html = render_diagnosis_section(
        bt_diag={
            "结论": "继续观察",
            "总收益": "12%",
            "夏普": "1.2",
            "胜率": "58%",
            "最大回撤": "-8%",
            "交易次数": "24",
            "Calmar": "1.1",
            "盈亏比": "1.4",
            "佣金": "0.1%",
            "滑点": "0.05%",
            "低流动性拦截": "3",
            "诊断": ["样本不足", "继续观察"],
        },
        next_check="T+1",
    )
    assert 'id="diagnosis"' in html
    assert "继续观察" in html
    assert "样本不足" in html
