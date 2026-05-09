from __future__ import annotations

import html
from pathlib import Path
from typing import Any, Callable

from src.dashboard_support import markdown_to_html_basic, preferred_backtest_table_html, tail_csv_as_html
from src.stock_dashboard_sections import render_table_export_block


def build_reports_render_contract(
    *,
    visible: bool,
    current_report: str,
    candidate_index: int,
    base_path: str,
    report_labels: dict[str, str],
    daily_md_text: str,
    translated_daily_md_text: str,
    health_csv: Path,
    leaderboard_csv: Path,
    latest_report_text: str,
    evolution_status: dict[str, Any],
    backtest_scope: dict[str, str],
    daily_md_download_href: str,
    health_csv_download_href: str,
    leaderboard_download_href: str,
    latest_report_download_href: str,
    report_href_builder: Callable[[str, int, str], str],
) -> dict[str, Any]:
    report_nav = "".join(
        f'<a class="report-link{" report-link-active" if key == current_report else ""}" href="{report_href_builder(key, candidate_index, base_path)}">{label}</a>'
        for key, label in report_labels.items()
    )
    current_report_export_href = {
        "research": daily_md_download_href,
        "health": health_csv_download_href,
        "leaderboard": leaderboard_download_href,
        "backtest": latest_report_download_href,
        "evolution": report_href_builder("evolution", candidate_index, base_path),
    }.get(current_report, daily_md_download_href)
    report_panels = {
        "research": (
            '<div class="report-workspace-title"><div class="eyebrow-inline">研究简报</div><h4>每日研究原文</h4></div>'
            '<div class="report-split">'
            f'<div class="pane"><h4>原文</h4><pre>{html.escape(daily_md_text)}</pre></div>'
            f'<div class="pane"><h4>中文阅读视图</h4>{markdown_to_html_basic(translated_daily_md_text)}</div>'
            "</div>"
        ),
        "health": (
            '<div class="report-workspace-title"><div class="eyebrow-inline">健康归档</div><h4>健康趋势表</h4></div>'
            + render_table_export_block(
                "健康趋势表",
                "health-trend-table",
                "health_trend_view.csv",
                tail_csv_as_html(health_csv, max_rows=20),
            )
        ),
        "leaderboard": (
            '<div class="report-workspace-title"><div class="eyebrow-inline">绩效排行</div><h4>回测排行榜</h4></div>'
            f'<div class="scope-note"><strong>{html.escape(backtest_scope["label"])}</strong><span>{html.escape(backtest_scope["detail"])}</span></div>'
            + render_table_export_block(
                "回测排行榜",
                "leaderboard-table",
                "backtest_leaderboard_view.csv",
                preferred_backtest_table_html(leaderboard_csv, max_rows=20),
            )
        ),
        "backtest": (
            '<div class="report-workspace-title"><div class="eyebrow-inline">回测报告</div><h4>最新回测报告</h4></div>'
            f"<pre>{html.escape(latest_report_text)}</pre>"
        ),
        "evolution": (
            '<div class="report-workspace-title"><div class="eyebrow-inline">自动进化</div><h4>冠军版本与晋级历史</h4></div>'
            '<div class="split-panel">'
            f'<div class="panel-subcard"><h4>当前冠军</h4><p>版本 {html.escape(str(evolution_status.get("champion_version", "-")))} ｜ walk-forward {html.escape(str(evolution_status.get("champion_walk_forward_score", "-")))} ｜ 稳定性 {html.escape(str(evolution_status.get("champion_stability", "-")))}</p><p>模型集 {html.escape(str(evolution_status.get("champion_models", "-")))}</p></div>'
            f'<div class="panel-subcard"><h4>最近动作</h4><p>动作 {html.escape(str(evolution_status.get("latest_action", "-")))}</p><p>{html.escape(str(evolution_status.get("latest_reason", "-")))}</p></div>'
            "</div>"
            + render_table_export_block(
                "进化历史",
                "evolution-history-table",
                "evolution_history_view.csv",
                (
                    "<table><thead><tr><th>版本</th><th>动作</th><th>原因</th><th>时间</th></tr></thead><tbody>"
                    + "".join(
                        f"<tr><td>{html.escape(str(row.get('version', '')))}</td><td>{html.escape(str(row.get('action', '')))}</td><td>{html.escape(str(row.get('reason', '')))}</td><td>{html.escape(str(row.get('created_at', '')))}</td></tr>"
                        for row in (evolution_status.get("history", []) or [])
                    )
                    + "</tbody></table>"
                ),
            )
        ),
    }
    return {
        "visible": visible,
        "current_report_href": report_href_builder(current_report, candidate_index, base_path),
        "current_report_export_href": current_report_export_href,
        "report_nav": report_nav,
        "report_workspace": report_panels[current_report],
    }


def render_reports_section(reports_render_contract: dict[str, Any]) -> str:
    if not bool(reports_render_contract.get("visible")):
        return ""
    return (
        '<div class="card" id="reports">'
        '<div class="section-title">'
        "<div>"
        '<div class="eyebrow">证据链</div>'
        "<h3>详细数据与原始报告</h3>"
        "</div>"
        '<div class="muted">按资料类型切换查看研究证据链，而不是在一个长页面里反复折叠</div>'
        "</div>"
        f'<div class="report-toolbar"><button class="tool-btn" type="button" data-copy-link="{reports_render_contract["current_report_href"]}">复制当前链接</button><a class="tool-btn" href="{reports_render_contract["current_report_export_href"]}">导出当前视图</a></div>'
        '<div class="report-room reports-workspace">'
        f'<div class="report-nav">{reports_render_contract["report_nav"]}</div>'
        f'<div class="report-panel">{reports_render_contract["report_workspace"]}</div>'
        "</div>"
        "</div>"
    )
