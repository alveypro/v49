from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from src.stock_dashboard_charts_section import build_stock_charts_section
from src.stock_dashboard_links_section import build_stock_links_section
from src.stock_dashboard_reports_section import build_stock_reports_section
from src.stock_dashboard_sections import render_guide_section
from src.stock_dashboard_view_model import build_stock_guide_view_model


@dataclass(frozen=True)
class StockResourceSections:
    links_section: str
    guide_section: str
    charts_section: str
    reports_section: str


def build_stock_resource_sections(
    *,
    visible: Callable[[str], bool],
    daily_md_href: str,
    daily_md_download_href: str,
    health_csv_href: str,
    health_csv_download_href: str,
    leaderboard_href: str,
    leaderboard_download_href: str,
    candidates_md_href: str,
    candidates_md_download_href: str,
    candidates_csv_href: str,
    candidates_csv_download_href: str,
    latest_report_href: str,
    latest_report_download_href: str,
    candidate_index: int,
    base_path: str,
    health_chart_html: str,
    backtest_equity_html: str,
    backtest_drawdown_html: str,
    backtest_chart_html: str,
    backtest_map_chart_html: str,
    candidate_map_chart_html: str,
    candidate_chart_html: str,
    current_report: str,
    daily_md_text: str,
    translated_daily_md_text: str,
    health_csv: Path,
    leaderboard_csv: Path,
    latest_report_text: str,
    evolution_status: dict[str, Any],
    backtest_scope: dict[str, str],
) -> StockResourceSections:
    guide_section = render_guide_section(build_stock_guide_view_model()) if visible("guide") else ""
    return StockResourceSections(
        links_section=build_stock_links_section(
            visible=visible("links"),
            daily_md_href=daily_md_href,
            daily_md_download_href=daily_md_download_href,
            health_csv_href=health_csv_href,
            health_csv_download_href=health_csv_download_href,
            leaderboard_href=leaderboard_href,
            leaderboard_download_href=leaderboard_download_href,
            candidates_md_href=candidates_md_href,
            candidates_md_download_href=candidates_md_download_href,
            candidates_csv_href=candidates_csv_href,
            candidates_csv_download_href=candidates_csv_download_href,
            latest_report_href=latest_report_href,
            latest_report_download_href=latest_report_download_href,
            candidate_index=candidate_index,
            base_path=base_path,
        ),
        guide_section=guide_section,
        charts_section=build_stock_charts_section(
            visible=visible("charts"),
            health_chart_html=health_chart_html,
            backtest_equity_html=backtest_equity_html,
            backtest_drawdown_html=backtest_drawdown_html,
            backtest_chart_html=backtest_chart_html,
            backtest_map_chart_html=backtest_map_chart_html,
            candidate_map_chart_html=candidate_map_chart_html,
            candidate_chart_html=candidate_chart_html,
        ),
        reports_section=build_stock_reports_section(
            visible=visible("reports"),
            current_report=current_report,
            candidate_index=candidate_index,
            base_path=base_path,
            daily_md_text=daily_md_text,
            translated_daily_md_text=translated_daily_md_text,
            health_csv=health_csv,
            leaderboard_csv=leaderboard_csv,
            latest_report_text=latest_report_text,
            evolution_status=evolution_status,
            backtest_scope=backtest_scope,
            daily_md_download_href=daily_md_download_href,
            health_csv_download_href=health_csv_download_href,
            leaderboard_download_href=leaderboard_download_href,
            latest_report_download_href=latest_report_download_href,
        ),
    )
