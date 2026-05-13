from __future__ import annotations

from pathlib import Path
from typing import Any

from src.dashboard_reports import build_reports_render_contract, render_reports_section
from src.stock_dashboard_constants import REPORT_LABELS
from src.stock_dashboard_url import report_href


def build_stock_reports_section(
    *,
    visible: bool,
    current_report: str,
    candidate_index: int,
    base_path: str,
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
) -> str:
    return render_reports_section(
        build_reports_render_contract(
            visible=visible,
            current_report=current_report,
            candidate_index=candidate_index,
            base_path=base_path,
            report_labels=REPORT_LABELS,
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
            report_href_builder=report_href,
        )
    )
