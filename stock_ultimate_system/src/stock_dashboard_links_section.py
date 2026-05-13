from __future__ import annotations

from src.stock_dashboard_sections import render_links_section
from src.stock_dashboard_url import report_href
from src.stock_dashboard_view_model import build_stock_links_view_model


def build_stock_links_section(
    *,
    visible: bool,
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
) -> str:
    if not visible:
        return ""
    return render_links_section(
        build_stock_links_view_model(
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
            evolution_report_href=report_href("evolution", candidate_index, base_path),
        )
    )
