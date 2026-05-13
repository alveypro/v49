from pathlib import Path

from src.stock_dashboard_sections_builder import StockResourceSections, build_stock_resource_sections


def _kwargs(tmp_path: Path, visible_names: set[str]) -> dict[str, object]:
    health_csv = tmp_path / "health.csv"
    health_csv.write_text("generated_at,score\n2026-05-13,90\n", encoding="utf-8")
    leaderboard_csv = tmp_path / "leaderboard.csv"
    leaderboard_csv.write_text("run_id,total_return\nrun-1,0.12\n", encoding="utf-8")
    return {
        "visible": lambda name: name in visible_names,
        "daily_md_href": "/file/daily.md",
        "daily_md_download_href": "/download/daily.md",
        "health_csv_href": "/file/health.csv",
        "health_csv_download_href": "/download/health.csv",
        "leaderboard_href": "/file/leaderboard.csv",
        "leaderboard_download_href": "/download/leaderboard.csv",
        "candidates_md_href": "/file/candidates.md",
        "candidates_md_download_href": "/download/candidates.md",
        "candidates_csv_href": "/file/candidates.csv",
        "candidates_csv_download_href": "/download/candidates.csv",
        "latest_report_href": "/file/report.md",
        "latest_report_download_href": "/download/report.md",
        "candidate_index": 1,
        "base_path": "/stock",
        "health_chart_html": "<div>health-chart</div>",
        "backtest_equity_html": "<div>equity</div>",
        "backtest_drawdown_html": "<div>drawdown</div>",
        "backtest_chart_html": "<div>backtest</div>",
        "backtest_map_chart_html": "<div>map</div>",
        "candidate_map_chart_html": "<div>candidate-map</div>",
        "candidate_chart_html": "<div>candidate-chart</div>",
        "current_report": "research",
        "daily_md_text": "# Daily",
        "translated_daily_md_text": "# 每日",
        "health_csv": health_csv,
        "leaderboard_csv": leaderboard_csv,
        "latest_report_text": "backtest report",
        "evolution_status": {"champion_version": "v9", "history": []},
        "backtest_scope": {"label": "长窗", "detail": "rolling"},
    }


def test_build_stock_resource_sections_respects_visibility(tmp_path: Path):
    sections = build_stock_resource_sections(**_kwargs(tmp_path, visible_names=set()))

    assert sections == StockResourceSections(
        links_section="",
        guide_section="",
        charts_section="",
        reports_section="",
    )


def test_build_stock_resource_sections_builds_visible_material_sections(tmp_path: Path):
    sections = build_stock_resource_sections(
        **_kwargs(tmp_path, visible_names={"links", "guide", "charts", "reports"})
    )

    assert 'id="links"' in sections.links_section
    assert "指标释义与使用建议" in sections.guide_section
    assert 'id="charts"' in sections.charts_section
    assert 'id="reports"' in sections.reports_section
    assert 'data-copy-link="/stock/?view=reports&candidate=1&report=research"' in sections.reports_section
