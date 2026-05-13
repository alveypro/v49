from src.stock_dashboard_links_section import build_stock_links_section


def _kwargs() -> dict[str, object]:
    return {
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
        "candidate_index": 2,
        "base_path": "/stock",
    }


def test_build_stock_links_section_returns_empty_when_hidden():
    assert build_stock_links_section(visible=False, **_kwargs()) == ""


def test_build_stock_links_section_renders_file_and_report_links():
    html = build_stock_links_section(visible=True, **_kwargs())

    assert 'id="links"' in html
    assert "每日研究原文" in html
    assert 'href="/file/daily.md"' in html
    assert 'href="/download/report.md"' in html
    assert 'href="/stock/?view=reports&amp;candidate=2&amp;report=evolution"' in html
