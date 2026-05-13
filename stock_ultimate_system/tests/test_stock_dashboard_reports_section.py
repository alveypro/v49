from pathlib import Path

from src.stock_dashboard_reports_section import build_stock_reports_section


def _kwargs(tmp_path: Path) -> dict[str, object]:
    health_csv = tmp_path / "health.csv"
    health_csv.write_text("generated_at,score\n2026-05-13,90\n", encoding="utf-8")
    leaderboard_csv = tmp_path / "leaderboard.csv"
    leaderboard_csv.write_text("run_id,total_return\nrun-1,0.12\n", encoding="utf-8")
    return {
        "candidate_index": 1,
        "base_path": "/stock",
        "daily_md_text": "# Daily\nraw",
        "translated_daily_md_text": "# 每日\n中文",
        "health_csv": health_csv,
        "leaderboard_csv": leaderboard_csv,
        "latest_report_text": "backtest report",
        "evolution_status": {
            "champion_version": "v9",
            "champion_walk_forward_score": "0.12",
            "champion_stability": "0.88",
            "champion_models": "model-a",
            "latest_action": "hold",
            "latest_reason": "observation",
            "history": [{"version": "v9", "action": "hold", "reason": "ok", "created_at": "2026-05-13"}],
        },
        "backtest_scope": {"label": "长窗", "detail": "rolling"},
        "daily_md_download_href": "/download/daily.md",
        "health_csv_download_href": "/download/health.csv",
        "leaderboard_download_href": "/download/leaderboard.csv",
        "latest_report_download_href": "/download/report.md",
    }


def test_build_stock_reports_section_returns_empty_when_hidden(tmp_path: Path):
    html = build_stock_reports_section(visible=False, current_report="research", **_kwargs(tmp_path))

    assert html == ""


def test_build_stock_reports_section_renders_research_report(tmp_path: Path):
    html = build_stock_reports_section(visible=True, current_report="research", **_kwargs(tmp_path))

    assert 'id="reports"' in html
    assert "每日研究原文" in html
    assert 'href="/download/daily.md"' in html
    assert 'data-copy-link="/stock/?view=reports&candidate=1&report=research"' in html


def test_build_stock_reports_section_renders_evolution_report(tmp_path: Path):
    html = build_stock_reports_section(visible=True, current_report="evolution", **_kwargs(tmp_path))

    assert "冠军版本与晋级历史" in html
    assert "v9" in html
    assert 'data-copy-link="/stock/?view=reports&candidate=1&report=evolution"' in html
