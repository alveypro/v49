from pathlib import Path

from src.dashboard_reports import build_reports_render_contract, render_reports_section


def _report_href(report_key: str, candidate_index: int, base_path: str) -> str:
    return f"{base_path}/?view=reports&candidate={candidate_index}&report={report_key}"

def test_render_reports_section_research_view(tmp_path):
    health_csv = tmp_path / "health.csv"
    health_csv.write_text("generated_at,score\n2026-04-05,90\n", encoding="utf-8")
    leaderboard_csv = tmp_path / "leaderboard.csv"
    leaderboard_csv.write_text(
        "run_id,source_type,stock_pool,total_return,sharpe_ratio,max_drawdown,win_rate,total_trades\n"
        "r1,official_research,000001.SZ|600036.SH,0.08,1.1,-0.04,0.58,28\n",
        encoding="utf-8",
    )

    html_text = render_reports_section(build_reports_render_contract(
        visible=True,
        current_report="research",
        candidate_index=0,
        base_path="/dash",
        report_labels={
            "research": "每日研究",
            "health": "健康趋势",
            "leaderboard": "回测排行榜",
            "backtest": "最新回测报告",
            "evolution": "自动进化",
        },
        daily_md_text="# Daily\n- item",
        translated_daily_md_text="# 每日\n- 项目",
        health_csv=health_csv,
        leaderboard_csv=leaderboard_csv,
        latest_report_text="report",
        evolution_status={"history": []},
        backtest_scope={"label": "scope", "detail": "detail"},
        daily_md_download_href="/download/research.md",
        health_csv_download_href="/download/health.csv",
        leaderboard_download_href="/download/leaderboard.csv",
        latest_report_download_href="/download/backtest.md",
        report_href_builder=_report_href,
    ))

    assert "每日研究原文" in html_text
    assert "中文阅读视图" in html_text
    assert "复制当前链接" in html_text
    assert "/dash/?view=reports&candidate=0&report=research" in html_text


def test_render_reports_section_evolution_view(tmp_path):
    health_csv = tmp_path / "health.csv"
    health_csv.write_text("generated_at,score\n2026-04-05,90\n", encoding="utf-8")
    leaderboard_csv = tmp_path / "leaderboard.csv"
    leaderboard_csv.write_text(
        "run_id,source_type,stock_pool,total_return,sharpe_ratio,max_drawdown,win_rate,total_trades\n"
        "r1,official_research,000001.SZ|600036.SH,0.08,1.1,-0.04,0.58,28\n",
        encoding="utf-8",
    )

    html_text = render_reports_section(build_reports_render_contract(
        visible=True,
        current_report="evolution",
        candidate_index=1,
        base_path="/dash",
        report_labels={
            "research": "每日研究",
            "health": "健康趋势",
            "leaderboard": "回测排行榜",
            "backtest": "最新回测报告",
            "evolution": "自动进化",
        },
        daily_md_text="daily",
        translated_daily_md_text="translated",
        health_csv=health_csv,
        leaderboard_csv=leaderboard_csv,
        latest_report_text="report",
        evolution_status={
            "champion_version": "evo_1",
            "champion_walk_forward_score": "0.1234",
            "champion_stability": "0.5678",
            "champion_models": "lightgbm / xgboost",
            "latest_action": "promote",
            "latest_reason": "better",
            "history": [{"version": "evo_1", "action": "promote", "reason": "better", "created_at": "2026-04-05"}],
        },
        backtest_scope={"label": "scope", "detail": "detail"},
        daily_md_download_href="/download/research.md",
        health_csv_download_href="/download/health.csv",
        leaderboard_download_href="/download/leaderboard.csv",
        latest_report_download_href="/download/backtest.md",
        report_href_builder=_report_href,
    ))

    assert "冠军版本与晋级历史" in html_text
    assert "evo_1" in html_text
    assert "lightgbm / xgboost" in html_text
    assert "evolution_history_view.csv" in html_text
