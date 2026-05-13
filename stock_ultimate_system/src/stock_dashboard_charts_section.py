from __future__ import annotations

from src.stock_dashboard_sections import render_charts_section
from src.stock_dashboard_view_model import build_stock_charts_view_model


def build_stock_charts_section(
    *,
    visible: bool,
    health_chart_html: str,
    backtest_equity_html: str,
    backtest_drawdown_html: str,
    backtest_chart_html: str,
    backtest_map_chart_html: str,
    candidate_map_chart_html: str,
    candidate_chart_html: str,
) -> str:
    if not visible:
        return ""
    return render_charts_section(
        build_stock_charts_view_model(
            health_chart_html=health_chart_html,
            backtest_equity_html=backtest_equity_html,
            backtest_drawdown_html=backtest_drawdown_html,
            backtest_chart_html=backtest_chart_html,
            backtest_map_chart_html=backtest_map_chart_html,
            candidate_map_chart_html=candidate_map_chart_html,
            candidate_chart_html=candidate_chart_html,
        )
    )
