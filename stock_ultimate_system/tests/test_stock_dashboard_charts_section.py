from src.stock_dashboard_charts_section import build_stock_charts_section


def _kwargs() -> dict[str, str]:
    return {
        "health_chart_html": "<div>health</div>",
        "backtest_equity_html": "<div>equity</div>",
        "backtest_drawdown_html": "<div>drawdown</div>",
        "backtest_chart_html": "<div>backtest</div>",
        "backtest_map_chart_html": "<div>map</div>",
        "candidate_map_chart_html": "<div>candidate-map</div>",
        "candidate_chart_html": "<div>candidate</div>",
    }


def test_build_stock_charts_section_returns_empty_when_hidden():
    assert build_stock_charts_section(visible=False, **_kwargs()) == ""


def test_build_stock_charts_section_renders_all_chart_blocks():
    html = build_stock_charts_section(visible=True, **_kwargs())

    assert 'id="charts"' in html
    assert "图形监控" in html
    assert "<div>health</div>" in html
    assert "<div>candidate</div>" in html
