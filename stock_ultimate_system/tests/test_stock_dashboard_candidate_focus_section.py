from src.stock_dashboard_candidate_focus_section import build_stock_candidate_focus_section


def _candidate_cards() -> list[dict[str, object]]:
    return [
        {
            "ts_code": "000001.SZ",
            "stock_name": "平安银行",
            "signal": "strong_buy",
            "risk_level": "low",
            "final_score": "95",
            "position_pct": "0.2",
        },
        {
            "ts_code": "000002.SZ",
            "stock_name": "万科A",
            "signal": "watch",
            "risk_level": "medium",
            "final_score": "88",
        },
    ]


def test_build_stock_candidate_focus_section_returns_empty_when_hidden():
    html = build_stock_candidate_focus_section(
        visible=False,
        top1=_candidate_cards()[0],
        top1_signal="strong_buy",
        top1_risk="low",
        candidate_cards=_candidate_cards(),
        candidate_index=0,
        candidate_detail_html="<div>detail</div>",
        base_path="/stock",
    )

    assert html == ""


def test_build_stock_candidate_focus_section_renders_terminal_and_navigation():
    html = build_stock_candidate_focus_section(
        visible=True,
        top1=_candidate_cards()[0],
        top1_signal="strong_buy",
        top1_risk="low",
        candidate_cards=_candidate_cards(),
        candidate_index=0,
        candidate_detail_html="<div>detail</div>",
        base_path="/stock",
    )

    assert 'id="candidate-focus"' in html
    assert "000001.SZ" in html
    assert "strong_buy" in html
    assert "20.0%" in html
    assert "候选 1 / 2" in html
    assert "/stock/?view=candidates&amp;candidate=1" in html
    assert "<div>detail</div>" in html
