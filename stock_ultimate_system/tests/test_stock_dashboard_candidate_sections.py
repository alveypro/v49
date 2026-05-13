from pathlib import Path

from src.stock_dashboard_candidate_sections import StockCandidateSections, build_stock_candidate_sections


def _candidate_cards() -> list[dict[str, object]]:
    return [
        {
            "ts_code": "000001.SZ",
            "stock_name": "平安银行",
            "industry": "银行",
            "signal": "strong_buy",
            "risk_level": "low",
            "final_score": "155",
            "pred_return": "8%",
            "position_pct": "0.2",
        },
        {
            "ts_code": "000002.SZ",
            "stock_name": "万科A",
            "industry": "地产",
            "signal": "watch",
            "risk_level": "medium",
            "final_score": "88",
            "pred_return": "5%",
        },
    ]


def _write_candidates_csv(tmp_path: Path) -> Path:
    path = tmp_path / "candidates.csv"
    path.write_text(
        "ts_code,stock_name,industry,signal,risk_level,final_score,direction_prob_up,pred_return,confidence,position_pct,stop_loss,take_profit,reason\n"
        "000001.SZ,平安银行,银行,strong_buy,low,155,0.7,0.08,0.9,0.2,10.0,12.0,model_bullish factor_strong\n",
        encoding="utf-8",
    )
    return path


def _kwargs(tmp_path: Path, visible_names: set[str]) -> dict[str, object]:
    return {
        "visible": lambda name: name in visible_names,
        "top1": _candidate_cards()[0],
        "top1_signal": "strong_buy",
        "top1_risk": "low",
        "candidate_cards": _candidate_cards(),
        "candidate_index": 0,
        "candidate_detail_html": "<div>detail</div>",
        "base_path": "/stock",
        "candidate_map_chart_html": "<div>map</div>",
        "candidate_chart_html": "<div>chart</div>",
        "candidates_csv": _write_candidates_csv(tmp_path),
        "candidate_generated_at": "2026-05-13",
        "basket_generated_at": "2026-05-13",
        "candidate_source_label": "正式候选",
        "generation_mode_label": "主链结果",
        "dominant_regime": "震荡",
        "risk_preference": "中性",
        "avg_risk_pressure": "0.3",
        "basket_dual_track_rows": [{"tone": "pointer", "text": "当前生效篮子 approved"}],
        "view_href": lambda view, idx, base_path: f"{base_path}/?view={view}&candidate={idx}",
    }


def test_build_stock_candidate_sections_respects_visibility(tmp_path: Path):
    sections = build_stock_candidate_sections(**_kwargs(tmp_path, visible_names=set()))

    assert isinstance(sections, StockCandidateSections)
    assert sections.candidate_focus_section == ""
    assert sections.actions_section == ""
    assert 'id="candidate-compare"' in sections.candidate_compare_section
    assert 'id="candidate-visuals"' in sections.candidate_visuals_section


def test_build_stock_candidate_sections_builds_visible_candidate_surfaces(tmp_path: Path):
    sections = build_stock_candidate_sections(
        **_kwargs(tmp_path, visible_names={"candidate_focus", "actions"})
    )

    assert 'id="candidate-focus"' in sections.candidate_focus_section
    assert "候选 1 / 2" in sections.candidate_focus_section
    assert "<div>detail</div>" in sections.candidate_focus_section
    assert 'id="candidate-actions"' in sections.actions_section
    assert "/stock/?view=candidates&amp;candidate=0" in sections.actions_section
    assert "优先关注" in sections.actions_section
