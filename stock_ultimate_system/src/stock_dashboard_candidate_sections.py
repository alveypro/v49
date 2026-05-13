from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from src.dashboard_support import build_candidate_actions_render_contract
from src.stock_dashboard_candidate_focus_section import build_stock_candidate_focus_section
from src.stock_dashboard_sections import (
    render_actions_section,
    render_candidate_compare_section,
    render_candidate_visuals_section,
)
from src.stock_dashboard_view_model import build_stock_actions_view_model, build_stock_candidate_compare_view_model


@dataclass(frozen=True)
class StockCandidateSections:
    candidate_focus_section: str
    candidate_compare_section: str
    candidate_visuals_section: str
    actions_section: str


def build_stock_candidate_sections(
    *,
    visible: Callable[[str], bool],
    top1: dict[str, Any],
    top1_signal: str,
    top1_risk: str,
    candidate_cards: list[dict[str, Any]],
    candidate_index: int,
    candidate_detail_html: str,
    base_path: str,
    candidate_map_chart_html: str,
    candidate_chart_html: str,
    candidates_csv: Path,
    candidate_generated_at: str,
    basket_generated_at: str,
    candidate_source_label: str,
    generation_mode_label: str,
    dominant_regime: str,
    risk_preference: str,
    avg_risk_pressure: str,
    basket_dual_track_rows: list[dict[str, str]],
    view_href: Callable[[str, int, str], str],
) -> StockCandidateSections:
    actions_section = ""
    if visible("actions"):
        actions_view_model = build_stock_actions_view_model(
            candidate_generated_at=candidate_generated_at,
            basket_generated_at=basket_generated_at,
            candidate_source_label=candidate_source_label,
            generation_mode_label=generation_mode_label,
            dominant_regime=dominant_regime,
            risk_preference=risk_preference,
            avg_risk_pressure=avg_risk_pressure,
            basket_dual_track_rows=basket_dual_track_rows,
        )
        actions_render_contract = build_candidate_actions_render_contract(
            candidates_csv,
            card_top_n=5,
            table_top_n=10,
            card_hrefs=[view_href("candidates", idx, base_path) for idx in range(5)],
        )
        actions_section = render_actions_section(
            actions_view_model=actions_view_model,
            actions_render_contract=actions_render_contract,
        )

    return StockCandidateSections(
        candidate_focus_section=build_stock_candidate_focus_section(
            visible=visible("candidate_focus"),
            top1=top1,
            top1_signal=top1_signal,
            top1_risk=top1_risk,
            candidate_cards=candidate_cards,
            candidate_index=candidate_index,
            candidate_detail_html=candidate_detail_html,
            base_path=base_path,
        ),
        candidate_compare_section=render_candidate_compare_section(
            build_stock_candidate_compare_view_model(candidate_cards=candidate_cards)
        ),
        candidate_visuals_section=render_candidate_visuals_section(
            candidate_map_chart_html=candidate_map_chart_html,
            candidate_chart_html=candidate_chart_html,
        ),
        actions_section=actions_section,
    )
