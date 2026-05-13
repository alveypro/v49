from __future__ import annotations

from typing import Any

from src.stock_dashboard_render_context import build_candidate_focus_render_contract
from src.stock_dashboard_sections import render_candidate_focus_section
from src.stock_dashboard_view_model import build_stock_candidate_focus_view_model


def build_stock_candidate_focus_section(
    *,
    visible: bool,
    top1: dict[str, Any],
    top1_signal: str,
    top1_risk: str,
    candidate_cards: list[dict[str, Any]],
    candidate_index: int,
    candidate_detail_html: str,
    base_path: str,
) -> str:
    if not visible:
        return ""
    candidate_focus_view_model = build_stock_candidate_focus_view_model(
        top1=top1,
        top1_signal=top1_signal,
        top1_risk=top1_risk,
        candidate_cards=candidate_cards,
        candidate_index=candidate_index,
    )
    return render_candidate_focus_section(
        candidate_focus_view_model=candidate_focus_view_model,
        candidate_focus_render_contract=build_candidate_focus_render_contract(
            candidate_focus_view_model=candidate_focus_view_model,
            candidate_cards=candidate_cards,
            candidate_index=candidate_index,
            base_path=base_path,
        ),
        candidate_detail_html=candidate_detail_html,
    )
