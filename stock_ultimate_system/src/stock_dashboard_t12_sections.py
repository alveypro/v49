from __future__ import annotations

from dataclasses import dataclass

from src.t12_governance_summary import (
    build_t12_governance_summary_view_model,
    render_t12_governance_summary_template,
)
from src.t12_overview_card import build_t12_overview_card_view_model, render_t12_overview_card_template


@dataclass(frozen=True)
class T12ReadOnlySections:
    overview_card_section: str
    governance_summary_section: str


def build_t12_read_only_sections(
    *,
    minimal_facts: dict[str, object],
    governance_source_facts: dict[str, object],
) -> T12ReadOnlySections:
    return T12ReadOnlySections(
        overview_card_section=render_t12_overview_card_template(
            build_t12_overview_card_view_model(minimal_facts)
        ),
        governance_summary_section=render_t12_governance_summary_template(
            build_t12_governance_summary_view_model(governance_source_facts)
        ),
    )
