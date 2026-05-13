from src.stock_dashboard_t12_sections import T12ReadOnlySections, build_t12_read_only_sections


def test_build_t12_read_only_sections_returns_overview_and_governance_html():
    sections = build_t12_read_only_sections(
        minimal_facts={
            "threshold_applied": True,
            "threshold_recommendation": "keep_thresholds",
            "source_timestamps": {"t12_threshold_control_latest.json": "2026-05-13 09:30:00"},
        },
        governance_source_facts={
            "audit_status": "passed",
            "result_lifecycle_stage": "L4",
            "risk_level": "low",
        },
    )

    assert isinstance(sections, T12ReadOnlySections)
    assert 'id="t12-overview-card"' in sections.overview_card_section
    assert "T12 阈值控制" in sections.overview_card_section
    assert 'id="t12-governance-summary"' in sections.governance_summary_section
    assert "T12 治理摘要" in sections.governance_summary_section


def test_build_t12_read_only_sections_escapes_source_facts():
    sections = build_t12_read_only_sections(
        minimal_facts={"source_timestamps": {}},
        governance_source_facts={"disabled_reason": "<script>alert(1)</script>"},
    )

    assert "<script>alert(1)</script>" not in sections.governance_summary_section
    assert "&lt;script&gt;alert(1)&lt;/script&gt;" in sections.governance_summary_section
