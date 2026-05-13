from pathlib import Path
from types import SimpleNamespace

import run_dashboard
from src import stock_dashboard_page_sections as page_sections


def _fake_render_inputs() -> SimpleNamespace:
    base = {
        "current_view": "overview",
        "candidate_index": 0,
        "candidate_cards": [{"ts_code": "000001.SZ"}],
        "view_labels": {"overview": "Overview"},
        "view_subtitle": "Overview subtitle",
        "health": {"score": "92"},
        "health_status": "ok",
        "health_tag": "ok",
        "headline_tone": "primary",
        "headline_detail": "detail",
        "current_basket_pointer_label": "pointer",
        "latest_basket_attempt_label": "attempt",
        "current_basket_pointer_status": "approved",
        "current_basket_pointer_basket_id": "basket-001",
        "latest_basket_attempt_status": "blocked",
        "latest_basket_attempt_blocking_reason": "sample",
        "top1": {"ts_code": "000001.SZ"},
        "top1_label": "000001.SZ",
        "top1_signal": "strong",
        "top1_risk": "low",
        "candidate_name": "Ping An",
        "candidate_artifact_status": {"generated_at": "2026-05-13"},
        "generation_mode_label": "chain",
        "update_status_label": "updated",
        "update_stage_label": "done",
        "candidate_count": 1,
        "candidate_score": "95",
        "candidate_timeline_label": "today",
        "run_freshness": "fresh",
        "report_state": "research",
        "db_latest_trade_date": "2026-05-13",
        "observation_timeline_label": "6/20",
        "prefilter_freshness_label": "fresh",
        "backtest_scope": {"label": "60d"},
        "governance_cycle_state": "observe",
        "governance_recommended_action_label": "observe",
        "governance_release_readiness": {"ready_for_release": False},
        "governance_fully_release_ready": False,
        "primary_conclusion": {"ts_code": "000001.SZ"},
        "decision_semantics": {},
        "market_snapshot": {"dominant_regime": "range", "risk_preference": "neutral", "avg_risk_pressure": "0.1"},
        "bt_diag": {},
        "blocker_semantics": {},
        "cockpit_model": {},
        "promotion_decision_label": "locked",
        "timeline_consistency_note": "aligned",
        "automation_health_label": "ok",
        "context": {"server_sync_preflight": {}},
        "primary_result_card_section": "",
        "primary_result": {"result_id": "primary-001"},
        "first_place_evidence_cockpit_section": "cockpit",
        "candidate_detail_html": "<div>detail</div>",
        "candidate_map_chart_html": "<div>map</div>",
        "candidate_chart_html": "<div>candidate-chart</div>",
        "candidates_csv": "ts_code\n000001.SZ\n",
        "basket_validation": {"summary": {}, "variants": {}},
        "candidate_basket_feedback": {},
        "evolution_status": {},
        "basket_summary": {},
        "prefilter_artifact_status": {},
        "effective_update_status": {},
        "automation_health": {},
        "update_health": {},
        "update_timeline_panel": "",
        "update_alerts_panel": "",
        "daily_research_runtime": {},
        "research_topology": {},
        "research_batch_status": {},
        "grid_backtest_status": {},
        "progress_pct_label": "0%",
        "execution_semantics": {},
        "governance_semantics": {},
        "evidence_semantics": {},
        "governance_cycle": {},
        "previous_stable_run_id": "run-prev",
        "governance_operator_message": "hold",
        "summary_lines": [],
        "stock_ai_explainer": {},
        "health_chart_html": "<div>health</div>",
        "backtest_equity_html": "<div>equity</div>",
        "backtest_drawdown_html": "<div>drawdown</div>",
        "backtest_chart_html": "<div>backtest</div>",
        "backtest_map_chart_html": "<div>backtest-map</div>",
        "daily_md_href": "/file/daily.md",
        "daily_md_download_href": "/download/daily.md",
        "health_csv_href": "/file/health.csv",
        "health_csv_download_href": "/download/health.csv",
        "leaderboard_href": "/file/leaderboard.csv",
        "leaderboard_download_href": "/download/leaderboard.csv",
        "candidates_md_href": "/file/candidates.md",
        "candidates_md_download_href": "/download/candidates.md",
        "candidates_csv_href": "/file/candidates.csv",
        "candidates_csv_download_href": "/download/candidates.csv",
        "latest_report_href": "/file/report.md",
        "latest_report_download_href": "/download/report.md",
        "current_report": "research",
        "daily_md_text": "# daily",
        "translated_daily_md_text": "# daily",
        "health_csv": "score\n92\n",
        "leaderboard_csv": "run\nr1\n",
        "latest_report_text": "# report",
        "candidate_source_label": "formal",
        "t12_sections": SimpleNamespace(overview_card_section="t12-overview", governance_summary_section="t12-gov"),
        "primary_result_bridge_context": {"initial_json_html": "{}"},
    }
    return SimpleNamespace(**base)


def test_compose_stock_dashboard_page_html_delegates_page_sections(monkeypatch):
    monkeypatch.setattr(
        page_sections,
        "build_stock_dashboard_shell_sections",
        lambda **kwargs: SimpleNamespace(
            home_view_model={"basket_dual_track_rows": []},
            primary_result_home_facts={},
            validation_basket_kpis=[],
            page_shell_context={"architecture_steps": []},
            external_system_summary_html="external-summary",
            overview_disclosure_view_model={},
            spotlight_html="spotlight",
            nav_html="nav",
            sidebar_status_html="sidebar",
            topbar_pills_html="topbar",
        ),
    )
    monkeypatch.setattr(page_sections, "stock_primary_result_card_html", lambda primary_result: "fallback-card")
    monkeypatch.setattr(page_sections, "stock_primary_result_bridge_shell_html", lambda primary_result: "bridge")
    monkeypatch.setattr(page_sections, "compose_primary_result_shell_html", lambda **kwargs: f"primary:{kwargs['primary_result_card_html']}")
    monkeypatch.setattr(page_sections, "build_section_visibility_context", lambda current_view: {})
    monkeypatch.setattr(page_sections, "build_stock_candidate_sections", lambda **kwargs: SimpleNamespace(candidate_compare_section="compare", candidate_focus_section="focus", actions_section="actions", candidate_visuals_section="visuals"))
    monkeypatch.setattr(page_sections, "render_market_snapshot_section", lambda market_snapshot: "market")
    monkeypatch.setattr(page_sections, "render_primary_result_home_brief_section", lambda facts: "brief")
    monkeypatch.setattr(page_sections, "render_research_visuals_section", lambda **kwargs: "research-visuals")
    monkeypatch.setattr(page_sections, "build_stock_diagnostics_sections", lambda **kwargs: SimpleNamespace(diagnostics_view_model={}, selection_funnel_section="funnel", diagnostics_appendix_section="appendix", validation_section="validation", prefilter_section="prefilter"))
    monkeypatch.setattr(page_sections, "build_stock_operations_section", lambda **kwargs: "operations")
    monkeypatch.setattr(page_sections, "build_stock_overview_sections", lambda **kwargs: SimpleNamespace(summary_section="summary", kpi_html="kpi", opportunity_cards_html="opportunities", overview_visuals_disclosure_section="visuals-disclosure", overview_operations_disclosure_section="ops-disclosure"))
    monkeypatch.setattr(page_sections, "build_stock_resource_sections", lambda **kwargs: SimpleNamespace(links_section="links", reports_section="reports", charts_section="charts", guide_section="guide"))
    monkeypatch.setattr(page_sections, "build_main_content_sections", lambda **kwargs: kwargs)
    monkeypatch.setattr(page_sections, "compose_main_content_html", lambda **kwargs: "main-content")
    monkeypatch.setattr(page_sections, "compose_stock_top_story_html", lambda **kwargs: "top-story")
    monkeypatch.setattr(page_sections, "build_dashboard_page_shell_contract", lambda **kwargs: kwargs)
    monkeypatch.setattr(page_sections, "compose_page_shell_html", lambda **kwargs: f"shell:{kwargs['main_content_html']}:{kwargs['top_story_html']}")
    monkeypatch.setattr(page_sections, "compose_primary_result_bridge_bootstrap_script", lambda context: "bootstrap")
    monkeypatch.setattr(page_sections, "compose_primary_result_bridge_client_script", lambda **kwargs: "bridge-client")
    monkeypatch.setattr(page_sections, "compose_table_export_script", lambda: "table-export")
    monkeypatch.setattr(page_sections, "build_page_interaction_context", lambda **kwargs: kwargs)
    monkeypatch.setattr(page_sections, "compose_page_interaction_script", lambda context: "interaction")
    monkeypatch.setattr(page_sections, "compose_inline_style_tag", lambda css: "<style>css</style>")
    monkeypatch.setattr(page_sections, "compose_dashboard_stylesheet", lambda **kwargs: "css")
    monkeypatch.setattr(page_sections, "compose_dashboard_script_tag", lambda *scripts: "<script>scripts</script>")
    monkeypatch.setattr(page_sections, "build_dashboard_asset_contract", lambda **kwargs: {"document_title": "Doc", **kwargs})

    html = page_sections.compose_stock_dashboard_page_html(
        render_inputs=_fake_render_inputs(),
        base_path="/stock",
        primary_result_core_compare_fields=("risk_level",),
        display_missing=lambda value, fallback: str(value or fallback),
        display_status_label=lambda value: str(value or "unknown"),
        is_t12_scope=lambda base_path: False,
        view_href=lambda view, idx, base_path: f"{base_path}/?view={view}&candidate={idx}",
    )

    assert "<title>Doc</title>" in html
    assert "shell:" in html
    assert "primary:fallback-card" in html
    assert "<script>scripts</script>" in html


def test_run_dashboard_stays_a_thin_launcher_boundary():
    source = Path(run_dashboard.__file__).read_text(encoding="utf-8")

    forbidden_imports = [
        "from src.stock_dashboard_sections import",
        "from src.stock_dashboard_page_composer import",
        "from src.stock_dashboard_render_context import",
        "from src.stock_dashboard_operations_section import",
        "from src.stock_dashboard_candidate_sections import",
        "from src.stock_dashboard_overview_sections import",
        "from src.stock_dashboard_diagnostics_sections import",
        "from src.stock_dashboard_sections_builder import",
    ]

    assert len(source.splitlines()) <= 350
    for forbidden in forbidden_imports:
        assert forbidden not in source
    assert "compose_stock_dashboard_page_html(" in source
