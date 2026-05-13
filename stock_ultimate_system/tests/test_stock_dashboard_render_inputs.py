from pathlib import Path
from types import SimpleNamespace

from src import stock_dashboard_render_inputs as render_inputs


def _domain_context(exp_dir: Path) -> dict[str, dict[str, object]]:
    return {
        "core": {
            "candidate_index": 3,
            "exp_dir": exp_dir,
            "health": {"score": 91},
            "health_status": "稳健",
            "health_tag": "ok",
            "bt_diag": {"结论": "继续观察"},
            "update_health": {"label": "ok"},
            "backtest_scope": {"label": "近60日"},
            "report_state": "ready",
            "run_freshness": "fresh",
            "headline_tone": "observe",
            "headline_detail": "证据闭合中",
        },
        "reports": {
            "daily_md_text": "daily",
            "translated_daily_md_text": "translated",
            "latest_report_text": "report",
            "daily_md_href": "/daily",
            "daily_md_download_href": "/daily?download=1",
            "health_csv": "score\n91\n",
            "health_csv_href": "/health",
            "health_csv_download_href": "/health?download=1",
            "leaderboard_csv": "run\nr1\n",
            "leaderboard_href": "/leaderboard",
            "leaderboard_download_href": "/leaderboard?download=1",
            "candidates_csv": "ts_code\n000001.SZ\n",
            "candidates_md_href": "/candidates.md",
            "candidates_md_download_href": "/candidates.md?download=1",
            "candidates_csv_href": "/candidates.csv",
            "candidates_csv_download_href": "/candidates.csv?download=1",
            "latest_report_href": "/report",
            "latest_report_download_href": "/report?download=1",
            "health_chart_html": "<div>health</div>",
            "backtest_equity_html": "<div>equity</div>",
            "backtest_drawdown_html": "<div>drawdown</div>",
            "backtest_chart_html": "<div>backtest</div>",
            "backtest_map_chart_html": "<div>map</div>",
            "candidate_chart_html": "<div>candidate</div>",
            "candidate_map_chart_html": "<div>candidate-map</div>",
            "candidate_detail_html": "<div>detail</div>",
        },
        "candidate": {
            "cards": [{"ts_code": "000001.SZ"}],
            "top1": {"ts_code": "000001.SZ"},
            "top1_signal": "strong_buy",
            "top1_risk": "low",
            "top1_label": "000001.SZ 平安银行",
            "summary_lines": ["只读观察"],
            "market_snapshot": {"dominant_regime": "震荡"},
            "basket_summary": {"expected_basket_return": 0.03},
            "basket_validation": {"summary": {}, "variants": {}},
            "candidate_basket_feedback": {"feedback_level": "review"},
            "candidate_artifact_status": {
                "generated_at": "2026-05-13",
                "basket_generated_at": "2026-05-13",
            },
            "prefilter_artifact_status": {"row_count": "12", "trade_date": "2026-05-13"},
            "candidate_score": "95",
            "candidate_name": "平安银行",
            "candidate_count": 12,
            "candidate_source_label": "正式候选",
            "generation_mode_label": "主链结果",
            "current_basket_pointer_status": "approved",
            "current_basket_pointer_updated_at": "2026-05-13T09:00:00",
            "current_basket_pointer_basket_id": "basket-001",
            "latest_basket_attempt_status": "blocked",
            "latest_basket_attempt_generated_at": "2026-05-13T10:00:00",
            "latest_basket_attempt_blocking_reason": "样本不足",
        },
        "governance": {
            "cycle": {"governance_inputs": {"governance_decision": "observe"}},
            "cycle_state": "observe_only",
            "recommended_action": "hold_observation",
            "operator_message": "继续观察",
            "release_readiness": {"ready_for_release": False},
            "fully_release_ready": False,
            "previous_stable_run_id": "run-prev",
        },
        "runtime": {
            "research_batch_status": {"status": "running"},
            "daily_research_runtime": {"state": "running"},
            "progress_pct_label": "70%",
            "effective_update_status": {"status": "up_to_date"},
            "automation_health": {"label": "ok"},
            "research_topology": {"nodes": []},
            "grid_backtest_status": {"state": "done"},
            "evolution_status": {"latest_capacity_gate_status": "yellow"},
            "update_timeline_panel": "<div>timeline</div>",
            "update_alerts_panel": "<div>alerts</div>",
        },
        "primary": {
            "primary_result": {"result_id": "primary:000001.SZ"},
            "first_place_evidence_cockpit_section": "<section>cockpit</section>",
            "primary_result_card_section": "<section>primary</section>",
            "stock_ai_explainer": {"visible": True, "summary_text": "AI explanation"},
            "primary_result_query": {"primary_conclusion": {"result_id": "primary:000001.SZ"}},
            "cockpit_model": {"next_check": "T+1"},
        },
        "t12": {
            "minimal_facts": {"scope": "t12"},
            "governance_source_facts": {"summary": "governance"},
        },
        "semantics": {
            "decision": {"headline": "primary"},
            "blocker": {"has_blocker": False},
            "execution": {"decision_action": "进入复核"},
            "evidence": {"score_gap": "12"},
            "governance": {"gate_overall_status": "通过"},
        },
    }


def test_build_stock_dashboard_render_inputs_maps_domain_runtime_and_bridge_contract(monkeypatch, tmp_path: Path):
    captured_runtime_kwargs: dict[str, object] = {}
    exp_dir = tmp_path / "data" / "experiments"
    exp_dir.mkdir(parents=True)

    monkeypatch.setattr(
        render_inputs,
        "build_dashboard_context",
        lambda root, candidate_index, base_path: {"root": root, "candidate_index": candidate_index, "base_path": base_path},
    )
    monkeypatch.setattr(
        render_inputs,
        "build_dashboard_domain_context",
        lambda context: _domain_context(exp_dir),
    )
    monkeypatch.setattr(render_inputs, "resolve_artifacts_path", lambda: tmp_path / "artifacts")
    monkeypatch.setattr(
        render_inputs,
        "load_dashboard_runtime_evidence",
        lambda artifacts_root, exp_dir: SimpleNamespace(
            observation_wait_status={"current_date": "2026-05-13"},
            daily_closure_latest={"status": "closed_success"},
        ),
    )

    def fake_build_stock_runtime_view_model(**kwargs):
        captured_runtime_kwargs.update(kwargs)
        return {
            "update_status_label": "已更新",
            "update_stage_label": "已完成",
            "prefilter_freshness_label": "预筛已闭合",
            "automation_health_label": "自动链路正常",
            "governance_recommended_action_label": "继续观察",
            "promotion_decision_label": "禁止发布",
            "db_latest_trade_date": "2026-05-13",
            "candidate_timeline_label": "候选 2026-05-13",
            "observation_timeline_label": "观察 6/20",
            "timeline_consistency_note": "数据库与候选时间一致",
            "current_basket_pointer_label": "当前生效篮子 approved",
            "latest_basket_attempt_label": "最新篮子尝试 blocked",
        }

    monkeypatch.setattr(render_inputs, "build_stock_runtime_view_model", fake_build_stock_runtime_view_model)
    monkeypatch.setattr(
        render_inputs,
        "build_t12_read_only_sections",
        lambda minimal_facts, governance_source_facts: SimpleNamespace(
            overview_card_section=f"t12:{minimal_facts['scope']}",
            governance_summary_section=f"governance:{governance_source_facts['summary']}",
        ),
    )
    monkeypatch.setattr(
        render_inputs,
        "build_stock_primary_result_bridge_context",
        lambda current_view, base_path, primary_result, bridge_enabled: {
            "initial_json_html": '{"ok":true}',
            "current_view": current_view,
            "base_path": base_path,
            "bridge_enabled": bridge_enabled,
            "result_id": primary_result["result_id"],
        },
    )

    inputs = render_inputs.build_stock_dashboard_render_inputs(
        root=tmp_path,
        current_view="unknown",
        candidate_index=99,
        current_report="not-a-report",
        base_path="/stock",
        report_labels={"research": "Research"},
        view_labels_builder=lambda base_path: {"overview": "Overview", "reports": "Reports"},
        view_subtitles_builder=lambda base_path: {"overview": "Overview subtitle", "reports": "Reports subtitle"},
        primary_result_bridge_enabled=True,
    )

    assert inputs.current_view == "overview"
    assert inputs.current_report == "research"
    assert inputs.candidate_index == 3
    assert inputs.view_subtitle == "Overview subtitle"
    assert inputs.primary_result["result_id"] == "primary:000001.SZ"
    assert inputs.primary_result_bridge_context["bridge_enabled"] is True
    assert inputs.primary_result_bridge_context["current_view"] == "overview"
    assert inputs.t12_sections.overview_card_section == "t12:t12"
    assert inputs.progress_pct_label == "70%"
    assert inputs.update_status_label == "已更新"
    assert inputs.governance_recommended_action_label == "继续观察"
    assert inputs.current_basket_pointer_label == "当前生效篮子 approved"
    assert captured_runtime_kwargs["observation_wait_status"] == {"current_date": "2026-05-13"}
    assert captured_runtime_kwargs["daily_closure_latest"] == {"status": "closed_success"}
    assert captured_runtime_kwargs["current_basket_pointer_basket_id"] == "basket-001"
