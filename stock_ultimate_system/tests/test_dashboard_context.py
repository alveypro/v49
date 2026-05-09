import json
from pathlib import Path

from src import dashboard_context
from src.current_result_pointer import CurrentResultPointerStore
from src.result_registry import ResultRegistry
from src.run_registry import RunRegistry
from src.utils import project_paths


def test_resolve_candidate_display_files_falls_back_to_interim_when_formal_is_empty(tmp_path):
    exp_dir = tmp_path / "data" / "experiments"
    exp_dir.mkdir(parents=True)
    (exp_dir / "candidates_top_latest.csv").write_text("", encoding="utf-8")
    (exp_dir / "candidates_top_latest.md").write_text("# formal\n", encoding="utf-8")
    (exp_dir / "candidates_top_interim_latest.csv").write_text(
        "ts_code,stock_name\n000001.SZ,平安银行\n",
        encoding="utf-8",
    )
    (exp_dir / "candidates_top_interim_latest.md").write_text("# interim\n", encoding="utf-8")

    csv_path, md_path, source_kind = dashboard_context._resolve_candidate_display_files(exp_dir)

    assert csv_path == exp_dir / "candidates_top_interim_latest.csv"
    assert md_path == exp_dir / "candidates_top_interim_latest.md"
    assert source_kind == "interim"


def test_build_dashboard_context_prefers_public_explanation_surface(tmp_path, monkeypatch):
    exp_dir = tmp_path / "data" / "experiments"
    rep_dir = tmp_path / "data" / "reports"
    exp_dir.mkdir(parents=True, exist_ok=True)
    rep_dir.mkdir(parents=True, exist_ok=True)
    (exp_dir / "daily_research_latest.md").write_text("- score: 90.00/100\n", encoding="utf-8")
    (exp_dir / "daily_health_trend_latest.csv").write_text("generated_at,score\n2026-04-05,90\n", encoding="utf-8")
    (exp_dir / "backtest_leaderboard.csv").write_text(
        "run_id,source_type,stock_pool,total_return,sharpe_ratio,max_drawdown,win_rate,total_trades\n",
        encoding="utf-8",
    )
    (exp_dir / "candidates_top_latest.csv").write_text(
        "ts_code,stock_name,industry,signal,risk_level,final_score\n000001.SZ,原始候选,银行,strong_buy,low,180\n",
        encoding="utf-8",
    )
    (exp_dir / "candidates_top_latest.md").write_text("# Candidates\n", encoding="utf-8")
    for name, content in {
        "candidates_basket_summary_latest.json": "{}",
        "candidates_basket_validation_latest.json": "{}",
        "candidates_audit_latest.json": '{"rows":[]}',
        "governance_cycle_latest.json": '{"cycle_state":"observe_only","recommended_action":"hold_observation","operator_message":"观察","release_readiness":{"ready_for_release":false}}',
        "t12_alert_center_latest.json": "{}",
        "t12_review_checklist_latest.json": "{}",
        "t12_threshold_control_latest.json": "{}",
        "t12_rollback_drill_latest.json": "{}",
    }.items():
        (exp_dir / name).write_text(content, encoding="utf-8")
    (exp_dir / "candidate_public_explanation_latest.json").write_text(
        json.dumps(
            {
                "schema_version": "candidate_public_explanation.v1",
                "status": "passed",
                "items": [
                    {
                        "ts_code": "600487.SH",
                        "stock_name": "亨通光电",
                        "risk_state": "degrade",
                        "external_display_allowed": False,
                        "why_watch": "进入观察名单的主要依据是产业链强度。",
                        "main_risk": "组合质量证明未通过。",
                        "invalid_when": "观察窗口无法补齐时失效。",
                        "next_observation": "看组合质量分是否恢复。",
                    }
                ],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    (exp_dir / "candidate_risk_state_latest.json").write_text(
        '{"schema_version":"candidate_risk_state.v1","status":"passed","state_counts":{"degrade":1}}',
        encoding="utf-8",
    )
    basket_dir = tmp_path / "artifacts" / "primary_result_candidate_baskets"
    basket_dir.mkdir(parents=True, exist_ok=True)
    (basket_dir / "feedback_latest.json").write_text("{}", encoding="utf-8")
    RunRegistry(runs_dir=tmp_path / "artifacts" / "run_registry").register(
        run_id="run-001",
        run_type="daily_research",
        producer="test",
        config_hash="cfg-001",
        data_snapshot_id="data-001",
        code_revision="rev-001",
        make_current=True,
    )
    ResultRegistry(results_dir=tmp_path / "artifacts" / "result_registry").register(
        record_id="result-record-001",
        result_id="primary:600487.SH",
        run_id="run-001",
        ts_code="600487.SH",
        stock_name="亨通光电",
        lifecycle_stage="L2",
        artifact_ids=["artifact:a"],
        make_current=True,
    )
    CurrentResultPointerStore(pointer_dir=tmp_path / "artifacts" / "current_result_pointer").point_to(
        pointer_snapshot_id="pointer-001",
        result_id="primary:600487.SH",
        run_id="run-001",
        lifecycle_id="lifecycle-001",
        artifact_ids=["artifact:a"],
        as_of_date="2026-05-06",
    )
    monkeypatch.setattr(
        dashboard_context,
        "resolve_experiments_path",
        lambda path=None: exp_dir if path in {None, ".", "", "data/experiments"} else exp_dir / Path(path),
    )
    monkeypatch.setattr(
        dashboard_context,
        "resolve_reports_path",
        lambda path=None: rep_dir if path in {None, ".", "", "data/reports"} else rep_dir / Path(path),
    )
    monkeypatch.setattr(
        dashboard_context,
        "resolve_artifacts_path",
        lambda path=None: (tmp_path / "artifacts") if path in {None, ".", "", "artifacts"} else (tmp_path / "artifacts" / Path(path)),
    )

    context = dashboard_context.build_dashboard_context(tmp_path, candidate_index=0, base_path="/stock")

    assert context["candidate_external_surface_mode"] == "public_explanation"
    assert context["candidate_source_label"] == "解释层观察状态"
    assert context["top1"]["ts_code"] == "600487.SH"
    assert context["top1"]["signal"] == "降级复核"
    assert "为什么关注" in context["candidate_detail_html"]
    assert "原始候选" not in context["candidate_detail_html"]


def test_build_dashboard_context_normalizes_candidate_index_and_links(tmp_path, monkeypatch):
    exp_dir = tmp_path / "data" / "experiments"
    rep_dir = tmp_path / "data" / "reports"
    exp_dir.mkdir(parents=True, exist_ok=True)
    rep_dir.mkdir(parents=True, exist_ok=True)

    (exp_dir / "daily_research_latest.md").write_text(
        "- score: 90.00/100\n- success_rate: 95.00%\n- failure_penalty: 1.0\n- category_penalty: 0.0\n",
        encoding="utf-8",
    )
    (exp_dir / "daily_health_trend_latest.csv").write_text(
        "generated_at,score\n2026-04-05 08:00:00,90\n",
        encoding="utf-8",
    )
    (exp_dir / "backtest_leaderboard.csv").write_text(
        "run_id,source_type,stock_pool,total_return,sharpe_ratio,max_drawdown,win_rate,total_trades\n"
        "r1,official_research,000001.SZ|600036.SH,0.05,1.0,-0.04,0.55,24\n",
        encoding="utf-8",
    )
    (exp_dir / "candidates_top_latest.csv").write_text(
        "ts_code,stock_name,industry,signal,risk_level,final_score,direction_prob_up,pred_return,confidence,position_pct,stop_loss,take_profit,reason\n"
        "000001.SZ,平安银行,银行,strong_buy,low,155,0.70,0.08,0.90,0.2,10.0,12.0,model_bullish factor_strong\n",
        encoding="utf-8",
    )
    (exp_dir / "candidates_top_latest.md").write_text("# Candidates\n", encoding="utf-8")
    (exp_dir / "candidates_basket_summary_latest.json").write_text("{}", encoding="utf-8")
    (exp_dir / "candidates_audit_latest.json").write_text(
        '{"rows":[{"ts_code":"000001.SZ","selection_reason":"top ranked after audit","diversification_penalty":0.1,"risk_overlay_penalty":0.2}]}',
        encoding="utf-8",
    )
    (exp_dir / "candidates_basket_validation_latest.json").write_text("{}", encoding="utf-8")
    (exp_dir / "governance_cycle_latest.json").write_text(
        '{"cycle_state":"observe_only","recommended_action":"hold_observation","operator_message":"当前仅满足观察条件，不满足正式发布条件。","release_readiness":{"ready_for_release":false,"ready_for_observation":true}}',
        encoding="utf-8",
    )
    (exp_dir / "t12_alert_center_latest.json").write_text('{"summary":{"overall_status":"pass"}}', encoding="utf-8")
    (exp_dir / "t12_review_checklist_latest.json").write_text('{"overall_status":"pass"}', encoding="utf-8")
    (exp_dir / "t12_threshold_control_latest.json").write_text(
        '{"applied": true, "recommendation": "tighten_thresholds"}',
        encoding="utf-8",
    )
    (exp_dir / "t12_rollback_drill_latest.json").write_text(
        '{"mode":"off","triggered":false,"would_rollback_apply":false}',
        encoding="utf-8",
    )
    (exp_dir / "t12_threshold_rollback_events.json").write_text('{"items":[]}', encoding="utf-8")
    (exp_dir / "t12_threshold_stable_snapshot.json").write_text(
        '{"thresholds":{"warn_hot_threshold":1}}',
        encoding="utf-8",
    )
    (rep_dir / "backtest_report_20260405_080000.md").write_text("# report\n", encoding="utf-8")
    basket_dir = tmp_path / "artifacts" / "primary_result_candidate_baskets"
    basket_dir.mkdir(parents=True, exist_ok=True)
    (basket_dir / "current.json").write_text(
        '{"status":"approved","basket_id":"basket-approved-001","updated_at":"2026-04-19T14:33:06+00:00"}',
        encoding="utf-8",
    )
    (basket_dir / "latest_attempt.json").write_text(
        '{"status":"blocked","basket_id":"basket-blocked-002","blocking_reasons":["top industry weight must stay within policy"]}',
        encoding="utf-8",
    )
    (basket_dir / "feedback_latest.json").write_text(
        '{"feedback_version":"primary_result_candidate_basket_feedback.v1","feedback_level":"review","summary_note":"recent basket observation requires review before trusting the current candidate profile","change_total":2,"window_label":"5D"}',
        encoding="utf-8",
    )
    RunRegistry(runs_dir=tmp_path / "artifacts" / "run_registry").register(
        run_id="run-001",
        run_type="daily_research",
        producer="test",
        config_hash="cfg-001",
        data_snapshot_id="data-001",
        code_revision="rev-001",
        make_current=True,
    )
    ResultRegistry(results_dir=tmp_path / "artifacts" / "result_registry").register(
        record_id="result-record-001",
        result_id="primary:000001.SZ",
        run_id="run-001",
        ts_code="000001.SZ",
        stock_name="平安银行",
        lifecycle_stage="L2",
        artifact_ids=["artifact:a"],
        make_current=True,
    )
    CurrentResultPointerStore(pointer_dir=tmp_path / "artifacts" / "current_result_pointer").point_to(
        pointer_snapshot_id="pointer-001",
        result_id="primary:000001.SZ",
        run_id="run-001",
        lifecycle_id="lifecycle-001",
        artifact_ids=["artifact:a"],
        as_of_date="2026-04-28",
    )

    monkeypatch.setattr(
        dashboard_context,
        "resolve_experiments_path",
        lambda path=None: exp_dir if path in {None, ".", "", "data/experiments"} else exp_dir / Path(path),
    )
    monkeypatch.setattr(
        dashboard_context,
        "resolve_reports_path",
        lambda path=None: rep_dir if path in {None, ".", "", "data/reports"} else rep_dir / Path(path),
    )
    monkeypatch.setattr(
        dashboard_context,
        "resolve_artifacts_path",
        lambda path=None: (tmp_path / "artifacts") if path in {None, ".", "", "artifacts"} else (tmp_path / "artifacts" / Path(path)),
    )

    context = dashboard_context.build_dashboard_context(tmp_path, candidate_index=99, base_path="/dash")

    assert context["candidate_index"] == 0
    assert context["top1"]["ts_code"] == "000001.SZ"
    assert context["primary_result"]["result_lifecycle_stage"] == "L2"
    assert 'class="stock-primary-result__conclusion"' in context["primary_result_card_html"]
    assert context["primary_result_runtime_metadata"]["stock_primary_result_runtime_mode"] == "canonical"
    assert context["t12_minimal_facts"]["threshold_applied"] is True
    assert context["governance_cycle_state"] == "observe_only"
    assert context["governance_recommended_action"] == "hold_observation"
    assert context["governance_release_readiness"]["ready_for_observation"] is True
    assert context["current_basket_pointer_status"] == "approved"
    assert context["current_basket_pointer_basket_id"] == "basket-approved-001"
    assert context["latest_basket_attempt_status"] == "blocked"
    assert context["candidate_basket_feedback"]["feedback_level"] == "review"
    assert "主结果 primary:000001.SZ" in context["headline_detail"]
    assert "候选篮指针 approved" in context["headline_detail"]
    assert "最新候选篮尝试于" in context["headline_detail"]
    assert context["primary_result_query"]["primary_conclusion"]["result_id"] == "primary:000001.SZ"
    assert context["namespace_home_semantics"]["decision"]["headline"] == "primary:000001.SZ 是当前唯一主结论对象"
    assert context["namespace_home_semantics"]["decision"]["top_candidate_code"] == "000001.SZ"
    assert context["namespace_home_semantics"]["blocker"]["blocker_source"] == "basket_attempt"
    assert context["namespace_home_semantics"]["execution"]["decision_action"] == "进入复核"
    assert context["namespace_home_semantics"]["governance"]["gate_overall_status"] == "通过"
    assert "top ranked after audit" in context["namespace_home_semantics"]["evidence"]["top_candidate_audit_summary"]
    assert context["daily_md_href"] == "/dash/file/data/experiments/daily_research_latest.md"
    assert context["latest_report_download_href"] == "/dash/download/data/reports/backtest_report_20260405_080000.md"
    assert context["health_status"] == "稳健"


def test_build_dashboard_context_keeps_primary_conclusion_on_pointer_when_top1_differs(tmp_path, monkeypatch):
    exp_dir = tmp_path / "data" / "experiments"
    rep_dir = tmp_path / "data" / "reports"
    exp_dir.mkdir(parents=True, exist_ok=True)
    rep_dir.mkdir(parents=True, exist_ok=True)
    (exp_dir / "daily_research_latest.md").write_text(
        "- score: 90.00/100\n- success_rate: 95.00%\n- failure_penalty: 1.0\n- category_penalty: 0.0\n",
        encoding="utf-8",
    )
    (exp_dir / "daily_health_trend_latest.csv").write_text("generated_at,score\n2026-04-05 08:00:00,90\n", encoding="utf-8")
    (exp_dir / "backtest_leaderboard.csv").write_text(
        "run_id,source_type,stock_pool,total_return,sharpe_ratio,max_drawdown,win_rate,total_trades\n"
        "r1,official_research,000002.SZ|600036.SH,0.05,1.0,-0.04,0.55,24\n",
        encoding="utf-8",
    )
    (exp_dir / "candidates_top_latest.csv").write_text(
        "ts_code,stock_name,industry,signal,risk_level,final_score,direction_prob_up,pred_return,confidence,position_pct,stop_loss,take_profit,reason\n"
        "000002.SZ,万科A,地产,strong_buy,low,165,0.70,0.08,0.90,0.2,10.0,12.0,model_bullish factor_strong\n"
        "000001.SZ,平安银行,银行,watch,medium,120,0.55,0.03,0.75,0.1,8.0,10.0,secondary candidate\n",
        encoding="utf-8",
    )
    (exp_dir / "candidates_top_latest.md").write_text("# Candidates\n", encoding="utf-8")
    (exp_dir / "candidates_basket_summary_latest.json").write_text("{}", encoding="utf-8")
    (exp_dir / "candidates_audit_latest.json").write_text('{"rows":[]}', encoding="utf-8")
    (exp_dir / "candidates_basket_validation_latest.json").write_text("{}", encoding="utf-8")
    (exp_dir / "governance_cycle_latest.json").write_text(
        '{"cycle_state":"observe_only","recommended_action":"hold_observation","operator_message":"当前仅满足观察条件，不满足正式发布条件。","release_readiness":{"ready_for_release":false,"ready_for_observation":true}}',
        encoding="utf-8",
    )
    (exp_dir / "daily_research_status_latest.json").write_text('{"state":"completed"}', encoding="utf-8")
    (exp_dir / "buylist_latest.json").write_text('{"items":[{"ts_code":"000001.SZ"}]}', encoding="utf-8")
    (exp_dir / "governance_audit_latest.json").write_text("{}", encoding="utf-8")
    (exp_dir / "t1_execution_checklist_latest.json").write_text("{}", encoding="utf-8")
    (exp_dir / "t12_rollback_drill_latest.json").write_text("{}", encoding="utf-8")
    (rep_dir / "backtest_report_20260405_080000.md").write_text("# report\n", encoding="utf-8")
    (tmp_path / "artifacts" / "primary_result_candidate_baskets").mkdir(parents=True, exist_ok=True)
    RunRegistry(runs_dir=tmp_path / "artifacts" / "run_registry").register(
        run_id="run-001",
        run_type="daily_research",
        producer="test",
        config_hash="cfg-001",
        data_snapshot_id="data-001",
        code_revision="rev-001",
        make_current=True,
    )
    ResultRegistry(results_dir=tmp_path / "artifacts" / "result_registry").register(
        record_id="result-record-001",
        result_id="primary:000001.SZ",
        run_id="run-001",
        ts_code="000001.SZ",
        stock_name="平安银行",
        lifecycle_stage="L2",
        artifact_ids=["artifact:a"],
        make_current=True,
    )
    CurrentResultPointerStore(pointer_dir=tmp_path / "artifacts" / "current_result_pointer").point_to(
        pointer_snapshot_id="pointer-001",
        result_id="primary:000001.SZ",
        run_id="run-001",
        lifecycle_id="lifecycle-001",
        artifact_ids=["artifact:a"],
        as_of_date="2026-04-28",
    )
    monkeypatch.setattr(
        dashboard_context,
        "resolve_experiments_path",
        lambda path=None: exp_dir if path in {None, ".", "", "data/experiments"} else exp_dir / Path(path),
    )
    monkeypatch.setattr(
        dashboard_context,
        "resolve_reports_path",
        lambda path=None: rep_dir if path in {None, ".", "", "data/reports"} else rep_dir / Path(path),
    )
    monkeypatch.setattr(
        dashboard_context,
        "resolve_artifacts_path",
        lambda path=None: (tmp_path / "artifacts") if path in {None, ".", "", "artifacts"} else (tmp_path / "artifacts" / Path(path)),
    )

    context = dashboard_context.build_dashboard_context(tmp_path, candidate_index=0, base_path="/stock")

    assert context["top1"]["ts_code"] == "000002.SZ"
    assert context["primary_result"]["ts_code"] == "000001.SZ"
    assert context["primary_result_query"]["primary_conclusion"]["ts_code"] == "000001.SZ"
    assert "主结果 primary:000001.SZ" in context["headline_detail"]
    assert context["namespace_home_semantics"]["decision"]["top_candidate_code"] == "000001.SZ"


def test_build_dashboard_context_uses_canonical_artifacts_dir_from_env(tmp_path, monkeypatch):
    local_root = tmp_path / "stock-root"
    local_root.mkdir(parents=True, exist_ok=True)
    exp_dir = tmp_path / "canonical" / "data" / "experiments"
    rep_dir = tmp_path / "canonical" / "data" / "reports"
    artifacts_dir = tmp_path / "canonical" / "artifacts"
    exp_dir.mkdir(parents=True, exist_ok=True)
    rep_dir.mkdir(parents=True, exist_ok=True)

    (exp_dir / "daily_research_latest.md").write_text(
        "- score: 90.00/100\n- success_rate: 95.00%\n- failure_penalty: 1.0\n- category_penalty: 0.0\n",
        encoding="utf-8",
    )
    (exp_dir / "daily_health_trend_latest.csv").write_text("generated_at,score\n2026-04-05 08:00:00,90\n", encoding="utf-8")
    (exp_dir / "backtest_leaderboard.csv").write_text(
        "run_id,source_type,stock_pool,total_return,sharpe_ratio,max_drawdown,win_rate,total_trades\n"
        "r1,official_research,000001.SZ|600036.SH,0.05,1.0,-0.04,0.55,24\n",
        encoding="utf-8",
    )
    (exp_dir / "candidates_top_latest.csv").write_text(
        "ts_code,stock_name,industry,signal,risk_level,final_score,direction_prob_up,pred_return,confidence,position_pct,stop_loss,take_profit,reason\n"
        "000001.SZ,平安银行,银行,strong_buy,low,155,0.70,0.08,0.90,0.2,10.0,12.0,model_bullish factor_strong\n",
        encoding="utf-8",
    )
    (exp_dir / "candidates_top_latest.md").write_text("# Candidates\n", encoding="utf-8")
    (exp_dir / "candidates_basket_summary_latest.json").write_text("{}", encoding="utf-8")
    (exp_dir / "candidates_audit_latest.json").write_text('{"rows":[]}', encoding="utf-8")
    (exp_dir / "candidates_basket_validation_latest.json").write_text("{}", encoding="utf-8")
    (exp_dir / "daily_research_status_latest.json").write_text('{"state":"completed"}', encoding="utf-8")
    (exp_dir / "buylist_latest.json").write_text('{"items":[{"ts_code":"000001.SZ"}]}', encoding="utf-8")
    (exp_dir / "governance_cycle_latest.json").write_text(
        '{"cycle_state":"observe_only","recommended_action":"hold_observation","operator_message":"当前仅满足观察条件，不满足正式发布条件。","release_readiness":{"ready_for_release":false,"ready_for_observation":true}}',
        encoding="utf-8",
    )
    (exp_dir / "governance_audit_latest.json").write_text("{}", encoding="utf-8")
    (exp_dir / "t1_execution_checklist_latest.json").write_text("{}", encoding="utf-8")
    (exp_dir / "t12_rollback_drill_latest.json").write_text("{}", encoding="utf-8")
    (rep_dir / "backtest_report_20260405_080000.md").write_text("# report\n", encoding="utf-8")

    basket_dir = artifacts_dir / "primary_result_candidate_baskets"
    basket_dir.mkdir(parents=True, exist_ok=True)
    (basket_dir / "current.json").write_text(
        '{"status":"approved","basket_id":"basket-approved-001","updated_at":"2026-04-19T14:33:06+00:00"}',
        encoding="utf-8",
    )
    (basket_dir / "latest_attempt.json").write_text(
        '{"status":"blocked","basket_id":"basket-blocked-002","blocking_reasons":["top industry weight must stay within policy"]}',
        encoding="utf-8",
    )
    (basket_dir / "feedback_latest.json").write_text("{}", encoding="utf-8")
    RunRegistry(runs_dir=artifacts_dir / "run_registry").register(
        run_id="run-001",
        run_type="daily_research",
        producer="test",
        config_hash="cfg-001",
        data_snapshot_id="data-001",
        code_revision="rev-001",
        make_current=True,
    )
    ResultRegistry(results_dir=artifacts_dir / "result_registry").register(
        record_id="result-record-001",
        result_id="primary:000001.SZ",
        run_id="run-001",
        ts_code="000001.SZ",
        stock_name="平安银行",
        lifecycle_stage="L2",
        artifact_ids=["artifact:a"],
        make_current=True,
    )
    CurrentResultPointerStore(pointer_dir=artifacts_dir / "current_result_pointer").point_to(
        pointer_snapshot_id="pointer-001",
        result_id="primary:000001.SZ",
        run_id="run-001",
        lifecycle_id="lifecycle-001",
        artifact_ids=["artifact:a"],
        as_of_date="2026-04-28",
    )
    (artifacts_dir / "primary_result_observation_wait_status_latest.json").write_text(
        '{"status":"pending_window","generated_at":"2026-04-18T03:32:19+00:00","result_id":"primary:000001.SZ","ts_code":"000001.SZ","stock_name":"平安银行","current_date":"2026-04-18","observation_window":{"started_at":"2026-04-20T01:30:00Z","has_started":false},"blocking_reasons":["current date must be on or after observation window start before closure checks"]}',
        encoding="utf-8",
    )
    (artifacts_dir / "primary_result_performance_evidence_latest.json").write_text(
        '{"generated_at":"2026-04-18T03:09:10+00:00","streams":[{"stream_id":"primary_result","entry_total":3,"windows":[{"blocking_reasons":["requires at least 20 ledger entries"]}]},{"stream_id":"candidate_basket","entry_total":5,"windows":[{"blocking_reasons":["requires at least 20 ledger entries"]}]}]}',
        encoding="utf-8",
    )
    (artifacts_dir / "primary_result_promotion_readiness_gate_latest.json").write_text(
        '{"generated_at":"2026-04-18T03:09:10+00:00","decision":"blocked","blocking_reasons":["performance evidence must be ready before promotion review"]}',
        encoding="utf-8",
    )
    (artifacts_dir / "primary_result_daily_operations_scoreboard_latest.json").write_text(
        '{"generated_at":"2026-04-18T03:09:10+00:00","overall_status":"yellow","score":79}',
        encoding="utf-8",
    )
    (exp_dir / "primary_result_daily_closure_latest.json").write_text(
        '{"status":"pending_window","generated_at":"2026-04-26T23:28:32+08:00","window_start":"2026-04-27T01:30:00Z","window_end":"2026-04-26"}',
        encoding="utf-8",
    )

    monkeypatch.setenv(project_paths.EXPERIMENTS_DIR_ENV, str(exp_dir))
    monkeypatch.setenv(project_paths.REPORTS_DIR_ENV, str(rep_dir))
    monkeypatch.setenv(project_paths.ARTIFACTS_DIR_ENV, str(artifacts_dir))

    context = dashboard_context.build_dashboard_context(local_root, candidate_index=0, base_path="/stock")

    assert context["first_place_evidence_cockpit"]["primary_progress"] == "3/20"
    assert context["first_place_evidence_cockpit"]["basket_progress"] == "5/20"
    assert context["first_place_evidence_cockpit"]["primary_needed"] == 17
    assert context["first_place_evidence_cockpit"]["basket_needed"] == 15


def test_build_dashboard_context_builds_stock_ai_explainer_only_for_stock_scope(tmp_path, monkeypatch):
    exp_dir = tmp_path / "data" / "experiments"
    rep_dir = tmp_path / "data" / "reports"
    exp_dir.mkdir(parents=True, exist_ok=True)
    rep_dir.mkdir(parents=True, exist_ok=True)
    (exp_dir / "daily_research_latest.md").write_text(
        "- score: 90.00/100\n- success_rate: 95.00%\n- failure_penalty: 1.0\n- category_penalty: 0.0\n",
        encoding="utf-8",
    )
    (exp_dir / "daily_health_trend_latest.csv").write_text("generated_at,score\n2026-04-05 08:00:00,90\n", encoding="utf-8")
    (exp_dir / "backtest_leaderboard.csv").write_text(
        "run_id,source_type,stock_pool,total_return,sharpe_ratio,max_drawdown,win_rate,total_trades\n"
        "r1,official_research,000001.SZ|600036.SH,0.05,1.0,-0.04,0.55,24\n",
        encoding="utf-8",
    )
    (exp_dir / "candidates_top_latest.csv").write_text(
        "ts_code,stock_name,industry,signal,risk_level,final_score,direction_prob_up,pred_return,confidence,position_pct,stop_loss,take_profit,reason\n"
        "000001.SZ,平安银行,银行,strong_buy,low,155,0.70,0.08,0.90,0.2,10.0,12.0,model_bullish factor_strong\n",
        encoding="utf-8",
    )
    (exp_dir / "candidates_top_latest.md").write_text("# Candidates\n", encoding="utf-8")
    (exp_dir / "candidates_basket_summary_latest.json").write_text("{}", encoding="utf-8")
    (exp_dir / "candidates_audit_latest.json").write_text('{"rows":[]}', encoding="utf-8")
    (exp_dir / "candidates_basket_validation_latest.json").write_text("{}", encoding="utf-8")
    (exp_dir / "governance_cycle_latest.json").write_text(
        '{"cycle_state":"observe_only","recommended_action":"hold_observation","operator_message":"当前仅满足观察条件，不满足正式发布条件。","release_readiness":{"ready_for_release":false,"ready_for_observation":true}}',
        encoding="utf-8",
    )
    (rep_dir / "backtest_report_20260405_080000.md").write_text("# report\n", encoding="utf-8")
    (tmp_path / "artifacts" / "primary_result_candidate_baskets").mkdir(parents=True, exist_ok=True)
    RunRegistry(runs_dir=tmp_path / "artifacts" / "run_registry").register(
        run_id="run-001",
        run_type="daily_research",
        producer="test",
        config_hash="cfg-001",
        data_snapshot_id="data-001",
        code_revision="rev-001",
        make_current=True,
    )
    ResultRegistry(results_dir=tmp_path / "artifacts" / "result_registry").register(
        record_id="result-record-001",
        result_id="primary:000001.SZ",
        run_id="run-001",
        ts_code="000001.SZ",
        stock_name="平安银行",
        lifecycle_stage="L2",
        artifact_ids=["artifact:a"],
        make_current=True,
    )
    CurrentResultPointerStore(pointer_dir=tmp_path / "artifacts" / "current_result_pointer").point_to(
        pointer_snapshot_id="pointer-001",
        result_id="primary:000001.SZ",
        run_id="run-001",
        lifecycle_id="lifecycle-001",
        artifact_ids=["artifact:a"],
        as_of_date="2026-04-28",
    )
    monkeypatch.setattr(
        dashboard_context,
        "resolve_experiments_path",
        lambda path=None: exp_dir if path in {None, ".", "", "data/experiments"} else exp_dir / Path(path),
    )
    monkeypatch.setattr(
        dashboard_context,
        "resolve_reports_path",
        lambda path=None: rep_dir if path in {None, ".", "", "data/reports"} else rep_dir / Path(path),
    )
    monkeypatch.setattr(
        dashboard_context,
        "resolve_artifacts_path",
        lambda path=None: (tmp_path / "artifacts") if path in {None, ".", "", "artifacts"} else (tmp_path / "artifacts" / Path(path)),
    )
    monkeypatch.setenv("STOCK_AI_F0_GLOBAL", "1")
    monkeypatch.setenv("STOCK_AI_F1_STOCK_EXPLAINER", "1")
    monkeypatch.setenv("STOCK_AI_F2_A1_EXPLAINER", "1")

    stock_context = dashboard_context.build_dashboard_context(tmp_path, candidate_index=0, base_path="/stock")
    t12_context = dashboard_context.build_dashboard_context(tmp_path, candidate_index=0, base_path="/t12")

    assert stock_context["stock_ai_explainer"]["visible"] is True
    assert "唯一 pointer 主结果为准" in str(stock_context["stock_ai_explainer"]["summary_text"])
    assert "provider_storage" in stock_context["stock_ai_explainer"]
    assert t12_context["stock_ai_explainer"]["visible"] is False
    assert t12_context["stock_ai_explainer"]["status"] == "disabled"
