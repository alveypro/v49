import json
from pathlib import Path

import pytest

import run_dashboard as dashboard_module
from run_dashboard import (
    STOCK_AI_RUNNER_API_PATH,
    STOCK_AI_RUNNER_OPS_PATH,
    STOCK_AI_RUNNER_RESULT_REPLAY_OPS_PATH,
    VIEW_LABELS,
    _build_stock_ai_runner_api_body,
    _build_primary_result_api_body,
    _is_main_site_scope,
    _resolve_namespace_id,
    _resolve_scope_id,
    _is_t12_scope,
    _is_valid_primary_result_payload,
    _merge_primary_result_for_card,
    _render_dashboard,
    _render_stock_ai_runner_ops_page,
    _render_stock_ai_runner_result_replay_page,
    _view_labels,
    _view_subtitles,
    DashboardHandler,
)
from src import dashboard_context
from src.artifact_registry import ArtifactRegistry, sha256_file
from src.current_result_pointer import CurrentResultPointerStore
from src.result_registry import ResultRegistry
from src.run_registry import RunRegistry


def _compat_dashboard_experiments_root() -> Path:
    return Path(dashboard_context.resolve_project_path("data/experiments"))


def _compat_dashboard_reports_root() -> Path:
    return Path(dashboard_context.resolve_project_path("data/reports"))


def _compat_dashboard_artifacts_root() -> Path:
    try:
        return Path(dashboard_context.resolve_project_path("artifacts"))
    except Exception:
        return _compat_dashboard_experiments_root().parents[1] / "artifacts"


@pytest.fixture(autouse=True)
def _apply_path_resolution_compatibility(monkeypatch):
    monkeypatch.setattr(dashboard_module, "PRIMARY_RESULT_BRIDGE_ENABLED", True, raising=False)
    monkeypatch.setattr(dashboard_context, "resolve_project_path", lambda rel=".": Path(rel), raising=False)
    monkeypatch.setattr(
        dashboard_context,
        "resolve_experiments_path",
        lambda path=None: _compat_dashboard_experiments_root()
        if path in {None, ".", "", "data/experiments"}
        else _compat_dashboard_experiments_root() / Path(path),
        raising=False,
    )
    monkeypatch.setattr(
        dashboard_context,
        "resolve_reports_path",
        lambda path=None: _compat_dashboard_reports_root()
        if path in {None, ".", "", "data/reports"}
        else _compat_dashboard_reports_root() / Path(path),
        raising=False,
    )
    monkeypatch.setattr(
        dashboard_context,
        "resolve_artifacts_path",
        lambda path=None: _compat_dashboard_artifacts_root()
        if path in {None, ".", "", "artifacts"}
        else _compat_dashboard_artifacts_root() / Path(path),
        raising=False,
    )
    monkeypatch.setattr(
        dashboard_module,
        "resolve_experiments_path",
        lambda path=None: dashboard_module.resolve_project_path("data/experiments")
        if path in {None, ".", "", "data/experiments"}
        else dashboard_module.resolve_project_path("data/experiments") / Path(path),
        raising=False,
    )
    monkeypatch.setattr(
        dashboard_module,
        "resolve_artifacts_path",
        lambda path=None: dashboard_module.resolve_project_path(".") / "artifacts"
        if path in {None, ".", "", "artifacts"}
        else (dashboard_module.resolve_project_path(".") / "artifacts" / Path(path)),
        raising=False,
    )


def _write_current_primary_pointer(
    tmp_path: Path,
    *,
    ts_code: str = "000001.SZ",
    stock_name: str = "平安银行",
    lifecycle_stage: str = "L4",
    source_scope: str = "stock",
) -> None:
    artifacts_dir = tmp_path / "artifacts"
    exp_dir = tmp_path / "data" / "experiments"
    exp_dir.mkdir(parents=True, exist_ok=True)
    run_id = "run-001"
    lifecycle_id = "lifecycle-001"
    started_at = "2026-04-28T09:20:00Z"
    completed_at = "2026-04-28T09:30:00Z"
    code_revision = "rev-001"
    step_ids = {
        "audit": "artifact:audit",
        "execution": "artifact:execution",
        "rollback": "artifact:rollback",
        "observation": "artifact:observation",
    }
    lifecycle_evidence_id = "artifact:lifecycle-evidence"

    RunRegistry(runs_dir=artifacts_dir / "run_registry").register(
        run_id=run_id,
        run_type="primary_result_lifecycle",
        producer="scripts.run_primary_result_lifecycle",
        config_hash="cfg-001",
        data_snapshot_id="data-001",
        code_revision=code_revision,
        created_at=started_at,
        make_current=True,
        metadata={
            "lifecycle_id": lifecycle_id,
            "result_id": f"primary:{ts_code}",
            "ts_code": ts_code,
            "source_scope": source_scope,
            "evidence_path": str(exp_dir / "primary_result_lifecycle_evidence_latest.json"),
            "artifact_registry_path": str(artifacts_dir / "artifact_registry.jsonl"),
            "producer": "scripts.run_primary_result_lifecycle",
            "code_revision": code_revision,
        },
    )

    step_specs = [
        ("audit", exp_dir / "primary_result_audit_latest.json", "audit_status", "passed"),
        ("execution", exp_dir / "primary_result_execution_latest.json", "execution_status", "ready"),
        ("rollback", exp_dir / "primary_result_rollback_latest.json", "rollback_status", "not_required"),
        ("observation", exp_dir / "primary_result_observation_latest.json", "observation_status", "observing"),
    ]
    registry = ArtifactRegistry(artifacts_dir / "artifact_registry.jsonl")
    steps = []
    for step_name, path, status_key, status_value in step_specs:
        path.write_text(
            json.dumps(
                {
                    "result_id": f"primary:{ts_code}",
                    "run_id": run_id,
                    "lifecycle_id": lifecycle_id,
                    "ts_code": ts_code,
                    "stock_name": stock_name,
                    status_key: status_value,
                    "generated_at": "2026-04-28T09:30:00Z",
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        registry.register_artifact(
            artifact_type=f"primary_result_{step_name}",
            run_id=run_id,
            path=path,
            producer="scripts.run_primary_result_lifecycle",
            code_revision=code_revision,
            artifact_id=step_ids[step_name],
            result_id=f"primary:{ts_code}",
            metadata={
                "lifecycle_id": lifecycle_id,
                "step": step_name,
                "status": status_value,
                "ts_code": ts_code,
            },
        )
        steps.append(
            {
                "step": step_name,
                "path": str(path),
                "exists": True,
                "sha256": sha256_file(path),
                "result_id": f"primary:{ts_code}",
                "ts_code": ts_code,
                "generated_at": "2026-04-28T09:30:00Z",
                "exit_code": 0,
            }
        )

    artifact_ids = [
        step_ids["audit"],
        step_ids["execution"],
        step_ids["rollback"],
        step_ids["observation"],
        lifecycle_evidence_id,
    ]
    ResultRegistry(results_dir=artifacts_dir / "result_registry").register(
        record_id="result-record-001",
        result_id=f"primary:{ts_code}",
        run_id=run_id,
        ts_code=ts_code,
        stock_name=stock_name,
        lifecycle_stage=lifecycle_stage,
        artifact_ids=artifact_ids,
        source_scope=source_scope,
        registered_at=completed_at,
        make_current=True,
        metadata={
            "lifecycle_id": lifecycle_id,
            "evidence_path": str(exp_dir / "primary_result_lifecycle_evidence_latest.json"),
            "lifecycle_status": "passed",
            "artifact_registry_path": str(artifacts_dir / "artifact_registry.jsonl"),
            "source_scope": source_scope,
            "producer": "scripts.run_primary_result_lifecycle",
            "code_revision": code_revision,
        },
    )
    CurrentResultPointerStore(pointer_dir=artifacts_dir / "current_result_pointer").point_to(
        pointer_snapshot_id="pointer-001",
        result_id=f"primary:{ts_code}",
        run_id=run_id,
        lifecycle_id=lifecycle_id,
        artifact_ids=artifact_ids,
        as_of_date="2026-04-28",
        source_scope=source_scope,
        updated_at=completed_at,
        metadata={
            "result_registry_record_id": "result-record-001",
            "evidence_path": str(exp_dir / "primary_result_lifecycle_evidence_latest.json"),
            "artifact_registry_path": str(artifacts_dir / "artifact_registry.jsonl"),
            "source_scope": source_scope,
            "producer": "scripts.run_primary_result_lifecycle",
            "code_revision": code_revision,
        },
    )
    lifecycle_evidence_path = exp_dir / "primary_result_lifecycle_evidence_latest.json"
    lifecycle_evidence_path.write_text(
        json.dumps(
            {
                "lifecycle_version": "primary_result_lifecycle.v1",
                "status": "passed",
                "started_at": started_at,
                "completed_at": completed_at,
                "result_id": f"primary:{ts_code}",
                "ts_code": ts_code,
                "stock_name": stock_name,
                "run_id": run_id,
                "lifecycle_id": lifecycle_id,
                "final_payload": {
                    "result_id": f"primary:{ts_code}",
                    "run_id": run_id,
                    "lifecycle_id": lifecycle_id,
                    "artifact_ids": artifact_ids,
                    "as_of_date": "2026-04-28",
                    "source_scope": source_scope,
                    "ts_code": ts_code,
                    "stock_name": stock_name,
                    "result_lifecycle_stage": lifecycle_stage,
                    "audit_status": "passed",
                    "execution_status": "ready",
                    "rollback_status": "not_required",
                    "observation_status": "observing",
                },
                "steps": steps,
                "registry_chain": {
                    "write_mode": "run_result_pointer_updated",
                    "result_registry": {
                        "record_id": "result-record-001",
                        "lifecycle_stage": lifecycle_stage,
                        "source_scope": source_scope,
                        "producer": "scripts.run_primary_result_lifecycle",
                        "code_revision": code_revision,
                    },
                    "current_result_pointer": {
                        "pointer_snapshot_id": "pointer-001",
                        "result_id": f"primary:{ts_code}",
                        "run_id": run_id,
                        "lifecycle_id": lifecycle_id,
                        "artifact_ids": artifact_ids,
                        "as_of_date": "2026-04-28",
                        "source_scope": source_scope,
                        "producer": "scripts.run_primary_result_lifecycle",
                        "code_revision": code_revision,
                    },
                },
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    registry.register_artifact(
        artifact_type="primary_result_lifecycle_evidence",
        run_id=run_id,
        path=lifecycle_evidence_path,
        producer="scripts.run_primary_result_lifecycle",
        code_revision=code_revision,
        artifact_id=lifecycle_evidence_id,
        result_id=f"primary:{ts_code}",
        parent_artifact_ids=artifact_ids[:-1],
        metadata={
            "lifecycle_id": lifecycle_id,
            "ts_code": ts_code,
            "status": "passed",
        },
    )


def test_primary_result_api_body_and_dashboard_card_coexist(tmp_path, monkeypatch):
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
    (exp_dir / "candidates_basket_validation_latest.json").write_text("{}", encoding="utf-8")
    (exp_dir / "governance_cycle_latest.json").write_text(
        '{"cycle_state":"release_ready","recommended_action":"run_release_pipeline","operator_message":"治理门禁通过，可进入正式 release pipeline。","governance_inputs":{"governance_decision":"promote_to_staging","governance_audit_status":"pass"},"release_readiness":{"ready_for_release":true,"ready_for_observation":false}}',
        encoding="utf-8",
    )
    (exp_dir / "daily_research_status_latest.json").write_text('{"state":"completed"}', encoding="utf-8")
    (exp_dir / "buylist_latest.json").write_text('{"items":[{"ts_code":"000001.SZ"}]}', encoding="utf-8")
    (exp_dir / "governance_audit_latest.json").write_text('{"summary":{"overall_status":"pass"}}', encoding="utf-8")
    (exp_dir / "t1_execution_checklist_latest.json").write_text('{"summary":{"overall_status":"pass"}}', encoding="utf-8")
    (exp_dir / "t12_rollback_drill_latest.json").write_text('{"triggered": false}', encoding="utf-8")
    (rep_dir / "backtest_report_20260405_080000.md").write_text("# report\n", encoding="utf-8")
    _write_current_primary_pointer(tmp_path)

    monkeypatch.setattr(
        dashboard_context,
        "resolve_project_path",
        lambda rel: {"data/experiments": exp_dir, "data/reports": rep_dir}[rel],
    )

    import run_dashboard as dashboard_module

    monkeypatch.setattr(
        dashboard_module,
        "resolve_project_path",
        lambda rel: {"data/experiments": exp_dir, "data/reports": rep_dir, ".": tmp_path}[rel],
    )

    payload = json.loads(_build_primary_result_api_body(tmp_path).decode("utf-8"))
    assert payload["schema_version"] == "primary_result_v1"
    assert payload["result_id"] == "primary:000001.SZ"
    assert payload["ts_code"] == "000001.SZ"
    assert payload["stock_name"] == "平安银行"
    assert payload["result_lifecycle_stage"] == "L4"
    assert payload["source_scope"] == "stock"
    assert payload["audit_status"] == "passed"
    assert payload["terminal_outcome"] is None
    assert payload["history_source_file"] == "primary_result_observation_latest.json"
    assert payload["history_generation_mode"] == "chain_verified"

    html_text = _render_dashboard(tmp_path, current_view="overview", candidate_index=0, current_report="research", base_path="/stock")
    assert "统一结果对象" in html_text
    assert "当前判断" in html_text
    assert "primary:000001.SZ" in html_text
    assert "唯一主结果对象" in html_text or "唯一业务结论入口" in html_text
    assert 'id="primary-result-initial-json"' in html_text
    assert "PRIMARY_RESULT_BRIDGE_ENABLED = true" in html_text
    assert 'id="primary-result-history"' in html_text
    assert 'id="primary-result-history-slot-a"' in html_text
    assert 'id="primary-result-history-slot-b"' in html_text
    assert 'id="primary-result-history-slot-c"' in html_text
    assert 'id="primary-result-disabled"' in html_text
    assert 'id="primary-result-invalid"' in html_text
    assert "研究闭环" in html_text
    assert "治理主链" in html_text
    assert "release_ready" in html_text
    assert "run_release_pipeline" in html_text
    assert "治理门禁通过，可进入正式 release pipeline。" in html_text
    assert "#primary-result-card .primary-result-callout-warning" in html_text
    assert "#primary-result-card .primary-result-sync-chip" in html_text


def test_primary_result_payload_schema_mismatch_must_fallback():
    server_fact = {
        "result_lifecycle_stage": "L2",
        "result_type": "candidate",
        "risk_level": "low",
    }
    bad_payload = {
        "schema_version": "primary_result_v0",
        "result_lifecycle_stage": "L3",
    }
    assert _is_valid_primary_result_payload(bad_payload) is False
    assert _merge_primary_result_for_card(server_fact, server_fact) == server_fact


def test_primary_result_payload_missing_stage_must_fallback():
    payload = {
        "schema_version": "primary_result_v1",
        "result_lifecycle_stage": "",
    }
    assert _is_valid_primary_result_payload(payload) is False


def test_primary_result_payload_with_many_nulls_allows_partial_update():
    server_fact = {
        "ts_code": "000001.SZ",
        "stock_name": "平安银行",
        "result_lifecycle_stage": "L2",
        "result_type": "candidate",
        "risk_level": "low",
        "audit_status": None,
        "terminal_outcome": None,
        "data_sync_note": "server note",
        "source_timestamps": {"x": "2026-04-12 09:00:00"},
    }
    payload = {
        "schema_version": "primary_result_v1",
        "result_lifecycle_stage": "L2",
        "result_type": "candidate",
        "risk_level": None,
        "audit_status": None,
        "terminal_outcome": None,
        "data_sync_note": "api note",
        "source_timestamps": {"x": "2026-04-12 09:01:00"},
    }
    assert _is_valid_primary_result_payload(payload) is True
    merged = _merge_primary_result_for_card(server_fact, payload)
    assert merged["result_lifecycle_stage"] == "L2"
    assert merged["risk_level"] is None
    assert merged["data_sync_note"] == "api note"


def test_primary_result_api_payload_carries_explanation_facts(tmp_path):
    exp_dir = tmp_path / "data" / "experiments"
    exp_dir.mkdir(parents=True, exist_ok=True)
    (exp_dir / "candidates_top_latest.csv").write_text(
        "ts_code,stock_name,industry,signal,risk_level,final_score\n"
        "000001.SZ,平安银行,银行,strong_buy,high,155\n",
        encoding="utf-8",
    )
    (exp_dir / "daily_research_status_latest.json").write_text('{"state":"completed"}', encoding="utf-8")
    (exp_dir / "buylist_latest.json").write_text('{"items":[{"ts_code":"000001.SZ"}]}', encoding="utf-8")
    (exp_dir / "governance_audit_latest.json").write_text("{}", encoding="utf-8")
    (exp_dir / "t1_execution_checklist_latest.json").write_text("{}", encoding="utf-8")
    (exp_dir / "t12_rollback_drill_latest.json").write_text("{}", encoding="utf-8")

    from src.unified_result_builder import build_primary_result_api_payload

    payload = build_primary_result_api_payload(exp_dir)
    assert payload["history_summary"] is not None
    assert "候选记录" in payload["history_summary"]
    assert payload["history_source_file"] == "buylist_latest.json"
    assert payload["history_source_timestamp"] is not None
    assert payload["disabled_reason"] == "当前风险较高，暂不建议继续推进。"
    assert payload["invalid_reason"] is None


def test_stock_ai_runner_api_body_exposes_read_only_query_contract(tmp_path, monkeypatch):
    exp_dir = tmp_path / "data" / "experiments"
    rep_dir = tmp_path / "data" / "reports"
    exp_dir.mkdir(parents=True, exist_ok=True)
    rep_dir.mkdir(parents=True, exist_ok=True)
    (exp_dir / "daily_research_latest.md").write_text("- score: 90.00/100\n", encoding="utf-8")
    (exp_dir / "daily_health_trend_latest.csv").write_text("generated_at,score\n2026-04-05 08:00:00,90\n", encoding="utf-8")
    (exp_dir / "backtest_leaderboard.csv").write_text(
        "run_id,source_type,stock_pool,total_return,sharpe_ratio,max_drawdown,win_rate,total_trades\n"
        "r1,official_research,000001.SZ|600036.SH,0.05,1.0,-0.04,0.55,24\n",
        encoding="utf-8",
    )
    (exp_dir / "candidates_top_latest.csv").write_text(
        "ts_code,stock_name,industry,signal,risk_level,final_score\n000001.SZ,平安银行,银行,strong_buy,low,155\n",
        encoding="utf-8",
    )
    (exp_dir / "candidates_top_latest.md").write_text("# Candidates\n", encoding="utf-8")
    (exp_dir / "candidates_basket_summary_latest.json").write_text("{}", encoding="utf-8")
    (exp_dir / "candidates_basket_validation_latest.json").write_text("{}", encoding="utf-8")
    (exp_dir / "daily_research_status_latest.json").write_text('{"state":"completed"}', encoding="utf-8")
    (exp_dir / "buylist_latest.json").write_text('{"items":[{"ts_code":"000001.SZ"}]}', encoding="utf-8")
    (exp_dir / "governance_audit_latest.json").write_text("{}", encoding="utf-8")
    (exp_dir / "t1_execution_checklist_latest.json").write_text("{}", encoding="utf-8")
    (exp_dir / "t12_rollback_drill_latest.json").write_text("{}", encoding="utf-8")
    (rep_dir / "backtest_report_20260405_080000.md").write_text("# report\n", encoding="utf-8")
    _write_current_primary_pointer(tmp_path)
    stock_ai_runner_dir = tmp_path / "artifacts" / "stock_ai_runner"
    stock_ai_runner_dir.mkdir(parents=True, exist_ok=True)
    (stock_ai_runner_dir / "read_model.json").write_text(
        json.dumps(
            {
                "schema_version": "stock_ai_runner_storage.v1",
                "provider_latest_health": {
                    "echo_summary": {
                        "provider_name": "echo_summary",
                        "latest_state": "timeout",
                        "is_problem": True,
                        "last_request_id": "req-1",
                        "last_result_id": "primary:000001.SZ",
                        "last_reason": "provider_timeout",
                        "last_status_code": 408,
                        "last_elapsed_ms": 1200,
                        "last_response_bytes": 64,
                        "last_final_status": "degraded",
                        "last_recorded_at": "2026-04-29T09:30:00Z",
                    }
                },
                "failure_top_causes": [{"reason": "provider_timeout", "count": 2}],
                "result_recent_attempts": {},
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    (stock_ai_runner_dir / "telemetry.jsonl").write_text(
        json.dumps(
            {
                "provider_name": "echo_summary",
                "request_id": "req-1",
                "result_id": "primary:000001.SZ",
                "status": "timeout",
                "status_code": 408,
                "response_bytes": 64,
                "elapsed_ms": 1200,
                "network_mode": "offline_only",
                "response_schema_version": "stock_ai_provider_adapter.v1",
                "recorded_at": "2026-04-29T09:30:00Z",
            },
            ensure_ascii=False,
        )
        + "\n",
        encoding="utf-8",
    )
    (stock_ai_runner_dir / "attempt_ledger.jsonl").write_text(
        "\n".join(
            [
                json.dumps(
                    {
                        "result_id": "primary:000001.SZ",
                        "provider_name": "echo_summary",
                        "request_id": "req-1",
                        "recorded_at": "2026-04-29T09:30:00Z",
                        "state": "ready",
                        "reason": "",
                        "final_status": "degraded",
                    },
                    ensure_ascii=False,
                ),
                json.dumps(
                    {
                        "result_id": "primary:000001.SZ",
                        "provider_name": "echo_summary",
                        "request_id": "req-1",
                        "recorded_at": "2026-04-29T09:30:00Z",
                        "state": "running",
                        "reason": "",
                        "final_status": "degraded",
                    },
                    ensure_ascii=False,
                ),
                json.dumps(
                    {
                        "result_id": "primary:000001.SZ",
                        "provider_name": "echo_summary",
                        "request_id": "req-1",
                        "recorded_at": "2026-04-29T09:30:00Z",
                        "state": "timeout",
                        "reason": "provider_timeout",
                        "final_status": "degraded",
                    },
                    ensure_ascii=False,
                ),
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(
        dashboard_context,
        "resolve_project_path",
        lambda rel: {"data/experiments": exp_dir, "data/reports": rep_dir}[rel],
    )
    import run_dashboard as dashboard_module

    monkeypatch.setattr(
        dashboard_module,
        "resolve_project_path",
        lambda rel: {"data/experiments": exp_dir, "data/reports": rep_dir, ".": tmp_path}[rel],
    )

    payload = json.loads(
        _build_stock_ai_runner_api_body(
            tmp_path,
            result_id="primary:000001.SZ",
            replay_window=4,
            recorded_at_from="2026-04-29T09:00:00Z",
            recorded_at_to="2026-04-29T10:00:00Z",
            health_window=4,
            trend_short_window=2,
            trend_long_window=4,
            top_n=2,
        ).decode("utf-8")
    )

    assert payload["schema_version"] == "stock_ai_runner_read_api.v1"
    assert payload["latest_health"][0]["provider_name"] == "echo_summary"
    assert payload["health_rollups"][0]["provider_name"] == "echo_summary"
    assert payload["trend_summaries"][0]["provider_name"] == "echo_summary"
    assert payload["failure_top_causes"][0]["reason"] == "provider_timeout"
    assert payload["result_replay"]["result_id"] == "primary:000001.SZ"
    assert payload["result_replay"]["recorded_at_to"] == "2026-04-29T10:00:00Z"


def test_stock_ai_runner_api_group_resources_are_split_and_stable(tmp_path, monkeypatch):
    exp_dir = tmp_path / "data" / "experiments"
    rep_dir = tmp_path / "data" / "reports"
    exp_dir.mkdir(parents=True, exist_ok=True)
    rep_dir.mkdir(parents=True, exist_ok=True)
    stock_ai_runner_dir = tmp_path / "artifacts" / "stock_ai_runner"
    stock_ai_runner_dir.mkdir(parents=True, exist_ok=True)
    (stock_ai_runner_dir / "read_model.json").write_text(
        json.dumps(
            {
                "schema_version": "stock_ai_runner_storage.v1",
                "provider_latest_health": {
                    "echo_summary": {
                        "provider_name": "echo_summary",
                        "latest_state": "timeout",
                        "is_problem": True,
                        "last_request_id": "req-1",
                        "last_result_id": "primary:000001.SZ",
                        "last_reason": "provider_timeout",
                        "last_status_code": 408,
                        "last_elapsed_ms": 1200,
                        "last_response_bytes": 64,
                        "last_final_status": "degraded",
                        "last_recorded_at": "2026-04-29T09:30:00Z",
                    }
                },
                "failure_top_causes": [{"reason": "provider_timeout", "count": 2}],
                "result_recent_attempts": {},
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    (stock_ai_runner_dir / "telemetry.jsonl").write_text(
        "\n".join(
            [
                json.dumps(
                    {
                        "provider_name": "echo_summary",
                        "request_id": "req-1",
                        "result_id": "primary:000001.SZ",
                        "status": "ok",
                        "status_code": 200,
                        "response_bytes": 64,
                        "elapsed_ms": 100,
                        "recorded_at": "2026-04-29T09:00:00Z",
                    },
                    ensure_ascii=False,
                ),
                json.dumps(
                    {
                        "provider_name": "echo_summary",
                        "request_id": "req-2",
                        "result_id": "primary:000001.SZ",
                        "status": "timeout",
                        "status_code": 408,
                        "response_bytes": 64,
                        "elapsed_ms": 1200,
                        "recorded_at": "2026-04-29T09:30:00Z",
                    },
                    ensure_ascii=False,
                ),
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    (stock_ai_runner_dir / "attempt_ledger.jsonl").write_text(
        "\n".join(
            [
                json.dumps(
                    {
                        "result_id": "primary:000001.SZ",
                        "provider_name": "echo_summary",
                        "request_id": "req-2",
                        "recorded_at": "2026-04-29T09:30:00Z",
                        "state": "ready",
                        "reason": "",
                        "final_status": "degraded",
                    },
                    ensure_ascii=False,
                ),
                json.dumps(
                    {
                        "result_id": "primary:000001.SZ",
                        "provider_name": "echo_summary",
                        "request_id": "req-2",
                        "recorded_at": "2026-04-29T09:30:00Z",
                        "state": "running",
                        "reason": "",
                        "final_status": "degraded",
                    },
                    ensure_ascii=False,
                ),
                json.dumps(
                    {
                        "result_id": "primary:000001.SZ",
                        "provider_name": "echo_summary",
                        "request_id": "req-2",
                        "recorded_at": "2026-04-29T09:30:00Z",
                        "state": "timeout",
                        "reason": "provider_timeout",
                        "final_status": "degraded",
                    },
                    ensure_ascii=False,
                ),
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(
        dashboard_context,
        "resolve_project_path",
        lambda rel: {"data/experiments": exp_dir, "data/reports": rep_dir}[rel],
    )
    import run_dashboard as dashboard_module
    monkeypatch.setattr(
        dashboard_module,
        "resolve_project_path",
        lambda rel: {"data/experiments": exp_dir, "data/reports": rep_dir, ".": tmp_path}[rel],
    )

    latest_health = json.loads(_build_stock_ai_runner_api_body(tmp_path, resource="latest-health").decode("utf-8"))
    trend_summaries = json.loads(
        _build_stock_ai_runner_api_body(tmp_path, resource="trend-summaries", trend_short_window=2, trend_long_window=4).decode("utf-8")
    )
    failure_top_causes = json.loads(
        _build_stock_ai_runner_api_body(tmp_path, resource="failure-top-causes", top_n=2).decode("utf-8")
    )
    provider_detail = json.loads(
        _build_stock_ai_runner_api_body(
            tmp_path,
            resource="provider-detail",
            provider_name="echo_summary",
            replay_window=4,
            recorded_at_from="2026-04-29T09:00:00Z",
            recorded_at_to="2026-04-29T10:00:00Z",
            health_window=4,
            trend_short_window=2,
            trend_long_window=4,
        ).decode("utf-8")
    )
    result_replay = json.loads(
        _build_stock_ai_runner_api_body(
            tmp_path,
            resource="result-replay",
            result_id="primary:000001.SZ",
            replay_window=4,
            recorded_at_from="2026-04-29T09:00:00Z",
            recorded_at_to="2026-04-29T10:00:00Z",
        ).decode("utf-8")
    )

    assert latest_health["resource"] == "latest-health"
    assert latest_health["latest_health"][0]["provider_name"] == "echo_summary"
    assert trend_summaries["resource"] == "trend-summaries"
    assert trend_summaries["trend_summaries"][0]["provider_name"] == "echo_summary"
    assert failure_top_causes["resource"] == "failure-top-causes"
    assert failure_top_causes["failure_top_causes"][0]["reason"] == "provider_timeout"
    assert provider_detail["resource"] == "provider-detail"
    assert provider_detail["provider_detail"]["provider_name"] == "echo_summary"
    assert provider_detail["provider_detail"]["provider_replay"]["provider_name"] == "echo_summary"
    assert result_replay["resource"] == "result-replay"
    assert result_replay["result_replay"]["result_id"] == "primary:000001.SZ"


def test_stock_ai_runner_ops_page_is_separate_from_business_dashboard(tmp_path, monkeypatch):
    exp_dir = tmp_path / "data" / "experiments"
    rep_dir = tmp_path / "data" / "reports"
    exp_dir.mkdir(parents=True, exist_ok=True)
    rep_dir.mkdir(parents=True, exist_ok=True)
    stock_ai_runner_dir = tmp_path / "artifacts" / "stock_ai_runner"
    stock_ai_runner_dir.mkdir(parents=True, exist_ok=True)
    (stock_ai_runner_dir / "read_model.json").write_text(
        json.dumps(
            {
                "schema_version": "stock_ai_runner_storage.v1",
                "provider_latest_health": {
                    "echo_summary": {
                        "provider_name": "echo_summary",
                        "latest_state": "ok",
                        "is_problem": False,
                        "last_request_id": "req-1",
                        "last_result_id": "primary:000001.SZ",
                        "last_reason": "",
                        "last_status_code": 200,
                        "last_elapsed_ms": 100,
                        "last_response_bytes": 64,
                        "last_final_status": "ready",
                        "last_recorded_at": "2026-04-29T09:00:00Z",
                    }
                },
                "failure_top_causes": [],
                "result_recent_attempts": {},
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    (stock_ai_runner_dir / "telemetry.jsonl").write_text("", encoding="utf-8")
    (stock_ai_runner_dir / "attempt_ledger.jsonl").write_text("", encoding="utf-8")
    monkeypatch.setattr(
        dashboard_context,
        "resolve_project_path",
        lambda rel: {"data/experiments": exp_dir, "data/reports": rep_dir}[rel],
    )
    import run_dashboard as dashboard_module
    monkeypatch.setattr(
        dashboard_module,
        "resolve_project_path",
        lambda rel: {"data/experiments": exp_dir, "data/reports": rep_dir, ".": tmp_path}[rel],
    )

    html_text = _render_stock_ai_runner_ops_page(tmp_path, base_path="/stock", provider_name="echo_summary")

    assert 'id="stock-ai-runner-ops"' in html_text
    assert f'href="/stock{STOCK_AI_RUNNER_API_PATH}/latest-health"' in html_text
    assert f'href="/stock{STOCK_AI_RUNNER_API_PATH}/trend-summaries"' in html_text
    assert f'href="/stock{STOCK_AI_RUNNER_API_PATH}/provider-detail?provider_name=echo_summary"' in html_text
    assert '/stock/ops/stock-ai-runner?provider=echo_summary' in html_text
    assert "Provider Drill-Down" in html_text
    assert 'id="primary-result-card"' not in html_text
    assert 'id="stock-ai-explainer"' not in html_text


def test_stock_ai_runner_ops_page_provider_query_switches_drilldown_target(tmp_path, monkeypatch):
    exp_dir = tmp_path / "data" / "experiments"
    rep_dir = tmp_path / "data" / "reports"
    exp_dir.mkdir(parents=True, exist_ok=True)
    rep_dir.mkdir(parents=True, exist_ok=True)
    stock_ai_runner_dir = tmp_path / "artifacts" / "stock_ai_runner"
    stock_ai_runner_dir.mkdir(parents=True, exist_ok=True)
    (stock_ai_runner_dir / "read_model.json").write_text(
        json.dumps(
            {
                "schema_version": "stock_ai_runner_storage.v1",
                "provider_latest_health": {
                    "echo_summary": {
                        "provider_name": "echo_summary",
                        "latest_state": "ok",
                        "is_problem": False,
                        "last_request_id": "req-e",
                        "last_result_id": "primary:000001.SZ",
                        "last_reason": "",
                        "last_status_code": 200,
                        "last_elapsed_ms": 100,
                        "last_response_bytes": 64,
                        "last_final_status": "ready",
                        "last_recorded_at": "2026-04-29T09:00:00Z",
                    },
                    "local_preview": {
                        "provider_name": "local_preview",
                        "latest_state": "blocked",
                        "is_problem": True,
                        "last_request_id": "req-l",
                        "last_result_id": "primary:000002.SZ",
                        "last_reason": "provider_status_code_error:503",
                        "last_status_code": 503,
                        "last_elapsed_ms": 12,
                        "last_response_bytes": 64,
                        "last_final_status": "blocked",
                        "last_recorded_at": "2026-04-29T09:10:00Z",
                    },
                },
                "failure_top_causes": [{"reason": "provider_status_code_error:503", "count": 1}],
                "result_recent_attempts": {},
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    (stock_ai_runner_dir / "telemetry.jsonl").write_text(
        "\n".join(
            [
                json.dumps(
                    {
                        "provider_name": "echo_summary",
                        "request_id": "req-e",
                        "result_id": "primary:000001.SZ",
                        "status": "ok",
                        "status_code": 200,
                        "response_bytes": 64,
                        "elapsed_ms": 100,
                        "recorded_at": "2026-04-29T09:00:00Z",
                    },
                    ensure_ascii=False,
                ),
                json.dumps(
                    {
                        "provider_name": "local_preview",
                        "request_id": "req-l",
                        "result_id": "primary:000002.SZ",
                        "status": "blocked",
                        "status_code": 503,
                        "response_bytes": 64,
                        "elapsed_ms": 12,
                        "recorded_at": "2026-04-29T09:10:00Z",
                    },
                    ensure_ascii=False,
                ),
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    (stock_ai_runner_dir / "attempt_ledger.jsonl").write_text(
        "\n".join(
            [
                json.dumps(
                    {
                        "result_id": "primary:000002.SZ",
                        "provider_name": "local_preview",
                        "request_id": "req-l",
                        "recorded_at": "2026-04-29T09:10:00Z",
                        "state": "blocked",
                        "reason": "provider_status_code_error:503",
                        "final_status": "blocked",
                    },
                    ensure_ascii=False,
                )
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(
        dashboard_context,
        "resolve_project_path",
        lambda rel: {"data/experiments": exp_dir, "data/reports": rep_dir}[rel],
    )
    import run_dashboard as dashboard_module
    monkeypatch.setattr(
        dashboard_module,
        "resolve_project_path",
        lambda rel: {"data/experiments": exp_dir, "data/reports": rep_dir, ".": tmp_path}[rel],
    )

    html_text = _render_stock_ai_runner_ops_page(tmp_path, base_path="/stock", provider_name="local_preview")

    assert "Provider Drill-Down · local_preview" in html_text
    assert "provider_status_code_error:503" in html_text
    assert '/stock/api/stock-ai-runner/provider-detail?provider_name=local_preview' in html_text


def test_stock_ai_runner_result_replay_page_renders_single_result_postmortem(tmp_path, monkeypatch):
    exp_dir = tmp_path / "data" / "experiments"
    rep_dir = tmp_path / "data" / "reports"
    exp_dir.mkdir(parents=True, exist_ok=True)
    rep_dir.mkdir(parents=True, exist_ok=True)
    stock_ai_runner_dir = tmp_path / "artifacts" / "stock_ai_runner"
    stock_ai_runner_dir.mkdir(parents=True, exist_ok=True)
    (stock_ai_runner_dir / "read_model.json").write_text(
        json.dumps(
            {
                "schema_version": "stock_ai_runner_storage.v1",
                "provider_latest_health": {},
                "failure_top_causes": [],
                "result_recent_attempts": {},
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    (stock_ai_runner_dir / "telemetry.jsonl").write_text("", encoding="utf-8")
    (stock_ai_runner_dir / "attempt_ledger.jsonl").write_text(
        "\n".join(
            [
                json.dumps(
                    {
                        "result_id": "primary:000088.SZ",
                        "provider_name": "echo_summary",
                        "request_id": "req-a",
                        "recorded_at": "2026-04-29T09:00:00Z",
                        "state": "ready",
                        "reason": "",
                        "final_status": "degraded",
                    },
                    ensure_ascii=False,
                ),
                json.dumps(
                    {
                        "result_id": "primary:000088.SZ",
                        "provider_name": "echo_summary",
                        "request_id": "req-a",
                        "recorded_at": "2026-04-29T09:00:00Z",
                        "state": "running",
                        "reason": "",
                        "final_status": "degraded",
                    },
                    ensure_ascii=False,
                ),
                json.dumps(
                    {
                        "result_id": "primary:000088.SZ",
                        "provider_name": "echo_summary",
                        "request_id": "req-a",
                        "recorded_at": "2026-04-29T09:00:00Z",
                        "state": "timeout",
                        "reason": "provider_timeout",
                        "final_status": "degraded",
                    },
                    ensure_ascii=False,
                ),
                json.dumps(
                    {
                        "result_id": "primary:000088.SZ",
                        "provider_name": "local_preview",
                        "request_id": "req-b",
                        "recorded_at": "2026-04-29T10:00:00Z",
                        "state": "blocked",
                        "reason": "provider_status_code_error:503",
                        "final_status": "blocked",
                    },
                    ensure_ascii=False,
                ),
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(
        dashboard_context,
        "resolve_project_path",
        lambda rel: {"data/experiments": exp_dir, "data/reports": rep_dir}[rel],
    )
    import run_dashboard as dashboard_module
    monkeypatch.setattr(
        dashboard_module,
        "resolve_project_path",
        lambda rel: {"data/experiments": exp_dir, "data/reports": rep_dir, ".": tmp_path}[rel],
    )

    html_text = _render_stock_ai_runner_result_replay_page(
        tmp_path,
        base_path="/stock",
        result_id="primary:000088.SZ",
        replay_window=8,
        recorded_at_from="2026-04-29T08:00:00Z",
        recorded_at_to="2026-04-29T10:30:00Z",
    )

    assert 'id="stock-ai-runner-result-replay"' in html_text
    assert "Result Replay · primary:000088.SZ" in html_text
    assert "Attempt Timeline" in html_text
    assert "Failure Panel" in html_text
    assert "provider_timeout" in html_text
    assert "provider_status_code_error:503" in html_text
    assert '/stock/api/stock-ai-runner/result-replay?result_id=primary%3A000088.SZ' in html_text
    assert '/stock/ops/stock-ai-runner' in html_text


def test_stock_ai_runner_api_route_is_not_enabled_for_t12(tmp_path):
    handler = object.__new__(DashboardHandler)
    handler.path = STOCK_AI_RUNNER_API_PATH
    handler.base_path = "/T12"
    handler.root_dir = tmp_path
    captured = {}

    def _send_error(code, message=None):
        captured["code"] = code
        captured["message"] = message

    handler.send_error = _send_error
    handler.send_response = lambda code: captured.setdefault("response_code", code)
    handler.send_header = lambda key, value: None
    handler.end_headers = lambda: None
    handler.wfile = type("WFile", (), {"write": lambda self, body: None})()

    handler.do_GET()

    assert captured["code"] == 404


def test_stock_ai_runner_ops_route_is_not_enabled_for_t12(tmp_path):
    handler = object.__new__(DashboardHandler)
    handler.path = STOCK_AI_RUNNER_OPS_PATH
    handler.base_path = "/T12"
    handler.root_dir = tmp_path
    captured = {}

    def _send_error(code, message=None):
        captured["code"] = code
        captured["message"] = message

    handler.send_error = _send_error
    handler.send_response = lambda code: captured.setdefault("response_code", code)
    handler.send_header = lambda key, value: None
    handler.end_headers = lambda: None
    handler.wfile = type("WFile", (), {"write": lambda self, body: None})()

    handler.do_GET()

    assert captured["code"] == 404


def test_stock_ai_runner_result_replay_route_is_not_enabled_for_t12(tmp_path):
    handler = object.__new__(DashboardHandler)
    handler.path = STOCK_AI_RUNNER_RESULT_REPLAY_OPS_PATH
    handler.base_path = "/T12"
    handler.root_dir = tmp_path
    captured = {}

    def _send_error(code, message=None):
        captured["code"] = code
        captured["message"] = message

    handler.send_error = _send_error
    handler.send_response = lambda code: captured.setdefault("response_code", code)
    handler.send_header = lambda key, value: None
    handler.end_headers = lambda: None
    handler.wfile = type("WFile", (), {"write": lambda self, body: None})()

    handler.do_GET()

    assert captured["code"] == 404


def test_primary_result_bridge_script_uses_view_model_render_path(tmp_path, monkeypatch):
    exp_dir = tmp_path / "data" / "experiments"
    rep_dir = tmp_path / "data" / "reports"
    exp_dir.mkdir(parents=True, exist_ok=True)
    rep_dir.mkdir(parents=True, exist_ok=True)
    (exp_dir / "daily_research_latest.md").write_text("- score: 90.00/100\n", encoding="utf-8")
    (exp_dir / "daily_health_trend_latest.csv").write_text("generated_at,score\n2026-04-05 08:00:00,90\n", encoding="utf-8")
    (exp_dir / "backtest_leaderboard.csv").write_text(
        "run_id,source_type,stock_pool,total_return,sharpe_ratio,max_drawdown,win_rate,total_trades\n"
        "r1,official_research,000001.SZ|600036.SH,0.05,1.0,-0.04,0.55,24\n",
        encoding="utf-8",
    )
    (exp_dir / "candidates_top_latest.csv").write_text(
        "ts_code,stock_name,industry,signal,risk_level,final_score\n000001.SZ,平安银行,银行,strong_buy,low,155\n",
        encoding="utf-8",
    )
    (exp_dir / "candidates_top_latest.md").write_text("# Candidates\n", encoding="utf-8")
    (exp_dir / "candidates_basket_summary_latest.json").write_text("{}", encoding="utf-8")
    (exp_dir / "candidates_basket_validation_latest.json").write_text("{}", encoding="utf-8")
    (exp_dir / "daily_research_status_latest.json").write_text('{"state":"completed"}', encoding="utf-8")
    (exp_dir / "buylist_latest.json").write_text('{"items":[{"ts_code":"000001.SZ"}]}', encoding="utf-8")
    (exp_dir / "governance_audit_latest.json").write_text("{}", encoding="utf-8")
    (exp_dir / "t1_execution_checklist_latest.json").write_text("{}", encoding="utf-8")
    (exp_dir / "t12_rollback_drill_latest.json").write_text("{}", encoding="utf-8")
    (rep_dir / "backtest_report_20260405_080000.md").write_text("# report\n", encoding="utf-8")
    _write_current_primary_pointer(tmp_path)

    monkeypatch.setattr(
        dashboard_context,
        "resolve_project_path",
        lambda rel: {"data/experiments": exp_dir, "data/reports": rep_dir}[rel],
    )
    import run_dashboard as dashboard_module
    monkeypatch.setattr(
        dashboard_module,
        "resolve_project_path",
        lambda rel: {"data/experiments": exp_dir, "data/reports": rep_dir, ".": tmp_path}[rel],
    )

    html_text = _render_dashboard(tmp_path, current_view="overview", candidate_index=0, current_report="research", base_path="/stock")
    assert "buildPrimaryResultCardViewModelFromFact" in html_text
    assert "renderPrimaryResultCardSlots" in html_text
    assert "renderPrimaryResultCard(fact)" in html_text
    assert "风险信息暂缺" in html_text or "风险提示" in html_text


def test_primary_result_interaction_scope_and_json_source_priority(tmp_path, monkeypatch):
    exp_dir = tmp_path / "data" / "experiments"
    rep_dir = tmp_path / "data" / "reports"
    exp_dir.mkdir(parents=True, exist_ok=True)
    rep_dir.mkdir(parents=True, exist_ok=True)
    (exp_dir / "daily_research_latest.md").write_text("- score: 90.00/100\n", encoding="utf-8")
    (exp_dir / "daily_health_trend_latest.csv").write_text("generated_at,score\n2026-04-05 08:00:00,90\n", encoding="utf-8")
    (exp_dir / "backtest_leaderboard.csv").write_text(
        "run_id,source_type,stock_pool,total_return,sharpe_ratio,max_drawdown,win_rate,total_trades\n"
        "r1,official_research,000001.SZ|600036.SH,0.05,1.0,-0.04,0.55,24\n",
        encoding="utf-8",
    )
    (exp_dir / "candidates_top_latest.csv").write_text(
        "ts_code,stock_name,industry,signal,risk_level,final_score\n000001.SZ,平安银行,银行,strong_buy,low,155\n",
        encoding="utf-8",
    )
    (exp_dir / "candidates_top_latest.md").write_text("# Candidates\n", encoding="utf-8")
    (exp_dir / "candidates_basket_summary_latest.json").write_text("{}", encoding="utf-8")
    (exp_dir / "candidates_basket_validation_latest.json").write_text("{}", encoding="utf-8")
    (exp_dir / "daily_research_status_latest.json").write_text('{"state":"completed"}', encoding="utf-8")
    (exp_dir / "buylist_latest.json").write_text('{"items":[{"ts_code":"000001.SZ"}]}', encoding="utf-8")
    (exp_dir / "governance_audit_latest.json").write_text("{}", encoding="utf-8")
    (exp_dir / "t1_execution_checklist_latest.json").write_text("{}", encoding="utf-8")
    (exp_dir / "t12_rollback_drill_latest.json").write_text("{}", encoding="utf-8")
    (rep_dir / "backtest_report_20260405_080000.md").write_text("# report\n", encoding="utf-8")
    _write_current_primary_pointer(tmp_path)

    monkeypatch.setattr(
        dashboard_context,
        "resolve_project_path",
        lambda rel: {"data/experiments": exp_dir, "data/reports": rep_dir}[rel],
    )
    import run_dashboard as dashboard_module
    monkeypatch.setattr(
        dashboard_module,
        "resolve_project_path",
        lambda rel: {"data/experiments": exp_dir, "data/reports": rep_dir, ".": tmp_path}[rel],
    )

    html_text = _render_dashboard(tmp_path, current_view="overview", candidate_index=0, current_report="research", base_path="/stock")
    assert "#primary-result-card .primary-result-action-btn" in html_text
    assert "let primaryResultEffectiveFact = null;" in html_text
    assert "primaryResultEffectiveFact = serverFact;" in html_text
    assert "primaryResultEffectiveFact = fact;" in html_text
    assert "JSON.stringify(primaryResultEffectiveFact, null, 2)" in html_text
    assert '#primary-result-card .primary-result-action-btn' in html_text
    assert '#primary-result-card .primary-result-live-status' in html_text
    assert '#primary-result-card .primary-result-action-btn:focus-visible' in html_text
    assert "primary-result" not in html_text.split("localStorage", 1)[1] if "localStorage" in html_text else True
    assert "primary-result" not in html_text.split("sessionStorage", 1)[1] if "sessionStorage" in html_text else True
    assert "primary-result" not in html_text.split("history.pushState", 1)[1] if "history.pushState" in html_text else True


def test_primary_result_interactions_do_not_change_bridge_contract(tmp_path, monkeypatch):
    exp_dir = tmp_path / "data" / "experiments"
    rep_dir = tmp_path / "data" / "reports"
    exp_dir.mkdir(parents=True, exist_ok=True)
    rep_dir.mkdir(parents=True, exist_ok=True)
    (exp_dir / "daily_research_latest.md").write_text("- score: 90.00/100\n", encoding="utf-8")
    (exp_dir / "daily_health_trend_latest.csv").write_text("generated_at,score\n2026-04-05 08:00:00,90\n", encoding="utf-8")
    (exp_dir / "backtest_leaderboard.csv").write_text(
        "run_id,source_type,stock_pool,total_return,sharpe_ratio,max_drawdown,win_rate,total_trades\n"
        "r1,official_research,000001.SZ|600036.SH,0.05,1.0,-0.04,0.55,24\n",
        encoding="utf-8",
    )
    (exp_dir / "candidates_top_latest.csv").write_text(
        "ts_code,stock_name,industry,signal,risk_level,final_score\n000001.SZ,平安银行,银行,strong_buy,low,155\n",
        encoding="utf-8",
    )
    (exp_dir / "candidates_top_latest.md").write_text("# Candidates\n", encoding="utf-8")
    (exp_dir / "candidates_basket_summary_latest.json").write_text("{}", encoding="utf-8")
    (exp_dir / "candidates_basket_validation_latest.json").write_text("{}", encoding="utf-8")
    (exp_dir / "daily_research_status_latest.json").write_text('{"state":"completed"}', encoding="utf-8")
    (exp_dir / "buylist_latest.json").write_text('{"items":[{"ts_code":"000001.SZ"}]}', encoding="utf-8")
    (exp_dir / "governance_audit_latest.json").write_text("{}", encoding="utf-8")
    (exp_dir / "t1_execution_checklist_latest.json").write_text("{}", encoding="utf-8")
    (exp_dir / "t12_rollback_drill_latest.json").write_text("{}", encoding="utf-8")
    (rep_dir / "backtest_report_20260405_080000.md").write_text("# report\n", encoding="utf-8")
    _write_current_primary_pointer(tmp_path)

    monkeypatch.setattr(
        dashboard_context,
        "resolve_project_path",
        lambda rel: {"data/experiments": exp_dir, "data/reports": rep_dir}[rel],
    )
    import run_dashboard as dashboard_module
    monkeypatch.setattr(
        dashboard_module,
        "resolve_project_path",
        lambda rel: {"data/experiments": exp_dir, "data/reports": rep_dir, ".": tmp_path}[rel],
    )

    html_text = _render_dashboard(tmp_path, current_view="overview", candidate_index=0, current_report="research", base_path="/stock")
    assert "loadPrimaryResultCard()" in html_text
    assert "fetch(PRIMARY_RESULT_API_URL" in html_text
    assert "window.setTimeout(() => controller.abort(), 1500)" in html_text
    assert "let primaryResultLiveStatusTimer = null;" in html_text
    assert "function announcePrimaryResultStatus(message)" in html_text
    assert "liveEl.textContent = '';" in html_text
    assert "window.setTimeout(() => {" in html_text
    assert "已复制制度事实摘要。" in html_text
    assert "已复制当前事实 JSON。" in html_text
    assert "复制失败，请重试。" in html_text
    assert "setAttribute('aria-expanded', expanded ? 'true' : 'false')" in html_text
    assert "body.setAttribute('aria-hidden', expanded ? 'false' : 'true')" in html_text
    assert "copyPrimaryResultText(primaryResultSummaryTextFromFact(primaryResultEffectiveFact))" in html_text
    assert "copyPrimaryResultText(JSON.stringify(primaryResultEffectiveFact, null, 2))" in html_text


def test_primary_result_accessibility_ids_and_semantics_present(tmp_path, monkeypatch):
    exp_dir = tmp_path / "data" / "experiments"
    rep_dir = tmp_path / "data" / "reports"
    exp_dir.mkdir(parents=True, exist_ok=True)
    rep_dir.mkdir(parents=True, exist_ok=True)
    (exp_dir / "daily_research_latest.md").write_text("- score: 90.00/100\n", encoding="utf-8")
    (exp_dir / "daily_health_trend_latest.csv").write_text("generated_at,score\n2026-04-05 08:00:00,90\n", encoding="utf-8")
    (exp_dir / "backtest_leaderboard.csv").write_text(
        "run_id,source_type,stock_pool,total_return,sharpe_ratio,max_drawdown,win_rate,total_trades\n"
        "r1,official_research,000001.SZ|600036.SH,0.05,1.0,-0.04,0.55,24\n",
        encoding="utf-8",
    )
    (exp_dir / "candidates_top_latest.csv").write_text(
        "ts_code,stock_name,industry,signal,risk_level,final_score\n000001.SZ,平安银行,银行,strong_buy,low,155\n",
        encoding="utf-8",
    )
    (exp_dir / "candidates_top_latest.md").write_text("# Candidates\n", encoding="utf-8")
    (exp_dir / "candidates_basket_summary_latest.json").write_text("{}", encoding="utf-8")
    (exp_dir / "candidates_basket_validation_latest.json").write_text("{}", encoding="utf-8")
    (exp_dir / "daily_research_status_latest.json").write_text('{"state":"completed"}', encoding="utf-8")
    (exp_dir / "buylist_latest.json").write_text('{"items":[{"ts_code":"000001.SZ"}]}', encoding="utf-8")
    (exp_dir / "governance_audit_latest.json").write_text("{}", encoding="utf-8")
    (exp_dir / "t1_execution_checklist_latest.json").write_text("{}", encoding="utf-8")
    (exp_dir / "t12_rollback_drill_latest.json").write_text("{}", encoding="utf-8")
    (rep_dir / "backtest_report_20260405_080000.md").write_text("# report\n", encoding="utf-8")
    _write_current_primary_pointer(tmp_path)

    monkeypatch.setattr(
        dashboard_context,
        "resolve_project_path",
        lambda rel: {"data/experiments": exp_dir, "data/reports": rep_dir}[rel],
    )
    import run_dashboard as dashboard_module
    monkeypatch.setattr(
        dashboard_module,
        "resolve_project_path",
        lambda rel: {"data/experiments": exp_dir, "data/reports": rep_dir, ".": tmp_path}[rel],
    )

    html_text = _render_dashboard(tmp_path, current_view="overview", candidate_index=0, current_report="research", base_path="/stock")
    assert html_text.count('id="primary-result-history-toggle"') == 1
    assert html_text.count('id="primary-result-history-body"') == 1
    assert html_text.count('id="primary-result-disabled-toggle"') == 1
    assert html_text.count('id="primary-result-disabled-body"') == 1
    assert html_text.count('id="primary-result-invalid-toggle"') == 1
    assert html_text.count('id="primary-result-invalid-body"') == 1
    assert 'aria-controls="primary-result-history-body"' in html_text
    assert 'aria-controls="primary-result-disabled-body"' in html_text
    assert 'aria-controls="primary-result-invalid-body"' in html_text
    assert 'aria-labelledby="primary-result-history-toggle"' in html_text
    assert 'aria-labelledby="primary-result-disabled-toggle"' in html_text
    assert 'aria-labelledby="primary-result-invalid-toggle"' in html_text
    assert 'id="primary-result-live-status"' in html_text
    assert 'aria-live="polite"' in html_text
    assert 'aria-atomic="true"' in html_text
    assert 'aria-label="复制制度事实摘要"' in html_text
    assert 'aria-label="复制当前已生效的事实 JSON"' in html_text


def test_t12_overview_card_is_mounted_with_independent_scope(tmp_path, monkeypatch):
    exp_dir = tmp_path / "data" / "experiments"
    rep_dir = tmp_path / "data" / "reports"
    exp_dir.mkdir(parents=True, exist_ok=True)
    rep_dir.mkdir(parents=True, exist_ok=True)
    (exp_dir / "daily_research_latest.md").write_text("- score: 90.00/100\n", encoding="utf-8")
    (exp_dir / "daily_health_trend_latest.csv").write_text("generated_at,score\n2026-04-05 08:00:00,90\n", encoding="utf-8")
    (exp_dir / "backtest_leaderboard.csv").write_text(
        "run_id,source_type,stock_pool,total_return,sharpe_ratio,max_drawdown,win_rate,total_trades\n"
        "r1,official_research,000001.SZ|600036.SH,0.05,1.0,-0.04,0.55,24\n",
        encoding="utf-8",
    )
    (exp_dir / "candidates_top_latest.csv").write_text(
        "ts_code,stock_name,industry,signal,risk_level,final_score\n000001.SZ,平安银行,银行,strong_buy,low,155\n",
        encoding="utf-8",
    )
    (exp_dir / "candidates_top_latest.md").write_text("# Candidates\n", encoding="utf-8")
    (exp_dir / "candidates_basket_summary_latest.json").write_text("{}", encoding="utf-8")
    (exp_dir / "candidates_basket_validation_latest.json").write_text("{}", encoding="utf-8")
    (exp_dir / "daily_research_status_latest.json").write_text('{"state":"completed"}', encoding="utf-8")
    (exp_dir / "buylist_latest.json").write_text('{"items":[{"ts_code":"000001.SZ"}]}', encoding="utf-8")
    (exp_dir / "governance_audit_latest.json").write_text("{}", encoding="utf-8")
    (exp_dir / "t1_execution_checklist_latest.json").write_text("{}", encoding="utf-8")
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
    _write_current_primary_pointer(tmp_path)
    (exp_dir / "t12_threshold_rollback_events.json").write_text('{"items":[]}', encoding="utf-8")
    (exp_dir / "t12_threshold_stable_snapshot.json").write_text(
        '{"thresholds":{"warn_hot_threshold":1}}',
        encoding="utf-8",
    )
    (rep_dir / "backtest_report_20260405_080000.md").write_text("# report\n", encoding="utf-8")

    monkeypatch.setattr(
        dashboard_context,
        "resolve_project_path",
        lambda rel: {"data/experiments": exp_dir, "data/reports": rep_dir}[rel],
    )
    import run_dashboard as dashboard_module
    monkeypatch.setattr(
        dashboard_module,
        "resolve_project_path",
        lambda rel: {"data/experiments": exp_dir, "data/reports": rep_dir, ".": tmp_path}[rel],
    )

    t12_html = _render_dashboard(tmp_path, current_view="t12", candidate_index=0, current_report="research", base_path="/T12")
    stock_html = _render_dashboard(tmp_path, current_view="overview", candidate_index=0, current_report="research", base_path="/stock")
    assert 'id="t12-overview-card"' in t12_html
    assert "#t12-overview-card .t12-overview-grid" in t12_html
    assert "#t12-overview-card .t12-overview-block" in t12_html
    assert "#t12-overview-card .primary-result-grid" not in t12_html
    assert "T12 最小制度总览" in t12_html
    assert 'id="t12-overview-card"' not in stock_html


def test_t12_governance_summary_mounts_only_in_t12_scope(tmp_path, monkeypatch):
    exp_dir = tmp_path / "data" / "experiments"
    rep_dir = tmp_path / "data" / "reports"
    exp_dir.mkdir(parents=True, exist_ok=True)
    rep_dir.mkdir(parents=True, exist_ok=True)
    (exp_dir / "daily_research_latest.md").write_text("- score: 90.00/100\n", encoding="utf-8")
    (exp_dir / "daily_health_trend_latest.csv").write_text("generated_at,score\n2026-04-05 08:00:00,90\n", encoding="utf-8")
    (exp_dir / "backtest_leaderboard.csv").write_text(
        "run_id,source_type,stock_pool,total_return,sharpe_ratio,max_drawdown,win_rate,total_trades\n"
        "r1,official_research,000001.SZ|600036.SH,0.05,1.0,-0.04,0.55,24\n",
        encoding="utf-8",
    )
    (exp_dir / "candidates_top_latest.csv").write_text(
        "ts_code,stock_name,industry,signal,risk_level,final_score\n000001.SZ,平安银行,银行,strong_buy,low,155\n",
        encoding="utf-8",
    )
    (exp_dir / "candidates_top_latest.md").write_text("# Candidates\n", encoding="utf-8")
    (exp_dir / "candidates_basket_summary_latest.json").write_text("{}", encoding="utf-8")
    (exp_dir / "candidates_basket_validation_latest.json").write_text("{}", encoding="utf-8")
    (exp_dir / "daily_research_status_latest.json").write_text('{"state":"completed"}', encoding="utf-8")
    (exp_dir / "buylist_latest.json").write_text('{"items":[{"ts_code":"000001.SZ"}]}', encoding="utf-8")
    (exp_dir / "governance_audit_latest.json").write_text("{}", encoding="utf-8")
    (exp_dir / "t1_execution_checklist_latest.json").write_text("{}", encoding="utf-8")
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

    monkeypatch.setattr(
        dashboard_context,
        "resolve_project_path",
        lambda rel: {"data/experiments": exp_dir, "data/reports": rep_dir}[rel],
    )
    import run_dashboard as dashboard_module
    monkeypatch.setattr(
        dashboard_module,
        "resolve_project_path",
        lambda rel: {"data/experiments": exp_dir, "data/reports": rep_dir, ".": tmp_path}[rel],
    )

    t12_html = _render_dashboard(tmp_path, current_view="t12", candidate_index=0, current_report="research", base_path="/T12")
    stock_html = _render_dashboard(tmp_path, current_view="overview", candidate_index=0, current_report="research", base_path="/stock")

    assert 'id="t12-governance-summary"' in t12_html
    assert "#t12-governance-summary .t12-governance-summary__grid" in t12_html
    assert 'id="primary-result-initial-json"' not in t12_html
    assert "const PRIMARY_RESULT_BRIDGE_ENABLED = false;" in t12_html
    assert "Governance Summary" in t12_html
    assert 'id="t12-governance-summary"' not in stock_html
    assert "t12-governance-summary__grid" not in stock_html
    assert "T12 治理摘要" not in stock_html
    assert "Governance Summary" not in stock_html


def test_t12_scope_helpers_keep_view_boundary_minimal():
    assert _is_t12_scope("/T12") is True
    assert _is_t12_scope("/apex/T12") is True
    assert _is_t12_scope("/stock") is False
    assert _is_main_site_scope("/") is True
    assert _is_main_site_scope("/apex") is True
    assert _is_main_site_scope("/stock") is False
    assert _resolve_namespace_id("/") == "production"
    assert _resolve_namespace_id("/stock") == "production"
    assert _resolve_namespace_id("/apex") == "apex"
    assert _resolve_namespace_id("/apex/stock") == "apex"
    assert _resolve_scope_id("/") == "main_site"
    assert _resolve_scope_id("/stock") == "stock"
    assert _resolve_scope_id("/T12") == "t12"
    assert _resolve_scope_id("/apex") == "main_site"
    assert _resolve_scope_id("/apex/stock") == "stock"
    assert _resolve_scope_id("/apex/T12") == "t12"
    t12_labels = _view_labels("/T12")
    stock_labels = _view_labels("/stock")
    assert set(t12_labels.keys()) == set(VIEW_LABELS.keys()) | {"t12"}
    assert set(stock_labels.keys()) == set(VIEW_LABELS.keys())
    assert _view_subtitles("/T12")["t12"] == "聚焦 T12 最小制度镜像与治理摘要，不扩成完整控制台。"


def test_stock_ai_explainer_renders_on_stock_when_flags_and_provider_stub_enabled(tmp_path, monkeypatch):
    exp_dir = tmp_path / "data" / "experiments"
    rep_dir = tmp_path / "data" / "reports"
    exp_dir.mkdir(parents=True, exist_ok=True)
    rep_dir.mkdir(parents=True, exist_ok=True)
    (exp_dir / "daily_research_latest.md").write_text("- score: 90.00/100\n", encoding="utf-8")
    (exp_dir / "daily_health_trend_latest.csv").write_text("generated_at,score\n2026-04-05 08:00:00,90\n", encoding="utf-8")
    (exp_dir / "backtest_leaderboard.csv").write_text(
        "run_id,source_type,stock_pool,total_return,sharpe_ratio,max_drawdown,win_rate,total_trades\n"
        "r1,official_research,000001.SZ|600036.SH,0.05,1.0,-0.04,0.55,24\n",
        encoding="utf-8",
    )
    (exp_dir / "candidates_top_latest.csv").write_text(
        "ts_code,stock_name,industry,signal,risk_level,final_score\n000001.SZ,平安银行,银行,strong_buy,low,155\n",
        encoding="utf-8",
    )
    (exp_dir / "candidates_top_latest.md").write_text("# Candidates\n", encoding="utf-8")
    (exp_dir / "candidates_basket_summary_latest.json").write_text("{}", encoding="utf-8")
    (exp_dir / "candidates_basket_validation_latest.json").write_text("{}", encoding="utf-8")
    (exp_dir / "daily_research_status_latest.json").write_text('{"state":"completed"}', encoding="utf-8")
    (exp_dir / "buylist_latest.json").write_text('{"items":[{"ts_code":"000001.SZ"}]}', encoding="utf-8")
    (exp_dir / "governance_audit_latest.json").write_text("{}", encoding="utf-8")
    (exp_dir / "t1_execution_checklist_latest.json").write_text("{}", encoding="utf-8")
    (exp_dir / "t12_rollback_drill_latest.json").write_text("{}", encoding="utf-8")
    (rep_dir / "backtest_report_20260405_080000.md").write_text("# report\n", encoding="utf-8")
    _write_current_primary_pointer(tmp_path)

    monkeypatch.setattr(
        dashboard_context,
        "resolve_project_path",
        lambda rel: {"data/experiments": exp_dir, "data/reports": rep_dir}[rel],
    )
    import run_dashboard as dashboard_module
    monkeypatch.setattr(
        dashboard_module,
        "resolve_project_path",
        lambda rel: {"data/experiments": exp_dir, "data/reports": rep_dir, ".": tmp_path}[rel],
    )
    monkeypatch.setenv("STOCK_AI_F0_GLOBAL", "1")
    monkeypatch.setenv("STOCK_AI_F1_STOCK_EXPLAINER", "1")
    monkeypatch.setenv("STOCK_AI_F2_A1_EXPLAINER", "1")
    monkeypatch.setenv("STOCK_AI_PROVIDER_STUB", "echo_summary")

    html_text = _render_dashboard(tmp_path, current_view="overview", candidate_index=0, current_report="research", base_path="/stock")

    assert 'id="stock-ai-explainer"' in html_text
    assert "AI 解释受控回声预览" in html_text
    assert "AI 解释" in html_text
    assert "主结果" in html_text


def test_stock_ai_explainer_stays_hidden_when_provider_stub_is_invalid(tmp_path, monkeypatch):
    exp_dir = tmp_path / "data" / "experiments"
    rep_dir = tmp_path / "data" / "reports"
    exp_dir.mkdir(parents=True, exist_ok=True)
    rep_dir.mkdir(parents=True, exist_ok=True)
    (exp_dir / "daily_research_latest.md").write_text("- score: 90.00/100\n", encoding="utf-8")
    (exp_dir / "daily_health_trend_latest.csv").write_text("generated_at,score\n2026-04-05 08:00:00,90\n", encoding="utf-8")
    (exp_dir / "backtest_leaderboard.csv").write_text(
        "run_id,source_type,stock_pool,total_return,sharpe_ratio,max_drawdown,win_rate,total_trades\n"
        "r1,official_research,000001.SZ|600036.SH,0.05,1.0,-0.04,0.55,24\n",
        encoding="utf-8",
    )
    (exp_dir / "candidates_top_latest.csv").write_text(
        "ts_code,stock_name,industry,signal,risk_level,final_score\n000001.SZ,平安银行,银行,strong_buy,low,155\n",
        encoding="utf-8",
    )
    (exp_dir / "candidates_top_latest.md").write_text("# Candidates\n", encoding="utf-8")
    (exp_dir / "candidates_basket_summary_latest.json").write_text("{}", encoding="utf-8")
    (exp_dir / "candidates_basket_validation_latest.json").write_text("{}", encoding="utf-8")
    (exp_dir / "daily_research_status_latest.json").write_text('{"state":"completed"}', encoding="utf-8")
    (exp_dir / "buylist_latest.json").write_text('{"items":[{"ts_code":"000001.SZ"}]}', encoding="utf-8")
    (exp_dir / "governance_audit_latest.json").write_text("{}", encoding="utf-8")
    (exp_dir / "t1_execution_checklist_latest.json").write_text("{}", encoding="utf-8")
    (exp_dir / "t12_rollback_drill_latest.json").write_text("{}", encoding="utf-8")
    (rep_dir / "backtest_report_20260405_080000.md").write_text("# report\n", encoding="utf-8")
    _write_current_primary_pointer(tmp_path)

    monkeypatch.setattr(
        dashboard_context,
        "resolve_project_path",
        lambda rel: {"data/experiments": exp_dir, "data/reports": rep_dir}[rel],
    )
    import run_dashboard as dashboard_module
    monkeypatch.setattr(
        dashboard_module,
        "resolve_project_path",
        lambda rel: {"data/experiments": exp_dir, "data/reports": rep_dir, ".": tmp_path}[rel],
    )
    monkeypatch.setenv("STOCK_AI_F0_GLOBAL", "1")
    monkeypatch.setenv("STOCK_AI_F1_STOCK_EXPLAINER", "1")
    monkeypatch.setenv("STOCK_AI_F2_A1_EXPLAINER", "1")
    monkeypatch.setenv("STOCK_AI_PROVIDER_STUB", "bad_stub")

    html_text = _render_dashboard(tmp_path, current_view="overview", candidate_index=0, current_report="research", base_path="/stock")

    assert 'id="stock-ai-explainer"' not in html_text
    assert "AI 解释受控回声预览" not in html_text


def test_stock_ai_explainer_never_mounts_in_t12_even_when_flags_enabled(tmp_path, monkeypatch):
    exp_dir = tmp_path / "data" / "experiments"
    rep_dir = tmp_path / "data" / "reports"
    exp_dir.mkdir(parents=True, exist_ok=True)
    rep_dir.mkdir(parents=True, exist_ok=True)
    (exp_dir / "daily_research_latest.md").write_text("- score: 90.00/100\n", encoding="utf-8")
    (exp_dir / "daily_health_trend_latest.csv").write_text("generated_at,score\n2026-04-05 08:00:00,90\n", encoding="utf-8")
    (exp_dir / "backtest_leaderboard.csv").write_text(
        "run_id,source_type,stock_pool,total_return,sharpe_ratio,max_drawdown,win_rate,total_trades\n"
        "r1,official_research,000001.SZ|600036.SH,0.05,1.0,-0.04,0.55,24\n",
        encoding="utf-8",
    )
    (exp_dir / "candidates_top_latest.csv").write_text(
        "ts_code,stock_name,industry,signal,risk_level,final_score\n000001.SZ,平安银行,银行,strong_buy,low,155\n",
        encoding="utf-8",
    )
    (exp_dir / "candidates_top_latest.md").write_text("# Candidates\n", encoding="utf-8")
    (exp_dir / "candidates_basket_summary_latest.json").write_text("{}", encoding="utf-8")
    (exp_dir / "candidates_basket_validation_latest.json").write_text("{}", encoding="utf-8")
    (exp_dir / "daily_research_status_latest.json").write_text('{"state":"completed"}', encoding="utf-8")
    (exp_dir / "buylist_latest.json").write_text('{"items":[{"ts_code":"000001.SZ"}]}', encoding="utf-8")
    (exp_dir / "governance_audit_latest.json").write_text("{}", encoding="utf-8")
    (exp_dir / "t1_execution_checklist_latest.json").write_text("{}", encoding="utf-8")
    (exp_dir / "t12_alert_center_latest.json").write_text('{"summary":{"overall_status":"pass"}}', encoding="utf-8")
    (exp_dir / "t12_review_checklist_latest.json").write_text('{"overall_status":"pass"}', encoding="utf-8")
    (exp_dir / "t12_threshold_control_latest.json").write_text('{"applied": true}', encoding="utf-8")
    (exp_dir / "t12_rollback_drill_latest.json").write_text("{}", encoding="utf-8")
    (exp_dir / "t12_threshold_rollback_events.json").write_text('{"items":[]}', encoding="utf-8")
    (exp_dir / "t12_threshold_stable_snapshot.json").write_text('{"thresholds":{"warn_hot_threshold":1}}', encoding="utf-8")
    (rep_dir / "backtest_report_20260405_080000.md").write_text("# report\n", encoding="utf-8")
    _write_current_primary_pointer(tmp_path)

    monkeypatch.setattr(
        dashboard_context,
        "resolve_project_path",
        lambda rel: {"data/experiments": exp_dir, "data/reports": rep_dir}[rel],
    )
    import run_dashboard as dashboard_module
    monkeypatch.setattr(
        dashboard_module,
        "resolve_project_path",
        lambda rel: {"data/experiments": exp_dir, "data/reports": rep_dir, ".": tmp_path}[rel],
    )
    monkeypatch.setenv("STOCK_AI_F0_GLOBAL", "1")
    monkeypatch.setenv("STOCK_AI_F1_STOCK_EXPLAINER", "1")
    monkeypatch.setenv("STOCK_AI_F2_A1_EXPLAINER", "1")
    monkeypatch.setenv("STOCK_AI_PROVIDER_STUB", "echo_summary")

    t12_html = _render_dashboard(tmp_path, current_view="t12", candidate_index=0, current_report="research", base_path="/T12")

    assert 'id="stock-ai-explainer"' not in t12_html
    assert "AI 解释受控回声预览" not in t12_html


@pytest.mark.parametrize(
    "stub_name",
    [
        "timeout_stub",
        "empty_response_stub",
        "non_dict_response_stub",
        "missing_payload_stub",
        "payload_not_dict_stub",
        "missing_required_fields_stub",
        "invalid_schema_stub",
        "forbidden_output_field_stub",
    ],
)
def test_stock_ai_explainer_anomaly_stubs_hide_block_without_breaking_dashboard(tmp_path, monkeypatch, stub_name):
    exp_dir = tmp_path / "data" / "experiments"
    rep_dir = tmp_path / "data" / "reports"
    exp_dir.mkdir(parents=True, exist_ok=True)
    rep_dir.mkdir(parents=True, exist_ok=True)
    (exp_dir / "daily_research_latest.md").write_text("- score: 90.00/100\n", encoding="utf-8")
    (exp_dir / "daily_health_trend_latest.csv").write_text("generated_at,score\n2026-04-05 08:00:00,90\n", encoding="utf-8")
    (exp_dir / "backtest_leaderboard.csv").write_text(
        "run_id,source_type,stock_pool,total_return,sharpe_ratio,max_drawdown,win_rate,total_trades\n"
        "r1,official_research,000001.SZ|600036.SH,0.05,1.0,-0.04,0.55,24\n",
        encoding="utf-8",
    )
    (exp_dir / "candidates_top_latest.csv").write_text(
        "ts_code,stock_name,industry,signal,risk_level,final_score\n000001.SZ,平安银行,银行,strong_buy,low,155\n",
        encoding="utf-8",
    )
    (exp_dir / "candidates_top_latest.md").write_text("# Candidates\n", encoding="utf-8")
    (exp_dir / "candidates_basket_summary_latest.json").write_text("{}", encoding="utf-8")
    (exp_dir / "candidates_basket_validation_latest.json").write_text("{}", encoding="utf-8")
    (exp_dir / "daily_research_status_latest.json").write_text('{"state":"completed"}', encoding="utf-8")
    (exp_dir / "buylist_latest.json").write_text('{"items":[{"ts_code":"000001.SZ"}]}', encoding="utf-8")
    (exp_dir / "governance_audit_latest.json").write_text("{}", encoding="utf-8")
    (exp_dir / "t1_execution_checklist_latest.json").write_text("{}", encoding="utf-8")
    (exp_dir / "t12_rollback_drill_latest.json").write_text("{}", encoding="utf-8")
    (rep_dir / "backtest_report_20260405_080000.md").write_text("# report\n", encoding="utf-8")
    _write_current_primary_pointer(tmp_path)

    monkeypatch.setattr(
        dashboard_context,
        "resolve_project_path",
        lambda rel: {"data/experiments": exp_dir, "data/reports": rep_dir}[rel],
    )
    import run_dashboard as dashboard_module
    monkeypatch.setattr(
        dashboard_module,
        "resolve_project_path",
        lambda rel: {"data/experiments": exp_dir, "data/reports": rep_dir, ".": tmp_path}[rel],
    )
    monkeypatch.setenv("STOCK_AI_F0_GLOBAL", "1")
    monkeypatch.setenv("STOCK_AI_F1_STOCK_EXPLAINER", "1")
    monkeypatch.setenv("STOCK_AI_F2_A1_EXPLAINER", "1")
    monkeypatch.setenv("STOCK_AI_PROVIDER_STUB", stub_name)

    html_text = _render_dashboard(tmp_path, current_view="overview", candidate_index=0, current_report="research", base_path="/stock")

    assert "当前判断" in html_text
    assert "primary:000001.SZ" in html_text
    assert 'id="stock-ai-explainer"' not in html_text


def test_unknown_base_path_falls_back_to_canonical_main_site_without_crashing():
    html_text = _render_dashboard(root=None, base_path="/foo")

    assert html_text.startswith("<!-- airivo-route-fallback:/foo -->")
    assert 'id="main-site-home"' in html_text
    assert 'href="/stock/"' in html_text
    assert 'href="/T12/"' in html_text


def test_primary_result_api_fail_closes_when_lifecycle_evidence_missing(tmp_path, monkeypatch):
    exp_dir = tmp_path / "data" / "experiments"
    rep_dir = tmp_path / "data" / "reports"
    exp_dir.mkdir(parents=True, exist_ok=True)
    rep_dir.mkdir(parents=True, exist_ok=True)
    (exp_dir / "candidates_top_latest.csv").write_text(
        "ts_code,stock_name,industry,signal,risk_level,final_score\n000001.SZ,平安银行,银行,strong_buy,low,155\n",
        encoding="utf-8",
    )
    (exp_dir / "daily_research_status_latest.json").write_text('{"state":"completed"}', encoding="utf-8")
    (exp_dir / "buylist_latest.json").write_text('{"items":[{"ts_code":"000001.SZ"}]}', encoding="utf-8")
    (exp_dir / "governance_audit_latest.json").write_text("{}", encoding="utf-8")
    (exp_dir / "t1_execution_checklist_latest.json").write_text("{}", encoding="utf-8")
    (exp_dir / "t12_rollback_drill_latest.json").write_text("{}", encoding="utf-8")
    _write_current_primary_pointer(tmp_path)
    monkeypatch.setattr(
        dashboard_context,
        "resolve_project_path",
        lambda rel: {"data/experiments": exp_dir, "data/reports": rep_dir}[rel],
    )
    import run_dashboard as dashboard_module
    monkeypatch.setattr(
        dashboard_module,
        "resolve_project_path",
        lambda rel: {"data/experiments": exp_dir, "data/reports": rep_dir, ".": tmp_path}[rel],
    )

    (exp_dir / "primary_result_lifecycle_evidence_latest.json").unlink()
    payload = json.loads(_build_primary_result_api_body(tmp_path).decode("utf-8"))

    assert payload["history_generation_mode"] == "blocked"
    assert payload["disabled_reason"] == "stock entry guard blocked primary result publication."
    assert payload["entry_guard"]["ok"] is False
    assert any("lifecycle_evidence" in problem for problem in payload["entry_guard"]["problems"])


def test_render_dashboard_fail_closes_when_lifecycle_evidence_missing(tmp_path, monkeypatch):
    exp_dir = tmp_path / "data" / "experiments"
    rep_dir = tmp_path / "data" / "reports"
    exp_dir.mkdir(parents=True, exist_ok=True)
    rep_dir.mkdir(parents=True, exist_ok=True)
    (exp_dir / "candidates_top_latest.csv").write_text(
        "ts_code,stock_name,industry,signal,risk_level,final_score\n000001.SZ,平安银行,银行,strong_buy,low,155\n",
        encoding="utf-8",
    )
    (exp_dir / "daily_research_status_latest.json").write_text('{"state":"completed"}', encoding="utf-8")
    (exp_dir / "buylist_latest.json").write_text('{"items":[{"ts_code":"000001.SZ"}]}', encoding="utf-8")
    (exp_dir / "governance_audit_latest.json").write_text("{}", encoding="utf-8")
    (exp_dir / "t1_execution_checklist_latest.json").write_text("{}", encoding="utf-8")
    (exp_dir / "t12_rollback_drill_latest.json").write_text("{}", encoding="utf-8")
    _write_current_primary_pointer(tmp_path)
    monkeypatch.setattr(
        dashboard_context,
        "resolve_project_path",
        lambda rel: {"data/experiments": exp_dir, "data/reports": rep_dir}[rel],
    )
    import run_dashboard as dashboard_module
    monkeypatch.setattr(
        dashboard_module,
        "resolve_project_path",
        lambda rel: {"data/experiments": exp_dir, "data/reports": rep_dir, ".": tmp_path}[rel],
    )

    (exp_dir / "primary_result_lifecycle_evidence_latest.json").unlink()
    html_text = _render_dashboard(tmp_path, current_view="overview", candidate_index=0, current_report="research", base_path="/stock")

    assert "/stock 当前禁止输出主结果" in html_text
    assert "Fail Closed" in html_text
    assert "primary_result_lifecycle_evidence_latest.json missing" in html_text
