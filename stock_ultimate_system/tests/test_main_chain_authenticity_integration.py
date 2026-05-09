from __future__ import annotations

import json
import shutil
import subprocess
from datetime import datetime, timezone
from pathlib import Path

import pytest

import run_dashboard as dashboard_module
from scripts.check_current_result_pointer_integrity import check_current_result_pointer_integrity
from scripts.run_primary_result_lifecycle import run_primary_result_lifecycle
from scripts.run_server_post_deploy_verification import run_server_post_deploy_verification
from src.artifact_registry import sha256_file
from src import dashboard_context
from src.run_registry import RunRegistry
from src.stock_entry_guard import evaluate_stock_entry_guard
from tests.primary_result_test_support import seed_current_primary_pointer


pytestmark = pytest.mark.integration


def _write_minimal_experiment_inputs(tmp_path: Path, *, risk_level: str = "low") -> Path:
    exp_dir = tmp_path / "data" / "experiments"
    rep_dir = tmp_path / "data" / "reports"
    exp_dir.mkdir(parents=True, exist_ok=True)
    rep_dir.mkdir(parents=True, exist_ok=True)

    (exp_dir / "daily_research_latest.md").write_text(
        "- score: 90.00/100\n- success_rate: 95.00%\n- failure_penalty: 1.0\n- category_penalty: 0.0\n",
        encoding="utf-8",
    )
    (exp_dir / "daily_health_trend_latest.csv").write_text(
        "generated_at,score\n2026-04-15 08:00:00,90\n",
        encoding="utf-8",
    )
    (exp_dir / "backtest_leaderboard.csv").write_text(
        "run_id,source_type,stock_pool,total_return,sharpe_ratio,max_drawdown,win_rate,total_trades\n"
        "r1,official_research,000001.SZ|600036.SH,0.05,1.0,-0.04,0.55,24\n",
        encoding="utf-8",
    )
    (exp_dir / "candidates_top_latest.csv").write_text(
        "ts_code,stock_name,industry,signal,risk_level,final_score,direction_prob_up,pred_return,confidence,position_pct,stop_loss,take_profit,reason\n"
        f"000001.SZ,平安银行,银行,strong_buy,{risk_level},155,0.70,0.08,0.90,0.2,10.0,12.0,model_bullish factor_strong\n"
        "000002.SZ,万科A,地产,watch,medium,120,0.55,0.03,0.75,0.1,8.0,10.0,secondary candidate\n",
        encoding="utf-8",
    )
    (exp_dir / "candidates_top_latest.md").write_text("# Candidates\n", encoding="utf-8")
    (exp_dir / "candidates_basket_summary_latest.json").write_text("{}", encoding="utf-8")
    (exp_dir / "candidates_basket_validation_latest.json").write_text("{}", encoding="utf-8")
    (exp_dir / "candidates_audit_latest.json").write_text(
        '{"rows":[{"ts_code":"000001.SZ","selection_reason":"top ranked after audit"}]}',
        encoding="utf-8",
    )
    (exp_dir / "governance_cycle_latest.json").write_text(
        '{"cycle_state":"observe_only","recommended_action":"hold_observation","operator_message":"当前仅满足观察条件。","release_readiness":{"ready_for_release":false,"ready_for_observation":true}}',
        encoding="utf-8",
    )
    (exp_dir / "daily_research_status_latest.json").write_text('{"state":"completed"}', encoding="utf-8")
    (exp_dir / "buylist_latest.json").write_text('{"items":[{"ts_code":"000001.SZ","stock_name":"平安银行"}]}', encoding="utf-8")
    (exp_dir / "governance_audit_latest.json").write_text("{}", encoding="utf-8")
    (exp_dir / "t1_execution_checklist_latest.json").write_text("{}", encoding="utf-8")
    (exp_dir / "t12_rollback_drill_latest.json").write_text('{"triggered": false}', encoding="utf-8")
    (rep_dir / "backtest_report_20260415_080000.md").write_text("# report\n", encoding="utf-8")

    basket_dir = tmp_path / "artifacts" / "primary_result_candidate_baskets"
    basket_dir.mkdir(parents=True, exist_ok=True)
    (basket_dir / "current.json").write_text(
        '{"status":"approved","basket_id":"basket-approved-001","updated_at":"2026-04-15T08:00:00+00:00"}',
        encoding="utf-8",
    )
    (basket_dir / "latest_attempt.json").write_text(
        '{"status":"approved","basket_id":"basket-approved-001","generated_at":"2026-04-15T08:00:00+00:00"}',
        encoding="utf-8",
    )
    (basket_dir / "feedback_latest.json").write_text(
        '{"feedback_version":"primary_result_candidate_basket_feedback.v1","feedback_level":"review","summary_note":"review before release","change_total":1,"window_label":"5D"}',
        encoding="utf-8",
    )

    seed_current_primary_pointer(tmp_path, ts_code="000001.SZ", stock_name="平安银行")
    return exp_dir


def _patch_runtime_paths(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    exp_dir = tmp_path / "data" / "experiments"
    rep_dir = tmp_path / "data" / "reports"
    artifacts_dir = tmp_path / "artifacts"

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
        lambda path=None: artifacts_dir if path in {None, ".", "", "artifacts"} else artifacts_dir / Path(path),
    )
    monkeypatch.setattr(
        dashboard_module,
        "resolve_experiments_path",
        lambda path=None: exp_dir if path in {None, ".", "", "data/experiments"} else exp_dir / Path(path),
    )
    monkeypatch.setattr(
        dashboard_module,
        "resolve_artifacts_path",
        lambda path=None: artifacts_dir if path in {None, ".", "", "artifacts"} else artifacts_dir / Path(path),
    )
    monkeypatch.setattr(
        dashboard_module,
        "resolve_project_path",
        lambda rel=".", *args, **kwargs: tmp_path if rel in {None, ".", ""} else tmp_path / str(rel),
    )


def _create_verification_app_dir(tmp_path: Path) -> Path:
    app_dir = tmp_path / "stock-ultimate" / "app"
    for relative_path in (
        "run_dashboard.py",
        "src/airivo_scope_registry.py",
        "scripts/build_server_activation_plan.py",
        "scripts/check_current_result_pointer_integrity.py",
        "scripts/run_stock_entry_guard.py",
        "scripts/run_server_post_deploy_verification.py",
        "deploy/aliyun/nginx.airivo.online.conf",
        "deploy/aliyun/stock-ultimate-dashboard.service",
        "deploy/aliyun/stock-ultimate-dashboard.service.d/canonical-artifacts.conf",
        "deploy/aliyun/stock-ultimate-entry-guard.service",
        "deploy/aliyun/stock-ultimate-entry-guard.timer",
    ):
        path = app_dir / relative_path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("x\n", encoding="utf-8")
    (app_dir / "data").mkdir(parents=True, exist_ok=True)
    shutil.copytree(tmp_path / "artifacts", app_dir / "artifacts", dirs_exist_ok=True)
    current_pointer_path = app_dir / "artifacts" / "current_result_pointer" / "current.json"
    current_pointer = json.loads(current_pointer_path.read_text(encoding="utf-8"))
    current_pointer["snapshot_path"] = str(
        app_dir / "artifacts" / "current_result_pointer" / "history" / f"{current_pointer['pointer_snapshot_id']}.json"
    )
    current_pointer_path.write_text(json.dumps(current_pointer, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    pointer_snapshot_path = app_dir / "artifacts" / "current_result_pointer" / "history" / f"{current_pointer['pointer_snapshot_id']}.json"
    pointer_snapshot = json.loads(pointer_snapshot_path.read_text(encoding="utf-8"))
    pointer_snapshot_metadata = pointer_snapshot.get("metadata")
    if isinstance(pointer_snapshot_metadata, dict):
        pointer_snapshot_metadata["evidence_path"] = str(app_dir / "data" / "experiments" / "primary_result_lifecycle_evidence_latest.json")
        pointer_snapshot_metadata["artifact_registry_path"] = str(app_dir / "artifacts" / "artifact_registry.jsonl")
    pointer_snapshot_path.write_text(json.dumps(pointer_snapshot, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    result_registry_current_path = app_dir / "artifacts" / "result_registry" / "current.json"
    result_registry_current = json.loads(result_registry_current_path.read_text(encoding="utf-8"))
    result_registry_current["entry_path"] = str(
        app_dir / "artifacts" / "result_registry" / "history" / f"{result_registry_current['record_id']}.json"
    )
    result_registry_current_path.write_text(
        json.dumps(result_registry_current, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    result_history_path = app_dir / "artifacts" / "result_registry" / "history" / f"{result_registry_current['record_id']}.json"
    result_history = json.loads(result_history_path.read_text(encoding="utf-8"))
    result_metadata = result_history.get("metadata")
    if isinstance(result_metadata, dict):
        result_metadata["artifact_registry_path"] = str(app_dir / "artifacts" / "artifact_registry.jsonl")
        result_metadata["evidence_path"] = str(app_dir / "data" / "experiments" / "primary_result_lifecycle_evidence_latest.json")
    result_history_path.write_text(json.dumps(result_history, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    run_registry_current_path = app_dir / "artifacts" / "run_registry" / "current.json"
    run_registry_current = json.loads(run_registry_current_path.read_text(encoding="utf-8"))
    run_registry_current["entry_path"] = str(
        app_dir / "artifacts" / "run_registry" / "history" / f"{run_registry_current['run_id']}.json"
    )
    run_registry_current_path.write_text(
        json.dumps(run_registry_current, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    run_history_path = app_dir / "artifacts" / "run_registry" / "history" / f"{run_registry_current['run_id']}.json"
    run_history = json.loads(run_history_path.read_text(encoding="utf-8"))
    run_metadata = run_history.get("metadata")
    if isinstance(run_metadata, dict):
        run_metadata["evidence_path"] = str(app_dir / "data" / "experiments" / "primary_result_lifecycle_evidence_latest.json")
        run_metadata["artifact_registry_path"] = str(app_dir / "artifacts" / "artifact_registry.jsonl")
    run_history_path.write_text(json.dumps(run_history, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return app_dir


def _active_service_runner(command, timeout=10.0):
    if command == ["nginx", "-t"]:
        return subprocess.CompletedProcess(command, 0, stdout="", stderr="nginx ok\n")
    if command == ["systemctl", "show", "stock-ultimate-dashboard.service", "-p", "Environment", "--no-pager"]:
        return subprocess.CompletedProcess(
            command,
            0,
            stdout=(
                "Environment=PYTHONUNBUFFERED=1 "
                "PYTHONPATH=/opt/stock-ultimate/app "
                "STOCK_ULTIMATE_ARTIFACTS_DIR=/opt/stock-ultimate/app/artifacts "
                "STOCK_ULTIMATE_EXPERIMENTS_DIR=/opt/stock-ultimate/app/data/experiments\n"
            ),
            stderr="",
        )
    return subprocess.CompletedProcess(command, 0, stdout="active\n", stderr="")


def _dashboard_html(primary_progress="1/20", basket_progress="8/20", promotion_gate="晋级锁定"):
    return (
        "<div>"
        f"<span>主结果证据</span><strong>{primary_progress}</strong>"
        f"<span>候选篮证据</span><strong>{basket_progress}</strong>"
        f"<span>晋级门禁</span><strong>{promotion_gate}</strong>"
        "</div>"
    )


def _ok_url_fetcher(url, timeout):
    if url.endswith("/stock/api/primary-result"):
        return 200, '{"schema_version":"primary_result_v1","result_id":"primary:000001.SZ"}'
    if url.endswith("/T12/api/stock-ai-runner") or url.endswith("/T12/ops/stock-ai-runner"):
        return 404, "not found"
    if url.endswith("/stock/") or url.endswith("/apex/stock/"):
        return 200, _dashboard_html()
    return 200, f"ok {url}"


def test_main_chain_authenticity_flow_passes_across_lifecycle_guard_api_dashboard_and_verification(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    exp_dir = _write_minimal_experiment_inputs(tmp_path)
    _patch_runtime_paths(monkeypatch, tmp_path)

    exit_code, lifecycle_payload = run_primary_result_lifecycle(
        exp_dir=exp_dir,
        max_source_age_hours=100000,
        observation_window_start="2026-04-15T07:40:00Z",
        now=datetime(2026, 4, 15, 7, 40, 20, tzinfo=timezone.utc),
    )

    assert exit_code == 0
    assert lifecycle_payload["status"] == "passed"

    artifacts_dir = tmp_path / "artifacts"
    integrity_exit, integrity_payload = check_current_result_pointer_integrity(
        pointer_dir=artifacts_dir / "current_result_pointer",
        results_dir=artifacts_dir / "result_registry",
        runs_dir=artifacts_dir / "run_registry",
        artifact_registry_path=artifacts_dir / "artifact_registry.jsonl",
    )
    assert integrity_exit == 0, integrity_payload
    assert integrity_payload["ok"] is True

    entry_guard = evaluate_stock_entry_guard(exp_dir=exp_dir, artifacts_dir=artifacts_dir)
    assert entry_guard["ok"] is True
    assert entry_guard["pointer"]["run_id"] == lifecycle_payload["run_id"]
    assert lifecycle_payload["final_payload"]["source_scope"] == "stock"

    api_payload = json.loads(dashboard_module._build_primary_result_api_body(tmp_path).decode("utf-8"))
    context = dashboard_context.build_dashboard_context(tmp_path, candidate_index=1, base_path="/stock")

    assert api_payload["result_id"] == "primary:000001.SZ"
    assert api_payload["run_id"] == lifecycle_payload["run_id"]
    assert api_payload["lifecycle_id"] == lifecycle_payload["lifecycle_id"]
    assert api_payload["artifact_ids"] == lifecycle_payload["final_payload"]["artifact_ids"]
    assert api_payload["source_scope"] == "stock"
    assert context["primary_result"]["result_id"] == api_payload["result_id"]
    assert context["primary_result"]["run_id"] == api_payload["run_id"]
    assert context["primary_result"]["lifecycle_id"] == api_payload["lifecycle_id"]
    assert context["primary_result_query"]["primary_conclusion"]["result_id"] == api_payload["result_id"]
    assert context["top1"]["ts_code"] == "000002.SZ"
    assert context["primary_result"]["ts_code"] == "000001.SZ"

    def url_fetcher(url, timeout):
        if url.endswith("/apex/stock/api/primary-result"):
            return 200, json.dumps(api_payload, ensure_ascii=False)
        if url.endswith("/stock/api/primary-result"):
            return 200, json.dumps(api_payload, ensure_ascii=False)
        return _ok_url_fetcher(url, timeout)

    app_dir = _create_verification_app_dir(tmp_path)
    activation_plan_path = tmp_path / "server_activation_plan.stock_scoped.integration.json"
    activation_plan_path.write_text(
        json.dumps(
            {
                "activation_plan_version": "server_activation_plan.v1",
                "status": "passed",
                "scope": "stock-scoped",
                "rollback_commands": ["systemctl restart stock-ultimate-dashboard.service"],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    verify_exit, verify_payload = run_server_post_deploy_verification(
        app_root=app_dir.parents[0],
        app_dir=app_dir,
        log_paths=[tmp_path / "dashboard.err.log", tmp_path / "entry_guard.err.log"],
        activation_plan_path=activation_plan_path,
        command_runner=_active_service_runner,
        url_fetcher=url_fetcher,
    )

    assert verify_exit == 0
    assert verify_payload["status"] == "passed"
    assert verify_payload["guard_ok"] is True
    assert verify_payload["pointer_integrity_ok"] is True
    checks = {check["check"]: check["passed"] for check in verify_payload["checks"]}
    assert checks["systemd_services_active"] is True
    assert checks["dashboard_http_targets"] is True
    assert checks["stock_entry_guard_ok"] is True
    assert checks["current_result_pointer_integrity"] is True


def test_main_chain_authenticity_fail_closes_when_pointer_artifact_chain_breaks(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    exp_dir = _write_minimal_experiment_inputs(tmp_path)
    _patch_runtime_paths(monkeypatch, tmp_path)

    exit_code, lifecycle_payload = run_primary_result_lifecycle(
        exp_dir=exp_dir,
        max_source_age_hours=100000,
        observation_window_start="2026-04-15T07:40:00Z",
        now=datetime(2026, 4, 15, 7, 40, 20, tzinfo=timezone.utc),
    )
    assert exit_code == 0
    assert lifecycle_payload["status"] == "passed"

    artifacts_dir = tmp_path / "artifacts"
    (artifacts_dir / "artifact_registry.jsonl").write_text("", encoding="utf-8")

    integrity_exit, integrity_payload = check_current_result_pointer_integrity(
        pointer_dir=artifacts_dir / "current_result_pointer",
        results_dir=artifacts_dir / "result_registry",
        runs_dir=artifacts_dir / "run_registry",
        artifact_registry_path=artifacts_dir / "artifact_registry.jsonl",
    )
    assert integrity_exit == 1
    assert integrity_payload["ok"] is False

    entry_guard = evaluate_stock_entry_guard(exp_dir=exp_dir, artifacts_dir=artifacts_dir)
    assert entry_guard["ok"] is False
    assert any("artifact registry entry missing" in problem for problem in entry_guard["problems"])

    api_payload = json.loads(dashboard_module._build_primary_result_api_body(tmp_path).decode("utf-8"))
    context = dashboard_context.build_dashboard_context(tmp_path, candidate_index=0, base_path="/stock")

    assert api_payload["disabled_reason"] == "stock entry guard blocked primary result publication."
    assert api_payload["history_generation_mode"] == "blocked"
    assert api_payload["entry_guard"]["ok"] is False
    assert "fail closed" in str(api_payload["data_sync_note"])
    assert context["primary_result"]["disabled_reason"] is not None
    assert "artifact_registry" in str(context["primary_result"]["disabled_reason"])
    assert context["primary_result"]["history_generation_mode"] == "blocked"


def test_main_chain_authenticity_fail_closes_when_old_lifecycle_evidence_is_replayed_as_latest(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    exp_dir = _write_minimal_experiment_inputs(tmp_path)
    _patch_runtime_paths(monkeypatch, tmp_path)

    exit_code, lifecycle_payload = run_primary_result_lifecycle(
        exp_dir=exp_dir,
        max_source_age_hours=100000,
        observation_window_start="2026-04-15T07:40:00Z",
        now=datetime(2026, 4, 15, 7, 40, 20, tzinfo=timezone.utc),
    )
    assert exit_code == 0
    assert lifecycle_payload["status"] == "passed"

    lifecycle_evidence_path = exp_dir / "primary_result_lifecycle_evidence_latest.json"
    replayed = json.loads(lifecycle_evidence_path.read_text(encoding="utf-8"))
    replayed["run_id"] = "run-old"
    replayed["lifecycle_id"] = "life-old"
    replayed["final_payload"]["run_id"] = "run-old"
    replayed["final_payload"]["lifecycle_id"] = "life-old"
    lifecycle_evidence_path.write_text(json.dumps(replayed, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    artifacts_dir = tmp_path / "artifacts"
    entry_guard = evaluate_stock_entry_guard(exp_dir=exp_dir, artifacts_dir=artifacts_dir)
    assert entry_guard["ok"] is False
    assert any("run_id does not match current_result_pointer" in problem for problem in entry_guard["problems"])
    assert any("lifecycle_id does not match current_result_pointer" in problem for problem in entry_guard["problems"])

    api_payload = json.loads(dashboard_module._build_primary_result_api_body(tmp_path).decode("utf-8"))

    assert api_payload["disabled_reason"] == "stock entry guard blocked primary result publication."
    assert api_payload["history_generation_mode"] == "blocked"
    assert api_payload["entry_guard"]["ok"] is False
    assert any("run_id does not match current_result_pointer" in problem for problem in api_payload["entry_guard"]["problems"])
    assert any("lifecycle_id does not match current_result_pointer" in problem for problem in api_payload["entry_guard"]["problems"])


def test_main_chain_authenticity_fail_closes_when_lifecycle_stage_disagrees_with_result_registry_current_stage(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    exp_dir = _write_minimal_experiment_inputs(tmp_path)
    _patch_runtime_paths(monkeypatch, tmp_path)

    exit_code, lifecycle_payload = run_primary_result_lifecycle(
        exp_dir=exp_dir,
        max_source_age_hours=100000,
        observation_window_start="2026-04-15T07:40:00Z",
        now=datetime(2026, 4, 15, 7, 40, 20, tzinfo=timezone.utc),
    )
    assert exit_code == 0
    assert lifecycle_payload["status"] == "passed"

    artifacts_dir = tmp_path / "artifacts"
    result_registry_current_path = artifacts_dir / "result_registry" / "current.json"
    current_result_pointer = json.loads(result_registry_current_path.read_text(encoding="utf-8"))
    result_record_path = Path(str(current_result_pointer["entry_path"]))
    result_record = json.loads(result_record_path.read_text(encoding="utf-8"))
    result_record["lifecycle_stage"] = "L2"
    result_record_path.write_text(json.dumps(result_record, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    entry_guard = evaluate_stock_entry_guard(exp_dir=exp_dir, artifacts_dir=artifacts_dir)
    assert entry_guard["ok"] is False
    assert any("result_lifecycle_stage does not match result_registry current stage" in problem for problem in entry_guard["problems"])

    api_payload = json.loads(dashboard_module._build_primary_result_api_body(tmp_path).decode("utf-8"))

    assert api_payload["disabled_reason"] == "stock entry guard blocked primary result publication."
    assert api_payload["history_generation_mode"] == "blocked"
    assert api_payload["entry_guard"]["ok"] is False
    assert any(
        "result_lifecycle_stage does not match result_registry current stage" in problem
        for problem in api_payload["entry_guard"]["problems"]
    )


def test_main_chain_authenticity_fail_closes_when_lifecycle_final_payload_artifact_ids_replay_old_run_artifact(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    exp_dir = _write_minimal_experiment_inputs(tmp_path)
    _patch_runtime_paths(monkeypatch, tmp_path)

    exit_code, lifecycle_payload = run_primary_result_lifecycle(
        exp_dir=exp_dir,
        max_source_age_hours=100000,
        observation_window_start="2026-04-15T07:40:00Z",
        now=datetime(2026, 4, 15, 7, 40, 20, tzinfo=timezone.utc),
    )
    assert exit_code == 0
    assert lifecycle_payload["status"] == "passed"

    lifecycle_evidence_path = exp_dir / "primary_result_lifecycle_evidence_latest.json"
    replayed = json.loads(lifecycle_evidence_path.read_text(encoding="utf-8"))
    replayed["final_payload"]["artifact_ids"] = ["run-old:audit-artifact"]
    lifecycle_evidence_path.write_text(json.dumps(replayed, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    entry_guard = evaluate_stock_entry_guard(exp_dir=exp_dir, artifacts_dir=tmp_path / "artifacts")
    assert entry_guard["ok"] is False
    assert "lifecycle evidence final_payload artifact_ids do not match current_result_pointer" in entry_guard["problems"]

    api_payload = json.loads(dashboard_module._build_primary_result_api_body(tmp_path).decode("utf-8"))
    assert api_payload["disabled_reason"] == "stock entry guard blocked primary result publication."
    assert api_payload["history_generation_mode"] == "blocked"
    assert "lifecycle evidence final_payload artifact_ids do not match current_result_pointer" in api_payload["entry_guard"]["problems"]


def test_main_chain_authenticity_fail_closes_when_lifecycle_step_file_is_overwritten_after_registration(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    exp_dir = _write_minimal_experiment_inputs(tmp_path)
    _patch_runtime_paths(monkeypatch, tmp_path)

    exit_code, lifecycle_payload = run_primary_result_lifecycle(
        exp_dir=exp_dir,
        max_source_age_hours=100000,
        observation_window_start="2026-04-15T07:40:00Z",
        now=datetime(2026, 4, 15, 7, 40, 20, tzinfo=timezone.utc),
    )
    assert exit_code == 0
    assert lifecycle_payload["status"] == "passed"

    audit_path = exp_dir / "primary_result_audit_latest.json"
    audit_path.write_text('{"tampered": true}\n', encoding="utf-8")

    integrity_exit, integrity_payload = check_current_result_pointer_integrity(
        pointer_dir=tmp_path / "artifacts" / "current_result_pointer",
        results_dir=tmp_path / "artifacts" / "result_registry",
        runs_dir=tmp_path / "artifacts" / "run_registry",
        artifact_registry_path=tmp_path / "artifacts" / "artifact_registry.jsonl",
    )
    assert integrity_exit == 1
    assert any("artifact registry sha256 mismatch" in problem for problem in integrity_payload["problems"])

    entry_guard = evaluate_stock_entry_guard(exp_dir=exp_dir, artifacts_dir=tmp_path / "artifacts")
    assert entry_guard["ok"] is False
    assert any("lifecycle step sha256 mismatch: audit" in problem for problem in entry_guard["problems"])

    api_payload = json.loads(dashboard_module._build_primary_result_api_body(tmp_path).decode("utf-8"))
    assert api_payload["disabled_reason"] == "stock entry guard blocked primary result publication."
    assert api_payload["history_generation_mode"] == "blocked"
    assert any("lifecycle step sha256 mismatch: audit" in problem for problem in api_payload["entry_guard"]["problems"])


def test_main_chain_authenticity_fail_closes_when_lifecycle_step_content_identity_is_tampered_even_if_sha_is_rebased(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    exp_dir = _write_minimal_experiment_inputs(tmp_path)
    _patch_runtime_paths(monkeypatch, tmp_path)

    exit_code, lifecycle_payload = run_primary_result_lifecycle(
        exp_dir=exp_dir,
        max_source_age_hours=100000,
        observation_window_start="2026-04-15T07:40:00Z",
        now=datetime(2026, 4, 15, 7, 40, 20, tzinfo=timezone.utc),
    )
    assert exit_code == 0
    assert lifecycle_payload["status"] == "passed"

    artifacts_dir = tmp_path / "artifacts"
    audit_path = exp_dir / "primary_result_audit_latest.json"
    tampered_audit = json.loads(audit_path.read_text(encoding="utf-8"))
    tampered_audit["result_id"] = "primary:999999.SZ"
    tampered_audit["ts_code"] = "999999.SZ"
    tampered_audit["run_id"] = "run-old"
    audit_path.write_text(json.dumps(tampered_audit, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    artifact_registry_path = artifacts_dir / "artifact_registry.jsonl"
    registry_lines = []
    for line in artifact_registry_path.read_text(encoding="utf-8").splitlines():
        payload = json.loads(line)
        if payload.get("artifact_id") == f"{lifecycle_payload['run_id']}:audit":
            payload["sha256"] = sha256_file(audit_path)
        registry_lines.append(json.dumps(payload, ensure_ascii=False, sort_keys=True))
    artifact_registry_path.write_text("\n".join(registry_lines) + "\n", encoding="utf-8")

    lifecycle_evidence_path = exp_dir / "primary_result_lifecycle_evidence_latest.json"
    lifecycle_evidence = json.loads(lifecycle_evidence_path.read_text(encoding="utf-8"))
    for step in lifecycle_evidence.get("steps", []):
        if step.get("step") == "audit":
            step["sha256"] = sha256_file(audit_path)
    lifecycle_evidence_path.write_text(json.dumps(lifecycle_evidence, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    registry_lines = []
    for line in artifact_registry_path.read_text(encoding="utf-8").splitlines():
        payload = json.loads(line)
        if payload.get("artifact_id") == f"{lifecycle_payload['run_id']}:lifecycle-evidence":
            payload["sha256"] = sha256_file(lifecycle_evidence_path)
        registry_lines.append(json.dumps(payload, ensure_ascii=False, sort_keys=True))
    artifact_registry_path.write_text("\n".join(registry_lines) + "\n", encoding="utf-8")

    integrity_exit, integrity_payload = check_current_result_pointer_integrity(
        pointer_dir=artifacts_dir / "current_result_pointer",
        results_dir=artifacts_dir / "result_registry",
        runs_dir=artifacts_dir / "run_registry",
        artifact_registry_path=artifact_registry_path,
    )
    assert integrity_exit == 0, integrity_payload

    entry_guard = evaluate_stock_entry_guard(exp_dir=exp_dir, artifacts_dir=artifacts_dir)
    assert entry_guard["ok"] is False
    assert "lifecycle step result_id mismatch: audit" in entry_guard["problems"]
    assert "lifecycle step ts_code mismatch: audit" in entry_guard["problems"]
    assert "lifecycle step run_id mismatch: audit" in entry_guard["problems"]

    api_payload = json.loads(dashboard_module._build_primary_result_api_body(tmp_path).decode("utf-8"))
    assert api_payload["disabled_reason"] == "stock entry guard blocked primary result publication."
    assert api_payload["history_generation_mode"] == "blocked"
    assert "lifecycle step result_id mismatch: audit" in api_payload["entry_guard"]["problems"]
    assert "lifecycle step ts_code mismatch: audit" in api_payload["entry_guard"]["problems"]
    assert "lifecycle step run_id mismatch: audit" in api_payload["entry_guard"]["problems"]


def test_main_chain_authenticity_fail_closes_when_lifecycle_evidence_step_metadata_is_tampered(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    exp_dir = _write_minimal_experiment_inputs(tmp_path)
    _patch_runtime_paths(monkeypatch, tmp_path)

    exit_code, lifecycle_payload = run_primary_result_lifecycle(
        exp_dir=exp_dir,
        max_source_age_hours=100000,
        observation_window_start="2026-04-15T07:40:00Z",
        now=datetime(2026, 4, 15, 7, 40, 20, tzinfo=timezone.utc),
    )
    assert exit_code == 0
    assert lifecycle_payload["status"] == "passed"

    artifacts_dir = tmp_path / "artifacts"
    lifecycle_evidence_path = exp_dir / "primary_result_lifecycle_evidence_latest.json"
    lifecycle_evidence = json.loads(lifecycle_evidence_path.read_text(encoding="utf-8"))
    for step in lifecycle_evidence.get("steps", []):
        if step.get("step") == "audit":
            step["result_id"] = "primary:999999.SZ"
            step["ts_code"] = "999999.SZ"
            step["generated_at"] = "1999-01-01T00:00:00Z"
    lifecycle_evidence_path.write_text(json.dumps(lifecycle_evidence, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    artifact_registry_path = artifacts_dir / "artifact_registry.jsonl"
    registry_lines = []
    for line in artifact_registry_path.read_text(encoding="utf-8").splitlines():
        payload = json.loads(line)
        if payload.get("artifact_id") == f"{lifecycle_payload['run_id']}:lifecycle-evidence":
            payload["sha256"] = sha256_file(lifecycle_evidence_path)
        registry_lines.append(json.dumps(payload, ensure_ascii=False, sort_keys=True))
    artifact_registry_path.write_text("\n".join(registry_lines) + "\n", encoding="utf-8")

    integrity_exit, integrity_payload = check_current_result_pointer_integrity(
        pointer_dir=artifacts_dir / "current_result_pointer",
        results_dir=artifacts_dir / "result_registry",
        runs_dir=artifacts_dir / "run_registry",
        artifact_registry_path=artifact_registry_path,
    )
    assert integrity_exit == 0, integrity_payload
    assert integrity_payload["ok"] is True

    entry_guard = evaluate_stock_entry_guard(exp_dir=exp_dir, artifacts_dir=artifacts_dir)
    assert entry_guard["ok"] is False
    assert "lifecycle evidence step result_id metadata mismatch: audit" in entry_guard["problems"]
    assert "lifecycle evidence step ts_code metadata mismatch: audit" in entry_guard["problems"]
    assert "lifecycle evidence step generated_at metadata mismatch: audit" in entry_guard["problems"]

    api_payload = json.loads(dashboard_module._build_primary_result_api_body(tmp_path).decode("utf-8"))
    assert api_payload["disabled_reason"] == "stock entry guard blocked primary result publication."
    assert api_payload["history_generation_mode"] == "blocked"
    assert "lifecycle evidence step result_id metadata mismatch: audit" in api_payload["entry_guard"]["problems"]
    assert "lifecycle evidence step ts_code metadata mismatch: audit" in api_payload["entry_guard"]["problems"]
    assert "lifecycle evidence step generated_at metadata mismatch: audit" in api_payload["entry_guard"]["problems"]


def test_main_chain_authenticity_fail_closes_when_lifecycle_step_generated_at_breaks_time_bounds(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    exp_dir = _write_minimal_experiment_inputs(tmp_path)
    _patch_runtime_paths(monkeypatch, tmp_path)

    exit_code, lifecycle_payload = run_primary_result_lifecycle(
        exp_dir=exp_dir,
        max_source_age_hours=100000,
        observation_window_start="2026-04-15T07:40:00Z",
        now=datetime(2026, 4, 15, 7, 40, 20, tzinfo=timezone.utc),
    )
    assert exit_code == 0
    assert lifecycle_payload["status"] == "passed"

    artifacts_dir = tmp_path / "artifacts"
    execution_path = exp_dir / "primary_result_execution_latest.json"
    tampered_execution = json.loads(execution_path.read_text(encoding="utf-8"))
    tampered_execution["generated_at"] = "1999-01-01T00:00:00Z"
    execution_path.write_text(json.dumps(tampered_execution, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    artifact_registry_path = artifacts_dir / "artifact_registry.jsonl"
    registry_lines = []
    for line in artifact_registry_path.read_text(encoding="utf-8").splitlines():
        payload = json.loads(line)
        if payload.get("artifact_id") == f"{lifecycle_payload['run_id']}:execution":
            payload["sha256"] = sha256_file(execution_path)
        registry_lines.append(json.dumps(payload, ensure_ascii=False, sort_keys=True))
    artifact_registry_path.write_text("\n".join(registry_lines) + "\n", encoding="utf-8")

    lifecycle_evidence_path = exp_dir / "primary_result_lifecycle_evidence_latest.json"
    lifecycle_evidence = json.loads(lifecycle_evidence_path.read_text(encoding="utf-8"))
    for step in lifecycle_evidence.get("steps", []):
        if step.get("step") == "execution":
            step["generated_at"] = "1999-01-01T00:00:00Z"
            step["sha256"] = sha256_file(execution_path)
    lifecycle_evidence_path.write_text(json.dumps(lifecycle_evidence, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    registry_lines = []
    for line in artifact_registry_path.read_text(encoding="utf-8").splitlines():
        payload = json.loads(line)
        if payload.get("artifact_id") == f"{lifecycle_payload['run_id']}:lifecycle-evidence":
            payload["sha256"] = sha256_file(lifecycle_evidence_path)
        registry_lines.append(json.dumps(payload, ensure_ascii=False, sort_keys=True))
    artifact_registry_path.write_text("\n".join(registry_lines) + "\n", encoding="utf-8")

    integrity_exit, integrity_payload = check_current_result_pointer_integrity(
        pointer_dir=artifacts_dir / "current_result_pointer",
        results_dir=artifacts_dir / "result_registry",
        runs_dir=artifacts_dir / "run_registry",
        artifact_registry_path=artifact_registry_path,
    )
    assert integrity_exit == 0, integrity_payload
    assert integrity_payload["ok"] is True

    entry_guard = evaluate_stock_entry_guard(exp_dir=exp_dir, artifacts_dir=artifacts_dir)
    assert entry_guard["ok"] is False
    assert "lifecycle step generated_at before lifecycle start: execution" in entry_guard["problems"]
    assert "lifecycle step generated_at sequence mismatch: execution" in entry_guard["problems"]

    api_payload = json.loads(dashboard_module._build_primary_result_api_body(tmp_path).decode("utf-8"))
    assert api_payload["disabled_reason"] == "stock entry guard blocked primary result publication."
    assert api_payload["history_generation_mode"] == "blocked"
    assert "lifecycle step generated_at before lifecycle start: execution" in api_payload["entry_guard"]["problems"]
    assert "lifecycle step generated_at sequence mismatch: execution" in api_payload["entry_guard"]["problems"]


def test_main_chain_authenticity_fail_closes_when_run_registry_current_stays_old_but_lifecycle_evidence_matches_new_run(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    exp_dir = _write_minimal_experiment_inputs(tmp_path)
    _patch_runtime_paths(monkeypatch, tmp_path)

    exit_code, lifecycle_payload = run_primary_result_lifecycle(
        exp_dir=exp_dir,
        max_source_age_hours=100000,
        observation_window_start="2026-04-15T07:40:00Z",
        now=datetime(2026, 4, 15, 7, 40, 20, tzinfo=timezone.utc),
    )
    assert exit_code == 0
    assert lifecycle_payload["status"] == "passed"

    artifacts_dir = tmp_path / "artifacts"
    run_registry = RunRegistry(runs_dir=artifacts_dir / "run_registry")
    run_registry.register(
        run_id="run-old",
        run_type="primary_result_lifecycle",
        producer="test",
        config_hash="cfg-old",
        data_snapshot_id="data-old",
        code_revision="rev-old",
        make_current=False,
    )
    run_current_path = artifacts_dir / "run_registry" / "current.json"
    run_current = json.loads(run_current_path.read_text(encoding="utf-8"))
    run_current["run_id"] = "run-old"
    run_current["entry_path"] = str(artifacts_dir / "run_registry" / "history" / "run-old.json")
    run_current_path.write_text(json.dumps(run_current, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    integrity_exit, integrity_payload = check_current_result_pointer_integrity(
        pointer_dir=artifacts_dir / "current_result_pointer",
        results_dir=artifacts_dir / "result_registry",
        runs_dir=artifacts_dir / "run_registry",
        artifact_registry_path=artifacts_dir / "artifact_registry.jsonl",
    )
    assert integrity_exit == 1
    assert "run registry current pointer does not match current result pointer run_id" in integrity_payload["problems"]
    assert "run registry current pointer history entry does not match current result pointer run_id" in integrity_payload["problems"]

    entry_guard = evaluate_stock_entry_guard(exp_dir=exp_dir, artifacts_dir=artifacts_dir)
    assert entry_guard["ok"] is False
    assert "run registry current pointer does not match current result pointer run_id" in entry_guard["problems"]
    assert "run registry current pointer history entry does not match current result pointer run_id" in entry_guard["problems"]

    api_payload = json.loads(dashboard_module._build_primary_result_api_body(tmp_path).decode("utf-8"))
    assert api_payload["disabled_reason"] == "stock entry guard blocked primary result publication."
    assert api_payload["history_generation_mode"] == "blocked"
    assert "run registry current pointer does not match current result pointer run_id" in api_payload["entry_guard"]["problems"]
    assert "run registry current pointer history entry does not match current result pointer run_id" in api_payload["entry_guard"]["problems"]


def test_main_chain_authenticity_fail_closes_when_non_main_chain_run_hijacks_run_registry_current(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    exp_dir = _write_minimal_experiment_inputs(tmp_path)
    _patch_runtime_paths(monkeypatch, tmp_path)

    exit_code, lifecycle_payload = run_primary_result_lifecycle(
        exp_dir=exp_dir,
        max_source_age_hours=100000,
        observation_window_start="2026-04-15T07:40:00Z",
        now=datetime(2026, 4, 15, 7, 40, 20, tzinfo=timezone.utc),
    )
    assert exit_code == 0
    assert lifecycle_payload["status"] == "passed"

    artifacts_dir = tmp_path / "artifacts"
    run_registry = RunRegistry(runs_dir=artifacts_dir / "run_registry")
    run_registry.register(
        run_id="run-sidecar",
        run_type="daily_research",
        producer="test",
        config_hash="cfg-sidecar",
        data_snapshot_id="data-sidecar",
        code_revision="rev-sidecar",
        make_current=False,
    )
    run_current_path = artifacts_dir / "run_registry" / "current.json"
    run_current = json.loads(run_current_path.read_text(encoding="utf-8"))
    run_current["run_id"] = "run-sidecar"
    run_current["run_type"] = "daily_research"
    run_current["entry_path"] = str(artifacts_dir / "run_registry" / "history" / "run-sidecar.json")
    run_current_path.write_text(json.dumps(run_current, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    integrity_exit, integrity_payload = check_current_result_pointer_integrity(
        pointer_dir=artifacts_dir / "current_result_pointer",
        results_dir=artifacts_dir / "result_registry",
        runs_dir=artifacts_dir / "run_registry",
        artifact_registry_path=artifacts_dir / "artifact_registry.jsonl",
    )
    assert integrity_exit == 1
    assert "run registry current pointer diverged to a non-main-chain run while formal main-chain pointer remains active" in integrity_payload["problems"]

    entry_guard = evaluate_stock_entry_guard(exp_dir=exp_dir, artifacts_dir=artifacts_dir)
    assert entry_guard["ok"] is False
    assert "run registry current pointer diverged to a non-main-chain run while formal main-chain pointer remains active" in entry_guard["problems"]

    api_payload = json.loads(dashboard_module._build_primary_result_api_body(tmp_path).decode("utf-8"))
    assert api_payload["disabled_reason"] == "stock entry guard blocked primary result publication."
    assert "run registry current pointer diverged to a non-main-chain run while formal main-chain pointer remains active" in api_payload["entry_guard"]["problems"]


def test_main_chain_authenticity_fail_closes_when_lifecycle_evidence_started_completed_bounds_are_tampered(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    exp_dir = _write_minimal_experiment_inputs(tmp_path)
    _patch_runtime_paths(monkeypatch, tmp_path)

    exit_code, lifecycle_payload = run_primary_result_lifecycle(
        exp_dir=exp_dir,
        max_source_age_hours=100000,
        observation_window_start="2026-04-15T07:40:00Z",
        now=datetime(2026, 4, 15, 7, 40, 20, tzinfo=timezone.utc),
    )
    assert exit_code == 0
    assert lifecycle_payload["status"] == "passed"

    artifacts_dir = tmp_path / "artifacts"
    lifecycle_evidence_path = exp_dir / "primary_result_lifecycle_evidence_latest.json"
    lifecycle_evidence = json.loads(lifecycle_evidence_path.read_text(encoding="utf-8"))
    lifecycle_evidence["started_at"] = "2026-04-15T07:40:30Z"
    lifecycle_evidence["completed_at"] = "2026-04-15T07:40:10Z"
    lifecycle_evidence_path.write_text(json.dumps(lifecycle_evidence, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    artifact_registry_path = artifacts_dir / "artifact_registry.jsonl"
    registry_lines = []
    for line in artifact_registry_path.read_text(encoding="utf-8").splitlines():
        payload = json.loads(line)
        if payload.get("artifact_id") == f"{lifecycle_payload['run_id']}:lifecycle-evidence":
            payload["sha256"] = sha256_file(lifecycle_evidence_path)
        registry_lines.append(json.dumps(payload, ensure_ascii=False, sort_keys=True))
    artifact_registry_path.write_text("\n".join(registry_lines) + "\n", encoding="utf-8")

    integrity_exit, integrity_payload = check_current_result_pointer_integrity(
        pointer_dir=artifacts_dir / "current_result_pointer",
        results_dir=artifacts_dir / "result_registry",
        runs_dir=artifacts_dir / "run_registry",
        artifact_registry_path=artifact_registry_path,
    )
    assert integrity_exit == 1
    assert "result registry current updated_at does not match lifecycle evidence completed_at" in integrity_payload["problems"]
    assert "current result pointer updated_at does not match lifecycle evidence completed_at" in integrity_payload["problems"]

    entry_guard = evaluate_stock_entry_guard(exp_dir=exp_dir, artifacts_dir=artifacts_dir)
    assert entry_guard["ok"] is False
    assert "lifecycle evidence started_at is after completed_at" in entry_guard["problems"]
    assert "lifecycle step generated_at before lifecycle start: audit" in entry_guard["problems"]
    assert "lifecycle step generated_at after lifecycle completion: audit" in entry_guard["problems"]

    api_payload = json.loads(dashboard_module._build_primary_result_api_body(tmp_path).decode("utf-8"))
    assert api_payload["disabled_reason"] == "stock entry guard blocked primary result publication."
    assert api_payload["history_generation_mode"] == "blocked"
    assert "lifecycle evidence started_at is after completed_at" in api_payload["entry_guard"]["problems"]
    assert "lifecycle step generated_at before lifecycle start: audit" in api_payload["entry_guard"]["problems"]
    assert "lifecycle step generated_at after lifecycle completion: audit" in api_payload["entry_guard"]["problems"]


def test_main_chain_authenticity_fail_closes_when_temporal_progression_drift_occurs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    exp_dir = _write_minimal_experiment_inputs(tmp_path)
    _patch_runtime_paths(monkeypatch, tmp_path)

    exit_code, lifecycle_payload = run_primary_result_lifecycle(
        exp_dir=exp_dir,
        max_source_age_hours=100000,
        observation_window_start="2026-04-15T07:40:00Z",
        now=datetime(2026, 4, 15, 7, 40, 20, tzinfo=timezone.utc),
    )
    assert exit_code == 0
    assert lifecycle_payload["status"] == "passed"

    artifacts_dir = tmp_path / "artifacts"
    run_current_path = artifacts_dir / "run_registry" / "current.json"
    run_current = json.loads(run_current_path.read_text(encoding="utf-8"))
    run_current["updated_at"] = "2026-05-01T08:00:00Z"
    run_current_path.write_text(json.dumps(run_current, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    result_current_path = artifacts_dir / "result_registry" / "current.json"
    result_current = json.loads(result_current_path.read_text(encoding="utf-8"))
    result_current["updated_at"] = "2026-05-01T07:30:00Z"
    result_current_path.write_text(json.dumps(result_current, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    pointer_current_path = artifacts_dir / "current_result_pointer" / "current.json"
    pointer_current = json.loads(pointer_current_path.read_text(encoding="utf-8"))
    pointer_current["updated_at"] = "2026-05-01T07:45:00Z"
    pointer_current_path.write_text(json.dumps(pointer_current, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    lifecycle_evidence_path = exp_dir / "primary_result_lifecycle_evidence_latest.json"
    lifecycle_evidence = json.loads(lifecycle_evidence_path.read_text(encoding="utf-8"))
    lifecycle_evidence["completed_at"] = "2026-05-01T07:00:00Z"
    lifecycle_evidence_path.write_text(json.dumps(lifecycle_evidence, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    artifact_registry_path = artifacts_dir / "artifact_registry.jsonl"
    registry_lines = []
    for line in artifact_registry_path.read_text(encoding="utf-8").splitlines():
        payload = json.loads(line)
        if payload.get("artifact_id") == f"{lifecycle_payload['run_id']}:lifecycle-evidence":
            payload["sha256"] = sha256_file(lifecycle_evidence_path)
        registry_lines.append(json.dumps(payload, ensure_ascii=False, sort_keys=True))
    artifact_registry_path.write_text("\n".join(registry_lines) + "\n", encoding="utf-8")

    integrity_exit, integrity_payload = check_current_result_pointer_integrity(
        pointer_dir=artifacts_dir / "current_result_pointer",
        results_dir=artifacts_dir / "result_registry",
        runs_dir=artifacts_dir / "run_registry",
        artifact_registry_path=artifact_registry_path,
    )
    assert integrity_exit == 1
    assert "run registry current updated_at is after result registry current updated_at" in integrity_payload["problems"]
    assert "result registry current updated_at does not match current result pointer updated_at" in integrity_payload["problems"]
    assert "result registry current updated_at does not match lifecycle evidence completed_at" in integrity_payload["problems"]
    assert "current result pointer updated_at does not match lifecycle evidence completed_at" in integrity_payload["problems"]
    assert "run registry current updated_at is after lifecycle evidence completed_at" in integrity_payload["problems"]

    entry_guard = evaluate_stock_entry_guard(exp_dir=exp_dir, artifacts_dir=artifacts_dir)
    assert entry_guard["ok"] is False
    assert "run registry current updated_at is after result registry current updated_at" in entry_guard["problems"]
    assert "result registry current updated_at does not match current result pointer updated_at" in entry_guard["problems"]
    assert "result registry current updated_at does not match lifecycle evidence completed_at" in entry_guard["problems"]
    assert "current result pointer updated_at does not match lifecycle evidence completed_at" in entry_guard["problems"]
    assert "run registry current updated_at is after lifecycle evidence completed_at" in entry_guard["problems"]

    api_payload = json.loads(dashboard_module._build_primary_result_api_body(tmp_path).decode("utf-8"))
    assert api_payload["disabled_reason"] == "stock entry guard blocked primary result publication."
    assert "run registry current updated_at is after result registry current updated_at" in api_payload["entry_guard"]["problems"]
    assert "result registry current updated_at does not match current result pointer updated_at" in api_payload["entry_guard"]["problems"]
    assert "result registry current updated_at does not match lifecycle evidence completed_at" in api_payload["entry_guard"]["problems"]
    assert "current result pointer updated_at does not match lifecycle evidence completed_at" in api_payload["entry_guard"]["problems"]
    assert "run registry current updated_at is after lifecycle evidence completed_at" in api_payload["entry_guard"]["problems"]


def test_main_chain_authenticity_fail_closes_when_run_registry_current_run_type_drifts_from_main_chain_lifecycle(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    exp_dir = _write_minimal_experiment_inputs(tmp_path)
    _patch_runtime_paths(monkeypatch, tmp_path)

    exit_code, lifecycle_payload = run_primary_result_lifecycle(
        exp_dir=exp_dir,
        max_source_age_hours=100000,
        observation_window_start="2026-04-15T07:40:00Z",
        now=datetime(2026, 4, 15, 7, 40, 20, tzinfo=timezone.utc),
    )
    assert exit_code == 0
    assert lifecycle_payload["status"] == "passed"

    artifacts_dir = tmp_path / "artifacts"
    run_current_path = artifacts_dir / "run_registry" / "current.json"
    run_current = json.loads(run_current_path.read_text(encoding="utf-8"))
    run_current["run_type"] = "daily_research"
    run_current_path.write_text(json.dumps(run_current, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    integrity_exit, integrity_payload = check_current_result_pointer_integrity(
        pointer_dir=artifacts_dir / "current_result_pointer",
        results_dir=artifacts_dir / "result_registry",
        runs_dir=artifacts_dir / "run_registry",
        artifact_registry_path=artifacts_dir / "artifact_registry.jsonl",
    )
    assert integrity_exit == 1
    assert "run registry current pointer run_type does not match main chain lifecycle type" in integrity_payload["problems"]

    entry_guard = evaluate_stock_entry_guard(exp_dir=exp_dir, artifacts_dir=artifacts_dir)
    assert entry_guard["ok"] is False
    assert "run registry current pointer run_type does not match main chain lifecycle type" in entry_guard["problems"]

    api_payload = json.loads(dashboard_module._build_primary_result_api_body(tmp_path).decode("utf-8"))
    assert api_payload["disabled_reason"] == "stock entry guard blocked primary result publication."
    assert api_payload["history_generation_mode"] == "blocked"
    assert "run registry current pointer run_type does not match main chain lifecycle type" in api_payload["entry_guard"]["problems"]


def test_main_chain_authenticity_fail_closes_when_run_registry_history_metadata_drifts_from_pointer_and_lifecycle_identity(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    exp_dir = _write_minimal_experiment_inputs(tmp_path)
    _patch_runtime_paths(monkeypatch, tmp_path)

    exit_code, lifecycle_payload = run_primary_result_lifecycle(
        exp_dir=exp_dir,
        max_source_age_hours=100000,
        observation_window_start="2026-04-15T07:40:00Z",
        now=datetime(2026, 4, 15, 7, 40, 20, tzinfo=timezone.utc),
    )
    assert exit_code == 0
    assert lifecycle_payload["status"] == "passed"

    artifacts_dir = tmp_path / "artifacts"
    run_history_path = artifacts_dir / "run_registry" / "history" / f"{lifecycle_payload['run_id']}.json"
    run_history = json.loads(run_history_path.read_text(encoding="utf-8"))
    run_history["metadata"]["lifecycle_id"] = "lifecycle-old"
    run_history["metadata"]["result_id"] = "primary:999999.SZ"
    run_history["metadata"]["ts_code"] = "999999.SZ"
    run_history_path.write_text(json.dumps(run_history, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    integrity_exit, integrity_payload = check_current_result_pointer_integrity(
        pointer_dir=artifacts_dir / "current_result_pointer",
        results_dir=artifacts_dir / "result_registry",
        runs_dir=artifacts_dir / "run_registry",
        artifact_registry_path=artifacts_dir / "artifact_registry.jsonl",
    )
    assert integrity_exit == 1
    assert "run registry history metadata lifecycle_id does not match current result pointer" in integrity_payload["problems"]
    assert "run registry history metadata result_id does not match current result pointer" in integrity_payload["problems"]
    assert "run registry history metadata ts_code does not match result registry current ts_code" in integrity_payload["problems"]

    entry_guard = evaluate_stock_entry_guard(exp_dir=exp_dir, artifacts_dir=artifacts_dir)
    assert entry_guard["ok"] is False
    assert "run registry history metadata lifecycle_id does not match current result pointer" in entry_guard["problems"]
    assert "run registry history metadata result_id does not match current result pointer" in entry_guard["problems"]
    assert "run registry history metadata ts_code does not match result registry current ts_code" in entry_guard["problems"]

    api_payload = json.loads(dashboard_module._build_primary_result_api_body(tmp_path).decode("utf-8"))
    assert api_payload["disabled_reason"] == "stock entry guard blocked primary result publication."
    assert api_payload["history_generation_mode"] == "blocked"
    assert "run registry history metadata lifecycle_id does not match current result pointer" in api_payload["entry_guard"]["problems"]
    assert "run registry history metadata result_id does not match current result pointer" in api_payload["entry_guard"]["problems"]
    assert "run registry history metadata ts_code does not match result registry current ts_code" in api_payload["entry_guard"]["problems"]


def test_main_chain_authenticity_fail_closes_when_final_stage_claim_does_not_match_steps_chain(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    exp_dir = _write_minimal_experiment_inputs(tmp_path)
    _patch_runtime_paths(monkeypatch, tmp_path)

    exit_code, lifecycle_payload = run_primary_result_lifecycle(
        exp_dir=exp_dir,
        max_source_age_hours=100000,
        observation_window_start="2026-04-15T07:40:00Z",
        now=datetime(2026, 4, 15, 7, 40, 20, tzinfo=timezone.utc),
    )
    assert exit_code == 0
    assert lifecycle_payload["status"] == "passed"

    artifacts_dir = tmp_path / "artifacts"
    lifecycle_evidence_path = exp_dir / "primary_result_lifecycle_evidence_latest.json"
    lifecycle_evidence = json.loads(lifecycle_evidence_path.read_text(encoding="utf-8"))
    lifecycle_evidence["final_payload"]["result_lifecycle_stage"] = "L5"
    lifecycle_evidence_path.write_text(json.dumps(lifecycle_evidence, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    artifact_registry_path = artifacts_dir / "artifact_registry.jsonl"
    registry_lines = []
    for line in artifact_registry_path.read_text(encoding="utf-8").splitlines():
        payload = json.loads(line)
        if payload.get("artifact_id") == f"{lifecycle_payload['run_id']}:lifecycle-evidence":
            payload["sha256"] = sha256_file(lifecycle_evidence_path)
        registry_lines.append(json.dumps(payload, ensure_ascii=False, sort_keys=True))
    artifact_registry_path.write_text("\n".join(registry_lines) + "\n", encoding="utf-8")

    result_current_path = artifacts_dir / "result_registry" / "current.json"
    result_current = json.loads(result_current_path.read_text(encoding="utf-8"))
    result_history_path = Path(str(result_current["entry_path"]))
    result_history = json.loads(result_history_path.read_text(encoding="utf-8"))
    result_history["lifecycle_stage"] = "L5"
    result_history_path.write_text(json.dumps(result_history, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    integrity_exit, integrity_payload = check_current_result_pointer_integrity(
        pointer_dir=artifacts_dir / "current_result_pointer",
        results_dir=artifacts_dir / "result_registry",
        runs_dir=artifacts_dir / "run_registry",
        artifact_registry_path=artifact_registry_path,
    )
    assert integrity_exit == 1
    assert "artifact_ids order/types do not match lifecycle stage sequence" in integrity_payload["problems"]

    entry_guard = evaluate_stock_entry_guard(exp_dir=exp_dir, artifacts_dir=artifacts_dir)
    assert entry_guard["ok"] is False
    assert "lifecycle evidence final_payload result_lifecycle_stage does not match steps chain" in entry_guard["problems"]

    api_payload = json.loads(dashboard_module._build_primary_result_api_body(tmp_path).decode("utf-8"))
    assert api_payload["disabled_reason"] == "stock entry guard blocked primary result publication."
    assert api_payload["history_generation_mode"] == "blocked"
    assert "lifecycle evidence final_payload result_lifecycle_stage does not match steps chain" in api_payload["entry_guard"]["problems"]


def test_main_chain_authenticity_fail_closes_when_result_registry_metadata_and_pointer_snapshot_record_id_drift(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    exp_dir = _write_minimal_experiment_inputs(tmp_path)
    _patch_runtime_paths(monkeypatch, tmp_path)

    exit_code, lifecycle_payload = run_primary_result_lifecycle(
        exp_dir=exp_dir,
        max_source_age_hours=100000,
        observation_window_start="2026-04-15T07:40:00Z",
        now=datetime(2026, 4, 15, 7, 40, 20, tzinfo=timezone.utc),
    )
    assert exit_code == 0
    assert lifecycle_payload["status"] == "passed"

    artifacts_dir = tmp_path / "artifacts"
    result_current_path = artifacts_dir / "result_registry" / "current.json"
    result_current = json.loads(result_current_path.read_text(encoding="utf-8"))
    result_history_path = Path(str(result_current["entry_path"]))
    result_history = json.loads(result_history_path.read_text(encoding="utf-8"))
    result_history["metadata"]["lifecycle_id"] = "lifecycle-old"
    result_history["metadata"]["evidence_path"] = str(exp_dir / "wrong_lifecycle_evidence.json")
    result_history_path.write_text(json.dumps(result_history, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    pointer_current_path = artifacts_dir / "current_result_pointer" / "current.json"
    pointer_current = json.loads(pointer_current_path.read_text(encoding="utf-8"))
    pointer_snapshot_path = Path(str(pointer_current["snapshot_path"]))
    pointer_snapshot = json.loads(pointer_snapshot_path.read_text(encoding="utf-8"))
    pointer_snapshot["metadata"]["result_registry_record_id"] = "result-record-old"
    pointer_snapshot_path.write_text(json.dumps(pointer_snapshot, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    integrity_exit, integrity_payload = check_current_result_pointer_integrity(
        pointer_dir=artifacts_dir / "current_result_pointer",
        results_dir=artifacts_dir / "result_registry",
        runs_dir=artifacts_dir / "run_registry",
        artifact_registry_path=artifacts_dir / "artifact_registry.jsonl",
    )
    assert integrity_exit == 1
    assert "current result pointer snapshot metadata result_registry_record_id does not match result registry current record" in integrity_payload["problems"]
    assert "result registry metadata lifecycle_id does not match current result pointer" in integrity_payload["problems"]

    entry_guard = evaluate_stock_entry_guard(exp_dir=exp_dir, artifacts_dir=artifacts_dir)
    assert entry_guard["ok"] is False
    assert "result registry metadata lifecycle_id does not match current result pointer" in entry_guard["problems"]
    assert "result registry metadata evidence_path does not match primary_result_lifecycle_evidence_latest.json" in entry_guard["problems"]

    api_payload = json.loads(dashboard_module._build_primary_result_api_body(tmp_path).decode("utf-8"))
    assert api_payload["disabled_reason"] == "stock entry guard blocked primary result publication."
    assert api_payload["history_generation_mode"] == "blocked"
    assert "result registry metadata lifecycle_id does not match current result pointer" in api_payload["entry_guard"]["problems"]
    assert "result registry metadata evidence_path does not match primary_result_lifecycle_evidence_latest.json" in api_payload["entry_guard"]["problems"]


def test_main_chain_authenticity_fail_closes_when_steps_to_artifact_ids_mapping_is_scrambled(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    exp_dir = _write_minimal_experiment_inputs(tmp_path)
    _patch_runtime_paths(monkeypatch, tmp_path)

    exit_code, lifecycle_payload = run_primary_result_lifecycle(
        exp_dir=exp_dir,
        max_source_age_hours=100000,
        observation_window_start="2026-04-15T07:40:00Z",
        now=datetime(2026, 4, 15, 7, 40, 20, tzinfo=timezone.utc),
    )
    assert exit_code == 0
    assert lifecycle_payload["status"] == "passed"

    artifacts_dir = tmp_path / "artifacts"
    artifact_registry_path = artifacts_dir / "artifact_registry.jsonl"
    lifecycle_evidence_path = exp_dir / "primary_result_lifecycle_evidence_latest.json"
    lifecycle_evidence = json.loads(lifecycle_evidence_path.read_text(encoding="utf-8"))
    audit_step = next(step for step in lifecycle_evidence["steps"] if step["step"] == "audit")
    execution_step = next(step for step in lifecycle_evidence["steps"] if step["step"] == "execution")
    audit_step["path"], execution_step["path"] = execution_step["path"], audit_step["path"]
    audit_step["sha256"], execution_step["sha256"] = execution_step["sha256"], audit_step["sha256"]
    lifecycle_evidence_path.write_text(json.dumps(lifecycle_evidence, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    registry_lines = []
    for line in artifact_registry_path.read_text(encoding="utf-8").splitlines():
        payload = json.loads(line)
        if payload.get("artifact_id") == f"{lifecycle_payload['run_id']}:lifecycle-evidence":
            payload["sha256"] = sha256_file(lifecycle_evidence_path)
        registry_lines.append(json.dumps(payload, ensure_ascii=False, sort_keys=True))
    artifact_registry_path.write_text("\n".join(registry_lines) + "\n", encoding="utf-8")

    integrity_exit, integrity_payload = check_current_result_pointer_integrity(
        pointer_dir=artifacts_dir / "current_result_pointer",
        results_dir=artifacts_dir / "result_registry",
        runs_dir=artifacts_dir / "run_registry",
        artifact_registry_path=artifact_registry_path,
    )
    assert integrity_exit == 0, integrity_payload
    assert integrity_payload["ok"] is True

    entry_guard = evaluate_stock_entry_guard(exp_dir=exp_dir, artifacts_dir=artifacts_dir)
    assert entry_guard["ok"] is False
    assert "lifecycle step artifact mapping path mismatch: audit" in entry_guard["problems"]
    assert "lifecycle step artifact mapping path mismatch: execution" in entry_guard["problems"]
    assert "lifecycle step artifact mapping sha256 mismatch: audit" in entry_guard["problems"]
    assert "lifecycle step artifact mapping sha256 mismatch: execution" in entry_guard["problems"]

    api_payload = json.loads(dashboard_module._build_primary_result_api_body(tmp_path).decode("utf-8"))
    assert api_payload["disabled_reason"] == "stock entry guard blocked primary result publication."
    assert api_payload["history_generation_mode"] == "blocked"
    assert "lifecycle step artifact mapping path mismatch: audit" in api_payload["entry_guard"]["problems"]
    assert "lifecycle step artifact mapping path mismatch: execution" in api_payload["entry_guard"]["problems"]


def test_main_chain_authenticity_fail_closes_when_result_registry_metadata_artifact_registry_path_and_status_drift(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    exp_dir = _write_minimal_experiment_inputs(tmp_path)
    _patch_runtime_paths(monkeypatch, tmp_path)

    exit_code, lifecycle_payload = run_primary_result_lifecycle(
        exp_dir=exp_dir,
        max_source_age_hours=100000,
        observation_window_start="2026-04-15T07:40:00Z",
        now=datetime(2026, 4, 15, 7, 40, 20, tzinfo=timezone.utc),
    )
    assert exit_code == 0
    assert lifecycle_payload["status"] == "passed"

    artifacts_dir = tmp_path / "artifacts"
    result_current_path = artifacts_dir / "result_registry" / "current.json"
    result_current = json.loads(result_current_path.read_text(encoding="utf-8"))
    result_history_path = Path(str(result_current["entry_path"]))
    result_history = json.loads(result_history_path.read_text(encoding="utf-8"))
    result_history["metadata"]["artifact_registry_path"] = str(artifacts_dir / "artifact_registry.old.jsonl")
    result_history["metadata"]["lifecycle_status"] = "failed"
    result_history_path.write_text(json.dumps(result_history, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    integrity_exit, integrity_payload = check_current_result_pointer_integrity(
        pointer_dir=artifacts_dir / "current_result_pointer",
        results_dir=artifacts_dir / "result_registry",
        runs_dir=artifacts_dir / "run_registry",
        artifact_registry_path=artifacts_dir / "artifact_registry.jsonl",
    )
    assert integrity_exit == 1
    assert "result registry metadata artifact_registry_path does not match active artifact registry" in integrity_payload["problems"]

    entry_guard = evaluate_stock_entry_guard(exp_dir=exp_dir, artifacts_dir=artifacts_dir)
    assert entry_guard["ok"] is False
    assert "result registry metadata artifact_registry_path does not match active artifact registry" in entry_guard["problems"]
    assert "result registry metadata lifecycle_status does not match lifecycle evidence status" in entry_guard["problems"]

    api_payload = json.loads(dashboard_module._build_primary_result_api_body(tmp_path).decode("utf-8"))
    assert api_payload["disabled_reason"] == "stock entry guard blocked primary result publication."
    assert api_payload["history_generation_mode"] == "blocked"
    assert "result registry metadata artifact_registry_path does not match active artifact registry" in api_payload["entry_guard"]["problems"]
    assert "result registry metadata lifecycle_status does not match lifecycle evidence status" in api_payload["entry_guard"]["problems"]


def test_main_chain_authenticity_fail_closes_when_lifecycle_evidence_parent_artifact_chain_is_tampered(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    exp_dir = _write_minimal_experiment_inputs(tmp_path)
    _patch_runtime_paths(monkeypatch, tmp_path)

    exit_code, lifecycle_payload = run_primary_result_lifecycle(
        exp_dir=exp_dir,
        max_source_age_hours=100000,
        observation_window_start="2026-04-15T07:40:00Z",
        now=datetime(2026, 4, 15, 7, 40, 20, tzinfo=timezone.utc),
    )
    assert exit_code == 0
    assert lifecycle_payload["status"] == "passed"

    artifacts_dir = tmp_path / "artifacts"
    artifact_registry_path = artifacts_dir / "artifact_registry.jsonl"
    registry_lines = []
    for line in artifact_registry_path.read_text(encoding="utf-8").splitlines():
        payload = json.loads(line)
        if payload.get("artifact_id") == f"{lifecycle_payload['run_id']}:lifecycle-evidence":
            parents = list(payload.get("parent_artifact_ids") or [])
            if len(parents) >= 2:
                parents[0], parents[1] = parents[1], parents[0]
            payload["parent_artifact_ids"] = parents
        registry_lines.append(json.dumps(payload, ensure_ascii=False, sort_keys=True))
    artifact_registry_path.write_text("\n".join(registry_lines) + "\n", encoding="utf-8")

    integrity_exit, integrity_payload = check_current_result_pointer_integrity(
        pointer_dir=artifacts_dir / "current_result_pointer",
        results_dir=artifacts_dir / "result_registry",
        runs_dir=artifacts_dir / "run_registry",
        artifact_registry_path=artifact_registry_path,
    )
    assert integrity_exit == 0, integrity_payload
    assert integrity_payload["ok"] is True

    entry_guard = evaluate_stock_entry_guard(exp_dir=exp_dir, artifacts_dir=artifacts_dir)
    assert entry_guard["ok"] is False
    assert "lifecycle evidence parent_artifact_ids do not match current step artifact chain" in entry_guard["problems"]
    assert "lifecycle evidence parent_artifact_ids do not match final_payload step artifact chain" in entry_guard["problems"]

    api_payload = json.loads(dashboard_module._build_primary_result_api_body(tmp_path).decode("utf-8"))
    assert api_payload["disabled_reason"] == "stock entry guard blocked primary result publication."
    assert api_payload["history_generation_mode"] == "blocked"
    assert "lifecycle evidence parent_artifact_ids do not match current step artifact chain" in api_payload["entry_guard"]["problems"]
    assert "lifecycle evidence parent_artifact_ids do not match final_payload step artifact chain" in api_payload["entry_guard"]["problems"]


def test_main_chain_authenticity_fail_closes_when_pointer_snapshot_metadata_paths_drift(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    exp_dir = _write_minimal_experiment_inputs(tmp_path)
    _patch_runtime_paths(monkeypatch, tmp_path)

    exit_code, lifecycle_payload = run_primary_result_lifecycle(
        exp_dir=exp_dir,
        max_source_age_hours=100000,
        observation_window_start="2026-04-15T07:40:00Z",
        now=datetime(2026, 4, 15, 7, 40, 20, tzinfo=timezone.utc),
    )
    assert exit_code == 0
    assert lifecycle_payload["status"] == "passed"

    artifacts_dir = tmp_path / "artifacts"
    pointer_current_path = artifacts_dir / "current_result_pointer" / "current.json"
    pointer_current = json.loads(pointer_current_path.read_text(encoding="utf-8"))
    pointer_snapshot_path = Path(str(pointer_current["snapshot_path"]))
    pointer_snapshot = json.loads(pointer_snapshot_path.read_text(encoding="utf-8"))
    pointer_snapshot["metadata"]["evidence_path"] = str(exp_dir / "wrong_lifecycle_evidence.json")
    pointer_snapshot["metadata"]["artifact_registry_path"] = str(artifacts_dir / "artifact_registry.old.jsonl")
    pointer_snapshot_path.write_text(json.dumps(pointer_snapshot, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    integrity_exit, integrity_payload = check_current_result_pointer_integrity(
        pointer_dir=artifacts_dir / "current_result_pointer",
        results_dir=artifacts_dir / "result_registry",
        runs_dir=artifacts_dir / "run_registry",
        artifact_registry_path=artifacts_dir / "artifact_registry.jsonl",
    )
    assert integrity_exit == 1
    assert "current result pointer snapshot metadata evidence_path does not match result registry current evidence_path" in integrity_payload["problems"]
    assert "current result pointer snapshot metadata artifact_registry_path does not match active artifact registry" in integrity_payload["problems"]

    entry_guard = evaluate_stock_entry_guard(exp_dir=exp_dir, artifacts_dir=artifacts_dir)
    assert entry_guard["ok"] is False
    assert "current result pointer snapshot metadata evidence_path does not match primary_result_lifecycle_evidence_latest.json" in entry_guard["problems"]
    assert "current result pointer snapshot metadata artifact_registry_path does not match active artifact registry" in entry_guard["problems"]

    api_payload = json.loads(dashboard_module._build_primary_result_api_body(tmp_path).decode("utf-8"))
    assert api_payload["disabled_reason"] == "stock entry guard blocked primary result publication."
    assert api_payload["history_generation_mode"] == "blocked"
    assert "current result pointer snapshot metadata evidence_path does not match primary_result_lifecycle_evidence_latest.json" in api_payload["entry_guard"]["problems"]
    assert "current result pointer snapshot metadata artifact_registry_path does not match active artifact registry" in api_payload["entry_guard"]["problems"]


def test_main_chain_authenticity_fail_closes_when_lifecycle_evidence_artifact_metadata_drift(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    exp_dir = _write_minimal_experiment_inputs(tmp_path)
    _patch_runtime_paths(monkeypatch, tmp_path)

    exit_code, lifecycle_payload = run_primary_result_lifecycle(
        exp_dir=exp_dir,
        max_source_age_hours=100000,
        observation_window_start="2026-04-15T07:40:00Z",
        now=datetime(2026, 4, 15, 7, 40, 20, tzinfo=timezone.utc),
    )
    assert exit_code == 0
    assert lifecycle_payload["status"] == "passed"

    artifacts_dir = tmp_path / "artifacts"
    artifact_registry_path = artifacts_dir / "artifact_registry.jsonl"
    registry_lines = []
    for line in artifact_registry_path.read_text(encoding="utf-8").splitlines():
        payload = json.loads(line)
        if payload.get("artifact_id") == f"{lifecycle_payload['run_id']}:lifecycle-evidence":
            metadata = dict(payload.get("metadata") or {})
            metadata["status"] = "failed"
            metadata["ts_code"] = "999999.SZ"
            metadata["lifecycle_id"] = "lifecycle-old"
            payload["metadata"] = metadata
        registry_lines.append(json.dumps(payload, ensure_ascii=False, sort_keys=True))
    artifact_registry_path.write_text("\n".join(registry_lines) + "\n", encoding="utf-8")

    integrity_exit, integrity_payload = check_current_result_pointer_integrity(
        pointer_dir=artifacts_dir / "current_result_pointer",
        results_dir=artifacts_dir / "result_registry",
        runs_dir=artifacts_dir / "run_registry",
        artifact_registry_path=artifact_registry_path,
    )
    assert integrity_exit == 0, integrity_payload
    assert integrity_payload["ok"] is True

    entry_guard = evaluate_stock_entry_guard(exp_dir=exp_dir, artifacts_dir=artifacts_dir)
    assert entry_guard["ok"] is False
    assert "lifecycle evidence artifact metadata status does not match lifecycle evidence status" in entry_guard["problems"]
    assert "lifecycle evidence artifact metadata ts_code does not match result registry current ts_code" in entry_guard["problems"]
    assert "lifecycle evidence artifact metadata lifecycle_id does not match current result pointer" in entry_guard["problems"]

    api_payload = json.loads(dashboard_module._build_primary_result_api_body(tmp_path).decode("utf-8"))
    assert api_payload["disabled_reason"] == "stock entry guard blocked primary result publication."
    assert api_payload["history_generation_mode"] == "blocked"
    assert "lifecycle evidence artifact metadata status does not match lifecycle evidence status" in api_payload["entry_guard"]["problems"]
    assert "lifecycle evidence artifact metadata ts_code does not match result registry current ts_code" in api_payload["entry_guard"]["problems"]
    assert "lifecycle evidence artifact metadata lifecycle_id does not match current result pointer" in api_payload["entry_guard"]["problems"]


def test_main_chain_authenticity_fail_closes_when_formal_chain_carries_repair_semantics(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    exp_dir = _write_minimal_experiment_inputs(tmp_path)
    _patch_runtime_paths(monkeypatch, tmp_path)

    exit_code, lifecycle_payload = run_primary_result_lifecycle(
        exp_dir=exp_dir,
        max_source_age_hours=100000,
        observation_window_start="2026-04-15T07:40:00Z",
        now=datetime(2026, 4, 15, 7, 40, 20, tzinfo=timezone.utc),
    )
    assert exit_code == 0
    assert lifecycle_payload["status"] == "passed"

    artifacts_dir = tmp_path / "artifacts"
    result_current_path = artifacts_dir / "result_registry" / "current.json"
    result_current = json.loads(result_current_path.read_text(encoding="utf-8"))
    result_history_path = Path(str(result_current["entry_path"]))
    result_history = json.loads(result_history_path.read_text(encoding="utf-8"))
    result_history["metadata"]["repair_mode"] = "bootstrap"
    result_history_path.write_text(json.dumps(result_history, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    lifecycle_evidence_path = exp_dir / "primary_result_lifecycle_evidence_latest.json"
    lifecycle_evidence = json.loads(lifecycle_evidence_path.read_text(encoding="utf-8"))
    lifecycle_evidence["registry_chain"]["write_mode"] = "run_only_failed_lifecycle"
    lifecycle_evidence_path.write_text(json.dumps(lifecycle_evidence, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    artifact_registry_path = artifacts_dir / "artifact_registry.jsonl"
    registry_lines = []
    for line in artifact_registry_path.read_text(encoding="utf-8").splitlines():
        payload = json.loads(line)
        if payload.get("artifact_id") == f"{lifecycle_payload['run_id']}:lifecycle-evidence":
            payload["sha256"] = sha256_file(lifecycle_evidence_path)
        registry_lines.append(json.dumps(payload, ensure_ascii=False, sort_keys=True))
    artifact_registry_path.write_text("\n".join(registry_lines) + "\n", encoding="utf-8")

    integrity_exit, integrity_payload = check_current_result_pointer_integrity(
        pointer_dir=artifacts_dir / "current_result_pointer",
        results_dir=artifacts_dir / "result_registry",
        runs_dir=artifacts_dir / "run_registry",
        artifact_registry_path=artifact_registry_path,
    )
    assert integrity_exit == 1
    assert "result registry metadata repair_mode is not allowed for current formal main chain" in integrity_payload["problems"]

    entry_guard = evaluate_stock_entry_guard(exp_dir=exp_dir, artifacts_dir=artifacts_dir)
    assert entry_guard["ok"] is False
    assert "result registry metadata repair_mode is not allowed for current formal main chain" in entry_guard["problems"]
    assert "lifecycle evidence registry_chain write_mode is not allowed for current formal main chain" in entry_guard["problems"]

    api_payload = json.loads(dashboard_module._build_primary_result_api_body(tmp_path).decode("utf-8"))
    assert api_payload["disabled_reason"] == "stock entry guard blocked primary result publication."
    assert "result registry metadata repair_mode is not allowed for current formal main chain" in api_payload["entry_guard"]["problems"]
    assert "lifecycle evidence registry_chain write_mode is not allowed for current formal main chain" in api_payload["entry_guard"]["problems"]


def test_main_chain_authenticity_fail_closes_when_step_artifact_metadata_mirrors_drift(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    exp_dir = _write_minimal_experiment_inputs(tmp_path)
    _patch_runtime_paths(monkeypatch, tmp_path)

    exit_code, lifecycle_payload = run_primary_result_lifecycle(
        exp_dir=exp_dir,
        max_source_age_hours=100000,
        observation_window_start="2026-04-15T07:40:00Z",
        now=datetime(2026, 4, 15, 7, 40, 20, tzinfo=timezone.utc),
    )
    assert exit_code == 0
    assert lifecycle_payload["status"] == "passed"

    artifacts_dir = tmp_path / "artifacts"
    artifact_registry_path = artifacts_dir / "artifact_registry.jsonl"
    registry_lines = []
    for line in artifact_registry_path.read_text(encoding="utf-8").splitlines():
        payload = json.loads(line)
        if payload.get("artifact_id") == f"{lifecycle_payload['run_id']}:audit":
            metadata = dict(payload.get("metadata") or {})
            metadata["step"] = "execution"
            metadata["status"] = "failed"
            metadata["lifecycle_id"] = "lifecycle-old"
            metadata["ts_code"] = "999999.SZ"
            payload["metadata"] = metadata
        registry_lines.append(json.dumps(payload, ensure_ascii=False, sort_keys=True))
    artifact_registry_path.write_text("\n".join(registry_lines) + "\n", encoding="utf-8")

    integrity_exit, integrity_payload = check_current_result_pointer_integrity(
        pointer_dir=artifacts_dir / "current_result_pointer",
        results_dir=artifacts_dir / "result_registry",
        runs_dir=artifacts_dir / "run_registry",
        artifact_registry_path=artifact_registry_path,
    )
    assert integrity_exit == 0, integrity_payload
    assert integrity_payload["ok"] is True

    entry_guard = evaluate_stock_entry_guard(exp_dir=exp_dir, artifacts_dir=artifacts_dir)
    assert entry_guard["ok"] is False
    assert "lifecycle step artifact metadata step mismatch: audit" in entry_guard["problems"]
    assert "lifecycle step artifact metadata status mismatch: audit" in entry_guard["problems"]
    assert "lifecycle step artifact metadata lifecycle_id mismatch: audit" in entry_guard["problems"]
    assert "lifecycle step artifact metadata ts_code mismatch: audit" in entry_guard["problems"]

    api_payload = json.loads(dashboard_module._build_primary_result_api_body(tmp_path).decode("utf-8"))
    assert api_payload["disabled_reason"] == "stock entry guard blocked primary result publication."
    assert "lifecycle step artifact metadata step mismatch: audit" in api_payload["entry_guard"]["problems"]
    assert "lifecycle step artifact metadata status mismatch: audit" in api_payload["entry_guard"]["problems"]
    assert "lifecycle step artifact metadata lifecycle_id mismatch: audit" in api_payload["entry_guard"]["problems"]
    assert "lifecycle step artifact metadata ts_code mismatch: audit" in api_payload["entry_guard"]["problems"]


def test_main_chain_authenticity_fail_closes_when_run_registry_metadata_mirrors_drift(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    exp_dir = _write_minimal_experiment_inputs(tmp_path)
    _patch_runtime_paths(monkeypatch, tmp_path)

    exit_code, lifecycle_payload = run_primary_result_lifecycle(
        exp_dir=exp_dir,
        max_source_age_hours=100000,
        observation_window_start="2026-04-15T07:40:00Z",
        now=datetime(2026, 4, 15, 7, 40, 20, tzinfo=timezone.utc),
    )
    assert exit_code == 0
    assert lifecycle_payload["status"] == "passed"

    artifacts_dir = tmp_path / "artifacts"
    run_current_path = artifacts_dir / "run_registry" / "current.json"
    run_current = json.loads(run_current_path.read_text(encoding="utf-8"))
    run_history_path = Path(str(run_current["entry_path"]))
    run_history = json.loads(run_history_path.read_text(encoding="utf-8"))
    run_history["metadata"]["evidence_path"] = str(exp_dir / "wrong_lifecycle_evidence.json")
    run_history["metadata"]["artifact_registry_path"] = str(artifacts_dir / "artifact_registry.old.jsonl")
    run_history["metadata"]["repair_mode"] = "bootstrap"
    run_history_path.write_text(json.dumps(run_history, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    integrity_exit, integrity_payload = check_current_result_pointer_integrity(
        pointer_dir=artifacts_dir / "current_result_pointer",
        results_dir=artifacts_dir / "result_registry",
        runs_dir=artifacts_dir / "run_registry",
        artifact_registry_path=artifacts_dir / "artifact_registry.jsonl",
    )
    assert integrity_exit == 1
    assert "run registry history metadata evidence_path does not match result registry current evidence_path" in integrity_payload["problems"]
    assert "run registry history metadata artifact_registry_path does not match active artifact registry" in integrity_payload["problems"]
    assert "run registry history metadata repair_mode is not allowed for current formal main chain" in integrity_payload["problems"]

    entry_guard = evaluate_stock_entry_guard(exp_dir=exp_dir, artifacts_dir=artifacts_dir)
    assert entry_guard["ok"] is False
    assert "run registry history metadata evidence_path does not match result registry current evidence_path" in entry_guard["problems"]
    assert "run registry history metadata artifact_registry_path does not match active artifact registry" in entry_guard["problems"]
    assert "run registry history metadata repair_mode is not allowed for current formal main chain" in entry_guard["problems"]

    api_payload = json.loads(dashboard_module._build_primary_result_api_body(tmp_path).decode("utf-8"))
    assert api_payload["disabled_reason"] == "stock entry guard blocked primary result publication."
    assert "run registry history metadata evidence_path does not match result registry current evidence_path" in api_payload["entry_guard"]["problems"]
    assert "run registry history metadata artifact_registry_path does not match active artifact registry" in api_payload["entry_guard"]["problems"]
    assert "run registry history metadata repair_mode is not allowed for current formal main chain" in api_payload["entry_guard"]["problems"]


def test_main_chain_authenticity_fail_closes_when_registry_chain_embedded_snapshots_drift(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    exp_dir = _write_minimal_experiment_inputs(tmp_path)
    _patch_runtime_paths(monkeypatch, tmp_path)

    exit_code, lifecycle_payload = run_primary_result_lifecycle(
        exp_dir=exp_dir,
        max_source_age_hours=100000,
        observation_window_start="2026-04-15T07:40:00Z",
        now=datetime(2026, 4, 15, 7, 40, 20, tzinfo=timezone.utc),
    )
    assert exit_code == 0
    assert lifecycle_payload["status"] == "passed"

    artifacts_dir = tmp_path / "artifacts"
    lifecycle_evidence_path = exp_dir / "primary_result_lifecycle_evidence_latest.json"
    lifecycle_evidence = json.loads(lifecycle_evidence_path.read_text(encoding="utf-8"))
    lifecycle_evidence["registry_chain"]["result_registry"]["record_id"] = "result-record-old"
    lifecycle_evidence["registry_chain"]["result_registry"]["lifecycle_stage"] = "L5"
    lifecycle_evidence["registry_chain"]["current_result_pointer"]["pointer_snapshot_id"] = "pointer-old"
    lifecycle_evidence["registry_chain"]["current_result_pointer"]["run_id"] = "run-old"
    lifecycle_evidence["registry_chain"]["current_result_pointer"]["artifact_ids"] = ["artifact:old"]
    lifecycle_evidence["registry_chain"]["current_result_pointer"]["as_of_date"] = "2026-04-01"
    lifecycle_evidence_path.write_text(json.dumps(lifecycle_evidence, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    artifact_registry_path = artifacts_dir / "artifact_registry.jsonl"
    registry_lines = []
    for line in artifact_registry_path.read_text(encoding="utf-8").splitlines():
        payload = json.loads(line)
        if payload.get("artifact_id") == f"{lifecycle_payload['run_id']}:lifecycle-evidence":
            payload["sha256"] = sha256_file(lifecycle_evidence_path)
        registry_lines.append(json.dumps(payload, ensure_ascii=False, sort_keys=True))
    artifact_registry_path.write_text("\n".join(registry_lines) + "\n", encoding="utf-8")

    integrity_exit, integrity_payload = check_current_result_pointer_integrity(
        pointer_dir=artifacts_dir / "current_result_pointer",
        results_dir=artifacts_dir / "result_registry",
        runs_dir=artifacts_dir / "run_registry",
        artifact_registry_path=artifact_registry_path,
    )
    assert integrity_exit == 0, integrity_payload
    assert integrity_payload["ok"] is True

    entry_guard = evaluate_stock_entry_guard(exp_dir=exp_dir, artifacts_dir=artifacts_dir)
    assert entry_guard["ok"] is False
    assert "lifecycle evidence registry_chain result_registry record_id does not match active result registry current record" in entry_guard["problems"]
    assert "lifecycle evidence registry_chain result_registry lifecycle_stage does not match active result registry current stage" in entry_guard["problems"]
    assert "lifecycle evidence registry_chain current_result_pointer snapshot_id does not match active current result pointer" in entry_guard["problems"]
    assert "lifecycle evidence registry_chain current_result_pointer run_id does not match active current result pointer" in entry_guard["problems"]
    assert "lifecycle evidence registry_chain current_result_pointer artifact_ids do not match active current result pointer" in entry_guard["problems"]
    assert "lifecycle evidence registry_chain current_result_pointer as_of_date does not match active current result pointer" in entry_guard["problems"]

    api_payload = json.loads(dashboard_module._build_primary_result_api_body(tmp_path).decode("utf-8"))
    assert api_payload["disabled_reason"] == "stock entry guard blocked primary result publication."
    assert "lifecycle evidence registry_chain result_registry record_id does not match active result registry current record" in api_payload["entry_guard"]["problems"]
    assert "lifecycle evidence registry_chain result_registry lifecycle_stage does not match active result registry current stage" in api_payload["entry_guard"]["problems"]
    assert "lifecycle evidence registry_chain current_result_pointer snapshot_id does not match active current result pointer" in api_payload["entry_guard"]["problems"]
    assert "lifecycle evidence registry_chain current_result_pointer run_id does not match active current result pointer" in api_payload["entry_guard"]["problems"]
    assert "lifecycle evidence registry_chain current_result_pointer artifact_ids do not match active current result pointer" in api_payload["entry_guard"]["problems"]
    assert "lifecycle evidence registry_chain current_result_pointer as_of_date does not match active current result pointer" in api_payload["entry_guard"]["problems"]


def test_main_chain_authenticity_fail_closes_when_lifecycle_final_payload_source_scope_drifts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    exp_dir = _write_minimal_experiment_inputs(tmp_path)
    _patch_runtime_paths(monkeypatch, tmp_path)

    exit_code, lifecycle_payload = run_primary_result_lifecycle(
        exp_dir=exp_dir,
        max_source_age_hours=100000,
        observation_window_start="2026-04-15T07:40:00Z",
        now=datetime(2026, 4, 15, 7, 40, 20, tzinfo=timezone.utc),
    )
    assert exit_code == 0
    assert lifecycle_payload["status"] == "passed"

    artifacts_dir = tmp_path / "artifacts"
    lifecycle_evidence_path = exp_dir / "primary_result_lifecycle_evidence_latest.json"
    lifecycle_evidence = json.loads(lifecycle_evidence_path.read_text(encoding="utf-8"))
    lifecycle_evidence["final_payload"]["source_scope"] = "apex"
    lifecycle_evidence_path.write_text(json.dumps(lifecycle_evidence, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    artifact_registry_path = artifacts_dir / "artifact_registry.jsonl"
    registry_lines = []
    for line in artifact_registry_path.read_text(encoding="utf-8").splitlines():
        payload = json.loads(line)
        if payload.get("artifact_id") == f"{lifecycle_payload['run_id']}:lifecycle-evidence":
            payload["sha256"] = sha256_file(lifecycle_evidence_path)
        registry_lines.append(json.dumps(payload, ensure_ascii=False, sort_keys=True))
    artifact_registry_path.write_text("\n".join(registry_lines) + "\n", encoding="utf-8")

    integrity_exit, integrity_payload = check_current_result_pointer_integrity(
        pointer_dir=artifacts_dir / "current_result_pointer",
        results_dir=artifacts_dir / "result_registry",
        runs_dir=artifacts_dir / "run_registry",
        artifact_registry_path=artifact_registry_path,
    )
    assert integrity_exit == 0, integrity_payload
    assert integrity_payload["ok"] is True

    entry_guard = evaluate_stock_entry_guard(exp_dir=exp_dir, artifacts_dir=artifacts_dir)
    assert entry_guard["ok"] is False
    assert "lifecycle evidence final_payload source_scope does not match current_result_pointer" in entry_guard["problems"]
    assert "lifecycle evidence final_payload source_scope does not match result_registry current source_scope" in entry_guard["problems"]

    api_payload = json.loads(dashboard_module._build_primary_result_api_body(tmp_path).decode("utf-8"))
    assert api_payload["disabled_reason"] == "stock entry guard blocked primary result publication."
    assert "lifecycle evidence final_payload source_scope does not match current_result_pointer" in api_payload["entry_guard"]["problems"]
    assert "lifecycle evidence final_payload source_scope does not match result_registry current source_scope" in api_payload["entry_guard"]["problems"]


def test_main_chain_authenticity_fail_closes_when_run_registry_source_scope_and_current_history_drift(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    exp_dir = _write_minimal_experiment_inputs(tmp_path)
    _patch_runtime_paths(monkeypatch, tmp_path)

    exit_code, lifecycle_payload = run_primary_result_lifecycle(
        exp_dir=exp_dir,
        max_source_age_hours=100000,
        observation_window_start="2026-04-15T07:40:00Z",
        now=datetime(2026, 4, 15, 7, 40, 20, tzinfo=timezone.utc),
    )
    assert exit_code == 0
    assert lifecycle_payload["status"] == "passed"

    artifacts_dir = tmp_path / "artifacts"
    run_current_path = artifacts_dir / "run_registry" / "current.json"
    run_current = json.loads(run_current_path.read_text(encoding="utf-8"))
    run_history_path = Path(str(run_current["entry_path"]))
    run_history = json.loads(run_history_path.read_text(encoding="utf-8"))
    run_history["metadata"]["source_scope"] = "apex"
    run_history_path.write_text(json.dumps(run_history, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    integrity_exit, integrity_payload = check_current_result_pointer_integrity(
        pointer_dir=artifacts_dir / "current_result_pointer",
        results_dir=artifacts_dir / "result_registry",
        runs_dir=artifacts_dir / "run_registry",
        artifact_registry_path=artifacts_dir / "artifact_registry.jsonl",
    )
    assert integrity_exit == 1
    assert "run registry history metadata source_scope does not match current result pointer" in integrity_payload["problems"]
    assert "run registry history metadata source_scope does not match result registry current source_scope" in integrity_payload["problems"]

    entry_guard = evaluate_stock_entry_guard(exp_dir=exp_dir, artifacts_dir=artifacts_dir)
    assert entry_guard["ok"] is False
    assert "run registry history metadata source_scope does not match current result pointer" in entry_guard["problems"]
    assert "run registry history metadata source_scope does not match result registry current source_scope" in entry_guard["problems"]

    api_payload = json.loads(dashboard_module._build_primary_result_api_body(tmp_path).decode("utf-8"))
    assert api_payload["disabled_reason"] == "stock entry guard blocked primary result publication."
    assert "run registry history metadata source_scope does not match current result pointer" in api_payload["entry_guard"]["problems"]
    assert "run registry history metadata source_scope does not match result registry current source_scope" in api_payload["entry_guard"]["problems"]


def test_main_chain_authenticity_fail_closes_when_registry_chain_embedded_source_scopes_drift(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    exp_dir = _write_minimal_experiment_inputs(tmp_path)
    _patch_runtime_paths(monkeypatch, tmp_path)

    exit_code, lifecycle_payload = run_primary_result_lifecycle(
        exp_dir=exp_dir,
        max_source_age_hours=100000,
        observation_window_start="2026-04-15T07:40:00Z",
        now=datetime(2026, 4, 15, 7, 40, 20, tzinfo=timezone.utc),
    )
    assert exit_code == 0
    assert lifecycle_payload["status"] == "passed"

    artifacts_dir = tmp_path / "artifacts"
    lifecycle_evidence_path = exp_dir / "primary_result_lifecycle_evidence_latest.json"
    lifecycle_evidence = json.loads(lifecycle_evidence_path.read_text(encoding="utf-8"))
    lifecycle_evidence["registry_chain"]["result_registry"]["source_scope"] = "apex"
    lifecycle_evidence["registry_chain"]["current_result_pointer"]["source_scope"] = "apex"
    lifecycle_evidence_path.write_text(json.dumps(lifecycle_evidence, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    artifact_registry_path = artifacts_dir / "artifact_registry.jsonl"
    registry_lines = []
    for line in artifact_registry_path.read_text(encoding="utf-8").splitlines():
        payload = json.loads(line)
        if payload.get("artifact_id") == f"{lifecycle_payload['run_id']}:lifecycle-evidence":
            payload["sha256"] = sha256_file(lifecycle_evidence_path)
        registry_lines.append(json.dumps(payload, ensure_ascii=False, sort_keys=True))
    artifact_registry_path.write_text("\n".join(registry_lines) + "\n", encoding="utf-8")

    integrity_exit, integrity_payload = check_current_result_pointer_integrity(
        pointer_dir=artifacts_dir / "current_result_pointer",
        results_dir=artifacts_dir / "result_registry",
        runs_dir=artifacts_dir / "run_registry",
        artifact_registry_path=artifact_registry_path,
    )
    assert integrity_exit == 0, integrity_payload
    assert integrity_payload["ok"] is True

    entry_guard = evaluate_stock_entry_guard(exp_dir=exp_dir, artifacts_dir=artifacts_dir)
    assert entry_guard["ok"] is False
    assert "lifecycle evidence registry_chain result_registry source_scope does not match active result registry current source_scope" in entry_guard["problems"]
    assert "lifecycle evidence registry_chain current_result_pointer source_scope does not match active current result pointer" in entry_guard["problems"]

    api_payload = json.loads(dashboard_module._build_primary_result_api_body(tmp_path).decode("utf-8"))
    assert api_payload["disabled_reason"] == "stock entry guard blocked primary result publication."
    assert "lifecycle evidence registry_chain result_registry source_scope does not match active result registry current source_scope" in api_payload["entry_guard"]["problems"]
    assert "lifecycle evidence registry_chain current_result_pointer source_scope does not match active current result pointer" in api_payload["entry_guard"]["problems"]


def test_main_chain_authenticity_fail_closes_when_temporal_progression_is_aligned_but_run_semantics_are_wrong(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    exp_dir = _write_minimal_experiment_inputs(tmp_path)
    _patch_runtime_paths(monkeypatch, tmp_path)

    exit_code, lifecycle_payload = run_primary_result_lifecycle(
        exp_dir=exp_dir,
        max_source_age_hours=100000,
        observation_window_start="2026-04-15T07:40:00Z",
        now=datetime(2026, 4, 15, 7, 40, 20, tzinfo=timezone.utc),
    )
    assert exit_code == 0
    assert lifecycle_payload["status"] == "passed"

    artifacts_dir = tmp_path / "artifacts"
    run_current_path = artifacts_dir / "run_registry" / "current.json"
    run_current = json.loads(run_current_path.read_text(encoding="utf-8"))
    run_history_path = Path(str(run_current["entry_path"]))
    run_history = json.loads(run_history_path.read_text(encoding="utf-8"))
    run_history["metadata"]["source_scope"] = "apex"
    run_history["metadata"]["repair_mode"] = "bootstrap"
    run_history_path.write_text(json.dumps(run_history, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    integrity_exit, integrity_payload = check_current_result_pointer_integrity(
        pointer_dir=artifacts_dir / "current_result_pointer",
        results_dir=artifacts_dir / "result_registry",
        runs_dir=artifacts_dir / "run_registry",
        artifact_registry_path=artifacts_dir / "artifact_registry.jsonl",
    )
    assert integrity_exit == 1
    assert "run registry current updated_at is chronologically aligned but history metadata carries non-main-chain semantics" in integrity_payload["problems"]

    entry_guard = evaluate_stock_entry_guard(exp_dir=exp_dir, artifacts_dir=artifacts_dir)
    assert entry_guard["ok"] is False
    assert "run registry current updated_at is chronologically aligned but history metadata carries non-main-chain semantics" in entry_guard["problems"]

    api_payload = json.loads(dashboard_module._build_primary_result_api_body(tmp_path).decode("utf-8"))
    assert api_payload["disabled_reason"] == "stock entry guard blocked primary result publication."
    assert "run registry current updated_at is chronologically aligned but history metadata carries non-main-chain semantics" in api_payload["entry_guard"]["problems"]


def test_main_chain_authenticity_fail_closes_when_pointer_snapshot_metadata_source_scope_drifts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    exp_dir = _write_minimal_experiment_inputs(tmp_path)
    _patch_runtime_paths(monkeypatch, tmp_path)

    exit_code, lifecycle_payload = run_primary_result_lifecycle(
        exp_dir=exp_dir,
        max_source_age_hours=100000,
        observation_window_start="2026-04-15T07:40:00Z",
        now=datetime(2026, 4, 15, 7, 40, 20, tzinfo=timezone.utc),
    )
    assert exit_code == 0
    assert lifecycle_payload["status"] == "passed"

    artifacts_dir = tmp_path / "artifacts"
    pointer_current_path = artifacts_dir / "current_result_pointer" / "current.json"
    pointer_current = json.loads(pointer_current_path.read_text(encoding="utf-8"))
    pointer_snapshot_path = Path(str(pointer_current["snapshot_path"]))
    pointer_snapshot = json.loads(pointer_snapshot_path.read_text(encoding="utf-8"))
    pointer_snapshot["metadata"]["source_scope"] = "apex"
    pointer_snapshot_path.write_text(json.dumps(pointer_snapshot, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    integrity_exit, integrity_payload = check_current_result_pointer_integrity(
        pointer_dir=artifacts_dir / "current_result_pointer",
        results_dir=artifacts_dir / "result_registry",
        runs_dir=artifacts_dir / "run_registry",
        artifact_registry_path=artifacts_dir / "artifact_registry.jsonl",
    )
    assert integrity_exit == 1
    assert "current result pointer snapshot metadata source_scope does not match active current result pointer" in integrity_payload["problems"]
    assert "current result pointer snapshot metadata source_scope does not match result registry current source_scope" in integrity_payload["problems"]

    entry_guard = evaluate_stock_entry_guard(exp_dir=exp_dir, artifacts_dir=artifacts_dir)
    assert entry_guard["ok"] is False
    assert "current result pointer snapshot metadata source_scope does not match active current result pointer" in entry_guard["problems"]
    assert "current result pointer snapshot metadata source_scope does not match lifecycle evidence final_payload source_scope" in entry_guard["problems"]

    api_payload = json.loads(dashboard_module._build_primary_result_api_body(tmp_path).decode("utf-8"))
    assert api_payload["disabled_reason"] == "stock entry guard blocked primary result publication."
    assert "current result pointer snapshot metadata source_scope does not match active current result pointer" in api_payload["entry_guard"]["problems"]
    assert "current result pointer snapshot metadata source_scope does not match lifecycle evidence final_payload source_scope" in api_payload["entry_guard"]["problems"]


def test_main_chain_authenticity_fail_closes_when_artifact_producer_and_code_revision_cross_repair_boundary(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    exp_dir = _write_minimal_experiment_inputs(tmp_path)
    _patch_runtime_paths(monkeypatch, tmp_path)

    exit_code, lifecycle_payload = run_primary_result_lifecycle(
        exp_dir=exp_dir,
        max_source_age_hours=100000,
        observation_window_start="2026-04-15T07:40:00Z",
        now=datetime(2026, 4, 15, 7, 40, 20, tzinfo=timezone.utc),
    )
    assert exit_code == 0
    assert lifecycle_payload["status"] == "passed"

    artifacts_dir = tmp_path / "artifacts"
    artifact_registry_path = artifacts_dir / "artifact_registry.jsonl"
    registry_lines = []
    for line in artifact_registry_path.read_text(encoding="utf-8").splitlines():
        payload = json.loads(line)
        artifact_id = str(payload.get("artifact_id") or "")
        if artifact_id in {
            f"{lifecycle_payload['run_id']}:audit",
            f"{lifecycle_payload['run_id']}:lifecycle-evidence",
        }:
            payload["producer"] = "scripts.repair_primary_result_chain"
            payload["code_revision"] = "rev-sidecar"
        registry_lines.append(json.dumps(payload, ensure_ascii=False, sort_keys=True))
    artifact_registry_path.write_text("\n".join(registry_lines) + "\n", encoding="utf-8")

    integrity_exit, integrity_payload = check_current_result_pointer_integrity(
        pointer_dir=artifacts_dir / "current_result_pointer",
        results_dir=artifacts_dir / "result_registry",
        runs_dir=artifacts_dir / "run_registry",
        artifact_registry_path=artifact_registry_path,
    )
    assert integrity_exit == 1
    assert f"artifact registry producer is not allowed for current formal main chain: {lifecycle_payload['run_id']}:audit" in integrity_payload["problems"]
    assert f"artifact registry code_revision does not match main chain run history: {lifecycle_payload['run_id']}:audit" in integrity_payload["problems"]
    assert f"artifact registry producer is not allowed for current formal main chain: {lifecycle_payload['run_id']}:lifecycle-evidence" in integrity_payload["problems"]
    assert f"artifact registry code_revision does not match main chain run history: {lifecycle_payload['run_id']}:lifecycle-evidence" in integrity_payload["problems"]

    entry_guard = evaluate_stock_entry_guard(exp_dir=exp_dir, artifacts_dir=artifacts_dir)
    assert entry_guard["ok"] is False
    assert "lifecycle step artifact producer is not allowed for current formal main chain: audit" in entry_guard["problems"]
    assert "lifecycle step artifact code_revision does not match main chain run history: audit" in entry_guard["problems"]
    assert "lifecycle evidence artifact producer is not allowed for current formal main chain" in entry_guard["problems"]
    assert "lifecycle evidence artifact code_revision does not match main chain run history" in entry_guard["problems"]

    api_payload = json.loads(dashboard_module._build_primary_result_api_body(tmp_path).decode("utf-8"))
    assert api_payload["disabled_reason"] == "stock entry guard blocked primary result publication."
    assert "lifecycle step artifact producer is not allowed for current formal main chain: audit" in api_payload["entry_guard"]["problems"]
    assert "lifecycle step artifact code_revision does not match main chain run history: audit" in api_payload["entry_guard"]["problems"]
    assert "lifecycle evidence artifact producer is not allowed for current formal main chain" in api_payload["entry_guard"]["problems"]
    assert "lifecycle evidence artifact code_revision does not match main chain run history" in api_payload["entry_guard"]["problems"]


def test_main_chain_authenticity_fail_closes_when_pointer_snapshot_metadata_producer_and_code_revision_drift(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    exp_dir = _write_minimal_experiment_inputs(tmp_path)
    _patch_runtime_paths(monkeypatch, tmp_path)

    exit_code, lifecycle_payload = run_primary_result_lifecycle(
        exp_dir=exp_dir,
        max_source_age_hours=100000,
        observation_window_start="2026-04-15T07:40:00Z",
        now=datetime(2026, 4, 15, 7, 40, 20, tzinfo=timezone.utc),
    )
    assert exit_code == 0
    assert lifecycle_payload["status"] == "passed"

    artifacts_dir = tmp_path / "artifacts"
    pointer_current_path = artifacts_dir / "current_result_pointer" / "current.json"
    pointer_current = json.loads(pointer_current_path.read_text(encoding="utf-8"))
    pointer_snapshot_path = Path(str(pointer_current["snapshot_path"]))
    pointer_snapshot = json.loads(pointer_snapshot_path.read_text(encoding="utf-8"))
    pointer_snapshot["metadata"]["producer"] = "scripts.repair_primary_result_chain"
    pointer_snapshot["metadata"]["code_revision"] = "rev-sidecar"
    pointer_snapshot_path.write_text(json.dumps(pointer_snapshot, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    integrity_exit, integrity_payload = check_current_result_pointer_integrity(
        pointer_dir=artifacts_dir / "current_result_pointer",
        results_dir=artifacts_dir / "result_registry",
        runs_dir=artifacts_dir / "run_registry",
        artifact_registry_path=artifacts_dir / "artifact_registry.jsonl",
    )
    assert integrity_exit == 1
    assert "current result pointer snapshot metadata producer does not match main chain run history" in integrity_payload["problems"]
    assert "current result pointer snapshot metadata code_revision does not match main chain run history" in integrity_payload["problems"]

    entry_guard = evaluate_stock_entry_guard(exp_dir=exp_dir, artifacts_dir=artifacts_dir)
    assert entry_guard["ok"] is False
    assert "current result pointer snapshot metadata producer does not match main chain run history" in entry_guard["problems"]
    assert "current result pointer snapshot metadata code_revision does not match main chain run history" in entry_guard["problems"]

    api_payload = json.loads(dashboard_module._build_primary_result_api_body(tmp_path).decode("utf-8"))
    assert api_payload["disabled_reason"] == "stock entry guard blocked primary result publication."
    assert "current result pointer snapshot metadata producer does not match main chain run history" in api_payload["entry_guard"]["problems"]
    assert "current result pointer snapshot metadata code_revision does not match main chain run history" in api_payload["entry_guard"]["problems"]


def test_main_chain_authenticity_fail_closes_when_registry_chain_embedded_producer_and_code_revision_drift(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    exp_dir = _write_minimal_experiment_inputs(tmp_path)
    _patch_runtime_paths(monkeypatch, tmp_path)

    exit_code, lifecycle_payload = run_primary_result_lifecycle(
        exp_dir=exp_dir,
        max_source_age_hours=100000,
        observation_window_start="2026-04-15T07:40:00Z",
        now=datetime(2026, 4, 15, 7, 40, 20, tzinfo=timezone.utc),
    )
    assert exit_code == 0
    assert lifecycle_payload["status"] == "passed"

    artifacts_dir = tmp_path / "artifacts"
    lifecycle_evidence_path = exp_dir / "primary_result_lifecycle_evidence_latest.json"
    lifecycle_evidence = json.loads(lifecycle_evidence_path.read_text(encoding="utf-8"))
    lifecycle_evidence["registry_chain"]["result_registry"]["producer"] = "scripts.repair_primary_result_chain"
    lifecycle_evidence["registry_chain"]["result_registry"]["code_revision"] = "rev-sidecar"
    lifecycle_evidence["registry_chain"]["current_result_pointer"]["producer"] = "scripts.repair_primary_result_chain"
    lifecycle_evidence["registry_chain"]["current_result_pointer"]["code_revision"] = "rev-sidecar"
    lifecycle_evidence_path.write_text(json.dumps(lifecycle_evidence, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    artifact_registry_path = artifacts_dir / "artifact_registry.jsonl"
    registry_lines = []
    for line in artifact_registry_path.read_text(encoding="utf-8").splitlines():
        payload = json.loads(line)
        if payload.get("artifact_id") == f"{lifecycle_payload['run_id']}:lifecycle-evidence":
            payload["sha256"] = sha256_file(lifecycle_evidence_path)
        registry_lines.append(json.dumps(payload, ensure_ascii=False, sort_keys=True))
    artifact_registry_path.write_text("\n".join(registry_lines) + "\n", encoding="utf-8")

    integrity_exit, integrity_payload = check_current_result_pointer_integrity(
        pointer_dir=artifacts_dir / "current_result_pointer",
        results_dir=artifacts_dir / "result_registry",
        runs_dir=artifacts_dir / "run_registry",
        artifact_registry_path=artifact_registry_path,
    )
    assert integrity_exit == 0, integrity_payload
    assert integrity_payload["ok"] is True

    entry_guard = evaluate_stock_entry_guard(exp_dir=exp_dir, artifacts_dir=artifacts_dir)
    assert entry_guard["ok"] is False
    assert "lifecycle evidence registry_chain result_registry producer does not match active result registry current metadata producer" in entry_guard["problems"]
    assert "lifecycle evidence registry_chain result_registry code_revision does not match active result registry current metadata code_revision" in entry_guard["problems"]
    assert "lifecycle evidence registry_chain current_result_pointer producer does not match active current result pointer snapshot metadata producer" in entry_guard["problems"]
    assert "lifecycle evidence registry_chain current_result_pointer code_revision does not match active current result pointer snapshot metadata code_revision" in entry_guard["problems"]

    api_payload = json.loads(dashboard_module._build_primary_result_api_body(tmp_path).decode("utf-8"))
    assert api_payload["disabled_reason"] == "stock entry guard blocked primary result publication."
    assert "lifecycle evidence registry_chain result_registry producer does not match active result registry current metadata producer" in api_payload["entry_guard"]["problems"]
    assert "lifecycle evidence registry_chain result_registry code_revision does not match active result registry current metadata code_revision" in api_payload["entry_guard"]["problems"]
    assert "lifecycle evidence registry_chain current_result_pointer producer does not match active current result pointer snapshot metadata producer" in api_payload["entry_guard"]["problems"]
    assert "lifecycle evidence registry_chain current_result_pointer code_revision does not match active current result pointer snapshot metadata code_revision" in api_payload["entry_guard"]["problems"]


def test_main_chain_authenticity_fail_closes_when_result_metadata_and_embedded_registry_chain_share_repair_semantics(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    exp_dir = _write_minimal_experiment_inputs(tmp_path)
    _patch_runtime_paths(monkeypatch, tmp_path)

    exit_code, lifecycle_payload = run_primary_result_lifecycle(
        exp_dir=exp_dir,
        max_source_age_hours=100000,
        observation_window_start="2026-04-15T07:40:00Z",
        now=datetime(2026, 4, 15, 7, 40, 20, tzinfo=timezone.utc),
    )
    assert exit_code == 0
    assert lifecycle_payload["status"] == "passed"

    artifacts_dir = tmp_path / "artifacts"
    result_current_path = artifacts_dir / "result_registry" / "current.json"
    result_current = json.loads(result_current_path.read_text(encoding="utf-8"))
    result_history_path = Path(str(result_current["entry_path"]))
    result_history = json.loads(result_history_path.read_text(encoding="utf-8"))
    result_history["metadata"]["producer"] = "scripts.repair_primary_result_chain"
    result_history["metadata"]["code_revision"] = "rev-sidecar"
    result_history_path.write_text(json.dumps(result_history, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    lifecycle_evidence_path = exp_dir / "primary_result_lifecycle_evidence_latest.json"
    lifecycle_evidence = json.loads(lifecycle_evidence_path.read_text(encoding="utf-8"))
    lifecycle_evidence["registry_chain"]["result_registry"]["producer"] = "scripts.repair_primary_result_chain"
    lifecycle_evidence["registry_chain"]["result_registry"]["code_revision"] = "rev-sidecar"
    lifecycle_evidence_path.write_text(json.dumps(lifecycle_evidence, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    artifact_registry_path = artifacts_dir / "artifact_registry.jsonl"
    registry_lines = []
    for line in artifact_registry_path.read_text(encoding="utf-8").splitlines():
        payload = json.loads(line)
        if payload.get("artifact_id") == f"{lifecycle_payload['run_id']}:lifecycle-evidence":
            payload["sha256"] = sha256_file(lifecycle_evidence_path)
        registry_lines.append(json.dumps(payload, ensure_ascii=False, sort_keys=True))
    artifact_registry_path.write_text("\n".join(registry_lines) + "\n", encoding="utf-8")

    integrity_exit, integrity_payload = check_current_result_pointer_integrity(
        pointer_dir=artifacts_dir / "current_result_pointer",
        results_dir=artifacts_dir / "result_registry",
        runs_dir=artifacts_dir / "run_registry",
        artifact_registry_path=artifact_registry_path,
    )
    assert integrity_exit == 1
    assert "result registry metadata producer does not match main chain run history" in integrity_payload["problems"]
    assert "result registry metadata code_revision does not match main chain run history" in integrity_payload["problems"]
    assert "current result pointer snapshot metadata producer does not match result registry current metadata producer" in integrity_payload["problems"]
    assert "current result pointer snapshot metadata code_revision does not match result registry current metadata code_revision" in integrity_payload["problems"]

    entry_guard = evaluate_stock_entry_guard(exp_dir=exp_dir, artifacts_dir=artifacts_dir)
    assert entry_guard["ok"] is False
    assert "result registry metadata producer does not match main chain run history" in entry_guard["problems"]
    assert "result registry metadata code_revision does not match main chain run history" in entry_guard["problems"]
    assert "lifecycle evidence registry_chain result_registry producer does not match main chain run history" in entry_guard["problems"]
    assert "lifecycle evidence registry_chain result_registry code_revision does not match main chain run history" in entry_guard["problems"]

    api_payload = json.loads(dashboard_module._build_primary_result_api_body(tmp_path).decode("utf-8"))
    assert api_payload["disabled_reason"] == "stock entry guard blocked primary result publication."
    assert "result registry metadata producer does not match main chain run history" in api_payload["entry_guard"]["problems"]
    assert "result registry metadata code_revision does not match main chain run history" in api_payload["entry_guard"]["problems"]
    assert "lifecycle evidence registry_chain result_registry producer does not match main chain run history" in api_payload["entry_guard"]["problems"]
    assert "lifecycle evidence registry_chain result_registry code_revision does not match main chain run history" in api_payload["entry_guard"]["problems"]
