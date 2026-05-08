from __future__ import annotations

import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path

import pytest

import run_dashboard as dashboard_module
import scripts.run_server_deploy_rollback as rollback_module
from scripts.check_current_result_pointer_integrity import check_current_result_pointer_integrity
from scripts.repair_primary_result_chain import repair_primary_result_chain
from scripts.run_primary_result_lifecycle import run_primary_result_lifecycle
from scripts.run_server_deploy_rollback import run_server_deploy_rollback
from src.artifact_source_audit import audit_artifact_source_pollution
from src import dashboard_context
from src.server_deploy_registry import ServerDeployRegistry
from src.stock_entry_guard import evaluate_stock_entry_guard
from tests.server_deploy_test_helpers import write_server_deploy_evidence_bundle
from tests.test_main_chain_authenticity_integration import _patch_runtime_paths, _write_minimal_experiment_inputs


pytestmark = pytest.mark.integration


def _ok_runner(command, timeout=None):
    return subprocess.CompletedProcess(command, 0, stdout="ok", stderr="")


def _ok_url_fetcher(url, timeout):
    if url.endswith("/stock/api/primary-result"):
        return 200, '{"schema_version":"primary_result_v1","result_id":"primary:000001.SZ"}'
    if url.endswith("/T12/api/stock-ai-runner") or url.endswith("/T12/ops/stock-ai-runner"):
        return 404, "not found"
    if url.endswith("/stock/") or url.endswith("/apex/stock/"):
        return 200, "<div><span>主结果证据</span><strong>1/20</strong><span>候选篮证据</span><strong>8/20</strong><span>晋级门禁</span><strong>晋级锁定</strong></div>"
    return 200, "ok"


def test_main_chain_repair_rebuild_restores_pointer_guard_api_and_dashboard(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    exp_dir = _write_minimal_experiment_inputs(tmp_path)
    _patch_runtime_paths(monkeypatch, tmp_path)

    artifacts_dir = tmp_path / "artifacts"
    if artifacts_dir.exists():
        for path in (
            artifacts_dir / "current_result_pointer",
            artifacts_dir / "result_registry",
            artifacts_dir / "run_registry",
            artifacts_dir / "artifact_registry.jsonl",
            artifacts_dir / "primary_result_lifecycle",
            artifacts_dir / "stock_entry_guard_latest.json",
        ):
            if path.is_dir():
                for child in sorted(path.rglob("*"), reverse=True):
                    if child.is_file():
                        child.unlink()
                    elif child.is_dir():
                        child.rmdir()
                path.rmdir()
            elif path.exists():
                path.unlink()

    exit_code, repair_payload = repair_primary_result_chain(
        exp_dir=exp_dir,
        artifacts_dir=artifacts_dir,
        max_source_age_hours=100000,
        observation_window_start="2026-04-15T07:40:00Z",
        now=datetime(2026, 4, 15, 7, 40, 20, tzinfo=timezone.utc),
    )

    assert exit_code == 0
    assert repair_payload["status"] == "passed"
    assert repair_payload["guard_payload"]["ok"] is True
    assert repair_payload["lifecycle_payload"]["status"] == "passed"
    assert repair_payload["lifecycle_registry_status"]["status"] in {"registered", "already_exists"}
    assert repair_payload["result_registry_reconcile_status"]["status"] == "registered"

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

    api_payload = json.loads(dashboard_module._build_primary_result_api_body(tmp_path).decode("utf-8"))
    context = dashboard_context.build_dashboard_context(tmp_path, candidate_index=1, base_path="/stock")

    assert api_payload["disabled_reason"] is None
    assert api_payload["history_generation_mode"] == "chain_verified"
    assert api_payload["result_id"] == "primary:000001.SZ"
    assert context["primary_result"]["result_id"] == api_payload["result_id"]
    assert context["primary_result"]["run_id"] == api_payload["run_id"]
    assert context["primary_result"]["lifecycle_id"] == api_payload["lifecycle_id"]
    assert context["primary_result_query"]["primary_conclusion"]["result_id"] == api_payload["result_id"]
    assert context["top1"]["ts_code"] == "000002.SZ"
    assert context["primary_result"]["ts_code"] == "000001.SZ"


def test_artifact_source_pollution_audit_detects_temp_path_pollution_without_overriding_main_chain(
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
    polluted_latest = artifacts_dir / "primary_result_candidate_baskets" / "observation_latest.json"
    polluted_latest.parent.mkdir(parents=True, exist_ok=True)
    polluted_latest.write_text(
        json.dumps(
            {
                "status": "blocked",
                "source_path": "/private/var/folders/db/pytest-of-mac/pytest-999/polluted.json",
                "snapshot_path": "/tmp/pytest-polluted/snapshot.json",
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    pollution_payload = audit_artifact_source_pollution(artifacts_dir=artifacts_dir)
    assert pollution_payload["polluted_file_total"] >= 1
    assert any(item["path"].endswith("observation_latest.json") for item in pollution_payload["polluted_files"])

    api_payload = json.loads(dashboard_module._build_primary_result_api_body(tmp_path).decode("utf-8"))
    context = dashboard_context.build_dashboard_context(tmp_path, candidate_index=1, base_path="/stock")

    assert api_payload["disabled_reason"] is None
    assert api_payload["history_generation_mode"] == "chain_verified"
    assert api_payload["result_id"] == "primary:000001.SZ"
    assert context["primary_result"]["result_id"] == "primary:000001.SZ"
    assert context["primary_result"]["history_generation_mode"] == "chain_verified"


def test_main_chain_fail_closes_when_current_pointer_snapshot_path_is_temp_derived(
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
    current_pointer_path = artifacts_dir / "current_result_pointer" / "current.json"
    current_pointer = json.loads(current_pointer_path.read_text(encoding="utf-8"))
    current_pointer["snapshot_path"] = "/private/var/folders/db/pytest-of-mac/pytest-999/current_result_pointer/history/pointer-001.json"
    current_pointer_path.write_text(json.dumps(current_pointer, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    integrity_exit, integrity_payload = check_current_result_pointer_integrity(
        pointer_dir=artifacts_dir / "current_result_pointer",
        results_dir=artifacts_dir / "result_registry",
        runs_dir=artifacts_dir / "run_registry",
        artifact_registry_path=artifacts_dir / "artifact_registry.jsonl",
    )
    assert integrity_exit == 1
    assert any("temporary or pytest-derived path" in problem for problem in integrity_payload["problems"])

    entry_guard = evaluate_stock_entry_guard(exp_dir=exp_dir, artifacts_dir=artifacts_dir)
    assert entry_guard["ok"] is False
    assert any(
        "temporary or pytest-derived path" in problem
        for problem in entry_guard["integrity_payload"]["problems"]
    )

    api_payload = json.loads(dashboard_module._build_primary_result_api_body(tmp_path).decode("utf-8"))
    context = dashboard_context.build_dashboard_context(tmp_path, candidate_index=1, base_path="/stock")

    assert api_payload["disabled_reason"] == "stock entry guard blocked primary result publication."
    assert api_payload["history_generation_mode"] == "blocked"
    assert context["primary_result"]["disabled_reason"] is not None
    assert "snapshot_path" in str(context["primary_result"]["disabled_reason"])
    assert context["primary_result"]["history_generation_mode"] == "blocked"


def test_main_chain_repair_fail_closes_when_seed_payload_is_unavailable(tmp_path: Path):
    exp_dir = tmp_path / "data" / "experiments"
    artifacts_dir = tmp_path / "artifacts"
    exp_dir.mkdir(parents=True, exist_ok=True)

    exit_code, repair_payload = repair_primary_result_chain(
        exp_dir=exp_dir,
        artifacts_dir=artifacts_dir,
        now=datetime(2026, 4, 15, 7, 40, 20, tzinfo=timezone.utc),
    )

    assert exit_code == 1
    assert repair_payload["status"] == "failed"
    assert "seed payload unavailable" in str(repair_payload["reason"])
    assert str(repair_payload["seed_payload"]["result_id"]).startswith("primary:")
    assert repair_payload["seed_payload"]["result_id"] != "primary:000001.SZ"
    current_pointer_path = artifacts_dir / "current_result_pointer" / "current.json"
    if current_pointer_path.exists():
        current_pointer = json.loads(current_pointer_path.read_text(encoding="utf-8"))
        assert current_pointer["result_id"] is None
        assert current_pointer["run_id"] is None
        assert current_pointer["lifecycle_id"] is None
    result_registry_dir = artifacts_dir / "result_registry"
    if result_registry_dir.exists():
        assert not any((result_registry_dir / "history").glob("*.json"))
        current_result_registry_path = result_registry_dir / "current.json"
        if current_result_registry_path.exists():
            current_result_registry = json.loads(current_result_registry_path.read_text(encoding="utf-8"))
            assert current_result_registry["result_id"] is None
    run_registry_dir = artifacts_dir / "run_registry"
    if run_registry_dir.exists():
        assert not any((run_registry_dir / "history").glob("*.json"))
        current_run_registry_path = run_registry_dir / "current.json"
        if current_run_registry_path.exists():
            current_run_registry = json.loads(current_run_registry_path.read_text(encoding="utf-8"))
            assert current_run_registry["run_id"] is None


def test_server_deploy_rollback_restores_registered_snapshot_and_keeps_pointer_evidence_consistent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    deployments_dir = tmp_path / "deployments"
    registry = ServerDeployRegistry(deployments_dir=deployments_dir)
    evidence_one = write_server_deploy_evidence_bundle(tmp_path / "release1", deployment_id="deploy-test001")
    evidence_two = write_server_deploy_evidence_bundle(tmp_path / "release2", deployment_id="deploy-test002")
    registry.register(evidence_bundle_path=evidence_one)
    registry.register(evidence_bundle_path=evidence_two)

    def fake_verification(**kwargs):
        output_path = kwargs.get("output_path")
        payload = {
            "post_deploy_version": "server_post_deploy_verification.v1",
            "status": "passed",
            "checks": [{"check": "systemd_services_active", "passed": True}],
        }
        if output_path:
            Path(output_path).write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        return 0, payload

    monkeypatch.setattr(rollback_module, "run_server_post_deploy_verification", fake_verification)

    rollback_output_path = tmp_path / "server_deploy_rollback.integration.json"
    verify_output_path = tmp_path / "server_post_deploy_verification.rollback.integration.json"
    activation_execution_output_path = tmp_path / "server_activation_execution.rollback.integration.json"
    exit_code, payload = run_server_deploy_rollback(
        deployments_dir=deployments_dir,
        rollback_deployment_id="deploy-test001",
        confirm_deployment_id="deploy-test001",
        output_path=rollback_output_path,
        post_deploy_output_path=verify_output_path,
        activation_execution_output_path=activation_execution_output_path,
        command_runner=_ok_runner,
        url_fetcher=_ok_url_fetcher,
    )

    assert exit_code == 0
    assert payload["status"] == "passed"
    assert payload["scope"] == "stock-scoped"
    assert payload["dry_run"] is False
    assert payload["post_deploy_verification"]["status"] == "passed"

    current = registry.get_current_pointer()
    current_snapshot = registry.get_current_snapshot()
    evidence_payload = json.loads(Path(str(current_snapshot["source_evidence_bundle_path"])).read_text(encoding="utf-8"))

    assert current["deployment_id"] == "deploy-test001"
    assert current["rollback_of_deployment_id"] == "deploy-test001"
    assert current_snapshot["deployment_id"] == "deploy-test001"
    assert current_snapshot["scope"] == "stock-scoped"
    assert current_snapshot["source_evidence_bundle_path"] == str(evidence_one)
    assert evidence_payload["deployment_id"] == "deploy-test001"
    assert evidence_payload["scope"] == "stock-scoped"
    assert evidence_payload["artifacts"]["activation_plan"]["path"] == payload["activation_plan_path"]
    assert json.loads(rollback_output_path.read_text(encoding="utf-8"))["status"] == "passed"
    assert json.loads(verify_output_path.read_text(encoding="utf-8"))["status"] == "passed"
    assert json.loads(activation_execution_output_path.read_text(encoding="utf-8"))["status"] == "passed"


def test_server_deploy_rollback_does_not_move_current_snapshot_when_post_verification_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    deployments_dir = tmp_path / "deployments"
    registry = ServerDeployRegistry(deployments_dir=deployments_dir)
    evidence_one = write_server_deploy_evidence_bundle(tmp_path / "release1", deployment_id="deploy-test001")
    evidence_two = write_server_deploy_evidence_bundle(tmp_path / "release2", deployment_id="deploy-test002")
    registry.register(evidence_bundle_path=evidence_one)
    registry.register(evidence_bundle_path=evidence_two)

    def failing_verification(**kwargs):
        output_path = kwargs.get("output_path")
        payload = {
            "post_deploy_version": "server_post_deploy_verification.v1",
            "status": "failed",
            "checks": [{"check": "dashboard_http_targets", "passed": False}],
        }
        if output_path:
            Path(output_path).write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        return 1, payload

    monkeypatch.setattr(rollback_module, "run_server_post_deploy_verification", failing_verification)

    rollback_output_path = tmp_path / "server_deploy_rollback.failed.integration.json"
    verify_output_path = tmp_path / "server_post_deploy_verification.rollback.failed.integration.json"
    exit_code, payload = run_server_deploy_rollback(
        deployments_dir=deployments_dir,
        rollback_deployment_id="deploy-test001",
        confirm_deployment_id="deploy-test001",
        output_path=rollback_output_path,
        post_deploy_output_path=verify_output_path,
        command_runner=_ok_runner,
        url_fetcher=_ok_url_fetcher,
    )

    current = registry.get_current_pointer()
    current_snapshot = registry.get_current_snapshot()

    assert exit_code == 1
    assert payload["status"] == "failed"
    assert payload["scope"] == "stock-scoped"
    assert payload["post_deploy_verification"]["status"] == "failed"
    assert current["deployment_id"] == "deploy-test002"
    assert current["rollback_of_deployment_id"] is None
    assert current_snapshot["deployment_id"] == "deploy-test002"
    assert json.loads(rollback_output_path.read_text(encoding="utf-8"))["status"] == "failed"
    assert json.loads(verify_output_path.read_text(encoding="utf-8"))["status"] == "failed"
