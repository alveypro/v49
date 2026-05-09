import json
import subprocess
import sys
from pathlib import Path

from scripts import run_server_post_deploy_verification as post_deploy_verification
from scripts.run_server_post_deploy_verification import run_server_post_deploy_verification
from src.artifact_registry import sha256_file


def _create_app_dir(tmp_path):
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
    (app_dir / "data").mkdir()
    (app_dir / "artifacts").mkdir()
    (app_dir / "data" / "experiments").mkdir(parents=True, exist_ok=True)
    (app_dir / "artifacts" / "current_result_pointer").mkdir(parents=True, exist_ok=True)
    (app_dir / "artifacts" / "result_registry").mkdir(parents=True, exist_ok=True)
    (app_dir / "artifacts" / "run_registry").mkdir(parents=True, exist_ok=True)
    audit_path = app_dir / "data" / "experiments" / "primary_result_audit_latest.json"
    audit_path.write_text(
        json.dumps(
            {
                "result_id": "primary:300750.SZ",
                "run_id": "run-1",
                "lifecycle_id": "life-1",
                "ts_code": "300750.SZ",
                "stock_name": "宁德时代",
                "audit_status": "passed",
                "generated_at": "2026-04-30T00:00:00+00:00",
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    lifecycle_evidence_path = app_dir / "data" / "experiments" / "primary_result_lifecycle_evidence_latest.json"
    (app_dir / "artifacts" / "current_result_pointer" / "current.json").write_text(
        json.dumps(
            {
                "result_id": "primary:300750.SZ",
                "run_id": "run-1",
                "lifecycle_id": "life-1",
                "artifact_ids": ["artifact-1", "artifact-2"],
                "as_of_date": "2026-04-30",
                "updated_at": "2026-04-30T00:00:00+00:00",
                "source_scope": "stock",
                "snapshot_path": str(app_dir / "artifacts" / "current_result_pointer" / "snapshot.json"),
            }
        ),
        encoding="utf-8",
    )
    (app_dir / "artifacts" / "current_result_pointer" / "snapshot.json").write_text(
        json.dumps(
            {
                "pointer_snapshot_id": "pointer-1",
                "result_id": "primary:300750.SZ",
                "run_id": "run-1",
                "lifecycle_id": "life-1",
                "artifact_ids": ["artifact-1", "artifact-2"],
                "as_of_date": "2026-04-30",
                "updated_at": "2026-04-30T00:00:00+00:00",
                "source_scope": "stock",
                "metadata": {
                    "result_registry_record_id": "result-record-1",
                    "evidence_path": str(lifecycle_evidence_path),
                    "artifact_registry_path": str(app_dir / "artifacts" / "artifact_registry.jsonl"),
                    "source_scope": "stock",
                    "producer": "scripts.run_primary_result_lifecycle",
                    "code_revision": "rev",
                },
            }
        ),
        encoding="utf-8",
    )
    lifecycle_evidence_path.write_text(
        json.dumps(
            {
                "lifecycle_version": "primary_result_lifecycle.v1",
                "status": "passed",
                "started_at": "2026-04-30T00:00:00+00:00",
                "completed_at": "2026-04-30T00:00:00+00:00",
                "result_id": "primary:300750.SZ",
                "ts_code": "300750.SZ",
                "stock_name": "宁德时代",
                "run_id": "run-1",
                "lifecycle_id": "life-1",
                "final_payload": {
                    "result_id": "primary:300750.SZ",
                    "run_id": "run-1",
                    "lifecycle_id": "life-1",
                    "artifact_ids": ["artifact-1", "artifact-2"],
                    "as_of_date": "2026-04-30",
                    "source_scope": "stock",
                    "ts_code": "300750.SZ",
                    "stock_name": "宁德时代",
                    "result_lifecycle_stage": "L3",
                    "audit_status": "passed",
                },
                "steps": [
                    {
                        "step": "audit",
                        "path": str(audit_path),
                        "exists": True,
                        "sha256": sha256_file(audit_path),
                        "result_id": "primary:300750.SZ",
                        "ts_code": "300750.SZ",
                        "generated_at": "2026-04-30T00:00:00+00:00",
                        "status": "passed",
                        "exit_code": 0,
                    }
                ],
                "registry_chain": {
                    "write_mode": "run_result_pointer_updated",
                    "result_registry": {
                        "record_id": "result-record-1",
                        "lifecycle_stage": "L3",
                        "source_scope": "stock",
                        "producer": "scripts.run_primary_result_lifecycle",
                        "code_revision": "rev",
                    },
                    "current_result_pointer": {
                        "pointer_snapshot_id": "pointer-1",
                        "result_id": "primary:300750.SZ",
                        "run_id": "run-1",
                        "lifecycle_id": "life-1",
                        "artifact_ids": ["artifact-1", "artifact-2"],
                        "as_of_date": "2026-04-30",
                        "source_scope": "stock",
                        "producer": "scripts.run_primary_result_lifecycle",
                        "code_revision": "rev",
                    },
                },
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    (app_dir / "artifacts" / "stock_entry_guard_latest.json").write_text(
        json.dumps({"ok": True, "run_id": "run-1", "lifecycle_id": "life-1", "problems": []}),
        encoding="utf-8",
    )
    (app_dir / "artifacts" / "artifact_registry.jsonl").write_text(
        json.dumps(
            {
                "artifact_id": "artifact-1",
                "artifact_type": "primary_result_audit",
                "result_id": "primary:300750.SZ",
                "run_id": "run-1",
                "path": str(audit_path),
                "sha256": sha256_file(audit_path),
                "created_at": "2026-04-30T00:00:00+00:00",
                "producer": "scripts.run_primary_result_lifecycle",
                "code_revision": "rev",
                "metadata": {
                    "lifecycle_id": "life-1",
                    "step": "audit",
                    "status": "passed",
                    "ts_code": "300750.SZ",
                },
            }
        )
        + "\n"
        + json.dumps(
            {
                "artifact_id": "artifact-2",
                "artifact_type": "primary_result_lifecycle_evidence",
                "result_id": "primary:300750.SZ",
                "run_id": "run-1",
                "path": str(lifecycle_evidence_path),
                "sha256": sha256_file(lifecycle_evidence_path),
                "created_at": "2026-04-30T00:00:00+00:00",
                "producer": "scripts.run_primary_result_lifecycle",
                "code_revision": "rev",
                "parent_artifact_ids": ["artifact-1"],
                "metadata": {
                    "lifecycle_id": "life-1",
                    "ts_code": "300750.SZ",
                    "status": "passed",
                },
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (app_dir / "artifacts" / "result_registry" / "current.json").write_text(
        json.dumps(
            {
                "pointer_version": "result_registry_current_pointer.v1",
                "record_id": "result-record-1",
                "result_id": "primary:300750.SZ",
                "run_id": "run-1",
                "updated_at": "2026-04-30T00:00:00+00:00",
                "entry_path": str(app_dir / "artifacts" / "result_registry" / "history" / "result-record-1.json"),
                "source_scope": "stock",
            }
        ),
        encoding="utf-8",
    )
    (app_dir / "artifacts" / "result_registry" / "history").mkdir(parents=True, exist_ok=True)
    (app_dir / "artifacts" / "result_registry" / "history" / "result-record-1.json").write_text(
        json.dumps(
            {
                "record_id": "result-record-1",
                "result_id": "primary:300750.SZ",
                "run_id": "run-1",
                "ts_code": "300750.SZ",
                "stock_name": "宁德时代",
                "lifecycle_stage": "L3",
                "artifact_ids": ["artifact-1", "artifact-2"],
                "registered_at": "2026-04-30T00:00:00+00:00",
                "source_scope": "stock",
                "metadata": {
                    "lifecycle_id": "life-1",
                    "evidence_path": str(lifecycle_evidence_path),
                    "lifecycle_status": "passed",
                    "artifact_registry_path": str(app_dir / "artifacts" / "artifact_registry.jsonl"),
                    "source_scope": "stock",
                    "producer": "scripts.run_primary_result_lifecycle",
                    "code_revision": "rev",
                },
            }
        ),
        encoding="utf-8",
    )
    (app_dir / "artifacts" / "run_registry" / "history").mkdir(parents=True, exist_ok=True)
    (app_dir / "artifacts" / "run_registry" / "history" / "run-1.json").write_text(
        json.dumps(
            {
                "run_id": "run-1",
                "run_type": "primary_result_lifecycle",
                "status": "completed",
                "created_at": "2026-04-30T00:00:00+00:00",
                "producer": "scripts.run_primary_result_lifecycle",
                "metadata": {
                    "lifecycle_id": "life-1",
                    "result_id": "primary:300750.SZ",
                    "ts_code": "300750.SZ",
                    "source_scope": "stock",
                    "evidence_path": str(lifecycle_evidence_path),
                    "artifact_registry_path": str(app_dir / "artifacts" / "artifact_registry.jsonl"),
                },
                "config_hash": "cfg",
                "data_snapshot_id": "snapshot",
                "code_revision": "rev",
            }
        ),
        encoding="utf-8",
    )
    (app_dir / "artifacts" / "run_registry" / "current.json").write_text(
        json.dumps(
            {
                "pointer_version": "run_registry_current_pointer.v1",
                "run_id": "run-1",
                "run_type": "primary_result_lifecycle",
                "updated_at": "2026-04-30T00:00:00+00:00",
                "entry_path": str(app_dir / "artifacts" / "run_registry" / "history" / "run-1.json"),
            }
        ),
        encoding="utf-8",
    )
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
    if "/apex/stock" in url:
        return 410, "gone"
    if url.endswith("/stock/api/primary-result"):
        return 200, '{"schema_version":"primary_result_v1","result_id":"primary:300750.SZ","run_id":"run-1","lifecycle_id":"life-1"}'
    if url.endswith("/T12/api/stock-ai-runner") or url.endswith("/T12/ops/stock-ai-runner"):
        return 404, "not found"
    if url.endswith("/stock/"):
        return 200, _dashboard_html()
    return 200, f"ok {url}"


def _write_activation_plan(tmp_path, scope="stock-scoped"):
    activation_plan_path = tmp_path / f"activation_plan_{scope}.json"
    activation_plan_path.write_text(
        json.dumps({"scope": scope, "rollback_commands": ["systemctl restart stock-ultimate-dashboard.service"]}),
        encoding="utf-8",
    )
    return activation_plan_path


def test_server_post_deploy_verification_passes_with_service_routes_and_rollback_hint(tmp_path):
    app_dir = _create_app_dir(tmp_path)
    activation_plan_path = _write_activation_plan(tmp_path, "stock-scoped")
    log_paths = [
        tmp_path / "main_site.err.log",
        tmp_path / "dashboard.err.log",
        tmp_path / "t12.err.log",
    ]
    for log_path in log_paths:
        log_path.write_text("startup ok\n", encoding="utf-8")

    exit_code, payload = run_server_post_deploy_verification(
        app_root=app_dir.parents[0],
        app_dir=app_dir,
        log_paths=log_paths,
        activation_plan_path=activation_plan_path,
        command_runner=_active_service_runner,
        url_fetcher=_ok_url_fetcher,
    )

    assert exit_code == 0
    assert payload["status"] == "passed"
    assert payload["rollout_scope"] == "stock-scoped"
    assert payload["rollback_hint"]["available"] is True
    assert payload["rollback_hint"]["scope"] == "stock-scoped"
    assert {check["check"]: check["passed"] for check in payload["checks"]} == {
        "systemd_services_active": True,
        "dashboard_service_canonical_environment": True,
        "nginx_config_valid": True,
        "dashboard_http_targets": True,
        "primary_result_api_pointer_alignment": True,
        "apex_stock_decommissioned": True,
        "stock_entry_guard_ok": True,
        "current_result_pointer_integrity": True,
        "required_app_paths": True,
        "protected_runtime_paths_observed": True,
        "service_error_log_scan": True,
    }
    assert payload["passed"] is True
    assert payload["target"] == "formal"
    assert payload["guard_ok"] is True
    assert payload["pointer_integrity_ok"] is True
    assert payload["t12_ai_runner_blocked"] is True
    latest_output_path = Path(payload["artifacts"]["latest_output_path"])
    history_output_path = Path(payload["artifacts"]["history_output_path"])
    assert latest_output_path.exists()
    assert history_output_path.exists()
    service_check = {check["check"]: check for check in payload["checks"]}["systemd_services_active"]
    assert [result["service_name"] for result in service_check["details"]["results"]] == [
        "stock-ultimate-dashboard.service",
        "stock-ultimate-entry-guard.timer",
    ]


def test_server_post_deploy_verification_rejects_failed_http_endpoint(tmp_path):
    app_dir = _create_app_dir(tmp_path)

    def failing_fetcher(url, timeout):
        if url.endswith("/stock/api/primary-result"):
            return 500, "error"
        if url.endswith("/T12/api/stock-ai-runner") or url.endswith("/T12/ops/stock-ai-runner"):
            return 404, "not found"
        return 200, "ok"

    exit_code, payload = run_server_post_deploy_verification(
        app_root=app_dir.parents[0],
        app_dir=app_dir,
        log_paths=[tmp_path / "missing.log"],
        command_runner=_active_service_runner,
        url_fetcher=failing_fetcher,
    )

    checks = {check["check"]: check for check in payload["checks"]}
    assert exit_code == 1
    assert payload["status"] == "failed"
    assert checks["dashboard_http_targets"]["passed"] is False


def test_server_post_deploy_verification_rejects_missing_dashboard_canonical_environment(tmp_path):
    app_dir = _create_app_dir(tmp_path)

    def runner(command, timeout=10.0):
        if command == ["nginx", "-t"]:
            return subprocess.CompletedProcess(command, 0, stdout="", stderr="")
        if command == ["systemctl", "show", "stock-ultimate-dashboard.service", "-p", "Environment", "--no-pager"]:
            return subprocess.CompletedProcess(command, 0, stdout="Environment=PYTHONUNBUFFERED=1\n", stderr="")
        return subprocess.CompletedProcess(command, 0, stdout="active\n", stderr="")

    exit_code, payload = run_server_post_deploy_verification(
        app_root=app_dir.parents[0],
        app_dir=app_dir,
        log_paths=[tmp_path / "missing.log"],
        command_runner=runner,
        url_fetcher=_ok_url_fetcher,
    )

    checks = {check["check"]: check for check in payload["checks"]}
    assert exit_code == 1
    assert payload["status"] == "failed"
    assert checks["dashboard_service_canonical_environment"]["passed"] is False


def test_server_post_deploy_verification_flags_live_apex_stock_content_for_stock_scoped_rollout(tmp_path):
    app_dir = _create_app_dir(tmp_path)
    activation_plan_path = _write_activation_plan(tmp_path, "stock-scoped")

    def stale_apex_fetcher(url, timeout):
        if url.endswith("/stock/api/primary-result"):
            return 200, '{"schema_version":"primary_result_v1","result_id":"primary:300750.SZ","run_id":"run-1","lifecycle_id":"life-1"}'
        if url.endswith("/T12/api/stock-ai-runner") or url.endswith("/T12/ops/stock-ai-runner"):
            return 404, "not found"
        if url.endswith("/apex/stock/"):
            return 200, _dashboard_html()
        if url.endswith("/apex/stock/api/primary-result"):
            return 200, '{"schema_version":"primary_result_v1","result_id":"primary:300750.SZ","run_id":"run-1","lifecycle_id":"life-1"}'
        if url.endswith("/stock/"):
            return 200, _dashboard_html(primary_progress="1/20", basket_progress="8/20", promotion_gate="晋级锁定")
        return 200, "ok"

    exit_code, payload = run_server_post_deploy_verification(
        app_root=app_dir.parents[0],
        app_dir=app_dir,
        activation_plan_path=activation_plan_path,
        log_paths=[tmp_path / "missing.log"],
        command_runner=_active_service_runner,
        url_fetcher=stale_apex_fetcher,
    )

    checks = {check["check"]: check for check in payload["checks"]}
    assert exit_code == 0
    assert payload["status"] == "passed"
    assert checks["apex_stock_decommissioned"]["passed"] is False
    assert "apex_stock_decommissioned" in payload["advisory_failures"]


def test_server_post_deploy_verification_rejects_primary_result_api_pointer_drift(tmp_path):
    app_dir = _create_app_dir(tmp_path)

    def drift_fetcher(url, timeout):
        if url.endswith("/stock/api/primary-result"):
            return 200, '{"schema_version":"primary_result_v1","result_id":"primary:300001.SZ","run_id":"run-x","lifecycle_id":"life-x"}'
        if url.endswith("/T12/api/stock-ai-runner") or url.endswith("/T12/ops/stock-ai-runner"):
            return 404, "not found"
        if "/apex/stock" in url:
            return 410, "gone"
        if url.endswith("/stock/"):
            return 200, _dashboard_html()
        return 200, "ok"

    exit_code, payload = run_server_post_deploy_verification(
        app_root=app_dir.parents[0],
        app_dir=app_dir,
        log_paths=[tmp_path / "missing.log"],
        command_runner=_active_service_runner,
        url_fetcher=drift_fetcher,
    )

    checks = {check["check"]: check for check in payload["checks"]}
    assert exit_code == 1
    assert payload["status"] == "failed"
    assert checks["dashboard_http_targets"]["passed"] is True
    assert checks["primary_result_api_pointer_alignment"]["passed"] is False
    assert checks["primary_result_api_pointer_alignment"]["details"]["mismatched_fields"] == [
        "result_id",
        "run_id",
        "lifecycle_id",
    ]


def test_server_post_deploy_verification_flags_live_apex_stock_api_content(tmp_path):
    app_dir = _create_app_dir(tmp_path)
    activation_plan_path = _write_activation_plan(tmp_path, "stock-scoped")

    def drift_fetcher(url, timeout):
        if url.endswith("/apex/stock/api/primary-result"):
            return 200, '{"schema_version":"primary_result_v1","result_id":"primary:300001.SZ","run_id":"run-x","lifecycle_id":"life-x"}'
        if url.endswith("/stock/api/primary-result"):
            return 200, '{"schema_version":"primary_result_v1","result_id":"primary:300750.SZ","run_id":"run-1","lifecycle_id":"life-1"}'
        if url.endswith("/T12/api/stock-ai-runner") or url.endswith("/T12/ops/stock-ai-runner"):
            return 404, "not found"
        if url.endswith("/stock/") or url.endswith("/apex/stock/"):
            return 200, _dashboard_html()
        return 200, "ok"

    exit_code, payload = run_server_post_deploy_verification(
        app_root=app_dir.parents[0],
        app_dir=app_dir,
        activation_plan_path=activation_plan_path,
        log_paths=[tmp_path / "missing.log"],
        command_runner=_active_service_runner,
        url_fetcher=drift_fetcher,
    )

    checks = {check["check"]: check for check in payload["checks"]}
    assert exit_code == 0
    assert payload["status"] == "passed"
    assert checks["dashboard_http_targets"]["passed"] is True
    assert checks["primary_result_api_pointer_alignment"]["passed"] is True
    assert checks["apex_stock_decommissioned"]["passed"] is False
    assert "apex_stock_decommissioned" in payload["advisory_failures"]


def test_server_post_deploy_verification_rejects_live_apex_stock_content_for_full_domain(tmp_path):
    app_dir = _create_app_dir(tmp_path)
    activation_plan_path = _write_activation_plan(tmp_path, "full-domain")

    def drift_fetcher(url, timeout):
        if url.endswith("/apex/stock/api/primary-result"):
            return 200, '{"schema_version":"primary_result_v1","result_id":"primary:300001.SZ","run_id":"run-x","lifecycle_id":"life-x"}'
        if url.endswith("/stock/api/primary-result"):
            return 200, '{"schema_version":"primary_result_v1","result_id":"primary:300750.SZ","run_id":"run-1","lifecycle_id":"life-1"}'
        if url.endswith("/T12/api/stock-ai-runner") or url.endswith("/T12/ops/stock-ai-runner"):
            return 404, "not found"
        if url.endswith("/stock/") or url.endswith("/apex/stock/"):
            return 200, _dashboard_html()
        return 200, "ok"

    exit_code, payload = run_server_post_deploy_verification(
        app_root=app_dir.parents[0],
        app_dir=app_dir,
        activation_plan_path=activation_plan_path,
        log_paths=[tmp_path / "missing.log"],
        command_runner=_active_service_runner,
        url_fetcher=drift_fetcher,
    )

    checks = {check["check"]: check for check in payload["checks"]}
    assert exit_code == 1
    assert payload["status"] == "failed"
    assert checks["apex_stock_decommissioned"]["passed"] is False
    assert payload["advisory_failures"] == []


def test_fetch_url_reads_enough_html_for_progress_contract(monkeypatch):
    html = ("x" * 5000) + _dashboard_html(primary_progress="1/20", basket_progress="8/20", promotion_gate="晋级锁定")

    class _FakeResponse:
        status = 200

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def read(self, _limit):
            return html.encode("utf-8")

    monkeypatch.setattr(post_deploy_verification, "urlopen", lambda request, timeout=5.0: _FakeResponse())

    status_code, body = post_deploy_verification._fetch_url("https://airivo.online/stock/")

    assert status_code == 200
    assert "主结果证据</span><strong>1/20" in body


def test_server_post_deploy_verification_rejects_html_for_primary_result_api(tmp_path):
    app_dir = _create_app_dir(tmp_path)

    def html_fetcher(url, timeout):
        if url.endswith("/stock/api/primary-result"):
            return 200, "<!doctype html><html><title>Airivo Alpha</title></html>"
        if url.endswith("/T12/api/stock-ai-runner") or url.endswith("/T12/ops/stock-ai-runner"):
            return 404, "not found"
        return 200, "ok"

    exit_code, payload = run_server_post_deploy_verification(
        app_root=app_dir.parents[0],
        app_dir=app_dir,
        log_paths=[tmp_path / "missing.log"],
        command_runner=_active_service_runner,
        url_fetcher=html_fetcher,
    )

    checks = {check["check"]: check for check in payload["checks"]}
    api_result = next(
        item
        for item in checks["dashboard_http_targets"]["details"]["results"]
        if item["name"] == "stock_primary_result_api"
    )
    assert exit_code == 1
    assert payload["status"] == "failed"
    assert api_result["content_expectation"] == "json_object"
    assert api_result["content_passed"] is False


def test_server_post_deploy_verification_rejects_guard_blocked_primary_result_api(tmp_path):
    app_dir = _create_app_dir(tmp_path)

    def blocked_fetcher(url, timeout):
        if url.endswith("/stock/api/primary-result"):
            return 200, json.dumps(
                {
                    "disabled_reason": "stock entry guard blocked primary result publication.",
                    "entry_guard": {"ok": False, "problems": ["primary_result_lifecycle_evidence_latest.json missing"]},
                },
                ensure_ascii=False,
            )
        if url.endswith("/T12/api/stock-ai-runner") or url.endswith("/T12/ops/stock-ai-runner"):
            return 404, "not found"
        return 200, "ok"

    exit_code, payload = run_server_post_deploy_verification(
        app_root=app_dir.parents[0],
        app_dir=app_dir,
        log_paths=[tmp_path / "missing.log"],
        command_runner=_active_service_runner,
        url_fetcher=blocked_fetcher,
    )

    checks = {check["check"]: check for check in payload["checks"]}
    api_result = next(
        item
        for item in checks["dashboard_http_targets"]["details"]["results"]
        if item["name"] == "stock_primary_result_api"
    )
    assert exit_code == 1
    assert payload["status"] == "failed"
    assert api_result["content_expectation"] == "json_object"
    assert api_result["content_passed"] is False


def test_server_post_deploy_verification_rejects_error_log_pattern(tmp_path):
    app_dir = _create_app_dir(tmp_path)
    log_path = tmp_path / "dashboard.err.log"
    log_path.write_text("Traceback: startup failed\n", encoding="utf-8")

    exit_code, payload = run_server_post_deploy_verification(
        app_root=app_dir.parents[0],
        app_dir=app_dir,
        log_paths=[log_path],
        command_runner=_active_service_runner,
        url_fetcher=_ok_url_fetcher,
    )

    checks = {check["check"]: check for check in payload["checks"]}
    assert exit_code == 1
    assert payload["status"] == "failed"
    assert checks["service_error_log_scan"]["passed"] is False
    assert checks["service_error_log_scan"]["details"]["results"][0]["matched_patterns"] == ["traceback"]


def test_server_post_deploy_verification_rejects_failed_nginx_config(tmp_path):
    app_dir = _create_app_dir(tmp_path)

    def runner(command, timeout=10.0):
        if command == ["nginx", "-t"]:
            return subprocess.CompletedProcess(command, 1, stdout="", stderr="nginx failed\n")
        return subprocess.CompletedProcess(command, 0, stdout="active\n", stderr="")

    exit_code, payload = run_server_post_deploy_verification(
        app_root=app_dir.parents[0],
        app_dir=app_dir,
        log_paths=[tmp_path / "missing.log"],
        command_runner=runner,
        url_fetcher=_ok_url_fetcher,
    )

    checks = {check["check"]: check for check in payload["checks"]}
    assert exit_code == 1
    assert payload["status"] == "failed"
    assert checks["nginx_config_valid"]["passed"] is False


def test_server_post_deploy_verification_cli_outputs_json(tmp_path):
    project_root = Path(__file__).resolve().parents[1]
    script_path = project_root / "scripts" / "run_server_post_deploy_verification.py"

    completed = subprocess.run(
        [
            sys.executable,
            str(script_path),
            "--app-root",
            str(tmp_path / "stock-ultimate"),
            "--app-dir",
            str(tmp_path / "missing-app"),
            "--json",
        ],
        cwd=project_root,
        capture_output=True,
        text=True,
        check=False,
    )
    payload = json.loads(completed.stdout)

    assert completed.returncode == 1
    assert payload["post_deploy_version"] == "server_post_deploy_verification.v1"
    assert payload["status"] == "failed"


def test_server_post_deploy_verification_rejects_t12_ai_runner_endpoint_not_blocked(tmp_path):
    app_dir = _create_app_dir(tmp_path)

    def unblocked_fetcher(url, timeout):
        if url.endswith("/stock/api/primary-result"):
            return 200, '{"status":"ok","result":{}}'
        if url.endswith("/T12/api/stock-ai-runner") or url.endswith("/T12/ops/stock-ai-runner"):
            return 200, "unexpected"
        return 200, "ok"

    exit_code, payload = run_server_post_deploy_verification(
        app_root=app_dir.parents[0],
        app_dir=app_dir,
        log_paths=[tmp_path / "missing.log"],
        command_runner=_active_service_runner,
        url_fetcher=unblocked_fetcher,
    )

    checks = {check["check"]: check for check in payload["checks"]}
    assert exit_code == 1
    assert payload["status"] == "failed"
    assert payload["t12_ai_runner_blocked"] is False
    blocked_results = {
        item["name"]: item["passed"]
        for item in checks["dashboard_http_targets"]["details"]["results"]
        if item["name"] in {"t12_ai_runner_api", "t12_ai_runner_ops"}
    }
    assert blocked_results == {"t12_ai_runner_api": False, "t12_ai_runner_ops": False}
