import json
import subprocess
import sys
from pathlib import Path

from scripts.build_server_activation_plan import REQUIRED_STAGING_FILES, build_server_activation_plan


def _create_staging_dir(tmp_path):
    staging_dir = tmp_path / "staging"
    for relative_path in REQUIRED_STAGING_FILES:
        path = staging_dir / relative_path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("x\n", encoding="utf-8")
    return staging_dir


def _write_domain_preflight(tmp_path, *, status="passed"):
    path = tmp_path / "domain_preflight.json"
    payload = {
        "domain_preflight_version": "server_domain_preflight.v1",
        "status": status,
        "target_domain": "airivo.online",
        "blocking_failures": [] if status == "passed" else [{"check": "target_domain_not_owned_by_unmanaged_config"}],
    }
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    return path


def test_build_server_activation_plan_requires_staging_files(tmp_path):
    staging_dir = _create_staging_dir(tmp_path)
    (staging_dir / "run_dashboard.py").unlink()

    exit_code, payload = build_server_activation_plan(
        staging_dir=staging_dir,
        release_id="release-20260414-001",
        app_root=tmp_path / "stock-ultimate",
    )

    assert exit_code == 1
    assert payload["status"] == "failed"
    assert payload["missing_required_files"] == ["run_dashboard.py"]
    assert payload["activation_commands"] == []
    assert payload["rollback_commands"] == []


def test_build_server_activation_plan_outputs_activation_and_rollback_commands(tmp_path):
    staging_dir = _create_staging_dir(tmp_path)
    output_path = tmp_path / "activation_plan.json"

    exit_code, payload = build_server_activation_plan(
        staging_dir=staging_dir,
        release_id="release-20260414-001",
        app_root=tmp_path / "stock-ultimate",
        output_path=output_path,
    )

    assert exit_code == 0
    assert payload["status"] == "passed"
    assert payload["scope"] == "stock-scoped"
    assert payload["layout"]["backup_dir"].endswith("release-20260414-001_previous_app")
    assert payload["layout"]["nginx_backup_path"].endswith("release-20260414-001_previous_nginx.airivo.online.conf")
    assert any("rsync -a --delete --exclude data/ --exclude artifacts/" in command for command in payload["activation_commands"])
    assert any("requirements.stock-scoped.txt" in command for command in payload["activation_commands"])
    assert "mkdir -p /etc/systemd/system/stock-ultimate-dashboard.service.d" in payload["activation_commands"]
    assert any(
        "stock-ultimate-dashboard.service.d/canonical-artifacts.conf" in command
        for command in payload["activation_commands"]
    )
    assert any("systemctl restart stock-ultimate-dashboard.service" == command for command in payload["activation_commands"])
    assert any("systemctl start stock-ultimate-entry-guard.service" == command for command in payload["activation_commands"])
    assert any("systemctl enable --now stock-ultimate-entry-guard.timer" == command for command in payload["activation_commands"])
    assert any("systemctl is-active --quiet stock-ultimate-entry-guard.timer" == command for command in payload["activation_commands"])
    assert any("rsync -a --delete" in command for command in payload["rollback_commands"])
    assert "mkdir -p /etc/systemd/system/stock-ultimate-dashboard.service.d" in payload["rollback_commands"]
    assert any(
        "stock-ultimate-dashboard.service.d/canonical-artifacts.conf" in command
        for command in payload["rollback_commands"]
    )
    assert any("systemctl start stock-ultimate-entry-guard.service" == command for command in payload["rollback_commands"])
    assert not any("stock-ultimate-main-site.service" in command for command in payload["activation_commands"])
    assert not any("stock-ultimate-t12.service" in command for command in payload["activation_commands"])
    assert not any("nginx -t" == command for command in payload["activation_commands"])
    assert not any("systemctl reload nginx" == command for command in payload["activation_commands"])
    assert not any("nginx.airivo.online.conf" in command for command in payload["activation_commands"])
    assert not any("stock-ultimate-main-site.service" in command for command in payload["rollback_commands"])
    assert not any("stock-ultimate-t12.service" in command for command in payload["rollback_commands"])
    assert not any("systemctl reload nginx" == command for command in payload["rollback_commands"])
    assert json.loads(output_path.read_text(encoding="utf-8"))["status"] == "passed"


def test_build_server_activation_plan_accepts_passed_domain_preflight(tmp_path):
    staging_dir = _create_staging_dir(tmp_path)
    preflight_path = _write_domain_preflight(tmp_path)

    exit_code, payload = build_server_activation_plan(
        staging_dir=staging_dir,
        release_id="release-20260414-domain-ok",
        app_root=tmp_path / "stock-ultimate",
        domain_preflight_json=preflight_path,
    )

    assert exit_code == 0
    assert payload["status"] == "passed"
    assert payload["domain_preflight"]["status"] == "passed"
    assert payload["activation_commands"]


def test_build_server_activation_plan_blocks_full_domain_scope_in_current_phase(tmp_path):
    staging_dir = _create_staging_dir(tmp_path)

    exit_code, payload = build_server_activation_plan(
        staging_dir=staging_dir,
        release_id="release-20260501-full-domain-blocked",
        scope="full-domain",
        app_root=tmp_path / "stock-ultimate",
    )

    assert exit_code == 1
    assert payload["status"] == "failed"
    assert payload["scope"] == "full-domain"
    assert payload["activation_commands"] == []
    assert payload["rollback_commands"] == []
    assert payload["blocking_errors"] == [
        "full-domain activation is blocked in the current phase; use stock-scoped rollout until ownership and T12 topology are frozen"
    ]


def test_build_server_activation_plan_rejects_failed_domain_preflight(tmp_path):
    staging_dir = _create_staging_dir(tmp_path)
    preflight_path = _write_domain_preflight(tmp_path, status="failed")

    exit_code, payload = build_server_activation_plan(
        staging_dir=staging_dir,
        release_id="release-20260414-domain-failed",
        app_root=tmp_path / "stock-ultimate",
        domain_preflight_json=preflight_path,
    )

    assert exit_code == 1
    assert payload["status"] == "failed"
    assert payload["blocking_errors"] == ["domain preflight status must be passed"]
    assert payload["activation_commands"] == []
    assert payload["rollback_commands"] == []


def test_build_server_activation_plan_cli_outputs_json(tmp_path):
    project_root = Path(__file__).resolve().parents[1]
    script_path = project_root / "scripts" / "build_server_activation_plan.py"
    staging_dir = _create_staging_dir(tmp_path)

    completed = subprocess.run(
        [
            sys.executable,
            str(script_path),
            "--staging-dir",
            str(staging_dir),
            "--release-id",
            "release-20260414-002",
            "--app-root",
            str(tmp_path / "stock-ultimate"),
            "--json",
        ],
        cwd=project_root,
        capture_output=True,
        text=True,
        check=True,
    )
    payload = json.loads(completed.stdout)

    assert payload["activation_plan_version"] == "server_activation_plan.v1"
    assert payload["status"] == "passed"
    assert payload["release_id"] == "release-20260414-002"
    assert payload["scope"] == "stock-scoped"
