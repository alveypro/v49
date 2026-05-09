#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path


ACTIVATION_PLAN_VERSION = "server_activation_plan.v1"
DEFAULT_APP_ROOT = Path("/opt/stock-ultimate")
DEFAULT_ACTIVATION_SCOPE = "stock-scoped"
SUPPORTED_ACTIVATION_SCOPES = ("stock-scoped", "full-domain")
STOCK_SCOPED_REQUIREMENTS_FILE = "requirements.stock-scoped.txt"
REQUIRED_STAGING_FILES = (
    "requirements.txt",
    STOCK_SCOPED_REQUIREMENTS_FILE,
    "run_dashboard.py",
    "run_update_database.py",
    "src/airivo_scope_registry.py",
    "scripts/run_server_sync_preflight.py",
    "scripts/build_server_sync_file_list.py",
    "scripts/build_server_activation_plan.py",
    "scripts/build_server_deploy_evidence_bundle.py",
    "scripts/build_server_go_live_readiness.py",
    "scripts/register_server_deploy.py",
    "scripts/run_server_activation_plan.py",
    "scripts/run_server_domain_preflight.py",
    "scripts/run_stock_entry_guard.py",
    "scripts/run_server_deploy_rollback.py",
    "scripts/run_server_post_deploy_verification.py",
    "deploy/aliyun/stock-ultimate-dashboard.service",
    "deploy/aliyun/stock-ultimate-dashboard.service.d/canonical-artifacts.conf",
    "deploy/aliyun/stock-ultimate-entry-guard.service",
    "deploy/aliyun/stock-ultimate-entry-guard.timer",
    "deploy/aliyun/stock-ultimate-main-site.service",
    "deploy/aliyun/stock-ultimate-t12.service",
    "deploy/aliyun/stock-ultimate-update.service",
    "deploy/aliyun/stock-ultimate-update.timer",
    "deploy/aliyun/stock-ultimate-daily-research.service",
    "deploy/aliyun/stock-ultimate-daily-research.timer",
    "deploy/aliyun/stock-ultimate-nightly-research.service",
    "deploy/aliyun/stock-ultimate-nightly-research.timer",
    "deploy/aliyun/stock-ultimate-weekly-long.service",
    "deploy/aliyun/stock-ultimate-weekly-long.timer",
    "deploy/aliyun/stock-ultimate-healthcheck.service",
    "deploy/aliyun/stock-ultimate-healthcheck.timer",
    "deploy/aliyun/logrotate.stock-ultimate",
    "deploy/aliyun/nginx.airivo.online.conf",
    "deploy/aliyun/settings.server.yaml",
)
REQUIRED_STAGING_FILES_BY_SCOPE = {
    "stock-scoped": (
        "requirements.txt",
        STOCK_SCOPED_REQUIREMENTS_FILE,
        "run_dashboard.py",
        "run_update_database.py",
        "src/airivo_scope_registry.py",
        "scripts/run_server_sync_preflight.py",
        "scripts/build_server_sync_file_list.py",
        "scripts/build_server_activation_plan.py",
        "scripts/build_server_deploy_evidence_bundle.py",
        "scripts/build_server_go_live_readiness.py",
        "scripts/register_server_deploy.py",
        "scripts/run_server_activation_plan.py",
        "scripts/run_server_domain_preflight.py",
        "scripts/run_stock_entry_guard.py",
        "scripts/run_server_deploy_rollback.py",
        "scripts/run_server_post_deploy_verification.py",
        "deploy/aliyun/stock-ultimate-dashboard.service",
        "deploy/aliyun/stock-ultimate-dashboard.service.d/canonical-artifacts.conf",
        "deploy/aliyun/stock-ultimate-entry-guard.service",
        "deploy/aliyun/stock-ultimate-entry-guard.timer",
        "deploy/aliyun/settings.server.yaml",
    ),
    "full-domain": REQUIRED_STAGING_FILES,
}


@dataclass(frozen=True)
class ServerActivationLayout:
    app_root: Path
    staging_dir: Path
    app_dir: Path
    releases_dir: Path
    release_id: str

    @property
    def backup_dir(self) -> Path:
        return self.releases_dir / f"{self.release_id}_previous_app"

    @property
    def nginx_backup_path(self) -> Path:
        return self.releases_dir / f"{self.release_id}_previous_nginx.airivo.online.conf"


def _normalize_scope(scope: str) -> str:
    normalized = str(scope).strip().lower()
    if normalized not in SUPPORTED_ACTIVATION_SCOPES:
        raise ValueError(
            f"unsupported activation scope: {scope!r}; expected one of {', '.join(SUPPORTED_ACTIVATION_SCOPES)}"
        )
    return normalized


def _quote(path: Path | str) -> str:
    text = str(path)
    return "'" + text.replace("'", "'\"'\"'") + "'"


def _missing_required_files(staging_dir: Path, *, scope: str) -> list[str]:
    required_files = REQUIRED_STAGING_FILES_BY_SCOPE[scope]
    return [
        relative_path
        for relative_path in required_files
        if not (staging_dir / relative_path).is_file()
    ]


def _build_stock_scoped_activation_commands(layout: ServerActivationLayout) -> list[str]:
    requirements_path = layout.app_dir / STOCK_SCOPED_REQUIREMENTS_FILE
    return [
        f"mkdir -p {_quote(layout.releases_dir)}",
        f"rm -rf {_quote(layout.backup_dir)}",
        f"cp -a {_quote(layout.app_dir)} {_quote(layout.backup_dir)}",
        f"rsync -a --delete --exclude data/ --exclude artifacts/ {_quote(str(layout.staging_dir) + '/')} {_quote(str(layout.app_dir) + '/')}",
        f"mkdir -p {_quote(layout.app_dir / 'config/server')}",
        f"cp {_quote(layout.app_dir / 'deploy/aliyun/settings.server.yaml')} {_quote(layout.app_dir / 'config/server/settings.yaml')}",
        f"{_quote(layout.app_root / '.venv/bin/pip')} install -r {_quote(requirements_path)}",
        f"cp {_quote(layout.app_dir / 'deploy/aliyun/stock-ultimate-dashboard.service')} /etc/systemd/system/",
        "mkdir -p /etc/systemd/system/stock-ultimate-dashboard.service.d",
        f"cp {_quote(layout.app_dir / 'deploy/aliyun/stock-ultimate-dashboard.service.d/canonical-artifacts.conf')} /etc/systemd/system/stock-ultimate-dashboard.service.d/canonical-artifacts.conf",
        f"cp {_quote(layout.app_dir / 'deploy/aliyun/stock-ultimate-entry-guard.service')} /etc/systemd/system/",
        f"cp {_quote(layout.app_dir / 'deploy/aliyun/stock-ultimate-entry-guard.timer')} /etc/systemd/system/",
        "systemctl daemon-reload",
        "systemctl restart stock-ultimate-dashboard.service",
        "systemctl start stock-ultimate-entry-guard.service",
        "systemctl enable --now stock-ultimate-entry-guard.timer",
        "systemctl is-active --quiet stock-ultimate-dashboard.service",
        "systemctl is-active --quiet stock-ultimate-entry-guard.timer",
    ]


def _build_stock_scoped_rollback_commands(layout: ServerActivationLayout) -> list[str]:
    return [
        f"test -d {_quote(layout.backup_dir)}",
        f"rsync -a --delete {_quote(str(layout.backup_dir) + '/')} {_quote(str(layout.app_dir) + '/')}",
        "mkdir -p /etc/systemd/system/stock-ultimate-dashboard.service.d",
        f"cp {_quote(layout.app_dir / 'deploy/aliyun/stock-ultimate-dashboard.service.d/canonical-artifacts.conf')} /etc/systemd/system/stock-ultimate-dashboard.service.d/canonical-artifacts.conf",
        "systemctl daemon-reload",
        "systemctl restart stock-ultimate-dashboard.service",
        "systemctl start stock-ultimate-entry-guard.service",
        "systemctl enable --now stock-ultimate-entry-guard.timer",
        "systemctl is-active --quiet stock-ultimate-dashboard.service",
        "systemctl is-active --quiet stock-ultimate-entry-guard.timer",
    ]


def _build_activation_commands(layout: ServerActivationLayout, *, scope: str) -> list[str]:
    if scope == "stock-scoped":
        return _build_stock_scoped_activation_commands(layout)
    raise ValueError(f"activation commands are not available for scope: {scope}")


def _build_rollback_commands(layout: ServerActivationLayout, *, scope: str) -> list[str]:
    if scope == "stock-scoped":
        return _build_stock_scoped_rollback_commands(layout)
    raise ValueError(f"rollback commands are not available for scope: {scope}")


def _write_output(path: str | Path | None, payload: dict[str, object]) -> None:
    if not path:
        return
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _read_json(path: str | Path) -> dict[str, object]:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"expected object json: {path}")
    return payload


def _domain_preflight_summary(path: str | Path | None) -> tuple[dict[str, object], list[str]]:
    if not path:
        return {"status": "not_provided"}, []
    resolved_path = Path(path)
    payload = _read_json(resolved_path)
    errors = []
    if payload.get("domain_preflight_version") != "server_domain_preflight.v1":
        errors.append("domain preflight version is invalid")
    if payload.get("status") != "passed":
        errors.append("domain preflight status must be passed")
    return (
        {
            "path": str(resolved_path),
            "version": payload.get("domain_preflight_version"),
            "status": payload.get("status"),
            "target_domain": payload.get("target_domain"),
            "blocking_failure_total": len(payload.get("blocking_failures", []) or []),
        },
        errors,
    )


def build_server_activation_plan(
    *,
    staging_dir: str | Path,
    release_id: str,
    scope: str = DEFAULT_ACTIVATION_SCOPE,
    app_root: str | Path = DEFAULT_APP_ROOT,
    app_dir: str | Path | None = None,
    releases_dir: str | Path | None = None,
    domain_preflight_json: str | Path | None = None,
    output_path: str | Path | None = None,
) -> tuple[int, dict[str, object]]:
    resolved_scope = _normalize_scope(scope)
    root = Path(app_root)
    layout = ServerActivationLayout(
        app_root=root,
        staging_dir=Path(staging_dir),
        app_dir=Path(app_dir) if app_dir else root / "app",
        releases_dir=Path(releases_dir) if releases_dir else root / "releases",
        release_id=release_id,
    )
    missing = _missing_required_files(layout.staging_dir, scope=resolved_scope)
    domain_preflight, domain_preflight_errors = _domain_preflight_summary(domain_preflight_json)
    blocking_errors = list(domain_preflight_errors)
    if resolved_scope == "full-domain":
        blocking_errors.append(
            "full-domain activation is blocked in the current phase; use stock-scoped rollout until ownership and T12 topology are frozen"
        )
    status = "passed" if not missing and not blocking_errors else "failed"
    payload: dict[str, object] = {
        "activation_plan_version": ACTIVATION_PLAN_VERSION,
        "status": status,
        "release_id": release_id,
        "scope": resolved_scope,
        "layout": {
            "app_root": str(layout.app_root),
            "staging_dir": str(layout.staging_dir),
            "app_dir": str(layout.app_dir),
            "releases_dir": str(layout.releases_dir),
            "backup_dir": str(layout.backup_dir),
            "nginx_backup_path": str(layout.nginx_backup_path),
        },
        "domain_preflight": domain_preflight,
        "required_staging_files": list(REQUIRED_STAGING_FILES_BY_SCOPE[resolved_scope]),
        "missing_required_files": missing,
        "blocking_errors": blocking_errors,
        "activation_commands": [] if status != "passed" else _build_activation_commands(layout, scope=resolved_scope),
        "rollback_commands": [] if status != "passed" else _build_rollback_commands(layout, scope=resolved_scope),
        "scope_constraints": {
            "allows_live_nginx_replacement": False,
            "allows_main_site_cutover": False,
            "allows_t12_cutover": False,
            "requires_8764_listener": False,
            "requires_8766_listener": False,
        }
        if resolved_scope == "stock-scoped"
        else {
            "allows_live_nginx_replacement": True,
            "allows_main_site_cutover": True,
            "allows_t12_cutover": True,
            "requires_8764_listener": True,
            "requires_8766_listener": True,
        },
    }
    _write_output(output_path, payload)
    return (0 if payload["status"] == "passed" else 1), payload


def main() -> int:
    parser = argparse.ArgumentParser(description="Build an auditable server staging activation plan.")
    parser.add_argument("--staging-dir", default="/opt/stock-ultimate/staging")
    parser.add_argument("--release-id", required=True)
    parser.add_argument("--scope", default=DEFAULT_ACTIVATION_SCOPE, choices=SUPPORTED_ACTIVATION_SCOPES)
    parser.add_argument("--app-root", default=str(DEFAULT_APP_ROOT))
    parser.add_argument("--app-dir")
    parser.add_argument("--releases-dir")
    parser.add_argument("--domain-preflight-json")
    parser.add_argument("--output")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    exit_code, payload = build_server_activation_plan(
        staging_dir=args.staging_dir,
        release_id=args.release_id,
        scope=args.scope,
        app_root=args.app_root,
        app_dir=args.app_dir,
        releases_dir=args.releases_dir,
        domain_preflight_json=args.domain_preflight_json,
        output_path=args.output,
    )
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print(
            json.dumps(
                {
                    "status": payload["status"],
                    "activation_plan_version": payload["activation_plan_version"],
                    "release_id": payload["release_id"],
                    "missing_required_files": payload["missing_required_files"],
                    "activation_command_total": len(payload["activation_commands"]),
                    "rollback_command_total": len(payload["rollback_commands"]),
                },
                ensure_ascii=False,
                indent=2,
            )
        )
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
