#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path


DOMAIN_PREFLIGHT_VERSION = "server_domain_preflight.v1"
DEFAULT_TARGET_DOMAIN = "airivo.online"
DEFAULT_NGINX_CONF_DIRS = ("/etc/nginx/conf.d", "/etc/nginx/sites-enabled")
MANAGED_BY_MARKER = "managed_by: stock_ultimate_system"
KNOWN_EXISTING_SYSTEM_MARKERS = ("v49.app",)


def _read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8", errors="replace")


def _write_output(path: str | Path | None, payload: dict[str, object]) -> None:
    if not path:
        return
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _iter_nginx_conf_files(conf_dirs: list[Path]) -> list[Path]:
    files: list[Path] = []
    for conf_dir in conf_dirs:
        if not conf_dir.exists() or not conf_dir.is_dir():
            continue
        files.extend(sorted(path for path in conf_dir.glob("*.conf") if path.is_file()))
    return files


def _scan_file(path: Path, *, target_domain: str) -> dict[str, object]:
    text = _read_text(path)
    mentioned_systems = [marker for marker in KNOWN_EXISTING_SYSTEM_MARKERS if marker in text]
    return {
        "path": str(path),
        "mentions_target_domain": target_domain in text,
        "managed_by_stock_ultimate": MANAGED_BY_MARKER in text,
        "mentions_known_existing_systems": mentioned_systems,
        "mentions_stock_route": "location /stock/" in text or "location = /stock" in text,
        "mentions_t12_route": "location /T12/" in text or "location = /T12" in text,
    }


def run_server_domain_preflight(
    *,
    target_domain: str = DEFAULT_TARGET_DOMAIN,
    nginx_conf_dirs: list[str | Path] | None = None,
    output_path: str | Path | None = None,
) -> tuple[int, dict[str, object]]:
    conf_dirs = [Path(path) for path in (nginx_conf_dirs or list(DEFAULT_NGINX_CONF_DIRS))]
    scanned_files = [_scan_file(path, target_domain=target_domain) for path in _iter_nginx_conf_files(conf_dirs)]
    target_domain_configs = [item for item in scanned_files if item["mentions_target_domain"]]
    unmanaged_target_domain_configs = [
        item
        for item in target_domain_configs
        if not item["managed_by_stock_ultimate"]
    ]
    target_domain_existing_system_conflicts = [
        item
        for item in target_domain_configs
        if item["mentions_known_existing_systems"]
    ]
    checks = [
        {
            "check": "nginx_conf_dirs_scanned",
            "passed": True,
            "details": {
                "nginx_conf_dirs": [str(path) for path in conf_dirs],
                "scanned_file_total": len(scanned_files),
            },
        },
        {
            "check": "target_domain_not_owned_by_unmanaged_config",
            "passed": not unmanaged_target_domain_configs,
            "details": {
                "target_domain": target_domain,
                "unmanaged_configs": unmanaged_target_domain_configs,
            },
        },
        {
            "check": "target_domain_not_mixed_with_existing_system",
            "passed": not target_domain_existing_system_conflicts,
            "details": {
                "target_domain": target_domain,
                "known_existing_system_markers": list(KNOWN_EXISTING_SYSTEM_MARKERS),
                "conflicts": target_domain_existing_system_conflicts,
            },
        },
    ]
    blocking_failures = [check for check in checks if not check["passed"]]
    payload = {
        "domain_preflight_version": DOMAIN_PREFLIGHT_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "status": "passed" if not blocking_failures else "failed",
        "target_domain": target_domain,
        "managed_by_marker": MANAGED_BY_MARKER,
        "known_existing_system_markers": list(KNOWN_EXISTING_SYSTEM_MARKERS),
        "scanned_files": scanned_files,
        "checks": checks,
        "blocking_failures": blocking_failures,
    }
    _write_output(output_path, payload)
    return (0 if payload["status"] == "passed" else 1), payload


def main() -> int:
    parser = argparse.ArgumentParser(description="Preflight nginx domain ownership before activating Airivo routes.")
    parser.add_argument("--target-domain", default=DEFAULT_TARGET_DOMAIN)
    parser.add_argument("--nginx-conf-dir", action="append", help="Directory containing nginx *.conf files. May be repeated.")
    parser.add_argument("--output")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    exit_code, payload = run_server_domain_preflight(
        target_domain=args.target_domain,
        nginx_conf_dirs=args.nginx_conf_dir,
        output_path=args.output,
    )
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print(
            json.dumps(
                {
                    "status": payload["status"],
                    "domain_preflight_version": payload["domain_preflight_version"],
                    "target_domain": payload["target_domain"],
                    "blocking_failure_total": len(payload["blocking_failures"]),
                },
                ensure_ascii=False,
                indent=2,
            )
        )
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
