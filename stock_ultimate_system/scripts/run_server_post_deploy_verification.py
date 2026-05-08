#!/usr/bin/env python3
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import subprocess
import sys
from pathlib import Path
from typing import Callable
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


POST_DEPLOY_VERSION = "server_post_deploy_verification.v1"
DEFAULT_APP_ROOT = Path("/opt/stock-ultimate")
DEFAULT_SERVICE_NAMES = (
    "stock-ultimate-dashboard.service",
    "stock-ultimate-entry-guard.timer",
)
DEFAULT_HTTP_TARGETS = (
    {
        "name": "formal_root",
        "url": "https://airivo.online/",
        "expected_statuses": (200, 301, 302, 304),
        "content_expectation": "status_only",
    },
    {
        "name": "stock",
        "url": "https://airivo.online/stock/",
        "expected_statuses": (200, 301, 302, 304),
        "content_expectation": "status_only",
    },
    {
        "name": "stock_primary_result_api",
        "url": "https://airivo.online/stock/api/primary-result",
        "expected_statuses": (200,),
        "content_expectation": "json_object",
    },
    {
        "name": "t12_ai_runner_api",
        "url": "https://airivo.online/T12/api/stock-ai-runner",
        "expected_statuses": (404,),
        "content_expectation": "status_only",
    },
    {
        "name": "t12_ai_runner_ops",
        "url": "https://airivo.online/T12/ops/stock-ai-runner",
        "expected_statuses": (404,),
        "content_expectation": "status_only",
    },
)
DEFAULT_ERROR_PATTERNS = ("traceback", "fatal", "critical", "exception")
DEFAULT_LOG_PATHS = (
    "/var/log/stock-ultimate/dashboard.err.log",
    "/var/log/stock-ultimate/entry_guard.err.log",
)
DEFAULT_RELEASE_VERIFICATION_DIRNAME = "release_verification"
DEFAULT_HTTP_BODY_SAMPLE_LIMIT = 262144
DEFAULT_CANONICAL_ARTIFACTS_DIR = "/opt/stock-ultimate/app/artifacts"
DEFAULT_CANONICAL_EXPERIMENTS_DIR = "/opt/stock-ultimate/app/data/experiments"
DEFAULT_DECOMMISSIONED_APEX_STOCK_URLS = (
    ("apex_stock", "https://airivo.online/apex/stock/"),
    ("apex_stock_api", "https://airivo.online/apex/stock/api/primary-result"),
)
DEFAULT_ROLLOUT_SCOPE = "stock-scoped"
STOCK_SCOPED_ADVISORY_CHECKS = frozenset(
    {
        "apex_stock_decommissioned",
    }
)

CommandRunner = Callable[..., subprocess.CompletedProcess[str]]
UrlFetcher = Callable[[str, float], tuple[int, str]]


def _run_command(command: list[str], timeout: float = 10.0) -> subprocess.CompletedProcess[str]:
    return subprocess.run(command, capture_output=True, text=True, timeout=timeout)


def _fetch_url(url: str, timeout: float = 5.0) -> tuple[int, str]:
    request = Request(url, headers={"User-Agent": "stock-ultimate-post-deploy-verifier"})
    try:
        with urlopen(request, timeout=timeout) as response:
            body = response.read(DEFAULT_HTTP_BODY_SAMPLE_LIMIT).decode("utf-8", errors="replace")
            return int(response.status), body
    except HTTPError as exc:
        body = exc.read(DEFAULT_HTTP_BODY_SAMPLE_LIMIT).decode("utf-8", errors="replace")
        return int(exc.code), body
    except URLError as exc:
        return 0, str(exc.reason)


def _check_systemd_service(
    service_names: list[str],
    command_runner: CommandRunner,
) -> dict[str, object]:
    results = []
    for service_name in service_names:
        try:
            completed = command_runner(["systemctl", "is-active", "--quiet", service_name], timeout=10.0)
            results.append(
                {
                    "service_name": service_name,
                    "passed": completed.returncode == 0,
                    "exit_code": completed.returncode,
                    "stdout": completed.stdout,
                    "stderr": completed.stderr,
                }
            )
        except FileNotFoundError as exc:
            results.append(
                {
                    "service_name": service_name,
                    "passed": False,
                    "exit_code": None,
                    "stdout": "",
                    "stderr": str(exc),
                }
            )
        except subprocess.TimeoutExpired as exc:
            results.append(
                {
                    "service_name": service_name,
                    "passed": False,
                    "exit_code": None,
                    "stdout": exc.stdout or "",
                    "stderr": f"command timed out after {exc.timeout} seconds",
                }
            )
    return {
        "check": "systemd_services_active",
        "passed": all(result["passed"] for result in results),
        "details": {
            "results": results,
        },
    }


def _check_dashboard_service_environment(
    command_runner: CommandRunner,
    *,
    service_name: str = "stock-ultimate-dashboard.service",
    required_env: dict[str, str] | None = None,
) -> dict[str, object]:
    expected = required_env or {
        "STOCK_ULTIMATE_ARTIFACTS_DIR": DEFAULT_CANONICAL_ARTIFACTS_DIR,
        "STOCK_ULTIMATE_EXPERIMENTS_DIR": DEFAULT_CANONICAL_EXPERIMENTS_DIR,
    }
    try:
        completed = command_runner(
            ["systemctl", "show", service_name, "-p", "Environment", "--no-pager"],
            timeout=10.0,
        )
    except FileNotFoundError as exc:
        return {
            "check": "dashboard_service_canonical_environment",
            "passed": False,
            "details": {
                "service_name": service_name,
                "stderr": str(exc),
                "expected": expected,
            },
        }
    except subprocess.TimeoutExpired as exc:
        return {
            "check": "dashboard_service_canonical_environment",
            "passed": False,
            "details": {
                "service_name": service_name,
                "stderr": f"command timed out after {exc.timeout} seconds",
                "expected": expected,
            },
        }

    output = "\n".join(part for part in (completed.stdout, completed.stderr) if part)
    env_line = ""
    for line in output.splitlines():
        if line.startswith("Environment="):
            env_line = line[len("Environment="):]
            break
    resolved_env: dict[str, str] = {}
    for token in env_line.split():
        if "=" not in token:
            continue
        key, value = token.split("=", 1)
        resolved_env[key] = value
    missing_or_mismatched = {
        key: value
        for key, value in expected.items()
        if resolved_env.get(key) != value
    }
    return {
        "check": "dashboard_service_canonical_environment",
        "passed": completed.returncode == 0 and not missing_or_mismatched,
        "details": {
            "service_name": service_name,
            "exit_code": completed.returncode,
            "expected": expected,
            "resolved": resolved_env,
            "missing_or_mismatched": missing_or_mismatched,
        },
    }


def _check_nginx_config(command_runner: CommandRunner) -> dict[str, object]:
    try:
        completed = command_runner(["nginx", "-t"], timeout=10.0)
        return {
            "check": "nginx_config_valid",
            "passed": completed.returncode == 0,
            "details": {
                "exit_code": completed.returncode,
                "stdout": completed.stdout,
                "stderr": completed.stderr,
            },
        }
    except FileNotFoundError as exc:
        return {
            "check": "nginx_config_valid",
            "passed": False,
            "details": {
                "exit_code": None,
                "stdout": "",
                "stderr": str(exc),
            },
        }
    except subprocess.TimeoutExpired as exc:
        return {
            "check": "nginx_config_valid",
            "passed": False,
            "details": {
                "exit_code": None,
                "stdout": exc.stdout or "",
                "stderr": f"command timed out after {exc.timeout} seconds",
            },
        }


def _join_url(base_url: str, endpoint: str) -> str:
    return base_url.rstrip("/") + "/" + endpoint.lstrip("/")


def _check_apex_stock_decommissioned(
    url_fetcher: UrlFetcher,
    timeout: float,
    *,
    urls: tuple[tuple[str, str], ...] = DEFAULT_DECOMMISSIONED_APEX_STOCK_URLS,
) -> dict[str, object]:
    results = []
    for name, url in urls:
        status_code, body = url_fetcher(url, timeout)
        results.append(
            {
                "name": name,
                "url": url,
                "status_code": status_code,
                "expected_status": 410,
                "passed": status_code == 410,
                "body_sample": body[:240],
            }
        )
    return {
        "check": "apex_stock_decommissioned",
        "passed": all(item["passed"] for item in results),
        "details": {
            "results": results,
        },
    }


def _check_http_endpoints(
    base_url: str,
    endpoints: list[str],
    url_fetcher: UrlFetcher,
    timeout: float,
) -> dict[str, object]:
    results = []
    for endpoint in endpoints:
        status_code, body = url_fetcher(_join_url(base_url, endpoint), timeout)
        results.append(
            {
                "endpoint": endpoint,
                "status_code": status_code,
                "passed": 200 <= status_code < 400,
                "body_sample": body[:240],
            }
        )
    return {
        "check": "dashboard_http_endpoints",
        "passed": all(item["passed"] for item in results),
        "details": {
            "base_url": base_url,
            "results": results,
        },
    }


def _normalize_http_target(target: tuple[str, str] | dict[str, object]) -> dict[str, object]:
    if isinstance(target, dict):
        return {
            "name": str(target.get("name") or "").strip(),
            "url": str(target.get("url") or "").strip(),
            "expected_statuses": tuple(target.get("expected_statuses") or (200, 301, 302, 303, 304)),
            "content_expectation": str(target.get("content_expectation") or "status_only").strip(),
        }
    name, url = target
    return {
        "name": name,
        "url": url,
        "expected_statuses": (200, 301, 302, 303, 304),
        "content_expectation": "status_only",
    }


def _check_http_targets(
    target_urls: list[tuple[str, str] | dict[str, object]],
    url_fetcher: UrlFetcher,
    timeout: float,
) -> dict[str, object]:
    results = []
    for raw_target in target_urls:
        target = _normalize_http_target(raw_target)
        name = str(target["name"])
        url = str(target["url"])
        status_code, body = url_fetcher(url, timeout)
        content_passed = True
        content_expectation = str(target["content_expectation"])
        decoded_json = None
        if content_expectation == "json_object":
            try:
                decoded_body = json.loads(body)
                content_passed = isinstance(decoded_body, dict)
                decoded_json = decoded_body if isinstance(decoded_body, dict) else None
                if content_passed and decoded_body.get("disabled_reason") == "stock entry guard blocked primary result publication.":
                    content_passed = False
            except json.JSONDecodeError:
                content_passed = False
        expected_statuses = tuple(int(item) for item in target["expected_statuses"])
        json_fields = (
            {
                "result_id": decoded_json.get("result_id"),
                "run_id": decoded_json.get("run_id"),
                "lifecycle_id": decoded_json.get("lifecycle_id"),
                "disabled_reason": decoded_json.get("disabled_reason"),
            }
            if isinstance(decoded_json, dict)
            else None
        )
        results.append(
            {
                "name": name,
                "url": url,
                "status_code": status_code,
                "expected_statuses": list(expected_statuses),
                "passed": status_code in expected_statuses and content_passed,
                "content_expectation": content_expectation,
                "content_passed": content_passed,
                "json_fields": json_fields,
                "body_sample": body[:240],
            }
        )
    return {
        "check": "dashboard_http_targets",
        "passed": all(item["passed"] for item in results),
        "details": {
            "results": results,
        },
    }


def _check_primary_result_api_pointer_alignment(
    resolved_app_dir: Path,
    http_check: dict[str, object],
) -> dict[str, object]:
    pointer_path = resolved_app_dir / "artifacts" / "current_result_pointer" / "current.json"
    if not pointer_path.exists():
        return {
            "check": "primary_result_api_pointer_alignment",
            "passed": False,
            "details": {
                "pointer_path": str(pointer_path),
                "reason": "current_result_pointer/current.json missing",
            },
        }
    try:
        pointer_payload = json.loads(pointer_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        return {
            "check": "primary_result_api_pointer_alignment",
            "passed": False,
            "details": {
                "pointer_path": str(pointer_path),
                "reason": f"invalid pointer json: {exc}",
            },
        }
    http_results = list((http_check.get("details") or {}).get("results") or [])
    api_result = next(
        (item for item in http_results if isinstance(item, dict) and item.get("name") == "stock_primary_result_api"),
        None,
    )
    if not isinstance(api_result, dict):
        return {
            "check": "primary_result_api_pointer_alignment",
            "passed": False,
            "details": {
                "pointer_path": str(pointer_path),
                "reason": "stock_primary_result_api result missing from dashboard_http_targets",
            },
        }
    json_fields = api_result.get("json_fields")
    if not isinstance(json_fields, dict):
        return {
            "check": "primary_result_api_pointer_alignment",
            "passed": False,
            "details": {
                "pointer_path": str(pointer_path),
                "reason": "stock_primary_result_api did not return structured primary result identity",
                "api_result": api_result,
            },
        }
    mismatched_fields = [
        field_name
        for field_name in ("result_id", "run_id", "lifecycle_id")
        if str(pointer_payload.get(field_name) or "").strip() != str(json_fields.get(field_name) or "").strip()
    ]
    return {
        "check": "primary_result_api_pointer_alignment",
        "passed": not mismatched_fields,
        "details": {
            "pointer_path": str(pointer_path),
            "pointer": {
                "result_id": pointer_payload.get("result_id"),
                "run_id": pointer_payload.get("run_id"),
                "lifecycle_id": pointer_payload.get("lifecycle_id"),
            },
            "api": json_fields,
            "mismatched_fields": mismatched_fields,
        },
    }


def _check_stock_entry_guard(resolved_app_dir: Path) -> dict[str, object]:
    guard_path = resolved_app_dir / "artifacts" / "stock_entry_guard_latest.json"
    if not guard_path.exists():
        return {
            "check": "stock_entry_guard_ok",
            "passed": False,
            "details": {
                "guard_path": str(guard_path),
                "exists": False,
                "ok": False,
                "problems": ["stock_entry_guard_latest.json missing"],
            },
        }
    try:
        payload = json.loads(guard_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        return {
            "check": "stock_entry_guard_ok",
            "passed": False,
            "details": {
                "guard_path": str(guard_path),
                "exists": True,
                "ok": False,
                "problems": [f"invalid json: {exc}"],
            },
        }
    guard_ok = payload.get("ok") is True
    return {
        "check": "stock_entry_guard_ok",
        "passed": guard_ok,
        "details": {
            "guard_path": str(guard_path),
            "exists": True,
            "ok": guard_ok,
            "problems": list(payload.get("problems") or []),
            "run_id": payload.get("run_id"),
            "lifecycle_id": payload.get("lifecycle_id"),
        },
    }


def _check_pointer_integrity(resolved_app_dir: Path) -> dict[str, object]:
    project_root = Path(__file__).resolve().parents[1]
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))
    from scripts.check_current_result_pointer_integrity import check_current_result_pointer_integrity

    exit_code, payload = check_current_result_pointer_integrity(
        pointer_dir=resolved_app_dir / "artifacts" / "current_result_pointer",
        results_dir=resolved_app_dir / "artifacts" / "result_registry",
        runs_dir=resolved_app_dir / "artifacts" / "run_registry",
        artifact_registry_path=resolved_app_dir / "artifacts" / "artifact_registry.jsonl",
    )
    return {
        "check": "current_result_pointer_integrity",
        "passed": exit_code == 0 and payload.get("ok") is True,
        "details": payload,
    }


def _check_required_paths(app_dir: Path) -> dict[str, object]:
    required_paths = (
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
    )
    missing = [relative_path for relative_path in required_paths if not (app_dir / relative_path).exists()]
    return {
        "check": "required_app_paths",
        "passed": not missing,
        "details": {
            "app_dir": str(app_dir),
            "required_paths": list(required_paths),
            "missing_paths": missing,
        },
    }


def _check_protected_runtime_paths(app_dir: Path) -> dict[str, object]:
    protected_paths = {
        "data": app_dir / "data",
        "artifacts": app_dir / "artifacts",
    }
    return {
        "check": "protected_runtime_paths_observed",
        "passed": True,
        "details": {
            name: {
                "path": str(path),
                "exists": path.exists(),
                "is_dir": path.is_dir(),
            }
            for name, path in protected_paths.items()
        },
    }


def _check_recent_log(
    log_path: Path,
    *,
    error_patterns: tuple[str, ...] = DEFAULT_ERROR_PATTERNS,
) -> dict[str, object]:
    if not log_path.exists():
        return {
            "check": "dashboard_error_log_scan",
            "passed": True,
            "details": {
                "log_path": str(log_path),
                "exists": False,
                "skipped": True,
            },
        }
    text = log_path.read_text(encoding="utf-8", errors="replace")
    tail = text[-12000:]
    matched_patterns = [
        pattern
        for pattern in error_patterns
        if pattern in tail.lower()
    ]
    return {
        "check": "dashboard_error_log_scan",
        "passed": not matched_patterns,
        "details": {
            "log_path": str(log_path),
            "exists": True,
            "matched_patterns": matched_patterns,
        },
    }


def _check_recent_logs(
    log_paths: list[Path],
    *,
    error_patterns: tuple[str, ...] = DEFAULT_ERROR_PATTERNS,
) -> dict[str, object]:
    results = []
    for log_path in log_paths:
        check = _check_recent_log(log_path, error_patterns=error_patterns)
        results.append(
            {
                "log_path": check["details"]["log_path"],
                "passed": check["passed"],
                "exists": check["details"]["exists"],
                "matched_patterns": check["details"].get("matched_patterns", []),
                "skipped": check["details"].get("skipped", False),
            }
        )
    return {
        "check": "service_error_log_scan",
        "passed": all(item["passed"] for item in results),
        "details": {
            "results": results,
        },
    }


def _rollback_hint(activation_plan_path: Path | None) -> dict[str, object]:
    if not activation_plan_path:
        return {
            "available": False,
            "reason": "activation plan path not provided",
        }
    if not activation_plan_path.exists():
        return {
            "available": False,
            "activation_plan_path": str(activation_plan_path),
            "reason": "activation plan file missing",
        }
    try:
        payload = json.loads(activation_plan_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        return {
            "available": False,
            "activation_plan_path": str(activation_plan_path),
            "reason": f"activation plan is invalid JSON: {exc}",
        }
    rollback_commands = payload.get("rollback_commands")
    return {
        "available": isinstance(rollback_commands, list) and bool(rollback_commands),
        "activation_plan_path": str(activation_plan_path),
        "scope": payload.get("scope", DEFAULT_ROLLOUT_SCOPE),
        "rollback_commands": rollback_commands if isinstance(rollback_commands, list) else [],
    }


def _activation_plan_scope(activation_plan_path: Path | None) -> str:
    if not activation_plan_path or not activation_plan_path.exists():
        return DEFAULT_ROLLOUT_SCOPE
    try:
        payload = json.loads(activation_plan_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return DEFAULT_ROLLOUT_SCOPE
    scope = payload.get("scope", DEFAULT_ROLLOUT_SCOPE)
    return str(scope)


def _write_output(path: str | Path | None, payload: dict[str, object]) -> None:
    if not path:
        return
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _build_release_verification_artifact_paths(resolved_app_dir: Path, verified_at: str) -> tuple[Path, Path]:
    artifact_dir = resolved_app_dir / "artifacts" / DEFAULT_RELEASE_VERIFICATION_DIRNAME
    timestamp = verified_at.replace("-", "").replace(":", "").replace("+00:00", "Z")
    history_path = artifact_dir / "history" / f"{timestamp}.json"
    latest_path = artifact_dir / "latest.json"
    return latest_path, history_path


def _classify_checks_for_scope(checks: list[dict[str, object]], rollout_scope: str) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    advisory_names = STOCK_SCOPED_ADVISORY_CHECKS if rollout_scope == "stock-scoped" else frozenset()
    blocking_checks: list[dict[str, object]] = []
    advisory_checks: list[dict[str, object]] = []
    for check in checks:
        check_name = str(check.get("check") or "").strip()
        if check_name in advisory_names:
            advisory_checks.append(check)
        else:
            blocking_checks.append(check)
    return blocking_checks, advisory_checks


def run_server_post_deploy_verification(
    *,
    app_root: str | Path = DEFAULT_APP_ROOT,
    app_dir: str | Path | None = None,
    service_names: list[str] | None = None,
    dashboard_base_url: str | None = None,
    endpoints: list[str] | None = None,
    target_urls: list[tuple[str, str]] | None = None,
    log_path: str | Path | None = None,
    log_paths: list[str | Path] | None = None,
    activation_plan_path: str | Path | None = None,
    output_path: str | Path | None = None,
    command_runner: CommandRunner = _run_command,
    url_fetcher: UrlFetcher = _fetch_url,
) -> tuple[int, dict[str, object]]:
    root = Path(app_root)
    resolved_app_dir = Path(app_dir) if app_dir else root / "app"
    verified_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    rollout_scope = _activation_plan_scope(Path(activation_plan_path) if activation_plan_path else None)
    http_check = (
        _check_http_endpoints(dashboard_base_url, endpoints or ["/"], url_fetcher, timeout=5.0)
        if dashboard_base_url
        else _check_http_targets(target_urls or list(DEFAULT_HTTP_TARGETS), url_fetcher, timeout=5.0)
    )
    guard_check = _check_stock_entry_guard(resolved_app_dir)
    pointer_integrity_check = _check_pointer_integrity(resolved_app_dir)
    checks = [
        _check_systemd_service(service_names or list(DEFAULT_SERVICE_NAMES), command_runner),
        _check_dashboard_service_environment(command_runner),
        _check_nginx_config(command_runner),
        http_check,
        _check_primary_result_api_pointer_alignment(resolved_app_dir, http_check),
        _check_apex_stock_decommissioned(url_fetcher, timeout=5.0),
        guard_check,
        pointer_integrity_check,
        _check_required_paths(resolved_app_dir),
        _check_protected_runtime_paths(resolved_app_dir),
        _check_recent_logs(
            [Path(path) for path in (log_paths or ([log_path] if log_path else list(DEFAULT_LOG_PATHS)))]
        ),
    ]
    blocking_checks, advisory_checks = _classify_checks_for_scope(checks, rollout_scope)
    t12_blocked = {
        result["name"]: result["passed"]
        for result in http_check["details"]["results"]
        if result["name"] in {"t12_ai_runner_api", "t12_ai_runner_ops"}
    }
    latest_output_path, history_output_path = _build_release_verification_artifact_paths(resolved_app_dir, verified_at)
    blocking_passed = all(check["passed"] for check in blocking_checks)
    advisory_failed = [check["check"] for check in advisory_checks if not check["passed"]]
    payload = {
        "post_deploy_version": POST_DEPLOY_VERSION,
        "schema_version": POST_DEPLOY_VERSION,
        "verified_at": verified_at,
        "target": "formal",
        "rollout_scope": rollout_scope,
        "status": "passed" if blocking_passed else "failed",
        "passed": blocking_passed,
        "app_root": str(root),
        "app_dir": str(resolved_app_dir),
        "services": (checks[0]["details"]["results"] if checks else []),
        "http_checks": (http_check["details"]["results"] if http_check["passed"] or not http_check["passed"] else []),
        "guard_ok": guard_check["passed"],
        "pointer_integrity_ok": pointer_integrity_check["passed"],
        "t12_ai_runner_blocked": bool(t12_blocked) and all(t12_blocked.values()),
        "checks": checks,
        "blocking_checks": [check["check"] for check in blocking_checks],
        "advisory_checks": [check["check"] for check in advisory_checks],
        "advisory_failures": advisory_failed,
        "rollback_hint": _rollback_hint(Path(activation_plan_path) if activation_plan_path else None),
        "artifacts": {
            "latest_output_path": str(latest_output_path),
            "history_output_path": str(history_output_path),
        },
    }
    _write_output(latest_output_path, payload)
    _write_output(history_output_path, payload)
    _write_output(output_path, payload)
    return (0 if payload["status"] == "passed" else 1), payload


def main() -> int:
    parser = argparse.ArgumentParser(description="Verify server state after activating a stock_ultimate_system deployment.")
    parser.add_argument("--app-root", default=str(DEFAULT_APP_ROOT))
    parser.add_argument("--app-dir")
    parser.add_argument("--service-name", action="append", help="Systemd service to verify. Defaults to the three Airivo scope services.")
    parser.add_argument("--dashboard-base-url", help="Legacy single base URL mode for endpoint checks.")
    parser.add_argument("--endpoint", action="append", help="Endpoint for --dashboard-base-url mode.")
    parser.add_argument("--target-url", action="append", help="Full URL to verify. Can be NAME=URL or plain URL.")
    parser.add_argument("--log-path", action="append", help="Service error log to scan. Defaults to main_site, dashboard, and t12 error logs.")
    parser.add_argument("--activation-plan")
    parser.add_argument("--output")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    target_urls = None
    if args.target_url:
        target_urls = []
        for index, item in enumerate(args.target_url, start=1):
            if "=" in item:
                name, url = item.split("=", 1)
            else:
                name, url = f"target_{index}", item
            target_urls.append((name, url))
    exit_code, payload = run_server_post_deploy_verification(
        app_root=args.app_root,
        app_dir=args.app_dir,
        service_names=args.service_name,
        dashboard_base_url=args.dashboard_base_url,
        endpoints=args.endpoint,
        target_urls=target_urls,
        log_paths=args.log_path,
        activation_plan_path=args.activation_plan,
        output_path=args.output,
    )
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print(
            json.dumps(
                {
                    "status": payload["status"],
                    "post_deploy_version": payload["post_deploy_version"],
                    "checks": [
                        {
                            "check": check["check"],
                            "passed": check["passed"],
                        }
                        for check in payload["checks"]
                    ],
                    "rollback_available": payload["rollback_hint"]["available"],
                },
                ensure_ascii=False,
                indent=2,
            )
        )
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
