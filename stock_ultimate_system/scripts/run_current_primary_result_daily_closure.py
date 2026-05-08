#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.primary_result_daily_closure_orchestrator import run_primary_result_daily_closure_orchestrator
from src.unified_result_builder import build_primary_result_api_payload
from src.utils.project_paths import resolve_project_path


MARKET_DATA_READINESS_VERSION = "primary_result_market_data_readiness.v1"


def _load_yaml(path: Path) -> dict[str, object]:
    if not path.exists():
        raise FileNotFoundError(f"config file missing: {path}")
    payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(payload, dict):
        raise ValueError(f"config file must contain an object: {path}")
    return payload


def _first_benchmark(settings: dict[str, object]) -> str:
    data = settings.get("data", {})
    if not isinstance(data, dict):
        return "000001.SH"
    raw = data.get("benchmark_indices", ["000001.SH"])
    if isinstance(raw, str):
        return raw.strip() or "000001.SH"
    if isinstance(raw, list):
        for item in raw:
            code = str(item or "").strip()
            if code:
                return code
    return "000001.SH"


def _data_value(settings: dict[str, object], key: str, default: str) -> str:
    data = settings.get("data", {})
    if not isinstance(data, dict):
        return default
    value = str(data.get(key, "") or "").strip()
    return value or default


def _observation_window_start(exp_dir: Path, *, expected_result_id: str, expected_ts_code: str) -> str:
    path = exp_dir / "primary_result_observation_latest.json"
    if not path.exists():
        raise FileNotFoundError(f"open observation artifact missing: {path}")
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"observation artifact must contain an object: {path}")
    artifact_result_id = str(payload.get("result_id") or "").strip()
    artifact_ts_code = str(payload.get("ts_code") or "").strip()
    if artifact_result_id != expected_result_id or artifact_ts_code != expected_ts_code:
        raise ValueError(
            "open observation artifact does not match current primary result; "
            f"expected {expected_result_id}/{expected_ts_code}, got {artifact_result_id or '-'} / {artifact_ts_code or '-'}"
        )
    window = payload.get("observation_window")
    if not isinstance(window, dict):
        raise ValueError("observation artifact missing observation_window")
    started_at = str(window.get("started_at") or "").strip()
    if not started_at:
        raise ValueError("observation window start is missing")
    return started_at


def _today_shanghai() -> str:
    return datetime.now(ZoneInfo("Asia/Shanghai")).strftime("%Y-%m-%d")


def _window_start_date(value: str) -> str:
    started_at = str(value or "").strip()
    if "T" in started_at:
        return started_at.split("T", 1)[0]
    return started_at[:10]


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _market_data_readiness_placeholder(
    *,
    status: str,
    ts_code: str,
    benchmark_ts_code: str,
    window_start: str | None,
    window_end: str | None,
    blocking_reasons: list[str],
    next_actions: list[str],
) -> dict[str, object]:
    return {
        "readiness_version": MARKET_DATA_READINESS_VERSION,
        "generated_at": datetime.now(ZoneInfo("Asia/Shanghai")).isoformat(),
        "status": status,
        "sqlite_db": None,
        "sqlite_table": None,
        "ts_code": ts_code,
        "benchmark_ts_code": benchmark_ts_code,
        "window_start": window_start,
        "window_end": window_end,
        "checks": [],
        "blocking_reasons": list(blocking_reasons),
        "next_actions": list(next_actions),
        "production_boundary": (
            "placeholder market data readiness is only written when current daily closure exits before the "
            "orchestrator can run; it prevents stale readiness evidence from a different candidate or test run "
            "from being treated as current"
        ),
    }


def run_current_primary_result_daily_closure(
    *,
    config_path: str | Path = "config/settings.yaml",
    exp_dir: str | Path = "data/experiments",
    candidate_index: int = 0,
    window_end: str | None = None,
    output_path: str | Path = "data/experiments/primary_result_daily_closure_latest.json",
) -> tuple[int, dict[str, object]]:
    resolved_config = resolve_project_path(config_path)
    resolved_exp_dir = resolve_project_path(exp_dir)
    readiness_output = resolved_exp_dir / "primary_result_market_data_readiness_latest.json"
    settings = _load_yaml(resolved_config)
    primary_result = build_primary_result_api_payload(
        resolved_exp_dir,
        candidate_index=candidate_index,
        require_current_pointer=True,
    )
    ts_code = str(primary_result.get("ts_code") or "").strip()
    result_id = str(primary_result.get("result_id") or "").strip()
    benchmark_ts_code = _first_benchmark(settings)
    if not ts_code or ts_code in {"-", "暂无"}:
        raise ValueError("current primary result ts_code is missing")

    try:
        resolved_window_start = _observation_window_start(
            resolved_exp_dir,
            expected_result_id=result_id,
            expected_ts_code=ts_code,
        )
    except ValueError as exc:
        payload = {
            "runner": "current_primary_result_daily_closure",
            "config_path": str(resolved_config),
            "exp_dir": str(resolved_exp_dir),
            "resolved_ts_code": ts_code,
            "orchestrator_version": "primary_result_daily_closure_orchestrator.v1",
            "generated_at": datetime.now(ZoneInfo("Asia/Shanghai")).isoformat(),
            "status": "blocked",
            "terminal_outcome": None,
            "stages": [],
            "blocking_reasons": [str(exc)],
            "next_actions": [
                "rebuild current primary result lifecycle before running daily closure",
                "do not reuse stale observation artifacts from a different candidate",
            ],
            "production_boundary": (
                "current daily closure runner only reads local lifecycle artifacts and local config; "
                "blocked reports do not import price history, close observations, or write performance ledger entries"
            ),
        }
        _write_json(
            readiness_output,
            _market_data_readiness_placeholder(
                status="blocked",
                ts_code=ts_code,
                benchmark_ts_code=benchmark_ts_code,
                window_start=None,
                window_end=window_end or _today_shanghai(),
                blocking_reasons=payload["blocking_reasons"],
                next_actions=payload["next_actions"],
            ),
        )
        if output_path:
            resolved_output = resolve_project_path(output_path)
            _write_json(resolved_output, payload)
        return 1, payload

    resolved_window_end = window_end or _today_shanghai()
    if _window_start_date(resolved_window_start) > resolved_window_end:
        payload = {
            "runner": "current_primary_result_daily_closure",
            "config_path": str(resolved_config),
            "exp_dir": str(resolved_exp_dir),
            "resolved_ts_code": ts_code,
            "orchestrator_version": "primary_result_daily_closure_orchestrator.v1",
            "generated_at": datetime.now(ZoneInfo("Asia/Shanghai")).isoformat(),
            "status": "pending_window",
            "terminal_outcome": None,
            "window_start": resolved_window_start,
            "window_end": resolved_window_end,
            "stages": [],
            "blocking_reasons": [],
            "next_actions": [
                "wait until the primary result observation window starts before running daily closure"
            ],
            "production_boundary": (
                "current daily closure runner only reads local lifecycle artifacts and local config; "
                "pending-window reports do not import price history, close observations, or write performance ledger entries"
            ),
        }
        _write_json(
            readiness_output,
            _market_data_readiness_placeholder(
                status="pending_window",
                ts_code=ts_code,
                benchmark_ts_code=benchmark_ts_code,
                window_start=resolved_window_start,
                window_end=resolved_window_end,
                blocking_reasons=[],
                next_actions=payload["next_actions"],
            ),
        )
        if output_path:
            resolved_output = resolve_project_path(output_path)
            _write_json(resolved_output, payload)
        return 0, payload

    exit_code, payload = run_primary_result_daily_closure_orchestrator(
        sqlite_db_path=_data_value(settings, "sqlite_db_path", "/opt/openclaw/permanent_stock_database.db"),
        sqlite_table=_data_value(settings, "sqlite_table", "daily_trading_data"),
        exp_dir=resolved_exp_dir,
        candidate_index=candidate_index,
        ts_code=ts_code,
        benchmark_ts_code=benchmark_ts_code,
        window_start=resolved_window_start,
        window_end=resolved_window_end,
        output_path=output_path,
    )
    payload = {
        "runner": "current_primary_result_daily_closure",
        "config_path": str(resolved_config),
        "exp_dir": str(resolved_exp_dir),
        "resolved_ts_code": ts_code,
        **payload,
    }
    if output_path:
        resolved_output = resolve_project_path(output_path)
        _write_json(resolved_output, payload)
    return exit_code, payload


def main() -> int:
    parser = argparse.ArgumentParser(description="Run daily closure for the current primary result from local config.")
    parser.add_argument("--config", default="config/settings.yaml")
    parser.add_argument("--exp-dir", default="data/experiments")
    parser.add_argument("--candidate-index", type=int, default=0)
    parser.add_argument("--window-end")
    parser.add_argument("--output", default="data/experiments/primary_result_daily_closure_latest.json")
    parser.add_argument(
        "--zero-on-blocked",
        action="store_true",
        help="Return 0 when a valid blocked report is produced; real script errors still return non-zero.",
    )
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()
    try:
        exit_code, payload = run_current_primary_result_daily_closure(
            config_path=args.config,
            exp_dir=args.exp_dir,
            candidate_index=args.candidate_index,
            window_end=args.window_end,
            output_path=args.output,
        )
    except Exception as exc:
        print(json.dumps({"status": "error", "error": str(exc)}, ensure_ascii=False, indent=2))
        return 1
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print(
            json.dumps(
                {
                    "status": payload["status"],
                    "resolved_ts_code": payload["resolved_ts_code"],
                    "terminal_outcome": payload["terminal_outcome"],
                    "blocking_reason_total": len(payload["blocking_reasons"]),
                },
                ensure_ascii=False,
                indent=2,
            )
        )
    if args.zero_on_blocked and payload.get("status") == "blocked":
        return 0
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
