#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.dashboard_support import read_json
from src.primary_result_observation import build_primary_result_observation
from src.primary_result_observation_metrics import calculate_primary_result_observation_metrics
from src.unified_result_builder import build_primary_result_api_payload
from src.utils.project_paths import resolve_project_path


def _write_output(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _default_window_start(exp_dir: Path) -> str | None:
    observation_path = exp_dir / "primary_result_observation_latest.json"
    if not observation_path.exists():
        return None
    observation = read_json(observation_path)
    window = observation.get("observation_window")
    if not isinstance(window, dict):
        return None
    started_at = window.get("started_at")
    return str(started_at) if started_at else None


def _missing_primary_value(value: object) -> bool:
    text = str(value or "").strip()
    return not text or text in {"-", "primary:unavailable", "对象信息暂缺", "对象暂缺", "名称暂缺"}


def run_primary_result_observation_metrics(
    *,
    exp_dir: str | Path = "data/experiments",
    candidate_index: int = 0,
    price_history_path: str | Path = "data/experiments/primary_result_price_history_latest.csv",
    benchmark_ts_code: str = "BENCHMARK",
    window_start: str | None = None,
    window_end: str,
    reason: str = "local price history observation window completed",
    min_success_return: float = 0.0,
    max_drawdown_floor: float = -0.08,
    metrics_output_path: str | Path | None = None,
    observation_output_path: str | Path | None = None,
    now: datetime | None = None,
) -> tuple[int, dict[str, object]]:
    resolved_exp_dir = resolve_project_path(exp_dir)
    resolved_window_start = window_start or _default_window_start(resolved_exp_dir)
    if not resolved_window_start:
        raise ValueError("window_start is required when no open observation artifact exists")

    primary_result_payload = build_primary_result_api_payload(
        resolved_exp_dir,
        candidate_index=candidate_index,
        require_current_pointer=True,
    )
    if (
        primary_result_payload.get("disabled") is True
        or _missing_primary_value(primary_result_payload.get("result_id"))
        or _missing_primary_value(primary_result_payload.get("ts_code"))
        or _missing_primary_value(primary_result_payload.get("execution_status"))
        or _missing_primary_value(primary_result_payload.get("rollback_status"))
    ):
        observation_payload = read_json(resolved_exp_dir / "primary_result_observation_latest.json")
        execution_payload = read_json(resolved_exp_dir / "primary_result_execution_latest.json")
        rollback_payload = read_json(resolved_exp_dir / "primary_result_rollback_latest.json")
        primary_result_payload = build_primary_result_api_payload(
            resolved_exp_dir,
            candidate_index=candidate_index,
            require_current_pointer=False,
        )
        for key in ("result_id", "ts_code", "stock_name"):
            if _missing_primary_value(primary_result_payload.get(key)):
                primary_result_payload[key] = (
                    observation_payload.get(key)
                    or execution_payload.get(key)
                    or rollback_payload.get(key)
                )
        if _missing_primary_value(primary_result_payload.get("execution_status")):
            primary_result_payload["execution_status"] = (
                observation_payload.get("source_execution_status")
                or execution_payload.get("execution_status")
            )
        if _missing_primary_value(primary_result_payload.get("rollback_status")):
            primary_result_payload["rollback_status"] = (
                observation_payload.get("source_rollback_status")
                or rollback_payload.get("rollback_status")
            )
    ts_code = str(primary_result_payload.get("ts_code", "") or "").strip()
    metrics = calculate_primary_result_observation_metrics(
        price_history_path=price_history_path,
        ts_code=ts_code,
        benchmark_ts_code=benchmark_ts_code,
        window_start=resolved_window_start,
        window_end=window_end,
    )
    observation = build_primary_result_observation(
        primary_result_payload,
        observation_status="completed",
        reason=reason,
        window_start=resolved_window_start,
        window_end=window_end,
        observed_return=float(metrics["observed_return"]),
        benchmark_return=float(metrics["benchmark_return"]),
        max_drawdown=float(metrics["max_drawdown"]),
        min_success_return=min_success_return,
        max_drawdown_floor=max_drawdown_floor,
        now=now,
    )
    payload = {
        "status": "passed" if observation["observation_status"] == "completed" else "failed",
        "result_id": observation["result_id"],
        "ts_code": observation["ts_code"],
        "benchmark_ts_code": benchmark_ts_code,
        "metrics": metrics,
        "observation": observation,
    }
    metrics_output = (
        Path(metrics_output_path)
        if metrics_output_path is not None
        else resolved_exp_dir / "primary_result_observation_metrics_latest.json"
    )
    observation_output = (
        Path(observation_output_path)
        if observation_output_path is not None
        else resolved_exp_dir / "primary_result_observation_latest.json"
    )
    _write_output(metrics_output, payload)
    _write_output(observation_output, observation)
    return (0 if payload["status"] == "passed" else 1), payload


def main() -> int:
    parser = argparse.ArgumentParser(description="Complete primary result observation from local price history CSV.")
    parser.add_argument("--exp-dir", default="data/experiments")
    parser.add_argument("--candidate-index", type=int, default=0)
    parser.add_argument("--price-history-csv", default="data/experiments/primary_result_price_history_latest.csv")
    parser.add_argument("--benchmark-ts-code", default="BENCHMARK")
    parser.add_argument("--window-start")
    parser.add_argument("--window-end", required=True)
    parser.add_argument("--reason", default="local price history observation window completed")
    parser.add_argument("--min-success-return", type=float, default=0.0)
    parser.add_argument("--max-drawdown-floor", type=float, default=-0.08)
    parser.add_argument("--metrics-output")
    parser.add_argument("--observation-output")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()
    try:
        exit_code, payload = run_primary_result_observation_metrics(
            exp_dir=args.exp_dir,
            candidate_index=args.candidate_index,
            price_history_path=args.price_history_csv,
            benchmark_ts_code=args.benchmark_ts_code,
            window_start=args.window_start,
            window_end=args.window_end,
            reason=args.reason,
            min_success_return=args.min_success_return,
            max_drawdown_floor=args.max_drawdown_floor,
            metrics_output_path=args.metrics_output,
            observation_output_path=args.observation_output,
        )
        if args.json:
            print(json.dumps(payload, ensure_ascii=False, indent=2))
        else:
            print(
                json.dumps(
                    {
                        "status": payload["status"],
                        "result_id": payload["result_id"],
                        "ts_code": payload["ts_code"],
                        "observed_return": payload["metrics"]["observed_return"],
                        "benchmark_return": payload["metrics"]["benchmark_return"],
                        "max_drawdown": payload["metrics"]["max_drawdown"],
                    },
                    ensure_ascii=False,
                    indent=2,
                )
            )
        return exit_code
    except Exception as exc:
        print(json.dumps({"status": "failed", "error": str(exc)}, ensure_ascii=False, indent=2))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
