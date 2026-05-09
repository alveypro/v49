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

from src.primary_result_observation import (
    PRIMARY_RESULT_OBSERVATION_ALLOWED_STATUSES,
    build_primary_result_observation,
)
from src.unified_result_builder import build_primary_result_api_payload
from src.utils.project_paths import resolve_project_path


def _write_output(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def run_primary_result_observation(
    *,
    exp_dir: str | Path = "data/experiments",
    candidate_index: int = 0,
    observation_status: str = "observing",
    reason: str = "local observation window opened",
    window_start: str | None = None,
    window_end: str | None = None,
    observed_return: float | None = None,
    benchmark_return: float | None = None,
    max_drawdown: float | None = None,
    min_success_return: float = 0.0,
    max_drawdown_floor: float = -0.08,
    output_path: str | Path | None = None,
    now: datetime | None = None,
    ignore_current_pointer: bool = False,
) -> tuple[int, dict[str, object]]:
    resolved_exp_dir = resolve_project_path(exp_dir)
    if not ignore_current_pointer:
        primary_result_payload = build_primary_result_api_payload(
            resolved_exp_dir,
            candidate_index=candidate_index,
            require_current_pointer=True,
        )
    else:
        primary_result_payload = build_primary_result_api_payload(
            resolved_exp_dir,
            candidate_index=candidate_index,
            require_current_pointer=False,
            ignore_current_pointer=True,
        )
    observation = build_primary_result_observation(
        primary_result_payload,
        observation_status=observation_status,
        reason=reason,
        window_start=window_start,
        window_end=window_end,
        observed_return=observed_return,
        benchmark_return=benchmark_return,
        max_drawdown=max_drawdown,
        min_success_return=min_success_return,
        max_drawdown_floor=max_drawdown_floor,
        now=now,
    )
    output = Path(output_path) if output_path is not None else resolved_exp_dir / "primary_result_observation_latest.json"
    _write_output(output, observation)
    return (0 if observation["observation_status"] in {"observing", "completed"} else 1), observation


def main() -> int:
    parser = argparse.ArgumentParser(description="Record a local observation status for the stock primary result.")
    parser.add_argument("--exp-dir", default="data/experiments")
    parser.add_argument("--candidate-index", type=int, default=0)
    parser.add_argument("--observation-status", choices=sorted(PRIMARY_RESULT_OBSERVATION_ALLOWED_STATUSES), default="observing")
    parser.add_argument("--reason", default="local observation window opened")
    parser.add_argument("--window-start")
    parser.add_argument("--window-end")
    parser.add_argument("--observed-return", type=float)
    parser.add_argument("--benchmark-return", type=float)
    parser.add_argument("--max-drawdown", type=float)
    parser.add_argument("--min-success-return", type=float, default=0.0)
    parser.add_argument("--max-drawdown-floor", type=float, default=-0.08)
    parser.add_argument("--output")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()
    exit_code, payload = run_primary_result_observation(
        exp_dir=args.exp_dir,
        candidate_index=args.candidate_index,
        observation_status=args.observation_status,
        reason=args.reason,
        window_start=args.window_start,
        window_end=args.window_end,
        observed_return=args.observed_return,
        benchmark_return=args.benchmark_return,
        max_drawdown=args.max_drawdown,
        min_success_return=args.min_success_return,
        max_drawdown_floor=args.max_drawdown_floor,
        output_path=args.output,
    )
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print(json.dumps({"observation_status": payload["observation_status"], "result_id": payload["result_id"]}, ensure_ascii=False, indent=2))
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
