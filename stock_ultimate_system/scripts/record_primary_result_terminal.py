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

from src.primary_result_terminal import PRIMARY_RESULT_TERMINAL_ALLOWED_OUTCOMES, build_primary_result_terminal
from src.unified_result_builder import build_primary_result_api_payload
from src.utils.project_paths import resolve_project_path


def _write_output(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _read_json(path: Path) -> dict[str, object]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _missing_primary_value(value: object) -> bool:
    text = str(value or "").strip()
    return not text or text in {"-", "primary:unavailable", "对象信息暂缺", "对象暂缺", "名称暂缺"}


def record_primary_result_terminal(
    *,
    terminal_outcome: str,
    reason: str,
    exp_dir: str | Path = "data/experiments",
    candidate_index: int = 0,
    output_path: str | Path | None = None,
    now: datetime | None = None,
) -> tuple[int, dict[str, object]]:
    resolved_exp_dir = resolve_project_path(exp_dir)
    primary_result_payload = build_primary_result_api_payload(
        resolved_exp_dir,
        candidate_index=candidate_index,
        require_current_pointer=True,
    )
    if (
        primary_result_payload.get("disabled") is True
        or _missing_primary_value(primary_result_payload.get("result_id"))
        or _missing_primary_value(primary_result_payload.get("ts_code"))
        or _missing_primary_value(primary_result_payload.get("observation_status"))
    ):
        observation_payload = _read_json(resolved_exp_dir / "primary_result_observation_latest.json")
        primary_result_payload = build_primary_result_api_payload(
            resolved_exp_dir,
            candidate_index=candidate_index,
            require_current_pointer=False,
        )
        for key in (
            "result_id",
            "ts_code",
            "stock_name",
            "observation_status",
            "observed_return",
            "benchmark_return",
            "max_drawdown",
        ):
            if _missing_primary_value(primary_result_payload.get(key)):
                primary_result_payload[key] = observation_payload.get(key)
    terminal = build_primary_result_terminal(
        primary_result_payload,
        terminal_outcome=terminal_outcome,
        reason=reason,
        now=now,
    )
    output = Path(output_path) if output_path is not None else resolved_exp_dir / "primary_result_terminal_latest.json"
    _write_output(output, terminal)
    return 0, terminal


def main() -> int:
    parser = argparse.ArgumentParser(description="Record an explicit terminal outcome for the stock primary result.")
    parser.add_argument("--exp-dir", default="data/experiments")
    parser.add_argument("--candidate-index", type=int, default=0)
    parser.add_argument("--terminal-outcome", required=True, choices=sorted(PRIMARY_RESULT_TERMINAL_ALLOWED_OUTCOMES))
    parser.add_argument("--reason", required=True)
    parser.add_argument("--output")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()
    exit_code, payload = record_primary_result_terminal(
        exp_dir=args.exp_dir,
        candidate_index=args.candidate_index,
        terminal_outcome=args.terminal_outcome,
        reason=args.reason,
        output_path=args.output,
    )
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print(json.dumps({"terminal_outcome": payload["terminal_outcome"], "result_id": payload["result_id"]}, ensure_ascii=False, indent=2))
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
