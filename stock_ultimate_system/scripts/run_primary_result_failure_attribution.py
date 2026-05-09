#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.primary_result_failure_attribution import build_primary_result_failure_attribution_from_paths


def _write_output(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description="Attribute failure or weak success for a closed primary result observation.")
    parser.add_argument("--observation-json", default="data/experiments/primary_result_observation_latest.json")
    parser.add_argument("--ledger-jsonl")
    parser.add_argument("--min-success-return", type=float, default=0.0)
    parser.add_argument("--min-excess-return", type=float, default=0.0)
    parser.add_argument("--max-drawdown-floor", type=float, default=-0.08)
    parser.add_argument("--output", default="data/experiments/primary_result_failure_attribution_latest.json")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()
    try:
        payload = build_primary_result_failure_attribution_from_paths(
            observation_path=args.observation_json,
            ledger_jsonl_path=args.ledger_jsonl,
            min_success_return=args.min_success_return,
            min_excess_return=args.min_excess_return,
            max_drawdown_floor=args.max_drawdown_floor,
        )
        _write_output(Path(args.output), payload)
        if args.json:
            print(json.dumps(payload, ensure_ascii=False, indent=2))
        else:
            print(
                json.dumps(
                    {
                        "status": payload["status"],
                        "result_id": payload["result_id"],
                        "outcome": payload["outcome"],
                        "attribution_required": payload["attribution_required"],
                        "primary_failure_category": payload["primary_failure_category"],
                    },
                    ensure_ascii=False,
                    indent=2,
                )
            )
        return 0
    except Exception as exc:
        print(json.dumps({"status": "failed", "error": str(exc)}, ensure_ascii=False, indent=2))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
