#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.primary_result_performance_evidence import build_primary_result_performance_evidence


def _parse_floors(value: str) -> tuple[int, ...]:
    return tuple(int(item.strip()) for item in value.split(",") if item.strip())


def main() -> int:
    parser = argparse.ArgumentParser(description="Build /stock primary and basket performance evidence report.")
    parser.add_argument("--primary-ledger-jsonl", default="artifacts/primary_result_performance/ledger.jsonl")
    parser.add_argument("--basket-ledger-jsonl", default="artifacts/primary_result_candidate_baskets/performance_ledger.jsonl")
    parser.add_argument("--output", default="artifacts/primary_result_performance_evidence_latest.json")
    parser.add_argument("--evidence-floors", default="20,60,120")
    parser.add_argument("--min-success-rate", type=float, default=0.5)
    parser.add_argument("--min-average-excess-return", type=float, default=0.0)
    parser.add_argument("--max-drawdown-floor", type=float, default=-0.08)
    parser.add_argument("--zero-on-failed", action="store_true")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()
    exit_code, payload = build_primary_result_performance_evidence(
        primary_ledger_jsonl=args.primary_ledger_jsonl,
        basket_ledger_jsonl=args.basket_ledger_jsonl,
        output_path=args.output,
        evidence_floors=_parse_floors(args.evidence_floors),
        min_success_rate=args.min_success_rate,
        min_average_excess_return=args.min_average_excess_return,
        max_drawdown_floor=args.max_drawdown_floor,
    )
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print(
            json.dumps(
                {
                    "status": payload["status"],
                    "evidence_floors": payload["evidence_floors"],
                    "next_actions": payload["next_actions"],
                },
                ensure_ascii=False,
                indent=2,
            )
        )
    if args.zero_on_failed and payload.get("status") == "failed":
        return 0
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
