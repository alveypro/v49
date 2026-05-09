#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sqlite3
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from openclaw.paths import db_path as default_db_path  # noqa: E402
from openclaw.services.strategy_competition_audit_service import (  # noqa: E402
    build_strategy_competition_portfolio_audit,
)


def _load_json(path: str) -> dict:
    if not str(path or "").strip():
        return {}
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"json payload must be object: {path}")
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description="Build industry-benchmark strategy competition and Top5 portfolio audit.")
    parser.add_argument("--db-path", default="", help="SQLite fact DB path.")
    parser.add_argument("--trade-date", default="", help="Fixed competition trade date.")
    parser.add_argument("--fixed-candidate-pool", required=True, help="Comma-separated, predeclared candidate strategies.")
    parser.add_argument("--alpha-model-cards", required=True, help="JSON object keyed by strategy.")
    parser.add_argument("--portfolio-constraints", default="", help="Optional JSON constraints object.")
    parser.add_argument("--independent-validator", required=True, help="JSON independent validator decision.")
    parser.add_argument("--shadow-execution", required=True, help="JSON shadow execution evidence.")
    parser.add_argument("--pre-trade-risk-controls", required=True, help="JSON pre-trade risk control evidence.")
    parser.add_argument("--output-dir", default="logs/openclaw/strategy_competition_audit")
    parser.add_argument("--operator-name", default="strategy_competition_portfolio_audit")
    args = parser.parse_args()

    conn = sqlite3.connect(str(args.db_path or default_db_path()), timeout=30)
    try:
        payload = build_strategy_competition_portfolio_audit(
            conn,
            trade_date=args.trade_date,
            fixed_candidate_pool=[item.strip() for item in args.fixed_candidate_pool.split(",") if item.strip()],
            alpha_model_cards=_load_json(args.alpha_model_cards),
            portfolio_constraints=_load_json(args.portfolio_constraints),
            independent_validator=_load_json(args.independent_validator),
            shadow_execution=_load_json(args.shadow_execution),
            pre_trade_risk_controls=_load_json(args.pre_trade_risk_controls),
            output_dir=args.output_dir,
            operator_name=args.operator_name,
        )
    finally:
        conn.close()
    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if payload.get("passed") is True else 2


if __name__ == "__main__":
    raise SystemExit(main())
