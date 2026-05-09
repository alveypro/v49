#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from openclaw.paths import db_path as default_db_path  # noqa: E402
from openclaw.services.ensemble_observation_promotion_apply_service import load_observation_promotion_records  # noqa: E402
from openclaw.services.rejected_backtest_artifact_ledger_service import load_rejected_backtest_artifacts  # noqa: E402
from openclaw.services.strategy_optimization_stage_service import build_strategy_optimization_stage_audit  # noqa: E402


JsonDict = Dict[str, Any]
DEFAULT_REJECTED_ARTIFACTS_PATH = "logs/openclaw/rejected_backtest_artifacts.jsonl"


def _load_rejected_artifacts(path: str) -> List[JsonDict]:
    return load_rejected_backtest_artifacts(path)


def _load_observation_promotions(path: str) -> List[JsonDict]:
    return load_observation_promotion_records(path)


def _write_json(path: Path, payload: JsonDict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")


def _write_markdown(path: Path, payload: JsonDict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "# Strategy Optimization Stage Audit",
        "",
        f"- audit_version: `{payload.get('audit_version')}`",
        f"- passed: `{payload.get('passed')}`",
        f"- trade_date: `{payload.get('trade_date')}`",
        f"- source_document: `{((payload.get('policy') or {}).get('source_document'))}`",
        "",
        "## Blocking Reasons",
        "",
    ]
    blocking = payload.get("blocking_reasons") or []
    lines.extend([f"- `{item}`" for item in blocking] if blocking else ["- none"])
    lines.extend(["", "## Eligible Strategies", ""])
    eligible = payload.get("eligible_strategies") or []
    lines.extend([f"- `{item}`" for item in eligible] if eligible else ["- none"])
    lines.extend(["", "## Observation Pool", ""])
    observation = payload.get("observation_pool") or []
    if observation:
        for item in observation:
            lines.append(f"- `{item.get('strategy')}` run_id=`{item.get('run_id')}` reason=`{item.get('reason')}`")
    else:
        lines.append("- none")
    lines.extend(["", "## Rejected Artifacts", ""])
    rejected = payload.get("rejected_artifacts") or []
    if rejected:
        for item in rejected:
            lines.append(f"- `{item.get('strategy', '')}` path=`{item.get('artifact_path') or item.get('path')}` reason=`{item.get('reason')}`")
    else:
        lines.append("- none")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def run_stage_audit(
    *,
    db_path: str,
    trade_date: str,
    rejected_artifacts_path: str = DEFAULT_REJECTED_ARTIFACTS_PATH,
    observation_promotions_path: str = "logs/openclaw/observation_promotion_records.jsonl",
    output_dir: str = "logs/openclaw",
) -> JsonDict:
    rejected_artifacts = _load_rejected_artifacts(rejected_artifacts_path)
    observation_promotions = _load_observation_promotions(observation_promotions_path)
    conn = sqlite3.connect(str(db_path), timeout=30)
    try:
        payload = build_strategy_optimization_stage_audit(
            conn,
            trade_date=trade_date,
            rejected_artifacts=rejected_artifacts,
            observation_promotion_records=observation_promotions,
        )
    finally:
        conn.close()

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir = Path(output_dir)
    json_path = out_dir / f"strategy_optimization_stage_audit_{ts}.json"
    md_path = out_dir / f"strategy_optimization_stage_audit_{ts}.md"
    _write_json(json_path, payload)
    _write_markdown(md_path, payload)
    payload["artifacts"] = {"json": str(json_path), "markdown": str(md_path)}
    _write_json(json_path, payload)
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description="Run read-only strategy optimization stage audit.")
    parser.add_argument("--db-path", default=str(default_db_path()), help="SQLite DB path.")
    parser.add_argument("--trade-date", default="", help="Optional trade date filter.")
    parser.add_argument(
        "--rejected-artifacts",
        default=DEFAULT_REJECTED_ARTIFACTS_PATH,
        help="JSON/JSONL file with rejected_artifacts ledger entries.",
    )
    parser.add_argument(
        "--observation-promotions",
        default="logs/openclaw/observation_promotion_records.jsonl",
        help="JSONL ledger with manually applied observation promotion records.",
    )
    parser.add_argument("--output-dir", default="logs/openclaw", help="Directory for JSON/Markdown audit artifacts.")
    parser.add_argument("--json", action="store_true", help="Print full JSON payload.")
    args = parser.parse_args()

    payload = run_stage_audit(
        db_path=args.db_path,
        trade_date=args.trade_date,
        rejected_artifacts_path=args.rejected_artifacts,
        observation_promotions_path=args.observation_promotions,
        output_dir=args.output_dir,
    )
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
    else:
        print(
            "strategy_optimization_stage_audit passed={passed} json={json_path} markdown={md_path}".format(
                passed=payload.get("passed"),
                json_path=(payload.get("artifacts") or {}).get("json"),
                md_path=(payload.get("artifacts") or {}).get("markdown"),
            )
        )
    return 0 if payload.get("passed") is True else 2


if __name__ == "__main__":
    raise SystemExit(main())
