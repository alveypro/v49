#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from openclaw.services.promotion_decision_artifact_service import build_promotion_decision_artifact  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="Build a read-only promotion decision artifact.")
    parser.add_argument("--strategy", required=True)
    parser.add_argument("--sweep-artifact", required=True)
    parser.add_argument("--stage-audit-artifact", required=True)
    parser.add_argument("--rejected-ledger", default="logs/openclaw/rejected_backtest_artifacts.jsonl")
    parser.add_argument("--output-dir", default="logs/openclaw/promotion_decisions")
    parser.add_argument("--operator-name", default="")
    parser.add_argument("--decision-id", default="")
    parser.add_argument("--execution-evidence-artifact", default="")
    args = parser.parse_args()
    execution_evidence = {}
    if args.execution_evidence_artifact:
        execution_evidence = json.loads(Path(args.execution_evidence_artifact).read_text(encoding="utf-8"))

    payload = build_promotion_decision_artifact(
        strategy=args.strategy,
        sweep_artifact_path=args.sweep_artifact,
        stage_audit_artifact_path=args.stage_audit_artifact,
        rejected_ledger_path=args.rejected_ledger,
        output_dir=args.output_dir,
        operator_name=args.operator_name,
        decision_id=args.decision_id,
        execution_evidence=execution_evidence,
    )
    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
