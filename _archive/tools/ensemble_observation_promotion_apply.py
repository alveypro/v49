#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from openclaw.services.ensemble_observation_promotion_apply_service import (  # noqa: E402
    build_ensemble_observation_promotion_apply,
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Apply an approved ensemble_core research-to-observation decision into the observation ledger."
    )
    parser.add_argument("--promotion-decision-artifact", required=True)
    parser.add_argument("--output-dir", default="logs/openclaw/ensemble_observation_promotion_apply")
    parser.add_argument("--observation-ledger", default="logs/openclaw/observation_promotion_records.jsonl")
    parser.add_argument("--operator-name", default="")
    parser.add_argument("--decision-id", default="")
    args = parser.parse_args()

    payload = build_ensemble_observation_promotion_apply(
        promotion_decision_artifact_path=args.promotion_decision_artifact,
        output_dir=args.output_dir,
        observation_ledger_path=args.observation_ledger,
        operator_name=args.operator_name,
        decision_id=args.decision_id,
    )
    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if payload.get("observation_pool_record_applied") is True else 2


if __name__ == "__main__":
    raise SystemExit(main())
