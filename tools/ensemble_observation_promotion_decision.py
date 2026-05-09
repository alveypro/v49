#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from openclaw.services.ensemble_observation_promotion_decision_service import (  # noqa: E402
    build_ensemble_observation_promotion_decision,
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Build a read-only ensemble_core research-to-observation promotion decision artifact."
    )
    parser.add_argument("--observation-gate-artifact", required=True)
    parser.add_argument("--stage-audit-artifact", required=True)
    parser.add_argument("--output-dir", default="logs/openclaw/ensemble_observation_promotion_decisions")
    parser.add_argument("--operator-name", default="")
    parser.add_argument("--decision-id", default="")
    args = parser.parse_args()

    payload = build_ensemble_observation_promotion_decision(
        observation_gate_artifact_path=args.observation_gate_artifact,
        stage_audit_artifact_path=args.stage_audit_artifact,
        output_dir=args.output_dir,
        operator_name=args.operator_name,
        decision_id=args.decision_id,
    )
    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
