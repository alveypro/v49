#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from openclaw.services.ensemble_risk_off_alpha_repair_review_service import (  # noqa: E402
    build_ensemble_risk_off_alpha_repair_review,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Review whether risk-off alpha repair can return to observation watch.")
    parser.add_argument("--monitor-artifact", required=True)
    parser.add_argument("--throttle-attribution-artifact", required=True)
    parser.add_argument("--failure-attribution-artifact", default="")
    parser.add_argument("--output-dir", default="logs/openclaw/ensemble_risk_off_alpha_repair_review")
    parser.add_argument("--operator-name", default="")
    args = parser.parse_args()
    payload = build_ensemble_risk_off_alpha_repair_review(
        monitor_artifact_path=args.monitor_artifact,
        throttle_attribution_artifact_path=args.throttle_attribution_artifact,
        failure_attribution_artifact_path=args.failure_attribution_artifact,
        output_dir=args.output_dir,
        operator_name=args.operator_name,
    )
    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if payload.get("risk_off_alpha_repair_passed") is True else 2


if __name__ == "__main__":
    raise SystemExit(main())
