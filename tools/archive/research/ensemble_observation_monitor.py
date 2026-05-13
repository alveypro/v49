#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from openclaw.services.ensemble_observation_monitor_service import build_ensemble_observation_monitor  # noqa: E402
from openclaw.services.ensemble_observation_promotion_apply_service import load_observation_promotion_records  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="Run read-only ensemble_core observation monitoring.")
    parser.add_argument("--observation-ledger", default="logs/openclaw/observation_promotion_records.jsonl")
    parser.add_argument("--shadow-benchmark-artifact", required=True)
    parser.add_argument("--stage-audit-artifact", default="")
    parser.add_argument("--output-dir", default="logs/openclaw/ensemble_observation_monitor")
    parser.add_argument("--operator-name", default="")
    args = parser.parse_args()

    payload = build_ensemble_observation_monitor(
        observation_records=load_observation_promotion_records(args.observation_ledger),
        shadow_benchmark_artifact_path=args.shadow_benchmark_artifact,
        stage_audit_artifact_path=args.stage_audit_artifact,
        output_dir=args.output_dir,
        operator_name=args.operator_name,
    )
    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if payload.get("observation_monitor_passed") is True else 2


if __name__ == "__main__":
    raise SystemExit(main())
