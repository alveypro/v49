#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from openclaw.services.ensemble_allocator_throttle_attribution_service import (  # noqa: E402
    build_ensemble_allocator_throttle_attribution,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Separate alpha return from allocator throttle risk-control contribution.")
    parser.add_argument("--throttled-benchmark-artifact", required=True)
    parser.add_argument("--unthrottled-benchmark-artifact", required=True)
    parser.add_argument("--output-dir", default="logs/openclaw/ensemble_allocator_throttle_attribution")
    parser.add_argument("--operator-name", default="")
    args = parser.parse_args()

    payload = build_ensemble_allocator_throttle_attribution(
        throttled_benchmark_artifact_path=args.throttled_benchmark_artifact,
        unthrottled_benchmark_artifact_path=args.unthrottled_benchmark_artifact,
        output_dir=args.output_dir,
        operator_name=args.operator_name,
    )
    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if not payload.get("blocking_reasons") else 2


if __name__ == "__main__":
    raise SystemExit(main())
