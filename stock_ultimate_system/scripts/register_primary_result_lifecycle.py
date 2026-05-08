#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.primary_result_lifecycle_registry import PrimaryResultLifecycleRegistry


def main() -> int:
    parser = argparse.ArgumentParser(description="Register or rollback an immutable primary result lifecycle snapshot.")
    parser.add_argument("--lifecycles-dir", default="artifacts/primary_result_lifecycle")
    parser.add_argument("--artifact-registry-path")
    parser.add_argument("--evidence-json")
    parser.add_argument("--lifecycle-id")
    parser.add_argument("--rollback-lifecycle-id")
    args = parser.parse_args()

    registry = PrimaryResultLifecycleRegistry(
        lifecycles_dir=args.lifecycles_dir,
        artifact_registry_path=args.artifact_registry_path,
    )
    try:
        if args.rollback_lifecycle_id:
            snapshot = registry.rollback(args.rollback_lifecycle_id)
            print(
                json.dumps(
                    {
                        "status": "rolled_back",
                        "lifecycle_id": snapshot["lifecycle_id"],
                        "result_id": snapshot["result_id"],
                        "ts_code": snapshot["ts_code"],
                        "snapshot_path": str(registry.history_dir / f"{snapshot['lifecycle_id']}.json"),
                        "current_pointer_path": str(registry.current_path),
                    },
                    ensure_ascii=False,
                    indent=2,
                )
            )
            return 0
        if not args.evidence_json:
            raise ValueError("--evidence-json is required unless --rollback-lifecycle-id is provided")
        snapshot = registry.register(evidence_path=args.evidence_json, lifecycle_id=args.lifecycle_id)
        print(
            json.dumps(
                {
                    "status": "registered",
                    "lifecycle_id": snapshot["lifecycle_id"],
                    "result_id": snapshot["result_id"],
                    "ts_code": snapshot["ts_code"],
                    "snapshot_path": str(registry.history_dir / f"{snapshot['lifecycle_id']}.json"),
                    "current_pointer_path": str(registry.current_path),
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
