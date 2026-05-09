#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.server_deploy_registry import ServerDeployRegistry


def main() -> int:
    parser = argparse.ArgumentParser(description="Register or rollback a server deployment evidence snapshot.")
    parser.add_argument("--deployments-dir", default="/opt/stock-ultimate/deployments")
    parser.add_argument("--evidence-bundle-json")
    parser.add_argument("--deployment-id")
    parser.add_argument("--rollback-deployment-id")
    args = parser.parse_args()

    registry = ServerDeployRegistry(deployments_dir=args.deployments_dir)
    try:
        if args.rollback_deployment_id:
            snapshot = registry.rollback(args.rollback_deployment_id)
            print(
                json.dumps(
                    {
                        "status": "rolled_back",
                        "deployment_id": snapshot["deployment_id"],
                        "scope": snapshot.get("scope"),
                        "snapshot_path": str(registry.history_dir / f"{snapshot['deployment_id']}.json"),
                        "current_pointer_path": str(registry.current_path),
                    },
                    ensure_ascii=False,
                    indent=2,
                )
            )
            return 0
        if not args.evidence_bundle_json:
            raise ValueError("--evidence-bundle-json is required unless --rollback-deployment-id is provided")
        snapshot = registry.register(
            evidence_bundle_path=args.evidence_bundle_json,
            deployment_id=args.deployment_id,
        )
        print(
            json.dumps(
                {
                    "status": "registered",
                    "deployment_id": snapshot["deployment_id"],
                    "scope": snapshot.get("scope"),
                    "snapshot_path": str(registry.history_dir / f"{snapshot['deployment_id']}.json"),
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
