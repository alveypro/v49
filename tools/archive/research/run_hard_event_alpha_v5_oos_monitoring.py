#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sqlite3
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from openclaw.services.research_repair_iteration_flow_service import (  # noqa: E402
    create_oos_monitoring_result_artifact,
    export_repair_flow_evidence_manifest,
)


DEFAULT_POLICY_AUDIT = (
    "logs/openclaw/repair_20260508_hard_event_alpha_neutral_consensus_turnover_guard_v5_unthrottled_benchmark/"
    "ensemble_rebuilt_candidate_shadow_benchmark_20260508_113410.json"
)
DEFAULT_OOS_PLAN = (
    "logs/openclaw/repair_20260508_hard_event_alpha_neutral_consensus_turnover_guard_v5_watch_oos_plan/"
    "oos_monitoring_plan_hard_event_alpha_candidate_v5_20260508_20260508_122046.json"
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Run predeclared v5 OOS monitoring without changing v5 rules.")
    parser.add_argument("--db-path", default="permanent_stock_database.db")
    parser.add_argument("--flow-id", default="hard_event_alpha_candidate_v5_20260508")
    parser.add_argument("--oos-plan", default=DEFAULT_OOS_PLAN)
    parser.add_argument("--source-benchmark-json", default=DEFAULT_POLICY_AUDIT)
    parser.add_argument("--output-dir", default="logs/openclaw/repair_20260508_hard_event_alpha_neutral_consensus_turnover_guard_v5_oos_monitor")
    parser.add_argument("--operator-name", default="hard_event_alpha_v5_oos_monitoring")
    args = parser.parse_args()

    plan = _load(args.oos_plan)
    source = _load(args.source_benchmark_json)
    policy_audit = str(source.get("source_candidate_policy_audit_json") or "")
    if not policy_audit:
        raise SystemExit("source benchmark missing source_candidate_policy_audit_json")
    windows = ",".join(str(item) for item in plan.get("oos_windows") or [])
    if not windows:
        raise SystemExit("OOS plan missing oos_windows")

    output_root = Path(args.output_dir)
    unthrottled_dir = output_root / "unthrottled"
    throttled_dir = output_root / "throttled"
    cmd = [
        sys.executable,
        "tools/ensemble_rebuilt_candidate_shadow_benchmark.py",
        "--candidate-policy-audit-json",
        policy_audit,
        "--as-of-dates",
        windows,
        "--candidate",
        "hard_event_alpha_candidate",
        "--db-path",
        str(args.db_path),
        "--target-gross-exposure",
        "1.0",
        "--neutral-gross-exposure",
        "1.0",
        "--paired-output-dir",
        str(throttled_dir),
        "--paired-target-gross-exposure",
        "0.75",
        "--paired-neutral-gross-exposure",
        "0.45",
        "--operator-name",
        args.operator_name + "_unthrottled",
        "--paired-operator-name",
        args.operator_name + "_throttled",
        "--output-dir",
        str(unthrottled_dir),
    ]
    result = subprocess.run(cmd, cwd=str(ROOT), capture_output=True, text=True, check=True)
    paths = json.loads(result.stdout)
    unthrottled_json = paths["primary"]["json"]
    throttled_json = paths["paired"]["json"]

    conn = sqlite3.connect(str(args.db_path))
    try:
        oos_result = create_oos_monitoring_result_artifact(
            conn,
            flow_id=args.flow_id,
            oos_plan_path=args.oos_plan,
            unthrottled_artifact_path=unthrottled_json,
            throttled_artifact_path=throttled_json,
            output_dir=str(output_root),
            operator_name=args.operator_name,
        )
        manifest = export_repair_flow_evidence_manifest(conn, flow_id=args.flow_id, output_dir=str(output_root))
    finally:
        conn.close()

    print(
        json.dumps(
            {
                "flow_id": args.flow_id,
                "unthrottled_benchmark_artifact": unthrottled_json,
                "throttled_benchmark_artifact": throttled_json,
                "oos_monitoring_result_artifact": oos_result["oos_monitoring_result_artifact"],
                "repair_flow_evidence_manifest": manifest["repair_flow_evidence_manifest"],
                "result_status": oos_result["result_status"],
                "blocking_reasons": oos_result["blocking_reasons"],
                "formal_candidate_allowed": False,
                "production_candidate_allowed": False,
            },
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
    )
    return 0


def _load(path: str) -> dict:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    return payload if isinstance(payload, dict) else {}


if __name__ == "__main__":
    raise SystemExit(main())
