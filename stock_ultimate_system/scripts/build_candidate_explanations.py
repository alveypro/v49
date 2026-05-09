#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.candidate_quality.explanation import build_candidate_explanations, write_candidate_explanation_payload
from src.utils.project_paths import resolve_experiments_path, resolve_project_path


def main() -> int:
    parser = argparse.ArgumentParser(description="Build public, internal, and rejection candidate explanations.")
    parser.add_argument("--exp-dir", default=None)
    parser.add_argument("--snapshot", default=None)
    parser.add_argument("--risk-state", default=None)
    parser.add_argument("--observation-result", default=None)
    parser.add_argument("--failure-attribution", default=None)
    parser.add_argument("--portfolio", default=None)
    parser.add_argument("--portfolio-quality", default=None)
    parser.add_argument("--portfolio-capacity", default=None)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    exp_dir = resolve_project_path(args.exp_dir) if args.exp_dir else resolve_experiments_path()
    public_payload, internal_payload, rejection_payload = build_candidate_explanations(
        snapshot_path=resolve_project_path(args.snapshot) if args.snapshot else exp_dir / "candidate_observation_snapshot_latest.json",
        risk_state_path=resolve_project_path(args.risk_state) if args.risk_state else exp_dir / "candidate_risk_state_latest.json",
        observation_result_path=resolve_project_path(args.observation_result)
        if args.observation_result
        else exp_dir / "candidate_observation_result_latest.json",
        failure_attribution_path=resolve_project_path(args.failure_attribution)
        if args.failure_attribution
        else exp_dir / "candidate_failure_attribution_latest.json",
        portfolio_path=resolve_project_path(args.portfolio) if args.portfolio else exp_dir / "candidate_portfolio_latest.json",
        portfolio_quality_path=resolve_project_path(args.portfolio_quality)
        if args.portfolio_quality
        else exp_dir / "candidate_portfolio_quality_latest.json",
        portfolio_capacity_path=resolve_project_path(args.portfolio_capacity)
        if args.portfolio_capacity
        else exp_dir / "portfolio_capacity_report_latest.json",
    )
    public_path = write_candidate_explanation_payload(
        public_payload,
        output_path=exp_dir / "candidate_public_explanation_latest.json",
    )
    internal_path = write_candidate_explanation_payload(
        internal_payload,
        output_path=exp_dir / "candidate_internal_explanation_latest.json",
    )
    rejection_path = write_candidate_explanation_payload(
        rejection_payload,
        output_path=exp_dir / "candidate_rejection_explanation_latest.json",
    )
    payload = {
        "public_status": public_payload.get("status"),
        "internal_status": internal_payload.get("status"),
        "rejection_status": rejection_payload.get("status"),
        "candidate_count": public_payload.get("candidate_count", 0),
        "rejection_count": rejection_payload.get("candidate_count", 0),
        "public_path": public_path,
        "internal_path": internal_path,
        "rejection_path": rejection_path,
    }
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print(
            f"public={payload['public_status']} internal={payload['internal_status']} "
            f"rejection={payload['rejection_status']}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
