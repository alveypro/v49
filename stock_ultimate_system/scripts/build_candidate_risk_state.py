#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.candidate_quality.risk_state import (
    CandidateRiskStateConfig,
    build_candidate_risk_state,
    write_risk_state_payload,
    write_risk_state_transitions,
)
from src.utils.project_paths import resolve_experiments_path, resolve_project_path


def main() -> int:
    parser = argparse.ArgumentParser(description="Build candidate risk state machine outputs.")
    parser.add_argument("--exp-dir", default=None)
    parser.add_argument("--snapshot", default=None)
    parser.add_argument("--observation-result", default=None)
    parser.add_argument("--failure-attribution", default=None)
    parser.add_argument("--portfolio-quality", default=None)
    parser.add_argument("--portfolio-capacity", default=None)
    parser.add_argument("--previous-transitions", default=None)
    parser.add_argument("--invalid-drawdown-threshold", type=float, default=-0.10)
    parser.add_argument("--invalid-return-threshold", type=float, default=-0.08)
    parser.add_argument("--degrade-drawdown-threshold", type=float, default=-0.06)
    parser.add_argument("--degrade-return-threshold", type=float, default=-0.04)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    exp_dir = resolve_project_path(args.exp_dir) if args.exp_dir else resolve_experiments_path()
    snapshot = resolve_project_path(args.snapshot) if args.snapshot else exp_dir / "candidate_observation_snapshot_latest.json"
    observation_result = (
        resolve_project_path(args.observation_result)
        if args.observation_result
        else exp_dir / "candidate_observation_result_latest.json"
    )
    failure_attribution = (
        resolve_project_path(args.failure_attribution)
        if args.failure_attribution
        else exp_dir / "candidate_failure_attribution_latest.json"
    )
    portfolio_quality = (
        resolve_project_path(args.portfolio_quality)
        if args.portfolio_quality
        else exp_dir / "candidate_portfolio_quality_latest.json"
    )
    portfolio_capacity = (
        resolve_project_path(args.portfolio_capacity)
        if args.portfolio_capacity
        else exp_dir / "portfolio_capacity_report_latest.json"
    )
    previous_transitions = (
        resolve_project_path(args.previous_transitions)
        if args.previous_transitions
        else exp_dir / "candidate_risk_state_transition.jsonl"
    )
    config = CandidateRiskStateConfig(
        invalid_drawdown_threshold=args.invalid_drawdown_threshold,
        invalid_return_threshold=args.invalid_return_threshold,
        degrade_drawdown_threshold=args.degrade_drawdown_threshold,
        degrade_return_threshold=args.degrade_return_threshold,
    )
    state, transitions, audit = build_candidate_risk_state(
        snapshot_path=snapshot,
        observation_result_path=observation_result,
        failure_attribution_path=failure_attribution,
        portfolio_quality_path=portfolio_quality,
        portfolio_capacity_path=portfolio_capacity,
        previous_transition_path=previous_transitions,
        config=config,
    )
    state_path = write_risk_state_payload(state, output_path=exp_dir / "candidate_risk_state_latest.json")
    transition_path = write_risk_state_transitions(transitions, output_path=exp_dir / "candidate_risk_state_transition.jsonl")
    audit_path = write_risk_state_payload(audit, output_path=exp_dir / "candidate_state_audit_latest.json")
    payload = {
        "status": state.get("status"),
        "state_path": state_path,
        "transition_path": transition_path,
        "audit_path": audit_path,
        "candidate_count": state.get("candidate_count", 0),
        "state_counts": state.get("state_counts", {}),
        "blocking_reasons": state.get("blocking_reasons", []),
    }
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print(f"{payload['status']}: {payload['state_counts']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
