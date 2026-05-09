#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sqlite3
import sys
from datetime import datetime

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from openclaw.services.research_repair_iteration_flow_service import (  # noqa: E402
    attach_repair_artifact,
    export_repair_flow_evidence_manifest,
)


DEFAULT_V5_OOS = (
    "logs/openclaw/repair_20260508_hard_event_alpha_neutral_consensus_turnover_guard_v5_oos_monitor/"
    "oos_monitoring_result_hard_event_alpha_candidate_v5_20260508_20260508_124358.json"
)
DEFAULT_V6_FLOW = (
    "logs/openclaw/repair_20260508_hard_event_alpha_v6_final_flow/"
    "research_repair_flow_snapshot_hard_event_alpha_candidate_v6_20260508_20260508_145751.json"
)
DEFAULT_V6_OOS = (
    "logs/openclaw/repair_20260508_hard_event_alpha_v6_oos_result/"
    "oos_monitoring_result_hard_event_alpha_candidate_v6_20260508_20260508_145719.json"
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Freeze v6 failed repair and compare v5/v6 hard-event alpha failures.")
    parser.add_argument("--db-path", default="permanent_stock_database.db")
    parser.add_argument("--flow-id", default="hard_event_alpha_candidate_v6_20260508")
    parser.add_argument("--v5-oos-result", default=DEFAULT_V5_OOS)
    parser.add_argument("--v6-flow-snapshot", default=DEFAULT_V6_FLOW)
    parser.add_argument("--v6-oos-result", default=DEFAULT_V6_OOS)
    parser.add_argument("--output-dir", default="logs/openclaw/repair_20260508_hard_event_alpha_v6_failure_comparison")
    parser.add_argument("--operator-name", default="hard_event_alpha_v6_failure_comparison")
    args = parser.parse_args()

    v5 = _load(args.v5_oos_result)
    v6_flow = _load(args.v6_flow_snapshot)
    v6_oos = _load(args.v6_oos_result)
    comparison = _build_comparison(
        v5=v5,
        v6_flow=v6_flow,
        v6_oos=v6_oos,
        operator_name=args.operator_name,
        source_artifacts={
            "v5_oos_result": args.v5_oos_result,
            "v6_flow_snapshot": args.v6_flow_snapshot,
            "v6_oos_result": args.v6_oos_result,
        },
    )
    output = Path(args.output_dir)
    output.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    comparison_path = output / f"v5_v6_failure_attribution_comparison_{args.flow_id}_{stamp}.json"
    review_path = output / f"v7_go_no_go_review_{args.flow_id}_{stamp}.json"
    comparison_path.write_text(json.dumps(comparison, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    review = comparison["v7_go_no_go_review"]
    review_path.write_text(json.dumps(review, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")

    conn = sqlite3.connect(str(args.db_path))
    try:
        attach_repair_artifact(
            conn,
            flow_id=args.flow_id,
            artifact_type="failure_attribution_comparison",
            artifact_path=str(comparison_path),
            summary={
                "conclusion": comparison["conclusion"],
                "v7_allowed": review["v7_predeclared_repair_allowed"],
            },
        )
        attach_repair_artifact(
            conn,
            flow_id=args.flow_id,
            artifact_type="v7_go_no_go_review",
            artifact_path=str(review_path),
            summary={
                "decision": review["decision"],
                "v7_allowed": review["v7_predeclared_repair_allowed"],
            },
        )
        manifest = export_repair_flow_evidence_manifest(conn, flow_id=args.flow_id, output_dir=args.output_dir)
    finally:
        conn.close()

    print(
        json.dumps(
            {
                "failure_attribution_comparison_artifact": str(comparison_path),
                "v7_go_no_go_review_artifact": str(review_path),
                "repair_flow_evidence_manifest": manifest["repair_flow_evidence_manifest"],
                "conclusion": comparison["conclusion"],
                "v7_predeclared_repair_allowed": review["v7_predeclared_repair_allowed"],
                "decision": review["decision"],
            },
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
    )
    return 0


def _build_comparison(*, v5: dict, v6_flow: dict, v6_oos: dict, operator_name: str, source_artifacts: dict) -> dict:
    v5_stats = v5.get("neutral_trade_stats") if isinstance(v5.get("neutral_trade_stats"), dict) else {}
    v6_risks = {item.get("risk_code"): item for item in v6_flow.get("watch_risks") or [] if isinstance(item, dict)}
    v6_oos_stats = v6_oos.get("neutral_trade_stats") if isinstance(v6_oos.get("neutral_trade_stats"), dict) else {}
    v5_over_veto = _metric_from_watch(v5, "over_veto_risk", fallback=v5_stats.get("avg_veto_rate"))
    v6_repair_over_veto = (v6_risks.get("over_veto_risk") or {}).get("metric_value")
    v6_oos_sparsity = _metric_from_watch(v6_oos, "neutral_signal_sparsity", fallback=v6_oos_stats.get("avg_position_count"))
    v6_oos_blocking = [str(item) for item in v6_oos.get("blocking_reasons") or []]
    v6_repair_blocking = [str(item) for item in v6_flow.get("blocking_reasons_json") or []]
    mechanism_gap = [
        "v6_reduced_repair_window_over_veto_but_did_not_restore_benchmark_validity",
        "v6_oos_lost_neutral_coverage",
        "v6_oos_missing_required_regime_evidence",
        "v6_oos_unthrottled_alpha_still_not_positive",
        "v6_risk_off_repair_regressed_oos",
    ]
    v7_conditions = [
        "define_one_new_alpha_body_mechanism_not_parameter_tweak",
        "explain_how_it_restores_oos_neutral_coverage_without_allocator_or_cash_fallback",
        "explain_how_it_preserves_risk_off_exhaustion_repair",
        "predeclare_rule_hash_before_any_replay",
        "reuse_or_predeclare_fixed_repair_and_oos_windows_before_replay",
        "declare_abandon_condition_if_oos_unthrottled_fails_again",
    ]
    v7_allowed = False
    return {
        "artifact_version": "hard_event_alpha_v5_v6_failure_attribution_comparison.v1",
        "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "operator_name": operator_name,
        "candidate": "hard_event_alpha_candidate",
        "research_only": True,
        "formal_candidate_allowed": False,
        "production_candidate_allowed": False,
        "source_artifacts": source_artifacts,
        "v5_failure_summary": {
            "result_status": v5.get("result_status"),
            "rule_hash": v5.get("rule_hash"),
            "blocking_reasons": v5.get("blocking_reasons") or [],
            "neutral_avg_position_count": v5_stats.get("avg_position_count"),
            "neutral_avg_veto_rate": v5_over_veto,
        },
        "v6_failure_summary": {
            "current_status": v6_flow.get("current_status"),
            "rule_hash": v6_flow.get("rule_hash"),
            "repair_blocking_reasons": v6_repair_blocking,
            "repair_over_veto_metric": v6_repair_over_veto,
            "oos_result_status": v6_oos.get("result_status"),
            "oos_blocking_reasons": v6_oos_blocking,
            "oos_neutral_avg_position_count": v6_oos_stats.get("avg_position_count"),
            "oos_neutral_signal_sparsity_metric": v6_oos_sparsity,
        },
        "observed_changes": {
            "over_veto_improved_in_repair_window": _safe_float(v6_repair_over_veto) < _safe_float(v5_over_veto),
            "neutral_coverage_worsened_in_oos": _safe_float(v6_oos_stats.get("avg_position_count")) < _safe_float(v5_stats.get("avg_position_count")),
            "oos_regime_evidence_missing": any("oos_missing_required_regime_split" in item for item in v6_oos_blocking),
            "risk_off_oos_regressed": "oos_risk_off_repair_regressed" in v6_oos_blocking,
        },
        "mechanism_diagnosis": "v6_direction_reduced_over_veto_but_failed_alpha_body_validation",
        "mechanism_gaps": mechanism_gap,
        "conclusion": "do_not_start_v7_until_new_alpha_body_mechanism_is_predeclared",
        "v7_go_no_go_review": {
            "artifact_version": "hard_event_alpha_v7_go_no_go_review.v1",
            "decision": "no_go_until_mechanism_predeclared",
            "v7_predeclared_repair_allowed": v7_allowed,
            "required_preconditions": v7_conditions,
            "abandon_condition": "if_v7_oos_unthrottled_fails_neutral_or_risk_off_again_archive_failed_research_candidate",
            "prohibited_actions": [
                "formal",
                "top",
                "production",
                "allocator_throttle_as_alpha",
                "cash_fallback_as_alpha",
                "observation_packaging",
                "posthoc_parameter_search",
            ],
            "formal_candidate_allowed": False,
            "production_candidate_allowed": False,
        },
    }


def _metric_from_watch(payload: dict, code: str, fallback=None):
    for item in payload.get("watch_risk_summary") or []:
        if isinstance(item, dict) and item.get("risk_code") == code:
            return item.get("metric_value")
    return fallback


def _safe_float(value) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 999999.0


def _load(path: str) -> dict:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    return payload if isinstance(payload, dict) else {}


if __name__ == "__main__":
    raise SystemExit(main())
