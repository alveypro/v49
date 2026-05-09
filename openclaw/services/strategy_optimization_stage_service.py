from __future__ import annotations

import json
import sqlite3
from typing import Any, Dict, Iterable, List

from openclaw.services.lineage_service import apply_professional_migrations
from openclaw.services.ensemble_observation_promotion_apply_service import valid_observation_promotion_records
from openclaw.services.strategy_maturity_service import build_strategy_maturity_plan
from openclaw.services.unified_strategy_recommendation_service import build_unified_system_recommendation
from strategies.registry import get_profile


JsonDict = Dict[str, Any]
TERMINAL_MISS_STATUSES = {"cancelled", "rejected", "expired", "manual_override"}


def _safe_json_loads(value: Any, fallback: Any) -> Any:
    try:
        parsed = json.loads(str(value or ""))
    except Exception:
        return fallback
    return parsed


def build_strategy_optimization_stage_audit(
    conn: sqlite3.Connection,
    *,
    trade_date: str = "",
    rejected_artifacts: Iterable[JsonDict] | None = None,
    observation_promotion_records: Iterable[JsonDict] | None = None,
    unified_recommendation: JsonDict | None = None,
) -> JsonDict:
    """Evaluate the optimization-upgrade stage against the execution plan.

    This service is intentionally read-only. It answers whether the current
    facts satisfy the stage checklist; it does not promote strategies, adjust
    parameters, or manufacture missing evidence.
    """

    apply_professional_migrations(conn)
    recommendation = unified_recommendation or build_unified_system_recommendation(conn, trade_date=trade_date)
    reviews = [dict(item) for item in recommendation.get("all_strategy_reviews") or [] if isinstance(item, dict)]
    eligible_pool = [dict(item) for item in recommendation.get("eligible_pool") or [] if isinstance(item, dict)]
    top_strategies = [dict(item) for item in recommendation.get("top_strategies") or [] if isinstance(item, dict)]
    top_stocks = [dict(item) for item in recommendation.get("top_stocks") or [] if isinstance(item, dict)]
    rejected = [dict(item) for item in rejected_artifacts or [] if isinstance(item, dict)]
    manual_observation_records = valid_observation_promotion_records(
        [dict(item) for item in observation_promotion_records or [] if isinstance(item, dict)]
    )

    review_by_strategy = {str(item.get("strategy") or ""): item for item in reviews}
    observation_pool = _merge_observation_pool(_build_observation_pool(reviews), manual_observation_records)
    research_only_pool = _build_research_only_pool(recommendation=recommendation, reviews=reviews, observation_pool=observation_pool)
    diagnostic_pool = _build_diagnostic_pool(reviews, observation_pool=observation_pool, research_only_pool=research_only_pool)
    competition_pool = _build_competition_pool(
        recommendation=recommendation,
        reviews=reviews,
        observation_pool=observation_pool,
        research_only_pool=research_only_pool,
    )
    formal_top_violations = _find_formal_top_violations(top_strategies=top_strategies, top_stocks=top_stocks, observation_pool=observation_pool)
    promotion_violations = _find_promotion_decision_violations(conn)
    rejected_artifact_violations = _find_rejected_artifact_violations(rejected)
    competition_strategies = {str(item.get("strategy") or "") for item in competition_pool}
    maturity_plan = build_strategy_maturity_plan(recommendation)

    checklist = {
        "strategies_entered_competition_answered": bool(reviews) and set(review_by_strategy.keys()).issubset(competition_strategies),
        "rejected_strategies_have_reasons": all(bool(item.get("blocking_reasons")) for item in diagnostic_pool),
        "parameter_sources_from_credible_artifacts_answered": True,
        "rejected_artifacts_recorded": bool(rejected),
        "experimental_uses_same_backtest_gate": _experimental_uses_same_backtest_gate(reviews),
        "no_formal_top_from_observation_pool": not formal_top_violations,
        "no_false_maturity_paths_detected": not (formal_top_violations or rejected_artifact_violations),
        "execution_evidence_required_for_promotion": not promotion_violations,
    }
    blocking = []
    for key, passed in checklist.items():
        if passed is not True:
            blocking.append(f"checklist_failed:{key}")
    if formal_top_violations:
        blocking.append("formal_top_contains_observation_or_blocked_strategy")
    if promotion_violations:
        blocking.append("promotion_decision_missing_execution_evidence")
    if rejected_artifact_violations:
        blocking.append("rejected_artifact_reused_or_missing_reason")

    return {
        "audit_version": "strategy_optimization_stage_audit.v1",
        "passed": not blocking,
        "blocking_reasons": blocking,
        "trade_date": str(trade_date or ""),
        "strategies_entered_competition": sorted(competition_strategies),
        "competition_pool": competition_pool,
        "eligible_strategies": [str(item.get("strategy") or "") for item in eligible_pool or top_strategies],
        "top_strategies": [str(item.get("strategy") or "") for item in top_strategies],
        "rejected_strategies": diagnostic_pool,
        "observation_pool": observation_pool,
        "observation_promotion_records": manual_observation_records,
        "research_only_pool": research_only_pool,
        "maturity_plan": maturity_plan,
        "rejected_artifacts": rejected,
        "formal_top_violations": formal_top_violations,
        "promotion_decision_violations": promotion_violations,
        "rejected_artifact_violations": rejected_artifact_violations,
        "checklist": checklist,
        "policy": {
            "source_document": "docs/AIRIVO_STRATEGY_OPTIMIZATION_UPGRADE_EXECUTION_PLAN.md",
            "single_fact_source": True,
            "read_only_audit": True,
            "forbidden_paths": [
                "lower_threshold_for_signal_count",
                "patch_labels_without_evidence",
                "publish_observation_pool_as_formal_top",
                "page_maturity_claim",
            ],
        },
    }


def _build_observation_pool(reviews: List[JsonDict]) -> List[JsonDict]:
    out = []
    for item in reviews:
        backtest = item.get("backtest_component") if isinstance(item.get("backtest_component"), dict) else {}
        if backtest.get("passed") is True and backtest.get("eligible_for_formal_ranking") is not True:
            out.append(
                {
                    "strategy": str(item.get("strategy") or ""),
                    "run_id": str(item.get("run_id") or ""),
                    "strategy_tier": str(item.get("strategy_tier") or ""),
                    "reason": "credible_backtest_but_quality_floor_failed",
                    "blocking_reasons": list(item.get("blocking_reasons") or []),
                    "backtest_component": backtest,
                }
            )
    return out


def _merge_observation_pool(observation_pool: List[JsonDict], manual_records: List[JsonDict]) -> List[JsonDict]:
    out = [dict(item) for item in observation_pool]
    existing = {str(item.get("strategy") or "") for item in out}
    for record in manual_records:
        strategy = str(record.get("strategy") or "")
        if not strategy or strategy in existing:
            continue
        out.append(
            {
                "strategy": strategy,
                "run_id": str(record.get("record_id") or ""),
                "strategy_tier": "experimental",
                "reason": str(record.get("reason") or "manual_research_only_to_observation_transition"),
                "blocking_reasons": list(record.get("blocking_reasons") or []),
                "backtest_component": {
                    "passed": False,
                    "eligible_for_formal_ranking": False,
                    "quality_floor_passed": False,
                    "source": "manual_observation_promotion_record",
                    "source_promotion_decision_id": str(record.get("source_promotion_decision_id") or ""),
                    "source_artifact_path": str(record.get("source_promotion_decision_artifact") or ""),
                },
                "promotion_record": {
                    "record_id": str(record.get("record_id") or ""),
                    "candidate": str(record.get("candidate") or ""),
                    "from_pool": str(record.get("from_pool") or ""),
                    "to_pool": str(record.get("to_pool") or ""),
                    "formal_pool_eligible": bool(record.get("formal_pool_eligible") is True),
                    "formal_ranking_allowed": bool(record.get("formal_ranking_allowed") is True),
                },
            }
        )
        existing.add(strategy)
    return out


def _build_research_only_pool(*, recommendation: JsonDict, reviews: List[JsonDict], observation_pool: List[JsonDict]) -> List[JsonDict]:
    observation = {str(item.get("strategy") or "") for item in observation_pool}
    explicit = [dict(item) for item in recommendation.get("research_only_pool") or [] if isinstance(item, dict)]
    if explicit:
        return [
            {
                "strategy": str(item.get("strategy") or ""),
                "run_id": str(item.get("run_id") or ""),
                "strategy_tier": str(item.get("strategy_tier") or ""),
                "reason": str(item.get("research_only_reason") or "research_only"),
                "required_to_compete": list(item.get("required_to_compete") or []),
                "blocking_reasons": list(item.get("blocking_reasons") or []),
            }
            for item in explicit
            if str(item.get("strategy") or "") not in observation
        ]
    out = []
    for item in reviews:
        if str(item.get("strategy") or "") in observation:
            continue
        if item.get("strategy") != "ai" or item.get("run_id"):
            continue
        out.append(
            {
                "strategy": "ai",
                "run_id": "",
                "strategy_tier": str(item.get("strategy_tier") or ""),
                "reason": "no_real_runtime_backtest_handler_or_explainable_fact_chain",
                "required_to_compete": [
                    "real_runtime_backtest_handler",
                    "explainable_signal_fact_chain",
                    "point_in_time_inputs",
                    "cost_slippage_fill_constraints",
                ],
                "blocking_reasons": list(item.get("blocking_reasons") or []),
            }
        )
    return out


def _build_diagnostic_pool(
    reviews: List[JsonDict],
    *,
    observation_pool: List[JsonDict],
    research_only_pool: List[JsonDict],
) -> List[JsonDict]:
    out = []
    observation = {str(item.get("strategy") or "") for item in observation_pool}
    research_only = {str(item.get("strategy") or "") for item in research_only_pool}
    for item in reviews:
        if item.get("eligible_for_daily_top3") is True:
            continue
        strategy = str(item.get("strategy") or "")
        if strategy in observation or strategy in research_only:
            continue
        backtest = item.get("backtest_component") if isinstance(item.get("backtest_component"), dict) else {}
        out.append(
            {
                "strategy": strategy,
                "run_id": str(item.get("run_id") or ""),
                "strategy_tier": str(item.get("strategy_tier") or ""),
                "blocking_reasons": list(item.get("blocking_reasons") or []),
                "backtest_component": backtest,
            }
        )
    return out


def _build_competition_pool(
    *,
    recommendation: JsonDict,
    reviews: List[JsonDict],
    observation_pool: List[JsonDict],
    research_only_pool: List[JsonDict],
) -> List[JsonDict]:
    explicit = [dict(item) for item in recommendation.get("competition_pool") or [] if isinstance(item, dict)]
    observation = {str(item.get("strategy") or "") for item in observation_pool}
    observation_by_strategy = {str(item.get("strategy") or ""): item for item in observation_pool}
    research_only = {str(item.get("strategy") or "") for item in research_only_pool}
    if explicit:
        out = []
        for item in explicit:
            strategy = str(item.get("strategy") or "")
            if strategy in observation:
                cleaned = {key: value for key, value in item.items() if key not in {"research_only_reason", "research_contract"}}
                observation_item = observation_by_strategy.get(strategy) or {}
                out.append(
                    {
                        **cleaned,
                        "competition_status": "observation",
                        "competes_for_formal_top": False,
                        "observation_reason": str(observation_item.get("reason") or "observation"),
                        "observation_promotion_record": observation_item.get("promotion_record") or {},
                    }
                )
            elif strategy in research_only:
                out.append({**item, "competition_status": "research_only", "competes_for_formal_top": False})
            else:
                out.append(item)
        return out
    out = []
    for item in reviews:
        strategy = str(item.get("strategy") or "")
        if item.get("eligible_for_daily_top3") is True:
            status = "formal_eligible"
        elif strategy in observation:
            status = "observation"
        elif strategy in research_only:
            status = "research_only"
        else:
            status = "diagnostic"
        out.append(
            {
                "strategy": strategy,
                "run_id": str(item.get("run_id") or ""),
                "strategy_tier": str(item.get("strategy_tier") or ""),
                "competition_status": status,
                "competes_for_formal_top": status == "formal_eligible",
                "blocking_reasons": list(item.get("blocking_reasons") or []),
            }
        )
    return out


def _find_formal_top_violations(*, top_strategies: List[JsonDict], top_stocks: List[JsonDict], observation_pool: List[JsonDict]) -> List[JsonDict]:
    observation = {str(item.get("strategy") or "") for item in observation_pool}
    eligible = {str(item.get("strategy") or "") for item in top_strategies if item.get("eligible_for_daily_top3") is True}
    violations = []
    for item in top_strategies:
        strategy = str(item.get("strategy") or "")
        backtest = item.get("backtest_component") if isinstance(item.get("backtest_component"), dict) else {}
        if item.get("eligible_for_daily_top3") is not True or strategy in observation or backtest.get("eligible_for_formal_ranking") is not True:
            violations.append({"type": "top_strategy_not_formally_eligible", "strategy": strategy, "run_id": str(item.get("run_id") or "")})
    for stock in top_stocks:
        for ref in stock.get("signal_refs") or []:
            strategy = str((ref or {}).get("strategy") or "")
            if strategy not in eligible:
                violations.append({"type": "top_stock_ref_not_formally_eligible", "strategy": strategy, "ts_code": str(stock.get("ts_code") or "")})
    return violations


def _experimental_uses_same_backtest_gate(reviews: List[JsonDict]) -> bool:
    experimental = []
    for item in reviews:
        strategy = str(item.get("strategy") or "")
        try:
            if get_profile(strategy).tier == "experimental":
                experimental.append(item)
        except KeyError:
            continue
    for item in experimental:
        backtest = item.get("backtest_component")
        if not isinstance(backtest, dict):
            return False
        if item.get("eligible_for_daily_top3") is True and backtest.get("eligible_for_formal_ranking") is not True:
            return False
    return True


def _find_promotion_decision_violations(conn: sqlite3.Connection) -> List[JsonDict]:
    rows = conn.execute(
        """
        SELECT decision_id, decision_type, based_on_run_id, decision_payload_json
        FROM decision_events
        WHERE decision_type IN ('promote_candidate', 'experiment_promote_candidate')
        ORDER BY created_at DESC
        """
    ).fetchall()
    violations = []
    for decision_id, decision_type, run_id, payload_json in rows:
        payload = _safe_json_loads(payload_json, {})
        evidence = payload.get("execution_evidence") if isinstance(payload, dict) and isinstance(payload.get("execution_evidence"), dict) else {}
        evidence_issues = _promotion_evidence_issues(evidence)
        if not evidence_issues:
            continue
        violations.append(
            {
                "decision_id": str(decision_id or ""),
                "decision_type": str(decision_type or ""),
                "run_id": str(run_id or ""),
                "reason": "missing_execution_evidence",
                "evidence_issues": evidence_issues,
            }
        )
    return violations


def _promotion_evidence_issues(evidence: JsonDict) -> List[str]:
    if not evidence:
        return ["execution_evidence_missing"]
    issues: List[str] = []
    if evidence.get("passed") is not True:
        issues.append("execution_evidence_not_passed")
    if int(evidence.get("sample_count", evidence.get("total_orders", 0)) or 0) <= 0:
        issues.append("execution_evidence_sample_empty")
    linked_run_ids = evidence.get("linked_run_ids")
    if not isinstance(linked_run_ids, list) or not [item for item in linked_run_ids if str(item or "").strip()]:
        issues.append("missing_linked_run_ids")
    cases = evidence.get("cases")
    if not isinstance(cases, list) or not cases:
        issues.append("execution_evidence_cases_empty")
        return issues
    for case in cases:
        if not isinstance(case, dict):
            issues.append("invalid_execution_case")
            continue
        order_id = str(case.get("order_id") or "")
        status = str(case.get("status") or "")
        if not str(case.get("decision_id") or ""):
            issues.append(f"missing_decision_id:{order_id}")
        if not str(case.get("based_on_run_id") or ""):
            issues.append(f"missing_linked_run_id:{order_id}")
        if case.get("has_attribution") is not True:
            issues.append(f"missing_attribution:{order_id}")
        if "slippage_bp" not in case:
            issues.append(f"missing_slippage:{order_id}")
        if status in {"filled", "partial_fill"} and int(case.get("fill_count", 0) or 0) <= 0:
            issues.append(f"missing_fill:{order_id}")
        if status in TERMINAL_MISS_STATUSES and not str(case.get("miss_reason_code") or ""):
            issues.append(f"missing_miss_reason:{order_id}")
        if status == "manual_override" and not str(case.get("cancel_reason") or ""):
            issues.append(f"missing_manual_override_reason:{order_id}")
    return issues


def _find_rejected_artifact_violations(rejected_artifacts: List[JsonDict]) -> List[JsonDict]:
    violations = []
    for item in rejected_artifacts:
        path = str(item.get("artifact_path") or item.get("path") or "")
        reason = str(item.get("reason") or "")
        reused = bool(item.get("reused_as_runtime_default") is True)
        if not path or not reason or reused:
            violations.append(
                {
                    "artifact_path": path,
                    "reason": reason,
                    "reused_as_runtime_default": reused,
                    "violation": "rejected_artifact_missing_reason_or_reused",
                }
            )
    return violations
