from __future__ import annotations

from typing import Any, Dict


JsonDict = Dict[str, Any]


RISK_FILTER_ONLY_SLEEVES = {"quality_low_vol", "sector_rotation"}
REBUILT_ALPHA_CANDIDATES = {"hard_event_alpha_candidate"}


def build_ensemble_sleeve_policy_audit(fact_chain: JsonDict | None) -> JsonDict:
    """Audit ensemble sleeve usage before any shadow portfolio construction.

    This gate is deliberately mechanical.  A sleeve can only enter the alpha
    candidate set when the persisted `sleeve_use_policy` and attribution fields
    agree.  Human narrative is not allowed to override negative IC, unstable
    RankIC, or risk-filter-only classification.
    """

    payload = fact_chain if isinstance(fact_chain, dict) else {}
    sleeves = payload.get("sleeves") if isinstance(payload.get("sleeves"), dict) else {}
    policy = payload.get("sleeve_use_policy") if isinstance(payload.get("sleeve_use_policy"), dict) else {}

    alpha_candidates: list[str] = []
    risk_filters: list[str] = []
    blocked: dict[str, list[str]] = {}
    violations: list[str] = []

    for sleeve, review_any in sorted(sleeves.items()):
        review = review_any if isinstance(review_any, dict) else {}
        declared = str(policy.get(sleeve) or review.get("recommended_use") or "research_blocked")
        reasons = _policy_blockers(sleeve=sleeve, declared=declared, review=review)
        if declared == "positive_alpha_candidate" and not reasons:
            alpha_candidates.append(sleeve)
        elif declared == "risk_filter_candidate" and sleeve in RISK_FILTER_ONLY_SLEEVES:
            risk_filters.append(sleeve)
        else:
            blocked[sleeve] = reasons or [f"sleeve_not_alpha_candidate:{declared}"]

        if declared == "positive_alpha_candidate" and reasons:
            violations.extend([f"{sleeve}:{reason}" for reason in reasons])
        if sleeve in RISK_FILTER_ONLY_SLEEVES and declared == "positive_alpha_candidate":
            violations.append(f"{sleeve}:risk_filter_only_sleeve_promoted_as_alpha")

    missing_policy = [sleeve for sleeve in sleeves if sleeve not in policy]
    violations.extend([f"{sleeve}:missing_sleeve_use_policy" for sleeve in missing_policy])

    return {
        "audit_version": "ensemble_sleeve_policy_audit.v1",
        "research_only": True,
        "passed": not violations,
        "alpha_candidate_sleeves": alpha_candidates,
        "risk_filter_sleeves": risk_filters,
        "blocked_sleeves": blocked,
        "excluded_sleeves": sorted(set(blocked) | set(risk_filters)),
        "policy_violations": violations,
        "hard_boundaries": [
            "do_not_feed_blocked_sleeves_into_positive_alpha_portfolio",
            "do_not_use_risk_filter_sleeves_as_positive_alpha",
            "do_not_override_sleeve_use_policy_with_manual_narrative",
        ],
    }


def build_rebuilt_alpha_candidate_sleeve_policy_audit(
    walk_forward_artifact: JsonDict | None,
    *,
    candidate: str = "hard_event_alpha_candidate",
    min_retained_windows: int = 4,
) -> JsonDict:
    """Audit a rebuilt alpha candidate before it can enter sleeve discussion.

    This is not a sleeve approval gate.  It exists to separate "the rebuilt
    candidate survived predeclared research validation" from "the candidate is
    now a tradable sleeve".  Portfolio construction, execution cost, capacity,
    and shadow benchmark evidence remain mandatory after this point.
    """

    payload = walk_forward_artifact if isinstance(walk_forward_artifact, dict) else {}
    walk_forward = payload.get("walk_forward") if isinstance(payload.get("walk_forward"), dict) else payload
    validation = walk_forward.get("validation_review") if isinstance(walk_forward.get("validation_review"), dict) else {}
    predeclared_gate = walk_forward.get("predeclared_gate") if isinstance(walk_forward.get("predeclared_gate"), dict) else {}
    actual_candidate = str(walk_forward.get("candidate") or candidate)
    blockers = _rebuilt_candidate_policy_blockers(
        walk_forward=walk_forward,
        validation=validation,
        predeclared_gate=predeclared_gate,
        expected_candidate=str(candidate),
        actual_candidate=actual_candidate,
        min_retained_windows=int(min_retained_windows),
    )
    discussion_eligible = not blockers
    return {
        "audit_version": "rebuilt_alpha_candidate_sleeve_policy_audit.v1",
        "research_only": True,
        "candidate": actual_candidate,
        "candidate_discussion_eligible": discussion_eligible,
        "sleeve_policy_approved": False,
        "observation_pool_eligible": False,
        "formal_pool_eligible": False,
        "blocking_reasons": blockers,
        "validation_summary": {
            "passed_predeclared_walk_forward_gate": bool(walk_forward.get("passed_predeclared_walk_forward_gate") is True),
            "retained_window_count": int(validation.get("retained_window_count", 0) or 0),
            "positive_retained_window_count": int(validation.get("positive_retained_window_count", 0) or 0),
            "excluded_window_count": int(validation.get("excluded_window_count", 0) or 0),
            "sample_count": int(validation.get("sample_count", 0) or 0),
            "raw_sample_count": int(validation.get("raw_sample_count", 0) or 0),
            "sample_retention": float(validation.get("sample_retention", 0.0) or 0.0),
            "ic": validation.get("ic"),
            "rank_ic": validation.get("rank_ic"),
        },
        "retained_window_reviews": list(validation.get("retained_window_reviews") or []),
        "required_next_evidence": [
            "independent_sleeve_policy_commit_with_candidate_rule_frozen",
            "research_only_shadow_portfolio_weights",
            "after_cost_shadow_benchmark",
            "capacity_and_turnover_constraints",
            "risk_contribution_and_regime_split",
        ],
        "hard_boundaries": [
            "candidate_discussion_eligibility_is_not_sleeve_approval",
            "do_not_promote_rebuilt_candidate_to_observation_without_shadow_portfolio_and_after_cost_benchmark",
            "do_not_use_predeclared_gate_walk_forward_as_formal_pool_evidence",
            "do_not_add_source_strategy_filter_without_new_predeclared_walk_forward",
        ],
    }


def _rebuilt_candidate_policy_blockers(
    *,
    walk_forward: JsonDict,
    validation: JsonDict,
    predeclared_gate: JsonDict,
    expected_candidate: str,
    actual_candidate: str,
    min_retained_windows: int,
) -> list[str]:
    blockers: list[str] = []
    if actual_candidate != expected_candidate:
        blockers.append(f"candidate_mismatch:{actual_candidate}!={expected_candidate}")
    if actual_candidate not in REBUILT_ALPHA_CANDIDATES:
        blockers.append(f"unsupported_rebuilt_alpha_candidate:{actual_candidate}")
    if bool(walk_forward.get("research_only") is True) is not True:
        blockers.append("walk_forward_not_research_only")
    if bool(walk_forward.get("passed_predeclared_walk_forward_gate") is True) is not True:
        blockers.append("predeclared_walk_forward_not_passed")
    if walk_forward.get("blocking_reasons"):
        blockers.append("walk_forward_has_blocking_reasons")
    if bool(predeclared_gate.get("declared_before_validation") is True) is not True:
        blockers.append("gate_not_declared_before_validation")
    if bool(predeclared_gate.get("source_strategy_filter_allowed") is False) is not True:
        blockers.append("source_strategy_filter_was_allowed")
    if bool(predeclared_gate.get("scenario_selection_allowed") is False) is not True:
        blockers.append("scenario_selection_was_allowed")

    retained_windows = int(validation.get("retained_window_count", 0) or 0)
    positive_windows = int(validation.get("positive_retained_window_count", 0) or 0)
    if retained_windows < int(min_retained_windows):
        blockers.append(f"insufficient_retained_windows:{retained_windows}/{int(min_retained_windows)}")
    if positive_windows != retained_windows:
        blockers.append("not_all_retained_windows_positive_ic_and_rank_ic")
    if validation.get("ic") is None or float(validation.get("ic") or 0.0) <= 0.0:
        blockers.append("retained_population_ic_not_positive")
    if validation.get("rank_ic") is None or float(validation.get("rank_ic") or 0.0) <= 0.0:
        blockers.append("retained_population_rank_ic_not_positive")
    if int(validation.get("excluded_window_count", 0) or 0) <= 0:
        blockers.append("missing_excluded_risk_off_window")
    return blockers


def _policy_blockers(*, sleeve: str, declared: str, review: JsonDict) -> list[str]:
    if declared != "positive_alpha_candidate":
        return []
    if sleeve in RISK_FILTER_ONLY_SLEEVES:
        return ["risk_filter_only_sleeve"]
    active = int(review.get("active_signal_count", 0) or 0)
    if active <= 0:
        return ["missing_active_sleeve_signals"]
    ic = review.get("ic")
    if ic is None or float(ic) <= 0.0:
        return ["non_positive_5d_ic"]
    rank_ic = review.get("rank_ic")
    if rank_ic is None or float(rank_ic) <= 0.0:
        return ["non_positive_5d_rank_ic"]
    multi = review.get("multi_horizon_attribution") if isinstance(review.get("multi_horizon_attribution"), dict) else {}
    if int(multi.get("positive_horizon_count", 0) or 0) < 2:
        return ["insufficient_positive_ic_horizons"]
    if int(multi.get("positive_rank_horizon_count", 0) or 0) < 2:
        return ["insufficient_positive_rank_ic_horizons"]
    return []
