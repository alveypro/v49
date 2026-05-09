from __future__ import annotations

import hashlib
import json
from typing import Any, Dict


JsonDict = Dict[str, Any]


HARD_EVENT_ALPHA_CANDIDATE_RULE_VERSION = "hard_event_alpha_candidate.neutral_consensus_turnover_guard.v5"
HARD_EVENT_ALPHA_CANDIDATE_V6_RULE_VERSION = "hard_event_alpha_candidate.neutral_over_veto_rebalance_guard.v6"


def build_rebuilt_candidate_rule_freeze(
    policy_audit: JsonDict | None,
    *,
    candidate: str = "hard_event_alpha_candidate",
    rule_version: str = HARD_EVENT_ALPHA_CANDIDATE_RULE_VERSION,
) -> JsonDict:
    """Freeze a rebuilt alpha candidate rule before portfolio construction.

    Freezing the rule does not approve it as a sleeve.  It only prevents later
    shadow portfolio tests from silently changing the candidate recipe.
    """

    audit = policy_audit if isinstance(policy_audit, dict) else {}
    if "audit" in audit and isinstance(audit.get("audit"), dict):
        audit = audit["audit"]
    candidate_name = str(candidate or audit.get("candidate") or "")
    rule = _rule_spec(candidate_name, rule_version=str(rule_version or ""))
    blockers: list[str] = []
    if not candidate_name:
        blockers.append("missing_candidate")
    if not rule:
        blockers.append(f"unsupported_candidate_rule:{candidate_name}")
    if audit.get("candidate_discussion_eligible") is not True:
        blockers.append("candidate_policy_discussion_not_eligible")
    if audit.get("sleeve_policy_approved") is True:
        blockers.append("candidate_already_marked_as_sleeve_approved")
    if audit.get("observation_pool_eligible") is True or audit.get("formal_pool_eligible") is True:
        blockers.append("candidate_policy_audit_attempted_pool_eligibility")

    return {
        "freeze_version": "rebuilt_candidate_rule_freeze.v1",
        "research_only": True,
        "candidate": candidate_name,
        "rule_version": rule.get("rule_version", ""),
        "rule_hash": _hash_rule(rule) if rule else "",
        "frozen": not blockers,
        "sleeve_policy_approved": False,
        "observation_pool_eligible": False,
        "formal_pool_eligible": False,
        "rule_spec": rule,
        "source_policy_audit_version": str(audit.get("audit_version") or ""),
        "blocking_reasons": blockers,
        "hard_boundaries": [
            "rule_freeze_is_not_sleeve_approval",
            "do_not_change_candidate_rule_inside_shadow_portfolio",
            "do_not_promote_from_rule_freeze_without_after_cost_shadow_benchmark",
        ],
    }


def _rule_spec(candidate: str, *, rule_version: str = HARD_EVENT_ALPHA_CANDIDATE_RULE_VERSION) -> JsonDict:
    if candidate != "hard_event_alpha_candidate":
        return {}
    version = str(rule_version or HARD_EVENT_ALPHA_CANDIDATE_RULE_VERSION)
    if version == HARD_EVENT_ALPHA_CANDIDATE_V6_RULE_VERSION:
        return _hard_event_alpha_candidate_v6_rule()
    if version not in {"", HARD_EVENT_ALPHA_CANDIDATE_RULE_VERSION}:
        return {}
    return _hard_event_alpha_candidate_v5_rule()


def _hard_event_alpha_candidate_v5_rule() -> JsonDict:
    return {
        "rule_version": HARD_EVENT_ALPHA_CANDIDATE_RULE_VERSION,
        "base_score": "hard_event_alpha",
        "tradability_floor": {"capacity_liquidity_min": 35.0},
        "confirmation_caps": {
            "money_flow_persistence": 62.0,
            "dragon_tiger_seat_quality": 58.0,
            "limit_break_structure": 58.0,
            "industry_crowding": 55.0,
        },
        "confirmation_formula": (
            "hard_event_alpha"
            "*(0.70+0.30*min(flow,62)/100)"
            "*(0.82+0.18*min(seat,58)/100)"
            "*(0.86+0.14*min(limit_structure,58)/100)"
            "*(0.92+0.08*min(crowding,55)/100)"
            "*exhaustion_penalty"
        ),
        "exhaustion_penalty": {
            "point_penalty": 0.18,
            "floor": 0.22,
            "points": [
                "capacity_liquidity>=70 and money_flow_persistence>=65",
                "money_flow_persistence>=75 and dragon_tiger_seat_quality>=70",
                "dragon_tiger_seat_quality>=70 and limit_break_structure>=60",
                "capacity_liquidity>=70 and dragon_tiger_seat_quality>=70 and limit_break_structure>=60",
                "money_flow_persistence>=75 and limit_break_structure<=50",
                "margin_pressure>=55 and money_flow_persistence>=65",
            ],
        },
        "risk_off_exhaustion_veto": {
            "activation": "market_regime_label == risk_off",
            "veto_rules": [
                "hard_event_alpha>=35 and capacity_liquidity>=70 and money_flow_persistence>=60 and limit_break_structure>=52",
                "hard_event_alpha>=35 and capacity_liquidity>=70 and dragon_tiger_seat_quality>=75 and limit_break_structure>=52",
                "source_strategy in v6,v8,v9 and hard_event_alpha>=30 and capacity_liquidity>=55 and money_flow_persistence>=50 and limit_break_structure>=45",
                "source_strategy != combo and hard_event_alpha>=30 and capacity_liquidity>=35 and money_flow_persistence>=50",
            ],
            "reason": "risk_off_high_event_flow_or_seat_capacity_exhaustion_or_missing_cross_strategy_consensus",
        },
        "neutral_noise_turnover_guard": {
            "activation": "market_regime_label == neutral",
            "veto_rules": [
                "hard_event_alpha<40",
                "source_strategy in v6,v8,v9 and hard_event_alpha<46 and money_flow_persistence<58",
                "source_strategy != combo and money_flow_persistence<52 and dragon_tiger_seat_quality<50 and limit_break_structure<50",
            ],
            "reason": "neutral_low_confirmation_hard_event_noise_or_turnover_churn",
        },
        "neutral_consensus_turnover_guard": {
            "activation": "market_regime_label == neutral",
            "veto_rules": [
                "source_strategy != combo and cross_strategy_source_count<2 and construction_score<45",
            ],
            "reason": "neutral_single_source_low_conviction_turnover_churn",
        },
    }


def _hard_event_alpha_candidate_v6_rule() -> JsonDict:
    return {
        "rule_version": HARD_EVENT_ALPHA_CANDIDATE_V6_RULE_VERSION,
        "base_score": "hard_event_alpha",
        "repair_objective": "reduce over-veto while preserving risk-off exhaustion repair and turnover discipline",
        "tradability_floor": {"capacity_liquidity_min": 35.0},
        "confirmation_caps": {
            "money_flow_persistence": 62.0,
            "dragon_tiger_seat_quality": 58.0,
            "limit_break_structure": 58.0,
            "industry_crowding": 55.0,
        },
        "confirmation_formula": (
            "hard_event_alpha"
            "*(0.70+0.30*min(flow,62)/100)"
            "*(0.82+0.18*min(seat,58)/100)"
            "*(0.86+0.14*min(limit_structure,58)/100)"
            "*(0.92+0.08*min(crowding,55)/100)"
            "*exhaustion_penalty"
        ),
        "exhaustion_penalty": {
            "point_penalty": 0.18,
            "floor": 0.22,
            "points": [
                "capacity_liquidity>=70 and money_flow_persistence>=65",
                "money_flow_persistence>=75 and dragon_tiger_seat_quality>=70",
                "dragon_tiger_seat_quality>=70 and limit_break_structure>=60",
                "capacity_liquidity>=70 and dragon_tiger_seat_quality>=70 and limit_break_structure>=60",
                "money_flow_persistence>=75 and limit_break_structure<=50",
                "margin_pressure>=55 and money_flow_persistence>=65",
            ],
        },
        "risk_off_exhaustion_veto": {
            "activation": "market_regime_label == risk_off",
            "inherited_from": "hard_event_alpha_candidate.neutral_consensus_turnover_guard.v5",
            "veto_rules": [
                "hard_event_alpha>=35 and capacity_liquidity>=70 and money_flow_persistence>=60 and limit_break_structure>=52",
                "hard_event_alpha>=35 and capacity_liquidity>=70 and dragon_tiger_seat_quality>=75 and limit_break_structure>=52",
                "source_strategy in v6,v8,v9 and hard_event_alpha>=30 and capacity_liquidity>=55 and money_flow_persistence>=50 and limit_break_structure>=45",
                "source_strategy != combo and hard_event_alpha>=30 and capacity_liquidity>=35 and money_flow_persistence>=50",
            ],
            "reason": "preserve_v5_risk_off_high_event_flow_or_seat_capacity_exhaustion_veto",
        },
        "neutral_over_veto_rebalance_guard": {
            "activation": "market_regime_label == neutral",
            "veto_rules": [
                "hard_event_alpha<38",
                "source_strategy in v6,v8,v9 and hard_event_alpha<44 and money_flow_persistence<55",
                "source_strategy != combo and money_flow_persistence<48 and dragon_tiger_seat_quality<48 and limit_break_structure<48",
                "source_strategy != combo and cross_strategy_source_count<2 and construction_score<42 and money_flow_persistence<55",
            ],
            "reason": "neutral_reduce_false_negative_veto_while_retaining_low_confirmation_turnover_filter",
        },
        "predeclared_failure_targets": [
            "over_veto",
            "neutral_alpha",
            "neutral_hit",
            "turnover",
            "risk_off_regression",
        ],
    }


def _hash_rule(rule: JsonDict) -> str:
    payload = json.dumps(rule, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()
