from __future__ import annotations

from typing import Any, Dict, Sequence

from openclaw.services.ensemble_alpha_rebuild_lab_service import _candidate_score
from openclaw.services.ensemble_alpha_rebuild_lab_service import hard_event_neutral_noise_turnover_veto
from openclaw.services.ensemble_alpha_rebuild_lab_service import hard_event_risk_off_exhaustion_veto
from openclaw.services.ensemble_sleeve_policy_audit_service import build_ensemble_sleeve_policy_audit


JsonDict = Dict[str, Any]


DEFAULT_PORTFOLIO_VALUE = 1_000_000.0
DEFAULT_SINGLE_NAME_CAP = 0.1
DEFAULT_INDUSTRY_CAP = 0.3
DEFAULT_CAPACITY_PARTICIPATION = 0.1


def build_ensemble_shadow_portfolio(
    fact_chain: JsonDict | None,
    *,
    max_positions: int = 10,
    portfolio_value: float = DEFAULT_PORTFOLIO_VALUE,
    single_name_cap: float = DEFAULT_SINGLE_NAME_CAP,
    industry_cap: float = DEFAULT_INDUSTRY_CAP,
    capacity_participation: float = DEFAULT_CAPACITY_PARTICIPATION,
    alpha_risk_budget: JsonDict | None = None,
) -> JsonDict:
    """Build a research-only portfolio from audited sleeve candidates.

    The constructor refuses to emit weights when the sleeve policy audit has no
    positive alpha candidates.  Risk filters can only throttle candidates; they
    never contribute positive alpha weight.
    """

    payload = fact_chain if isinstance(fact_chain, dict) else {}
    policy_audit = payload.get("sleeve_policy_audit")
    if not isinstance(policy_audit, dict):
        policy_audit = build_ensemble_sleeve_policy_audit(payload)
    alpha_sleeves = [str(item) for item in policy_audit.get("alpha_candidate_sleeves") or []]
    risk_filters = [str(item) for item in policy_audit.get("risk_filter_sleeves") or []]
    blockers: list[str] = []
    if policy_audit.get("passed") is not True:
        blockers.append("sleeve_policy_audit_not_passed")
    if not alpha_sleeves:
        blockers.append("missing_positive_alpha_candidate_sleeves")
    if blockers:
        return _blocked(policy_audit=policy_audit, risk_filters=risk_filters, blocking=blockers)

    budget = _normalized_budget(alpha_sleeves, alpha_risk_budget)
    rows = _candidate_rows(
        payload.get("sample_facts") or [],
        alpha_sleeves=alpha_sleeves,
        risk_filters=risk_filters,
        risk_budget=budget,
    )
    rows = [row for row in rows if float(row.get("construction_score", 0.0) or 0.0) > 0.0]
    if not rows:
        return _blocked(policy_audit=policy_audit, risk_filters=risk_filters, blocking=["missing_positive_construction_scores"])

    rows = sorted(rows, key=lambda row: float(row.get("construction_score", 0.0) or 0.0), reverse=True)[: int(max_positions or 0)]
    total_score = sum(float(row.get("construction_score", 0.0) or 0.0) for row in rows)
    industry_weights: dict[str, float] = {}
    weights: list[JsonDict] = []
    constraint_hits: list[JsonDict] = []
    for row in rows:
        raw_weight = float(row.get("construction_score", 0.0) or 0.0) / total_score if total_score > 0.0 else 0.0
        capped = min(raw_weight, float(single_name_cap))
        if capped < raw_weight:
            constraint_hits.append({"ts_code": row["ts_code"], "constraint": "single_name_cap", "raw_weight": raw_weight, "capped_weight": capped})
        industry = str(row.get("industry") or "unknown")
        remaining_industry = max(0.0, float(industry_cap) - industry_weights.get(industry, 0.0))
        if capped > remaining_industry:
            constraint_hits.append({"ts_code": row["ts_code"], "constraint": "industry_cap", "raw_weight": capped, "capped_weight": remaining_industry})
            capped = remaining_industry
        amount = float(row.get("latest_amount", 0.0) or 0.0)
        capacity_cap = (amount * float(capacity_participation)) / float(portfolio_value or DEFAULT_PORTFOLIO_VALUE) if amount > 0.0 else 0.0
        if capacity_cap > 0.0 and capped > capacity_cap:
            constraint_hits.append({"ts_code": row["ts_code"], "constraint": "capacity_constraint", "raw_weight": capped, "capped_weight": capacity_cap})
            capped = capacity_cap
        if capped <= 0.0:
            continue
        industry_weights[industry] = industry_weights.get(industry, 0.0) + capped
        weights.append({**row, "raw_weight": round(raw_weight, 6), "weight": round(capped, 6)})

    invested = sum(float(row.get("weight", 0.0) or 0.0) for row in weights)
    return {
        "portfolio_version": "ensemble_shadow_portfolio.v1",
        "research_only": True,
        "not_for_production": True,
        "shadow_weights": weights,
        "excluded_sleeves": policy_audit.get("excluded_sleeves") or [],
        "constraint_hits": constraint_hits,
        "risk_budget": budget,
        "industry_exposure": {key: round(value, 6) for key, value in sorted(industry_weights.items())},
        "capacity_usage": _capacity_usage(weights, portfolio_value=portfolio_value),
        "turnover_estimate": round(invested, 6),
        "cash_weight": round(max(0.0, 1.0 - invested), 6),
        "blocking_reasons": [],
        "hard_boundaries": [
            "do_not_write_shadow_weights_to_formal_top_stocks",
            "do_not_use_risk_filters_as_positive_alpha",
            "do_not_bypass_after_cost_benchmark_before_observation",
        ],
    }


def build_rebuilt_candidate_shadow_portfolio(
    fact_chain: JsonDict | None,
    *,
    candidate_policy_audit: JsonDict | None,
    rule_freeze: JsonDict | None,
    market_regime_label: str = "",
    max_positions: int = 10,
    portfolio_value: float = DEFAULT_PORTFOLIO_VALUE,
    single_name_cap: float = DEFAULT_SINGLE_NAME_CAP,
    industry_cap: float = DEFAULT_INDUSTRY_CAP,
    capacity_participation: float = DEFAULT_CAPACITY_PARTICIPATION,
    target_gross_exposure: float = 1.0,
    neutral_gross_exposure: float = 1.0,
) -> JsonDict:
    """Build research-only shadow weights from one frozen rebuilt candidate."""

    payload = fact_chain if isinstance(fact_chain, dict) else {}
    audit = candidate_policy_audit if isinstance(candidate_policy_audit, dict) else {}
    if "audit" in audit and isinstance(audit.get("audit"), dict):
        audit = audit["audit"]
    freeze = rule_freeze if isinstance(rule_freeze, dict) else {}
    if "rule_freeze" in freeze and isinstance(freeze.get("rule_freeze"), dict):
        freeze = freeze["rule_freeze"]
    candidate = str(freeze.get("candidate") or audit.get("candidate") or "hard_event_alpha_candidate")
    blockers: list[str] = []
    if audit.get("candidate_discussion_eligible") is not True:
        blockers.append("candidate_policy_discussion_not_eligible")
    if audit.get("sleeve_policy_approved") is True:
        blockers.append("candidate_policy_unexpectedly_approved_sleeve")
    if audit.get("observation_pool_eligible") is True or audit.get("formal_pool_eligible") is True:
        blockers.append("candidate_policy_attempted_pool_eligibility")
    if freeze.get("frozen") is not True:
        blockers.append("candidate_rule_not_frozen")
    if freeze.get("sleeve_policy_approved") is True:
        blockers.append("rule_freeze_unexpectedly_approved_sleeve")
    if payload.get("research_only") is not True:
        blockers.append("fact_chain_not_research_only")
    items = payload.get("sample_facts") if isinstance(payload.get("sample_facts"), list) else []
    if not items:
        blockers.append("missing_sample_facts")
    if blockers:
        return _rebuilt_blocked(candidate=candidate, audit=audit, freeze=freeze, blocking=blockers)

    rows = []
    vetoed_signals: list[JsonDict] = []
    neutral_vetoed_signals: list[JsonDict] = []
    seen_codes: set[str] = set()
    rule_version = str(freeze.get("rule_version") or "")
    source_counts = _cross_strategy_source_counts(items)
    for item in items:
        code = str(item.get("ts_code") or "")
        if not code or code in seen_codes:
            continue
        seen_codes.add(code)
        base_score = _candidate_score(candidate, item)
        veto = (
            hard_event_risk_off_exhaustion_veto(item, market_regime_label=str(market_regime_label or ""))
            if candidate == "hard_event_alpha_candidate"
            else {"vetoed": False, "multiplier": 1.0}
        )
        neutral_veto = (
            hard_event_neutral_noise_turnover_veto(
                item,
                market_regime_label=str(market_regime_label or ""),
                construction_score=float(base_score or 0.0),
                cross_strategy_source_count=int(source_counts.get(code, 1) or 1),
                rule_version=rule_version,
            )
            if candidate == "hard_event_alpha_candidate" and _neutral_guard_enabled(rule_version)
            else {"vetoed": False, "multiplier": 1.0}
        )
        if veto.get("vetoed") is True:
            vetoed_signals.append(
                {
                    "ts_code": code,
                    "source_strategy": str(item.get("strategy") or ""),
                    "base_score": round(float(base_score or 0.0), 6),
                    "reason": str(veto.get("reason") or ""),
                    "components": veto.get("components") or {},
                }
            )
        if neutral_veto.get("vetoed") is True:
            neutral_vetoed_signals.append(
                {
                    "ts_code": code,
                    "source_strategy": str(item.get("strategy") or ""),
                    "base_score": round(float(base_score or 0.0), 6),
                    "reason": str(neutral_veto.get("reason") or ""),
                    "components": neutral_veto.get("components") or {},
                }
            )
        score = (
            float(base_score or 0.0)
            * float(veto.get("multiplier", 1.0) or 0.0)
            * float(neutral_veto.get("multiplier", 1.0) or 0.0)
        )
        if score <= 0.0:
            continue
        features = item.get("tushare_pro_alpha_features") if isinstance(item.get("tushare_pro_alpha_features"), dict) else {}
        evidence = features.get("evidence") if isinstance(features.get("evidence"), dict) else {}
        rows.append(
            {
                "ts_code": code,
                "industry": str(evidence.get("industry") or "unknown"),
                "alpha_score": round(score, 6),
                "risk_filter_multiplier": 1.0,
                "construction_score": round(score, 6),
                "latest_amount": float(evidence.get("latest_amount", 0.0) or 0.0),
                "source_strategy": str(item.get("strategy") or ""),
                "cross_strategy_source_count": int(source_counts.get(code, 1) or 1),
                "candidate": candidate,
            }
        )
    rows = [row for row in rows if row.get("ts_code")]
    if not rows:
        return _rebuilt_blocked(candidate=candidate, audit=audit, freeze=freeze, blocking=["missing_positive_candidate_scores"])
    return _construct_weight_payload(
        rows=rows,
        candidate=candidate,
        audit=audit,
        freeze=freeze,
        max_positions=max_positions,
        portfolio_value=portfolio_value,
        single_name_cap=single_name_cap,
        industry_cap=industry_cap,
        capacity_participation=capacity_participation,
        market_regime_label=market_regime_label,
        target_gross_exposure=target_gross_exposure,
        neutral_gross_exposure=neutral_gross_exposure,
        risk_off_vetoed_signals=vetoed_signals,
        neutral_vetoed_signals=neutral_vetoed_signals,
    )


def _construct_weight_payload(
    *,
    rows: Sequence[JsonDict],
    candidate: str,
    audit: JsonDict,
    freeze: JsonDict,
    max_positions: int,
    portfolio_value: float,
    single_name_cap: float,
    industry_cap: float,
    capacity_participation: float,
    market_regime_label: str = "",
    target_gross_exposure: float = 1.0,
    neutral_gross_exposure: float = 1.0,
    risk_off_vetoed_signals: Sequence[JsonDict] | None = None,
    neutral_vetoed_signals: Sequence[JsonDict] | None = None,
) -> JsonDict:
    selected = sorted(rows, key=lambda row: float(row.get("construction_score", 0.0) or 0.0), reverse=True)[: int(max_positions or 0)]
    total_score = sum(float(row.get("construction_score", 0.0) or 0.0) for row in selected)
    industry_weights: dict[str, float] = {}
    weights: list[JsonDict] = []
    constraint_hits: list[JsonDict] = []
    for row in selected:
        raw_weight = float(row.get("construction_score", 0.0) or 0.0) / total_score if total_score > 0.0 else 0.0
        capped = min(raw_weight, float(single_name_cap))
        if capped < raw_weight:
            constraint_hits.append({"ts_code": row["ts_code"], "constraint": "single_name_cap", "raw_weight": raw_weight, "capped_weight": capped})
        industry = str(row.get("industry") or "unknown")
        remaining_industry = max(0.0, float(industry_cap) - industry_weights.get(industry, 0.0))
        if capped > remaining_industry:
            constraint_hits.append({"ts_code": row["ts_code"], "constraint": "industry_cap", "raw_weight": capped, "capped_weight": remaining_industry})
            capped = remaining_industry
        amount = float(row.get("latest_amount", 0.0) or 0.0)
        capacity_cap = (amount * float(capacity_participation)) / float(portfolio_value or DEFAULT_PORTFOLIO_VALUE) if amount > 0.0 else 0.0
        if capacity_cap > 0.0 and capped > capacity_cap:
            constraint_hits.append({"ts_code": row["ts_code"], "constraint": "capacity_constraint", "raw_weight": capped, "capped_weight": capacity_cap})
            capped = capacity_cap
        if capped <= 0.0:
            continue
        industry_weights[industry] = industry_weights.get(industry, 0.0) + capped
        weights.append({**row, "raw_weight": round(raw_weight, 6), "weight": round(capped, 6)})

    regime = str(market_regime_label or "").strip() or "unknown"
    gross_target = _effective_gross_exposure(
        market_regime_label=regime,
        target_gross_exposure=target_gross_exposure,
        neutral_gross_exposure=neutral_gross_exposure,
    )
    pre_throttle_invested = sum(float(row.get("weight", 0.0) or 0.0) for row in weights)
    allocator_controls = {
        "market_regime_label": regime,
        "target_gross_exposure": round(gross_target, 6),
        "base_target_gross_exposure": round(max(0.0, min(1.0, float(target_gross_exposure or 0.0))), 6),
        "neutral_gross_exposure": round(max(0.0, min(1.0, float(neutral_gross_exposure or 0.0))), 6),
        "pre_throttle_invested_weight": round(pre_throttle_invested, 6),
    }
    if pre_throttle_invested > gross_target >= 0.0:
        scale = gross_target / pre_throttle_invested if pre_throttle_invested > 0.0 else 0.0
        constraint_hits.append(
            {
                "constraint": "gross_exposure_throttle",
                "market_regime_label": regime,
                "raw_weight": pre_throttle_invested,
                "capped_weight": gross_target,
            }
        )
        weights = [{**row, "weight": round(float(row.get("weight", 0.0) or 0.0) * scale, 6)} for row in weights]
        industry_weights = _industry_weights(weights)

    invested = sum(float(row.get("weight", 0.0) or 0.0) for row in weights)
    return {
        "portfolio_version": "rebuilt_candidate_shadow_portfolio.v1",
        "research_only": True,
        "not_for_production": True,
        "candidate": candidate,
        "rule_version": str(freeze.get("rule_version") or ""),
        "rule_hash": str(freeze.get("rule_hash") or ""),
        "candidate_discussion_eligible": bool(audit.get("candidate_discussion_eligible") is True),
        "sleeve_policy_approved": False,
        "allocator_controls": allocator_controls,
        "risk_off_vetoed_signals": list(risk_off_vetoed_signals or []),
        "neutral_vetoed_signals": list(neutral_vetoed_signals or []),
        "shadow_weights": weights,
        "constraint_hits": constraint_hits,
        "industry_exposure": {key: round(value, 6) for key, value in sorted(industry_weights.items())},
        "capacity_usage": _capacity_usage(weights, portfolio_value=portfolio_value),
        "turnover_estimate": round(invested, 6),
        "cash_weight": round(max(0.0, 1.0 - invested), 6),
        "blocking_reasons": [] if weights else ["missing_allocatable_shadow_weights"],
        "hard_boundaries": [
            "rebuilt_candidate_shadow_weights_are_research_only",
            "do_not_write_shadow_weights_to_formal_top_stocks",
            "do_not_promote_without_after_cost_shadow_benchmark",
            "do_not_change_candidate_rule_after_freeze",
            "allocator_throttle_is_not_alpha_improvement",
        ],
    }


def _rebuilt_blocked(*, candidate: str, audit: JsonDict, freeze: JsonDict, blocking: Sequence[str]) -> JsonDict:
    return {
        "portfolio_version": "rebuilt_candidate_shadow_portfolio.v1",
        "research_only": True,
        "not_for_production": True,
        "candidate": candidate,
        "rule_version": str(freeze.get("rule_version") or ""),
        "rule_hash": str(freeze.get("rule_hash") or ""),
        "candidate_discussion_eligible": bool(audit.get("candidate_discussion_eligible") is True),
        "sleeve_policy_approved": False,
        "shadow_weights": [],
        "constraint_hits": [],
        "industry_exposure": {},
        "capacity_usage": {},
        "turnover_estimate": 0.0,
        "cash_weight": 1.0,
        "blocking_reasons": list(blocking),
        "hard_boundaries": [
            "rebuilt_candidate_shadow_weights_are_research_only",
            "do_not_write_shadow_weights_to_formal_top_stocks",
            "do_not_promote_without_after_cost_shadow_benchmark",
            "do_not_change_candidate_rule_after_freeze",
        ],
    }


def _neutral_guard_enabled(rule_version: str) -> bool:
    text = str(rule_version or "")
    return (
        "neutral_noise_turnover_guard.v4" in text
        or "neutral_consensus_turnover_guard.v5" in text
        or "neutral_over_veto_rebalance_guard.v6" in text
    )


def _cross_strategy_source_counts(items: Sequence[JsonDict]) -> dict[str, int]:
    sources: dict[str, set[str]] = {}
    for item in items:
        code = str(item.get("ts_code") or "")
        if not code:
            continue
        source = str(item.get("strategy") or "")
        if source:
            sources.setdefault(code, set()).add(source)
    return {code: len(values) for code, values in sources.items()}


def _blocked(*, policy_audit: JsonDict, risk_filters: Sequence[str], blocking: Sequence[str]) -> JsonDict:
    return {
        "portfolio_version": "ensemble_shadow_portfolio.v1",
        "research_only": True,
        "not_for_production": True,
        "shadow_weights": [],
        "excluded_sleeves": policy_audit.get("excluded_sleeves") or list(risk_filters),
        "constraint_hits": [],
        "risk_budget": {},
        "industry_exposure": {},
        "capacity_usage": {},
        "turnover_estimate": 0.0,
        "cash_weight": 1.0,
        "blocking_reasons": list(blocking),
        "hard_boundaries": [
            "do_not_write_shadow_weights_to_formal_top_stocks",
            "do_not_use_risk_filters_as_positive_alpha",
            "do_not_bypass_after_cost_benchmark_before_observation",
        ],
    }


def _normalized_budget(alpha_sleeves: Sequence[str], value: JsonDict | None) -> JsonDict:
    raw = value if isinstance(value, dict) else {}
    out = {sleeve: max(0.0, float(raw.get(sleeve, 1.0) or 0.0)) for sleeve in alpha_sleeves}
    total = sum(out.values())
    if total <= 0.0:
        return {sleeve: round(1.0 / len(alpha_sleeves), 6) for sleeve in alpha_sleeves}
    return {sleeve: round(weight / total, 6) for sleeve, weight in out.items()}


def _candidate_rows(
    items: Sequence[JsonDict],
    *,
    alpha_sleeves: Sequence[str],
    risk_filters: Sequence[str],
    risk_budget: JsonDict,
) -> list[JsonDict]:
    out: list[JsonDict] = []
    seen: set[str] = set()
    for item in items:
        code = str(item.get("ts_code") or "")
        if not code or code in seen:
            continue
        seen.add(code)
        sleeve_scores = item.get("sleeve_scores") if isinstance(item.get("sleeve_scores"), dict) else {}
        alpha_score = sum(
            _score(sleeve_scores, sleeve) * float(risk_budget.get(sleeve, 0.0) or 0.0)
            for sleeve in alpha_sleeves
        )
        filter_multiplier = _filter_multiplier(sleeve_scores, risk_filters)
        features = item.get("tushare_pro_alpha_features") if isinstance(item.get("tushare_pro_alpha_features"), dict) else {}
        evidence = features.get("evidence") if isinstance(features.get("evidence"), dict) else {}
        out.append(
            {
                "ts_code": code,
                "industry": str(evidence.get("industry") or "unknown"),
                "alpha_score": round(alpha_score, 6),
                "risk_filter_multiplier": round(filter_multiplier, 6),
                "construction_score": round(alpha_score * filter_multiplier, 6),
                "latest_amount": float(evidence.get("latest_amount", 0.0) or 0.0),
                "source_strategy": str(item.get("strategy") or ""),
            }
        )
    return out


def _score(sleeve_scores: JsonDict, sleeve: str) -> float:
    score = (sleeve_scores.get(sleeve) or {}).get("score") if isinstance(sleeve_scores.get(sleeve), dict) else 0.0
    return max(0.0, min(100.0, float(score or 0.0)))


def _filter_multiplier(sleeve_scores: JsonDict, risk_filters: Sequence[str]) -> float:
    if not risk_filters:
        return 1.0
    multiplier = 1.0
    for sleeve in risk_filters:
        score = _score(sleeve_scores, sleeve)
        multiplier *= 0.7 + 0.3 * (score / 100.0)
    return max(0.0, min(1.0, multiplier))


def _capacity_usage(weights: Sequence[JsonDict], *, portfolio_value: float) -> JsonDict:
    out: JsonDict = {}
    for row in weights:
        amount = float(row.get("latest_amount", 0.0) or 0.0)
        usage = (float(row.get("weight", 0.0) or 0.0) * float(portfolio_value or DEFAULT_PORTFOLIO_VALUE)) / amount if amount > 0.0 else 0.0
        out[str(row.get("ts_code") or "")] = round(usage, 6)
    return out


def _effective_gross_exposure(
    *,
    market_regime_label: str,
    target_gross_exposure: float,
    neutral_gross_exposure: float,
) -> float:
    base = max(0.0, min(1.0, float(target_gross_exposure or 0.0)))
    neutral = max(0.0, min(1.0, float(neutral_gross_exposure or 0.0)))
    if str(market_regime_label or "").strip() == "neutral":
        return min(base, neutral)
    return base


def _industry_weights(weights: Sequence[JsonDict]) -> dict[str, float]:
    out: dict[str, float] = {}
    for row in weights:
        industry = str(row.get("industry") or "unknown")
        out[industry] = out.get(industry, 0.0) + float(row.get("weight", 0.0) or 0.0)
    return out
