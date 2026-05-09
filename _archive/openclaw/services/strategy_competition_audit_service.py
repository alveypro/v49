from __future__ import annotations

from datetime import datetime
import hashlib
import json
import math
import sqlite3
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional
from uuid import uuid4

import numpy as np

try:
    import cvxpy as cp
except Exception:  # pragma: no cover - fallback path when solver dependency is missing.
    cp = None

from openclaw.services.lineage_service import apply_professional_migrations, canonical_json
from openclaw.services.benchmark_industry_contract_service import build_benchmark_industry_contract
from openclaw.services.unified_strategy_recommendation_service import build_unified_system_recommendation
from strategies.registry import get_profile


JsonDict = Dict[str, Any]
DEFAULT_CONSTRAINTS: JsonDict = {
    "max_single_name_weight": 0.25,
    "max_industry_weight": 0.45,
    "min_liquidity_amount": 500000.0,
    "max_estimated_cost_bps": 35.0,
    "predeclared_cost_bps": 15.0,
    "top_stock_count": 5,
    "portfolio_notional": 1000000.0,
    "max_order_value": 250000.0,
    "max_turnover": 1.0,
    "risk_lookback_days": 20,
    "min_volatility_floor": 0.01,
    "max_single_risk_contribution": 0.4,
    "risk_covariance_shrinkage": 0.35,
    "same_industry_correlation_scale": 1.0,
    "cross_industry_correlation_scale": 0.6,
    "adaptive_shrinkage_min": 0.15,
    "adaptive_shrinkage_max": 0.8,
    "max_abs_size_exposure": 0.35,
    "max_abs_liquidity_exposure": 0.35,
    "factor_value_weight": 0.2,
    "factor_momentum_weight": 0.2,
    "factor_quality_weight": 0.2,
    "factor_exposure_projection_iterations": 6,
    "max_abs_value_exposure": 0.35,
    "max_abs_momentum_exposure": 0.35,
    "max_abs_quality_exposure": 0.35,
    "qp_enable": True,
    "qp_alpha_scale": 1.0,
    "qp_risk_aversion": 8.0,
    "qp_cost_aversion": 0.2,
    "qp_solver": "OSQP",
    "industry_neutral_tolerance": 0.08,
    "max_portfolio_variance": 0.0008,
    "benchmark_max_staleness_days": 3,
}
REQUIRED_MODEL_CARD_FIELDS = ("model_card", "hypothesis", "rule_hash", "data_hash", "code_hash")
REQUIRED_PRE_TRADE_CONTROLS = (
    "single_name_weight_limit",
    "industry_weight_limit",
    "liquidity_check",
    "price_limit_check",
    "suspension_check",
    "turnover_budget",
    "order_value_limit",
    "risk_contribution_limit",
    "size_factor_exposure_limit",
    "liquidity_factor_exposure_limit",
)
PROHIBITED_COMPETITION_STATUSES = {"research_only", "diagnostic", "observation", "failed", "archived"}
MAX_WEIGHT_OPTIMIZATION_ITERATIONS = 16


def _now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _new_id(prefix: str) -> str:
    now = datetime.now()
    return f"{prefix}_{now.strftime('%Y%m%d_%H%M%S')}_{uuid4().hex[:8]}"


def _hash_payload(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def _normalize_pool(pool: Iterable[Any]) -> List[str]:
    out: List[str] = []
    for item in pool or []:
        value = str(item or "").strip().lower()
        if value and value not in out:
            out.append(value)
    return out


def build_alpha_model_cards_from_recommendation(
    recommendation: JsonDict,
    *,
    fixed_candidate_pool: Iterable[Any],
    hypothesis_overrides: Optional[Dict[str, str]] = None,
) -> Dict[str, JsonDict]:
    """Derive model-card skeletons from real strategy run metadata.

    This helper does not approve a strategy. It only converts existing run
    lineage into auditable model-card inputs so the competition audit can state
    exactly which rule/data/code versions were considered.
    """

    overrides = {str(k or "").lower(): str(v or "") for k, v in (hypothesis_overrides or {}).items()}
    reviews = {
        str(item.get("strategy") or "").lower(): item
        for item in recommendation.get("all_strategy_reviews") or []
        if isinstance(item, dict)
    }
    cards: Dict[str, JsonDict] = {}
    for strategy in _normalize_pool(fixed_candidate_pool):
        review = reviews.get(strategy) or {}
        profile_payload: JsonDict = {}
        try:
            profile = get_profile(strategy)
            profile_payload = {
                "tier": profile.tier,
                "stage": profile.stage,
                "role": profile.role,
                "default_score_threshold": profile.default_score_threshold,
                "default_holding_days": profile.default_holding_days,
            }
        except Exception:
            profile_payload = {"tier": "unknown", "stage": "unknown", "role": ""}
        hypothesis = overrides.get(strategy) or _default_strategy_hypothesis(strategy=strategy, review=review)
        data_version = str(review.get("data_version") or "")
        code_version = str(review.get("code_version") or "")
        param_version = str(review.get("param_version") or "")
        run_id = str(review.get("run_id") or "")
        evidence = {
            "run_id": run_id,
            "strategy": strategy,
            "trade_date": str(review.get("trade_date") or ""),
            "data_version": data_version,
            "code_version": code_version,
            "param_version": param_version,
            "backtest_component": review.get("backtest_component") if isinstance(review.get("backtest_component"), dict) else {},
            "execution_component": review.get("execution_component") if isinstance(review.get("execution_component"), dict) else {},
            "decision_state": review.get("decision_state") if isinstance(review.get("decision_state"), dict) else {},
        }
        cards[strategy] = {
            "alpha_id": strategy,
            "status": "formal_eligible" if review.get("eligible_for_daily_top3") is True else str(review.get("competition_status") or "blocked"),
            "model_card": {
                "strategy": strategy,
                "profile": profile_payload,
                "lineage": evidence,
                "limitations": [
                    "model_card_derived_from_lineage_not_a_production_approval",
                    "production_requires_independent_validation_shadow_execution_and_pre_trade_controls",
                ],
            },
            "hypothesis": hypothesis,
            "rule_hash": _hash_payload({"strategy": strategy, "param_version": param_version}),
            "data_hash": _hash_payload({"strategy": strategy, "data_version": data_version}),
            "code_hash": _hash_payload({"strategy": strategy, "code_version": code_version}),
            "evidence_manifest": str(review.get("artifact_path") or ""),
        }
    return cards


def build_blocked_independent_validation_stub(*, reason: str = "independent_validation_not_performed") -> JsonDict:
    return {
        "validator_name": "",
        "validator_role": "independent_validator",
        "decision": "blocked",
        "conflict_of_interest_attestation": False,
        "reviewed_artifacts": [],
        "blocking_reasons": [str(reason or "independent_validation_not_performed")],
    }


def build_blocked_shadow_execution_stub(*, reason: str = "shadow_execution_evidence_not_available") -> JsonDict:
    return {
        "passed": False,
        "sample_count": 0,
        "artifact": "",
        "blocking_reasons": [str(reason or "shadow_execution_evidence_not_available")],
    }


def build_blocked_pre_trade_controls_stub(*, reason: str = "pre_trade_risk_controls_not_available") -> JsonDict:
    return {
        "passed": False,
        "controls": {},
        "blocking_reasons": [str(reason or "pre_trade_risk_controls_not_available")],
    }


def build_pre_trade_risk_controls_from_recommendation(
    conn: sqlite3.Connection,
    *,
    recommendation: JsonDict,
    portfolio_constraints: Optional[JsonDict] = None,
    output_path: str | Path | None = None,
) -> JsonDict:
    """Build deterministic pre-trade checks for the current Top5 candidate.

    This is still pre-production evidence. It verifies order-level constraints
    against current DB facts, but it does not replace independent validation or
    shadow execution.
    """

    constraints = _merge_constraints(portfolio_constraints)
    top_stocks = [dict(item) for item in recommendation.get("top_stocks") or [] if isinstance(item, dict)]
    required_count = int(constraints.get("top_stock_count") or 5)
    blocking: List[str] = []
    if len(top_stocks) != required_count:
        blocking.append(f"top5_count_mismatch:{len(top_stocks)}/{required_count}")
    notional = float(constraints.get("portfolio_notional") or 0.0)
    max_order_value = float(constraints.get("max_order_value") or 0.0)
    max_single = float(constraints.get("max_single_name_weight") or 0.0)
    max_industry = float(constraints.get("max_industry_weight") or 0.0)
    min_liquidity = float(constraints.get("min_liquidity_amount") or 0.0)
    max_turnover = float(constraints.get("max_turnover") or 0.0)
    max_single_risk_contribution = float(constraints.get("max_single_risk_contribution") or 0.0)
    facts_by_code = {
        str(stock.get("ts_code") or ""): _latest_stock_facts(conn, str(stock.get("ts_code") or ""))
        for stock in top_stocks
        if str(stock.get("ts_code") or "")
    }
    industry_by_code = _industry_by_code_from_facts(facts_by_code)
    series_by_code = _build_return_series_map(conn, top_stocks=top_stocks, constraints=constraints)
    risk_by_code = _build_risk_proxy_map(series_by_code=series_by_code, constraints=constraints)
    plan = _build_top5_weight_plan(
        top_stocks=top_stocks,
        facts_by_code=facts_by_code,
        risk_by_code=risk_by_code,
        series_by_code=series_by_code,
        industry_by_code=industry_by_code,
        constraints=constraints,
        max_single=max_single,
        max_industry=max_industry,
    )
    weight_by_code = plan.get("weights") if isinstance(plan.get("weights"), dict) else {}
    cash_weight = float(plan.get("cash_weight") or 0.0)
    industry_weights: Dict[str, float] = {}
    orders: List[JsonDict] = []
    for stock in top_stocks:
        ts_code = str(stock.get("ts_code") or "")
        facts = facts_by_code.get(ts_code) or _latest_stock_facts(conn, ts_code)
        industry = str(facts.get("industry") or "unknown")
        weight = float(weight_by_code.get(ts_code, 0.0) or 0.0)
        industry_weights[industry] = industry_weights.get(industry, 0.0) + weight
        order_value = notional * weight
        close_price = float(facts.get("close_price") or 0.0)
        amount = float(facts.get("amount") or 0.0)
        pct_chg = float(facts.get("pct_chg") or 0.0)
        order_checks = {
            "single_name_weight_limit": weight <= max_single,
            "liquidity_check": amount >= min_liquidity,
            "price_limit_check": pct_chg < 9.5,
            "suspension_check": close_price > 0.0 and amount > 0.0,
            "order_value_limit": order_value <= max_order_value,
        }
        for key, passed in order_checks.items():
            if passed is not True:
                blocking.append(f"pre_trade_order_check_failed:{ts_code}:{key}")
        orders.append(
            {
                "ts_code": ts_code,
                "side": "buy",
                "target_weight": round(weight, 6),
                "estimated_order_value": round(order_value, 4),
                "latest_facts": {
                    "industry": industry,
                    "close_price": close_price,
                    "amount": amount,
                    "pct_chg": pct_chg,
                    "latest_trade_date": facts.get("latest_trade_date"),
                },
                "checks": order_checks,
            }
        )
    risk_summary = _covariance_risk_contribution_summary(
        weights=weight_by_code,
        series_by_code=series_by_code,
        industry_by_code=industry_by_code,
        facts_by_code=facts_by_code,
        constraints=constraints,
        min_volatility_floor=float(constraints.get("min_volatility_floor") or 0.01),
    )
    risk_share_by_code = risk_summary.get("share") if isinstance(risk_summary.get("share"), dict) else {}
    factor_summary = (
        risk_summary.get("factor_exposure_summary")
        if isinstance(risk_summary.get("factor_exposure_summary"), dict)
        else {}
    )
    size_factor = (
        factor_summary.get("size", {}).get("factor_by_code")
        if isinstance(factor_summary.get("size"), dict)
        and isinstance(factor_summary.get("size", {}).get("factor_by_code"), dict)
        else {}
    )
    liquidity_factor = (
        factor_summary.get("liquidity", {}).get("factor_by_code")
        if isinstance(factor_summary.get("liquidity"), dict)
        and isinstance(factor_summary.get("liquidity", {}).get("factor_by_code"), dict)
        else {}
    )
    for order in orders:
        ts_code = str(order.get("ts_code") or "")
        risk_share = float(risk_share_by_code.get(ts_code, 0.0) or 0.0)
        checks = order.get("checks") if isinstance(order.get("checks"), dict) else {}
        checks["risk_contribution_limit"] = risk_share <= max_single_risk_contribution
        checks["size_factor_exposure_limit"] = bool(
            (factor_summary.get("size") or {}).get("within_limit", True)
            if isinstance(factor_summary.get("size"), dict)
            else True
        )
        checks["liquidity_factor_exposure_limit"] = bool(
            (factor_summary.get("liquidity") or {}).get("within_limit", True)
            if isinstance(factor_summary.get("liquidity"), dict)
            else True
        )
        order["checks"] = checks
        order["risk_contribution_share"] = round(risk_share, 6)
        order["factor_exposures"] = {
            "size": round(float(size_factor.get(ts_code, 0.0) or 0.0), 6),
            "liquidity": round(float(liquidity_factor.get(ts_code, 0.0) or 0.0), 6),
        }
        if checks["risk_contribution_limit"] is not True:
            blocking.append(
                f"pre_trade_order_check_failed:{ts_code}:risk_contribution_limit:{round(risk_share, 6)}/{max_single_risk_contribution}"
            )
    for factor_name in ("size", "liquidity"):
        factor_payload = factor_summary.get(factor_name) if isinstance(factor_summary.get(factor_name), dict) else {}
        if factor_payload and factor_payload.get("within_limit") is not True:
            blocking.append(
                "pre_trade_factor_exposure_above_cap:"
                f"{factor_name}:{round(float(factor_payload.get('portfolio_exposure') or 0.0), 6)}/{round(float(factor_payload.get('cap') or 0.0), 6)}"
            )
    industry_check = all(value <= max_industry for value in industry_weights.values())
    allocated_weight = sum(float(item.get("target_weight") or 0.0) for item in orders)
    turnover_check = allocated_weight <= max_turnover
    if cash_weight > 0.01:
        blocking.append(f"pre_trade_weight_unallocated:{round(cash_weight, 6)}")
    if not industry_check:
        for industry, industry_weight in industry_weights.items():
            if industry_weight > max_industry:
                blocking.append(f"pre_trade_industry_weight_above_cap:{industry}:{round(industry_weight, 6)}/{max_industry}")
    if not turnover_check:
        blocking.append(f"pre_trade_turnover_above_budget:{round(sum(float(item.get('target_weight') or 0.0) for item in orders), 6)}/{max_turnover}")
    controls = {
        "single_name_weight_limit": all((item.get("checks") or {}).get("single_name_weight_limit") is True for item in orders),
        "industry_weight_limit": industry_check,
        "liquidity_check": all((item.get("checks") or {}).get("liquidity_check") is True for item in orders),
        "price_limit_check": all((item.get("checks") or {}).get("price_limit_check") is True for item in orders),
        "suspension_check": all((item.get("checks") or {}).get("suspension_check") is True for item in orders),
        "turnover_budget": turnover_check,
        "order_value_limit": all((item.get("checks") or {}).get("order_value_limit") is True for item in orders),
        "risk_contribution_limit": all((item.get("checks") or {}).get("risk_contribution_limit") is True for item in orders),
        "size_factor_exposure_limit": all((item.get("checks") or {}).get("size_factor_exposure_limit") is True for item in orders),
        "liquidity_factor_exposure_limit": all((item.get("checks") or {}).get("liquidity_factor_exposure_limit") is True for item in orders),
    }
    payload: JsonDict = {
        "artifact_version": "pre_trade_risk_controls.v1",
        "passed": not blocking and all(value is True for value in controls.values()),
        "blocking_reasons": blocking,
        "controls": controls,
        "portfolio_constraints": constraints,
        "orders": orders,
        "industry_weights": {key: round(value, 6) for key, value in sorted(industry_weights.items())},
        "weight_plan": {
            "method": str(plan.get("method") or "multifactor_hard_constraint_weighting_v4"),
            "allocated_weight": round(allocated_weight, 6),
            "cash_weight": round(cash_weight, 6),
            **(
                {
                    "industry_neutral_source": str(plan.get("industry_neutral_source") or ""),
                    "industry_neutral_tolerance": round(float(plan.get("industry_neutral_tolerance") or 0.0), 6),
                    "industry_neutral_targets": {
                        key: round(float(value), 6)
                        for key, value in sorted((plan.get("industry_neutral_targets") or {}).items())
                    },
                    "industry_neutral_deviation": {
                        key: round(float(value), 6)
                        for key, value in sorted((plan.get("industry_neutral_deviation") or {}).items())
                    },
                    "industry_neutral_within_tolerance": bool(plan.get("industry_neutral_within_tolerance") is True),
                }
                if isinstance(plan.get("industry_neutral_targets"), dict)
                else {}
            ),
        },
        "risk_budget": {
            "max_single_risk_contribution": max_single_risk_contribution,
            "risk_proxy_by_code": {code: round(float(value), 6) for code, value in sorted(risk_by_code.items())},
            "risk_contribution_share_by_code": {
                code: round(float(value), 6) for code, value in sorted(risk_share_by_code.items())
            },
            "risk_contribution_model": str(risk_summary.get("covariance_model") or ""),
            "portfolio_variance": round(float(risk_summary.get("portfolio_variance") or 0.0), 8),
            "base_shrinkage": round(float(risk_summary.get("base_shrinkage") or 0.0), 6),
            "shrinkage_intensity": round(float(risk_summary.get("shrinkage_intensity") or 0.0), 6),
            "same_industry_correlation_scale": round(float(risk_summary.get("same_industry_correlation_scale") or 0.0), 6),
            "cross_industry_correlation_scale": round(float(risk_summary.get("cross_industry_correlation_scale") or 0.0), 6),
            "factor_exposure_summary": {
                key: {
                    "portfolio_exposure": round(float((value or {}).get("portfolio_exposure") or 0.0), 6),
                    "cap": round(float((value or {}).get("cap") or 0.0), 6),
                    "within_limit": bool((value or {}).get("within_limit", True)),
                }
                for key, value in factor_summary.items()
                if isinstance(value, dict)
            },
        },
        "hard_boundaries": [
            "pre_trade_controls_do_not_replace_shadow_execution",
            "pre_trade_controls_do_not_replace_independent_validation",
            "pre_trade_controls_do_not_create_production_approval",
        ],
    }
    if output_path is not None:
        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        payload["artifact"] = str(path)
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return payload


def _default_strategy_hypothesis(*, strategy: str, review: JsonDict) -> str:
    return (
        f"{strategy} competes only on pre-existing signal lineage, credible backtest gate, "
        "and fixed Top5 consensus contribution; this statement is not production approval."
    )


def _merge_constraints(constraints: Optional[JsonDict]) -> JsonDict:
    merged = dict(DEFAULT_CONSTRAINTS)
    if isinstance(constraints, dict):
        for key, value in constraints.items():
            if str(key or "").strip():
                merged[key] = value
    return merged


def _bounded_nonnegative(value: Any, fallback: float = 0.0) -> float:
    try:
        parsed = float(value or fallback)
    except (TypeError, ValueError):
        parsed = float(fallback)
    return max(0.0, parsed)


def _normalize_score_map(values: Dict[str, float]) -> Dict[str, float]:
    if not values:
        return {}
    mean = sum(values.values()) / float(len(values))
    variance = sum((item - mean) ** 2 for item in values.values()) / float(len(values))
    std = math.sqrt(max(variance, 1e-12))
    return {key: (value - mean) / std for key, value in values.items()}


def _factor_weight_bias_by_code(
    *,
    facts_by_code: Dict[str, JsonDict],
    risk_by_code: Dict[str, float],
    constraints: JsonDict,
) -> Dict[str, float]:
    value_raw = {
        code: -math.log(max(1.0, float((facts_by_code.get(code) or {}).get("circ_mv") or 1.0)))
        for code in facts_by_code
    }
    momentum_raw = {
        code: float((facts_by_code.get(code) or {}).get("pct_chg") or 0.0) / 100.0
        for code in facts_by_code
    }
    quality_raw = {code: -float(risk_by_code.get(code, 0.0) or 0.0) for code in facts_by_code}
    value_z = _normalize_score_map(value_raw)
    momentum_z = _normalize_score_map(momentum_raw)
    quality_z = _normalize_score_map(quality_raw)
    value_w = float(constraints.get("factor_value_weight") or 0.0)
    momentum_w = float(constraints.get("factor_momentum_weight") or 0.0)
    quality_w = float(constraints.get("factor_quality_weight") or 0.0)
    combined = {
        code: value_w * value_z.get(code, 0.0)
        + momentum_w * momentum_z.get(code, 0.0)
        + quality_w * quality_z.get(code, 0.0)
        for code in facts_by_code
    }
    # Map arbitrary score scale to positive multiplicative bias around 1.0.
    return {code: max(0.25, min(2.5, 1.0 + combined_score)) for code, combined_score in combined.items()}


def _base_weight_scores(
    top_stocks: List[JsonDict],
    facts_by_code: Dict[str, JsonDict],
    risk_by_code: Dict[str, float],
    constraints: JsonDict,
) -> Dict[str, float]:
    # Multi-factor baseline: consensus score with liquidity and risk-adjusted factor bias.
    raw: Dict[str, float] = {}
    factor_bias = _factor_weight_bias_by_code(
        facts_by_code=facts_by_code,
        risk_by_code=risk_by_code,
        constraints=constraints,
    )
    for stock in top_stocks:
        ts_code = str(stock.get("ts_code") or "")
        if not ts_code:
            continue
        score = _bounded_nonnegative(stock.get("final_stock_score"), fallback=0.0)
        amount = _bounded_nonnegative((facts_by_code.get(ts_code) or {}).get("amount"), fallback=0.0)
        liquidity_boost = max(1.0, amount / 1_000_000.0)
        risk_proxy = max(1e-6, float(risk_by_code.get(ts_code, 0.01) or 0.01))
        bias = float(factor_bias.get(ts_code, 1.0) or 1.0)
        raw[ts_code] = max(1e-9, (score * liquidity_boost * bias) / risk_proxy)
    if not raw:
        return {}
    total = sum(raw.values())
    if total <= 0:
        equal = 1.0 / float(len(raw))
        return {code: equal for code in raw}
    return {code: value / total for code, value in raw.items()}


def _redistribute_with_single_cap(
    weights: Dict[str, float],
    *,
    max_single: float,
) -> Dict[str, float]:
    if not weights:
        return {}
    out = {code: max(0.0, float(weight)) for code, weight in weights.items()}
    cap = max(0.0, float(max_single))
    for _ in range(MAX_WEIGHT_OPTIMIZATION_ITERATIONS):
        overflow = 0.0
        capped: set[str] = set()
        for code, weight in out.items():
            if weight > cap:
                overflow += weight - cap
                out[code] = cap
                capped.add(code)
        if overflow <= 1e-12:
            break
        uncapped = [code for code in out if code not in capped and out[code] < cap - 1e-12]
        if not uncapped:
            break
        total_uncapped = sum(out[code] for code in uncapped)
        if total_uncapped <= 1e-12:
            equal_add = overflow / float(len(uncapped))
            for code in uncapped:
                out[code] += equal_add
        else:
            for code in uncapped:
                out[code] += overflow * (out[code] / total_uncapped)
    return out


def _apply_industry_cap(
    weights: Dict[str, float],
    *,
    industry_by_code: Dict[str, str],
    max_industry: float,
    max_single: float,
) -> Dict[str, float]:
    if not weights:
        return {}
    cap = max(0.0, float(max_industry))
    out = dict(weights)
    for _ in range(MAX_WEIGHT_OPTIMIZATION_ITERATIONS):
        industry_totals: Dict[str, float] = {}
        for code, weight in out.items():
            industry = str(industry_by_code.get(code) or "unknown")
            industry_totals[industry] = industry_totals.get(industry, 0.0) + max(0.0, float(weight))
        violating = {industry: total for industry, total in industry_totals.items() if total > cap + 1e-12}
        if not violating:
            break
        released = 0.0
        for industry, total in violating.items():
            ratio = cap / total if total > 0 else 0.0
            for code, weight in list(out.items()):
                if str(industry_by_code.get(code) or "unknown") != industry:
                    continue
                new_weight = weight * ratio
                released += max(0.0, weight - new_weight)
                out[code] = new_weight
        if released <= 1e-12:
            break
        receiver_codes = [
            code
            for code in out
            if out[code] < max_single - 1e-12
            and industry_totals.get(str(industry_by_code.get(code) or "unknown"), 0.0) < cap - 1e-12
        ]
        if not receiver_codes:
            break
        receiver_total = sum(out[code] for code in receiver_codes)
        if receiver_total <= 1e-12:
            equal_add = released / float(len(receiver_codes))
            for code in receiver_codes:
                out[code] += equal_add
        else:
            for code in receiver_codes:
                out[code] += released * (out[code] / receiver_total)
        out = _redistribute_with_single_cap(out, max_single=max_single)
    return out


def _normalize_weight_plan(weights: Dict[str, float]) -> Dict[str, float]:
    if not weights:
        return {}
    nonnegative = {code: max(0.0, float(weight)) for code, weight in weights.items()}
    total = sum(nonnegative.values())
    if total <= 1e-12:
        equal = 1.0 / float(len(nonnegative))
        return {code: equal for code in nonnegative}
    return {code: weight / total for code, weight in nonnegative.items()}


def _build_top5_weight_plan(
    *,
    top_stocks: List[JsonDict],
    facts_by_code: Dict[str, JsonDict],
    risk_by_code: Dict[str, float],
    series_by_code: Dict[str, List[float]],
    industry_by_code: Dict[str, str],
    constraints: JsonDict,
    max_single: float,
    max_industry: float,
) -> Dict[str, Any]:
    qp_solution = _solve_top5_weight_plan_qp(
        top_stocks=top_stocks,
        facts_by_code=facts_by_code,
        risk_by_code=risk_by_code,
        series_by_code=series_by_code,
        industry_by_code=industry_by_code,
        constraints=constraints,
        max_single=max_single,
        max_industry=max_industry,
    )
    if isinstance(qp_solution, dict) and qp_solution.get("weights"):
        return qp_solution
    base = _base_weight_scores(top_stocks, facts_by_code, risk_by_code, constraints)
    if not base:
        return {"weights": {}, "cash_weight": 1.0, "method": "multifactor_hard_constraint_weighting_v4"}
    fallback_industry_by_code = {
        code: str(industry_by_code.get(code) or (facts_by_code.get(code) or {}).get("industry") or "unknown")
        for code in base
    }
    step_single = _redistribute_with_single_cap(base, max_single=max_single)
    step_industry = _apply_industry_cap(
        step_single,
        industry_by_code=fallback_industry_by_code,
        max_industry=max_industry,
        max_single=max_single,
    )
    normalized = _normalize_weight_plan(step_industry)
    projection_iterations = int(constraints.get("factor_exposure_projection_iterations") or 0)
    projection_iterations = max(0, min(32, projection_iterations))
    projected = dict(normalized)
    for _ in range(projection_iterations):
        summary = _factor_exposure_summary(
            weights=projected,
            facts_by_code=facts_by_code,
            constraints=constraints,
        )
        size_payload = summary.get("size") if isinstance(summary.get("size"), dict) else {}
        liquidity_payload = summary.get("liquidity") if isinstance(summary.get("liquidity"), dict) else {}
        if size_payload.get("within_limit") is True and liquidity_payload.get("within_limit") is True:
            break
        for factor_name in ("size", "liquidity"):
            factor_payload = summary.get(factor_name) if isinstance(summary.get(factor_name), dict) else {}
            if not factor_payload or factor_payload.get("within_limit") is True:
                continue
            factor_by_code = (
                factor_payload.get("factor_by_code")
                if isinstance(factor_payload.get("factor_by_code"), dict)
                else {}
            )
            exposure = float(factor_payload.get("portfolio_exposure") or 0.0)
            if abs(exposure) <= 1e-12 or not factor_by_code:
                continue
            positive_side = exposure > 0
            tighten = [code for code in projected if (float(factor_by_code.get(code, 0.0) or 0.0) > 0) == positive_side]
            loosen = [code for code in projected if code not in tighten]
            if not tighten or not loosen:
                continue
            tighten_total = sum(projected.get(code, 0.0) for code in tighten)
            loosen_total = sum(projected.get(code, 0.0) for code in loosen)
            if tighten_total <= 1e-12 or loosen_total <= 1e-12:
                continue
            transfer = min(0.1 * tighten_total, abs(exposure))
            for code in tighten:
                projected[code] = max(0.0, projected[code] - transfer * (projected[code] / tighten_total))
            for code in loosen:
                projected[code] = max(0.0, projected[code] + transfer * (projected[code] / loosen_total))
        projected = _normalize_weight_plan(projected)
        projected = _redistribute_with_single_cap(projected, max_single=max_single)
        projected = _apply_industry_cap(
            projected,
            industry_by_code=fallback_industry_by_code,
            max_industry=max_industry,
            max_single=max_single,
        )
        projected = _normalize_weight_plan(projected)
    normalized = projected
    cash_weight = max(0.0, 1.0 - sum(normalized.values()))
    return {
        "weights": normalized,
        "cash_weight": cash_weight,
        "method": "multifactor_hard_constraint_weighting_v4",
    }


def _estimate_cost_bps(
    *,
    order_value: float,
    amount: float,
    base_bps: float,
    max_bps: float,
) -> float:
    # Capacity-aware impact proxy: larger participation vs amount gets higher expected cost.
    if amount <= 0:
        return max_bps
    participation = max(0.0, min(1.0, float(order_value) / float(amount)))
    impact = 25.0 * participation
    return min(max_bps, max(0.0, base_bps + impact))


def _estimate_volatility_proxy(
    values: List[float],
    *,
    min_floor: float,
) -> float:
    if not values:
        return max(min_floor, 0.02)
    if len(values) == 1:
        return max(min_floor, abs(values[0]))
    mean = sum(values) / float(len(values))
    variance = sum((item - mean) ** 2 for item in values) / float(len(values))
    return max(min_floor, math.sqrt(max(0.0, variance)))


def _load_return_series(
    conn: sqlite3.Connection,
    *,
    ts_code: str,
    lookback_days: int,
) -> List[float]:
    rows = conn.execute(
        """
        SELECT pct_chg
        FROM daily_trading_data
        WHERE ts_code = ?
        ORDER BY trade_date DESC
        LIMIT ?
        """,
        (str(ts_code or ""), max(1, int(lookback_days or 1))),
    ).fetchall()
    series = [float(row[0] or 0.0) / 100.0 for row in rows]
    series.reverse()
    return series


def _build_return_series_map(
    conn: sqlite3.Connection,
    *,
    top_stocks: List[JsonDict],
    constraints: JsonDict,
) -> Dict[str, List[float]]:
    lookback_days = int(constraints.get("risk_lookback_days") or 20)
    out: Dict[str, List[float]] = {}
    for stock in top_stocks:
        ts_code = str(stock.get("ts_code") or "")
        if not ts_code:
            continue
        out[ts_code] = _load_return_series(conn, ts_code=ts_code, lookback_days=lookback_days)
    return out


def _industry_by_code_from_facts(facts_by_code: Dict[str, JsonDict]) -> Dict[str, str]:
    return {code: str((facts_by_code.get(code) or {}).get("industry") or "unknown") for code in facts_by_code}


def _zscore(values: Dict[str, float]) -> Dict[str, float]:
    if not values:
        return {}
    series = list(values.values())
    mean = sum(series) / float(len(series))
    variance = sum((item - mean) ** 2 for item in series) / float(len(series))
    std = math.sqrt(max(variance, 1e-12))
    return {key: (value - mean) / std for key, value in values.items()}


def _factor_vectors_by_code(
    *,
    codes: List[str],
    facts_by_code: Dict[str, JsonDict],
    risk_by_code: Optional[Dict[str, float]],
) -> Dict[str, Dict[str, float]]:
    by_code = {code: facts_by_code.get(code) or {} for code in codes}
    size_raw = {
        code: -math.log(max(1.0, float((by_code.get(code) or {}).get("circ_mv") or 1.0)))
        for code in codes
    }
    liquidity_raw = {
        code: math.log(max(1.0, float((by_code.get(code) or {}).get("amount") or 1.0)))
        for code in codes
    }
    value_raw = {
        code: -math.log(max(1.0, float((by_code.get(code) or {}).get("total_mv") or 1.0)))
        for code in codes
    }
    momentum_raw = {
        code: float((by_code.get(code) or {}).get("pct_chg") or 0.0) / 100.0
        for code in codes
    }
    quality_raw = {
        code: -float((risk_by_code or {}).get(code, 0.0) or 0.0)
        for code in codes
    }
    return {
        "size": _zscore(size_raw),
        "liquidity": _zscore(liquidity_raw),
        "value": _zscore(value_raw),
        "momentum": _zscore(momentum_raw),
        "quality": _zscore(quality_raw),
    }


def _factor_cap_by_name(constraints: JsonDict) -> Dict[str, float]:
    return {
        "size": max(0.0, float(constraints.get("max_abs_size_exposure") or 0.0)),
        "liquidity": max(0.0, float(constraints.get("max_abs_liquidity_exposure") or 0.0)),
        "value": max(0.0, float(constraints.get("max_abs_value_exposure") or 0.0)),
        "momentum": max(0.0, float(constraints.get("max_abs_momentum_exposure") or 0.0)),
        "quality": max(0.0, float(constraints.get("max_abs_quality_exposure") or 0.0)),
    }


def _normalize_industry_weight_map(values: Dict[str, float]) -> Dict[str, float]:
    cleaned: Dict[str, float] = {}
    for key, value in (values or {}).items():
        industry = str(key or "").strip()
        if not industry:
            continue
        cleaned[industry] = max(0.0, float(value or 0.0))
    total = sum(cleaned.values())
    if total <= 1e-12:
        return {}
    return {key: val / total for key, val in cleaned.items()}


def _resolve_industry_neutral_targets(
    *,
    industries: List[str],
    constraints: JsonDict,
    fallback_targets: Dict[str, float],
) -> tuple[Dict[str, float], str]:
    raw_external = constraints.get("benchmark_industry_weights")
    external = (
        _normalize_industry_weight_map(raw_external)
        if isinstance(raw_external, dict)
        else {}
    )
    if external:
        target = {
            industry: float(external.get(industry, fallback_targets.get(industry, 0.0)) or 0.0)
            for industry in industries
        }
        normalized_target = _normalize_industry_weight_map(target)
        return normalized_target, str(constraints.get("benchmark_industry_source") or "external_constraints")
    normalized_fallback = _normalize_industry_weight_map(fallback_targets)
    return normalized_fallback, "candidate_pool_proxy"


def _normalize_benchmark_constituents(values: Any) -> List[JsonDict]:
    out: List[JsonDict] = []
    for item in values or []:
        if isinstance(item, dict):
            ts_code = str(item.get("ts_code") or "").strip()
            if not ts_code:
                continue
            out.append(
                {
                    "ts_code": ts_code,
                    "industry": str(item.get("industry") or "unknown"),
                    "weight": float(item.get("weight") or 0.0),
                }
            )
            continue
        ts_code = str(item or "").strip()
        if ts_code:
            out.append({"ts_code": ts_code, "industry": "unknown", "weight": 0.0})
    # deterministic ordering for stable hashes
    out.sort(key=lambda row: str(row.get("ts_code") or ""))
    return out


def _build_benchmark_contract(
    *,
    trade_date: str,
    top5_rows: List[JsonDict],
    risk_summary: JsonDict,
    constraints: JsonDict,
    provided_contract: Optional[JsonDict] = None,
) -> JsonDict:
    if isinstance(provided_contract, dict) and provided_contract:
        # Re-normalize and re-hash to guarantee canonical contract integrity.
        return build_benchmark_industry_contract(
            benchmark_trade_date=str(provided_contract.get("benchmark_trade_date") or trade_date or ""),
            source=str(provided_contract.get("source") or constraints.get("benchmark_industry_source") or "external_index_contract"),
            provider_batch_id=str(provided_contract.get("provider_batch_id") or ""),
            provider_snapshot_id=str(provided_contract.get("provider_snapshot_id") or ""),
            approved_by=str(provided_contract.get("approved_by") or ""),
            approved_at=str(provided_contract.get("approved_at") or ""),
            approval_signature=str(provided_contract.get("approval_signature") or ""),
            approval_signature_algo=str(provided_contract.get("approval_signature_algo") or "sha256_secret_v1"),
            approval_key_id=str(provided_contract.get("approval_key_id") or "benchmark_default_key"),
            provider_receipt_hash=str(provided_contract.get("provider_receipt_hash") or ""),
            industry_weights=(
                provided_contract.get("industry_weights")
                if isinstance(provided_contract.get("industry_weights"), dict)
                else {}
            ),
            constituents=(
                provided_contract.get("constituents")
                if isinstance(provided_contract.get("constituents"), list)
                else []
            ),
        )
    weight_plan = risk_summary.get("weight_plan") if isinstance(risk_summary.get("weight_plan"), dict) else {}
    source = str(weight_plan.get("industry_neutral_source") or constraints.get("benchmark_industry_source") or "candidate_pool_proxy")
    benchmark_trade_date = str(constraints.get("benchmark_trade_date") or trade_date or "")
    raw_weights = (
        constraints.get("benchmark_industry_weights")
        if isinstance(constraints.get("benchmark_industry_weights"), dict)
        else weight_plan.get("industry_neutral_targets")
    )
    industry_weights = _normalize_industry_weight_map(raw_weights if isinstance(raw_weights, dict) else {})
    constituents = _normalize_benchmark_constituents(constraints.get("benchmark_constituents"))
    if not constituents:
        derived: List[JsonDict] = []
        for row in top5_rows:
            if not isinstance(row, dict):
                continue
            ts_code = str(row.get("ts_code") or "").strip()
            if not ts_code:
                continue
            risk = row.get("risk") if isinstance(row.get("risk"), dict) else {}
            derived.append(
                {
                    "ts_code": ts_code,
                    "industry": str(risk.get("industry") or "unknown"),
                    "weight": float(row.get("weight") or 0.0),
                }
            )
        constituents = _normalize_benchmark_constituents(derived)
    return build_benchmark_industry_contract(
        benchmark_trade_date=benchmark_trade_date,
        source=source,
        provider_batch_id=str(constraints.get("benchmark_provider_batch_id") or "derived_batch"),
        provider_snapshot_id=str(constraints.get("benchmark_provider_snapshot_id") or "derived_snapshot"),
        approved_by=str(constraints.get("benchmark_approved_by") or "system_auto_approval"),
        approved_at=str(constraints.get("benchmark_approved_at") or _now_text()),
        approval_signature=str(constraints.get("benchmark_approval_signature") or f"auto_sig:{benchmark_trade_date}"),
        approval_signature_algo=str(constraints.get("benchmark_approval_signature_algo") or "sha256_secret_v1"),
        approval_key_id=str(constraints.get("benchmark_approval_key_id") or "benchmark_default_key"),
        provider_receipt_hash=str(constraints.get("benchmark_provider_receipt_hash") or ""),
        signing_secret=str(constraints.get("benchmark_signing_secret") or ""),
        industry_weights=industry_weights,
        constituents=constituents,
    )


def _factor_exposure_summary(
    *,
    weights: Dict[str, float],
    facts_by_code: Dict[str, JsonDict],
    constraints: JsonDict,
    risk_by_code: Optional[Dict[str, float]] = None,
) -> Dict[str, JsonDict]:
    if not weights:
        return {}
    codes = list(weights.keys())
    factor_by_name = _factor_vectors_by_code(
        codes=codes,
        facts_by_code=facts_by_code,
        risk_by_code=risk_by_code,
    )
    cap_by_name = _factor_cap_by_name(constraints)
    summary: Dict[str, JsonDict] = {}
    for factor_name, factor_by_code in factor_by_name.items():
        cap = float(cap_by_name.get(factor_name, 0.0) or 0.0)
        exposure = sum(
            float(weights.get(code, 0.0) or 0.0) * float(factor_by_code.get(code, 0.0) or 0.0)
            for code in codes
        )
        summary[factor_name] = {
            "portfolio_exposure": exposure,
            "cap": cap,
            "within_limit": abs(exposure) <= cap if cap > 0 else True,
            "factor_by_code": factor_by_code,
        }
    return summary


def _build_covariance_payload(
    *,
    codes: List[str],
    series_by_code: Dict[str, List[float]],
    industry_by_code: Dict[str, str],
    constraints: JsonDict,
    min_volatility_floor: float,
) -> Dict[str, Any]:
    if not codes:
        return {
            "codes": [],
            "matrix": np.zeros((0, 0)),
            "covariance_model": "adaptive_shrunk_industry_block_covariance_v3",
            "portfolio_variance": 0.0,
            "base_shrinkage": max(0.0, min(1.0, float(constraints.get("risk_covariance_shrinkage") or 0.0))),
            "shrinkage_intensity": max(0.0, min(1.0, float(constraints.get("risk_covariance_shrinkage") or 0.0))),
            "same_industry_correlation_scale": float(constraints.get("same_industry_correlation_scale") or 1.0),
            "cross_industry_correlation_scale": float(constraints.get("cross_industry_correlation_scale") or 1.0),
        }
    min_len = min((len(series_by_code.get(code) or []) for code in codes), default=0)
    min_var = max(1e-8, float(min_volatility_floor) ** 2)
    base_shrinkage = max(0.0, min(1.0, float(constraints.get("risk_covariance_shrinkage") or 0.0)))
    same_industry_scale = max(0.0, float(constraints.get("same_industry_correlation_scale") or 1.0))
    cross_industry_scale = max(0.0, float(constraints.get("cross_industry_correlation_scale") or 1.0))
    if min_len <= 0:
        diag = np.diag([min_var for _ in codes])
        return {
            "codes": codes,
            "matrix": diag,
            "covariance_model": "adaptive_shrunk_industry_block_covariance_v3",
            "portfolio_variance": 0.0,
            "base_shrinkage": base_shrinkage,
            "shrinkage_intensity": base_shrinkage,
            "same_industry_correlation_scale": same_industry_scale,
            "cross_industry_correlation_scale": cross_industry_scale,
        }
    aligned = {
        code: list((series_by_code.get(code) or [])[-min_len:])
        for code in codes
    }
    shrinkage = _adaptive_shrinkage_intensity(
        base_shrinkage=base_shrinkage,
        aligned=aligned,
        min_len=min_len,
        lookback_days=int(constraints.get("risk_lookback_days") or 20),
        min_cap=max(0.0, min(1.0, float(constraints.get("adaptive_shrinkage_min") or 0.0))),
        max_cap=max(0.0, min(1.0, float(constraints.get("adaptive_shrinkage_max") or 1.0))),
    )
    means = {code: (sum(values) / float(len(values)) if values else 0.0) for code, values in aligned.items()}
    matrix = np.zeros((len(codes), len(codes)))
    for i, left in enumerate(codes):
        for j, right in enumerate(codes):
            left_values = aligned[left]
            right_values = aligned[right]
            if not left_values or not right_values:
                cov = 0.0
            else:
                cov = sum(
                    (left_values[idx] - means[left]) * (right_values[idx] - means[right])
                    for idx in range(min_len)
                ) / float(min_len)
            if i == j:
                matrix[i, j] = max(min_var, cov)
            else:
                same_industry = str(industry_by_code.get(left) or "unknown") == str(industry_by_code.get(right) or "unknown")
                scaled_cov = cov * (same_industry_scale if same_industry else cross_industry_scale)
                matrix[i, j] = (1.0 - shrinkage) * scaled_cov
    avg_variance = float(np.trace(matrix) / float(len(codes)))
    for i in range(len(codes)):
        matrix[i, i] = (1.0 - shrinkage) * matrix[i, i] + shrinkage * max(min_var, avg_variance)
    # Numerical PSD projection for stable QP solving.
    symmetric = 0.5 * (matrix + matrix.T)
    eigvals, eigvecs = np.linalg.eigh(symmetric)
    eigvals = np.maximum(eigvals, 1e-10)
    psd = eigvecs @ np.diag(eigvals) @ eigvecs.T
    return {
        "codes": codes,
        "matrix": psd,
        "covariance_model": "adaptive_shrunk_industry_block_covariance_v3",
        "portfolio_variance": 0.0,
        "base_shrinkage": base_shrinkage,
        "shrinkage_intensity": shrinkage,
        "same_industry_correlation_scale": same_industry_scale,
        "cross_industry_correlation_scale": cross_industry_scale,
    }


def _solve_top5_weight_plan_qp(
    *,
    top_stocks: List[JsonDict],
    facts_by_code: Dict[str, JsonDict],
    risk_by_code: Dict[str, float],
    series_by_code: Dict[str, List[float]],
    industry_by_code: Dict[str, str],
    constraints: JsonDict,
    max_single: float,
    max_industry: float,
) -> Optional[Dict[str, Any]]:
    if cp is None or constraints.get("qp_enable", True) is not True:
        return None
    codes = [str(stock.get("ts_code") or "") for stock in top_stocks if str(stock.get("ts_code") or "")]
    if not codes:
        return None
    score_by_code = {
        str(stock.get("ts_code") or ""): _bounded_nonnegative(stock.get("final_stock_score"), fallback=0.0)
        for stock in top_stocks
        if str(stock.get("ts_code") or "")
    }
    alpha = _normalize_score_map(score_by_code)
    factor_bias = _factor_weight_bias_by_code(
        facts_by_code=facts_by_code,
        risk_by_code=risk_by_code,
        constraints=constraints,
    )
    alpha_vec = np.array([float(alpha.get(code, 0.0) or 0.0) * float(factor_bias.get(code, 1.0) or 1.0) for code in codes], dtype=float)
    covariance = _build_covariance_payload(
        codes=codes,
        series_by_code=series_by_code,
        industry_by_code=industry_by_code,
        constraints=constraints,
        min_volatility_floor=float(constraints.get("min_volatility_floor") or 0.01),
    )
    sigma = covariance.get("matrix")
    if not isinstance(sigma, np.ndarray) or sigma.shape != (len(codes), len(codes)):
        return None
    weights = cp.Variable(len(codes))
    objective_parts = {
        "alpha_scale": max(0.0, float(constraints.get("qp_alpha_scale") or 1.0)),
        "risk_aversion": max(0.0, float(constraints.get("qp_risk_aversion") or 0.0)),
        "cost_aversion": max(0.0, float(constraints.get("qp_cost_aversion") or 0.0)),
    }
    max_cost = max(0.0, float(constraints.get("max_estimated_cost_bps") or 0.0))
    base_bps = max(0.0, float(constraints.get("predeclared_cost_bps") or 0.0))
    notional = max(0.0, float(constraints.get("portfolio_notional") or 0.0))
    impact_coef = np.array(
        [
            25.0 * (notional / max(1.0, float((facts_by_code.get(code) or {}).get("amount") or 1.0)))
            for code in codes
        ],
        dtype=float,
    )
    risk_expr = cp.quad_form(weights, sigma)
    cost_expr = base_bps * cp.sum(weights) + cp.sum(cp.multiply(impact_coef, cp.square(weights)))
    utility_expr = (
        objective_parts["alpha_scale"] * (alpha_vec @ weights)
        - objective_parts["risk_aversion"] * risk_expr
        - objective_parts["cost_aversion"] * (cost_expr / 100.0)
    )
    constraints_list: List[Any] = [
        weights >= 0.0,
        cp.sum(weights) == 1.0,
        weights <= max(0.0, float(max_single)),
        cost_expr <= max_cost,
    ]
    max_variance = max(0.0, float(constraints.get("max_portfolio_variance") or 0.0))
    if max_variance > 0.0:
        constraints_list.append(risk_expr <= max_variance)
    group_indices: Dict[str, List[int]] = {}
    for idx, code in enumerate(codes):
        group_indices.setdefault(str(industry_by_code.get(code) or "unknown"), []).append(idx)
    base_weights = _base_weight_scores(top_stocks, facts_by_code, risk_by_code, constraints)
    base_group_weight = {
        industry: sum(float(base_weights.get(codes[idx], 0.0) or 0.0) for idx in indices)
        for industry, indices in group_indices.items()
    }
    neutral_targets, neutral_source = _resolve_industry_neutral_targets(
        industries=list(group_indices.keys()),
        constraints=constraints,
        fallback_targets=base_group_weight,
    )
    neutral_tol = max(0.0, float(constraints.get("industry_neutral_tolerance") or 0.0))
    for industry, indices in group_indices.items():
        industry_expr = cp.sum(weights[indices])
        constraints_list.append(industry_expr <= max(0.0, float(max_industry)))
        target = float(neutral_targets.get(industry, base_group_weight.get(industry, 0.0)) or 0.0)
        constraints_list.append(industry_expr >= max(0.0, target - neutral_tol))
        constraints_list.append(industry_expr <= min(max(0.0, float(max_industry)), target + neutral_tol))
    factor_vectors = _factor_vectors_by_code(
        codes=codes,
        facts_by_code=facts_by_code,
        risk_by_code=risk_by_code,
    )
    factor_caps = _factor_cap_by_name(constraints)
    for factor_name, vector_by_code in factor_vectors.items():
        cap = float(factor_caps.get(factor_name, 0.0) or 0.0)
        if cap <= 0:
            continue
        factor_vector = np.array([float(vector_by_code.get(code, 0.0) or 0.0) for code in codes], dtype=float)
        exposure_expr = factor_vector @ weights
        constraints_list.append(exposure_expr <= cap)
        constraints_list.append(exposure_expr >= -cap)
    problem = cp.Problem(cp.Maximize(utility_expr), constraints_list)
    try:
        solver_name = str(constraints.get("qp_solver") or "OSQP").upper()
        selected_solver = getattr(cp, solver_name, cp.OSQP)
        problem.solve(solver=selected_solver, verbose=False)
    except Exception:
        return None
    if problem.status not in {cp.OPTIMAL, cp.OPTIMAL_INACCURATE}:
        return None
    solved = [max(0.0, float(value or 0.0)) for value in list(weights.value)]
    solved_total = sum(solved)
    if solved_total <= 1e-12:
        return None
    normalized = {code: solved[idx] / solved_total for idx, code in enumerate(codes)}
    cash_weight = max(0.0, 1.0 - sum(normalized.values()))
    solved_industry_weight = {
        industry: sum(normalized.get(codes[idx], 0.0) for idx in indices)
        for industry, indices in group_indices.items()
    }
    industry_deviation = {
        industry: float(solved_industry_weight.get(industry, 0.0) - neutral_targets.get(industry, 0.0))
        for industry in group_indices
    }
    return {
        "weights": normalized,
        "cash_weight": cash_weight,
        "method": "qp_cvxpy_risk_cost_factor_neutral_v1",
        "qp_solver_status": str(problem.status),
        "qp_solver_name": str(constraints.get("qp_solver") or "OSQP"),
        "objective_value": float(problem.value or 0.0),
        "industry_neutral_source": neutral_source,
        "industry_neutral_tolerance": neutral_tol,
        "industry_neutral_targets": {key: float(val) for key, val in sorted(neutral_targets.items())},
        "industry_neutral_deviation": {key: float(val) for key, val in sorted(industry_deviation.items())},
        "industry_neutral_within_tolerance": all(abs(float(val)) <= neutral_tol + 1e-8 for val in industry_deviation.values()),
    }


def _adaptive_shrinkage_intensity(
    *,
    base_shrinkage: float,
    aligned: Dict[str, List[float]],
    min_len: int,
    lookback_days: int,
    min_cap: float,
    max_cap: float,
) -> float:
    if not aligned or min_len <= 0:
        return max(min_cap, min(max_cap, base_shrinkage))
    codes = list(aligned.keys())
    means = {code: (sum(aligned[code]) / float(len(aligned[code])) if aligned[code] else 0.0) for code in codes}
    corr_sum = 0.0
    corr_count = 0
    for i, left in enumerate(codes):
        for j, right in enumerate(codes):
            if i >= j:
                continue
            left_values = aligned[left]
            right_values = aligned[right]
            cov = sum(
                (left_values[idx] - means[left]) * (right_values[idx] - means[right])
                for idx in range(min_len)
            ) / float(min_len)
            left_var = sum((item - means[left]) ** 2 for item in left_values) / float(min_len)
            right_var = sum((item - means[right]) ** 2 for item in right_values) / float(min_len)
            denom = math.sqrt(max(left_var, 1e-12) * max(right_var, 1e-12))
            corr = cov / denom if denom > 0 else 0.0
            corr_sum += abs(corr)
            corr_count += 1
    avg_abs_corr = corr_sum / float(corr_count) if corr_count > 0 else 0.0
    sample_shortfall = max(0.0, 1.0 - (float(min_len) / float(max(1, lookback_days))))
    adaptive = base_shrinkage + 0.25 * sample_shortfall + 0.15 * avg_abs_corr
    return max(min_cap, min(max_cap, adaptive))


def _build_risk_proxy_map(
    *,
    series_by_code: Dict[str, List[float]],
    constraints: JsonDict,
) -> Dict[str, float]:
    min_floor = max(1e-4, float(constraints.get("min_volatility_floor") or 0.01))
    risk: Dict[str, float] = {}
    for ts_code, values in series_by_code.items():
        risk[ts_code] = _estimate_volatility_proxy(values, min_floor=min_floor)
    return risk


def _covariance_risk_contribution_summary(
    *,
    weights: Dict[str, float],
    series_by_code: Dict[str, List[float]],
    industry_by_code: Dict[str, str],
    facts_by_code: Dict[str, JsonDict],
    constraints: JsonDict,
    min_volatility_floor: float,
) -> Dict[str, Dict[str, float]]:
    codes = [code for code in weights if code in series_by_code]
    if not codes:
        return {
            "contribution": {},
            "share": {},
            "volatility_proxy_by_code": {},
            "covariance_model": "adaptive_shrunk_industry_block_covariance_v3",
            "portfolio_variance": 0.0,
            "factor_exposure_summary": {},
        }
    covariance_payload = _build_covariance_payload(
        codes=codes,
        series_by_code=series_by_code,
        industry_by_code=industry_by_code,
        constraints=constraints,
        min_volatility_floor=min_volatility_floor,
    )
    matrix = covariance_payload.get("matrix")
    if not isinstance(matrix, np.ndarray):
        matrix = np.diag([max(1e-8, float(min_volatility_floor) ** 2) for _ in codes])
    weight_vec = np.array([float(weights.get(code, 0.0) or 0.0) for code in codes], dtype=float)
    marginal_vec = matrix @ weight_vec
    contribution_vec = np.maximum(0.0, weight_vec * marginal_vec)
    contribution = {code: float(contribution_vec[idx]) for idx, code in enumerate(codes)}
    total = sum(contribution.values())
    share = {code: (value / total if total > 0 else 0.0) for code, value in contribution.items()}
    risk_by_code = {code: float(math.sqrt(max(1e-12, matrix[idx, idx]))) for idx, code in enumerate(codes)}
    return {
        "contribution": contribution,
        "share": share,
        "volatility_proxy_by_code": risk_by_code,
        "covariance_model": str(covariance_payload.get("covariance_model") or "adaptive_shrunk_industry_block_covariance_v3"),
        "portfolio_variance": max(0.0, total),
        "base_shrinkage": float(covariance_payload.get("base_shrinkage") or 0.0),
        "shrinkage_intensity": float(covariance_payload.get("shrinkage_intensity") or 0.0),
        "same_industry_correlation_scale": float(covariance_payload.get("same_industry_correlation_scale") or 0.0),
        "cross_industry_correlation_scale": float(covariance_payload.get("cross_industry_correlation_scale") or 0.0),
        "factor_exposure_summary": _factor_exposure_summary(
            weights=weights,
            facts_by_code=facts_by_code,
            constraints=constraints,
            risk_by_code=risk_by_code,
        ),
    }


def _latest_stock_facts(conn: sqlite3.Connection, ts_code: str) -> JsonDict:
    basic = conn.execute(
        "SELECT name, industry, circ_mv, total_mv FROM stock_basic WHERE ts_code = ?",
        (str(ts_code or ""),),
    ).fetchone()
    daily = conn.execute(
        """
        SELECT trade_date, close_price, amount, pct_chg, turnover_rate
        FROM daily_trading_data
        WHERE ts_code = ?
        ORDER BY trade_date DESC
        LIMIT 1
        """,
        (str(ts_code or ""),),
    ).fetchone()
    return {
        "name": str((basic or [""])[0] or ""),
        "industry": str((basic or ["", ""])[1] or "unknown"),
        "circ_mv": float((basic or ["", "", 0.0])[2] or 0.0),
        "total_mv": float((basic or ["", "", 0.0, 0.0])[3] or 0.0),
        "latest_trade_date": str((daily or [""])[0] or ""),
        "close_price": float((daily or ["", 0.0])[1] or 0.0),
        "amount": float((daily or ["", 0.0, 0.0])[2] or 0.0),
        "pct_chg": float((daily or ["", 0.0, 0.0, 0.0])[3] or 0.0),
        "turnover_rate": float((daily or ["", 0.0, 0.0, 0.0, 0.0])[4] or 0.0),
    }


def _model_card_failures(strategy: str, model_card: JsonDict) -> List[str]:
    failures: List[str] = []
    if not isinstance(model_card, dict) or not model_card:
        return [f"model_card_missing:{strategy}"]
    for field in REQUIRED_MODEL_CARD_FIELDS:
        if not str(model_card.get(field) or "").strip():
            failures.append(f"model_card_missing_{field}:{strategy}")
    if str(model_card.get("status") or "").lower() in {"failed", "research_only", "archived"}:
        failures.append(f"model_card_status_not_competition_eligible:{strategy}")
    return failures


def _validate_independent_validator(payload: JsonDict) -> tuple[bool, List[str]]:
    failures: List[str] = []
    if not isinstance(payload, dict) or not payload:
        return False, ["independent_validator_missing"]
    if str(payload.get("decision") or "").lower() != "approved":
        failures.append("independent_validator_not_approved")
    if str(payload.get("validator_role") or "").lower() != "independent_validator":
        failures.append("independent_validator_role_invalid")
    if not str(payload.get("validator_name") or "").strip():
        failures.append("independent_validator_name_missing")
    if payload.get("conflict_of_interest_attestation") is not True:
        failures.append("independent_validator_conflict_attestation_missing")
    reviewed = payload.get("reviewed_artifacts")
    if not isinstance(reviewed, list) or not reviewed:
        failures.append("independent_validator_reviewed_artifacts_missing")
    return not failures, failures


def _validate_shadow_execution(payload: JsonDict) -> tuple[bool, List[str]]:
    payload = _normalize_shadow_execution_payload(payload)
    failures: List[str] = []
    if not isinstance(payload, dict) or not payload:
        return False, ["shadow_execution_missing"]
    if payload.get("passed") is not True:
        failures.append("shadow_execution_not_passed")
    if int(payload.get("sample_count", payload.get("total_orders", 0)) or 0) <= 0:
        failures.append("shadow_execution_sample_empty")
    if not str(payload.get("artifact") or payload.get("artifact_path") or "").strip():
        failures.append("shadow_execution_artifact_missing")
    return not failures, failures


def _normalize_shadow_execution_payload(payload: JsonDict) -> JsonDict:
    if isinstance(payload, dict) and payload.get("artifact_version") == "strategy_competition_shadow_execution_plan.v1":
        nested = payload.get("shadow_execution") if isinstance(payload.get("shadow_execution"), dict) else {}
        return {
            **nested,
            "source_plan_artifact": str(payload.get("artifact_path") or ""),
            "source_plan_status": str(payload.get("plan_status") or ""),
        }
    if isinstance(payload, dict) and payload.get("artifact_version") == "strategy_competition_shadow_execution_evidence.v1":
        nested = payload.get("shadow_execution") if isinstance(payload.get("shadow_execution"), dict) else {}
        return {
            **nested,
            "source_evidence_artifact": str(payload.get("artifact_path") or ""),
            "source_plan_artifact": str(payload.get("source_plan_artifact") or ""),
            "source_feedback_artifact": str(payload.get("source_feedback_artifact") or ""),
            "source_evidence_status": str(payload.get("status") or ""),
        }
    return payload if isinstance(payload, dict) else {}


def _validate_pre_trade_controls(payload: JsonDict) -> tuple[bool, List[str]]:
    failures: List[str] = []
    if not isinstance(payload, dict) or not payload:
        return False, ["pre_trade_risk_controls_missing"]
    if payload.get("passed") is not True:
        failures.append("pre_trade_risk_controls_not_passed")
    controls = payload.get("controls") if isinstance(payload.get("controls"), dict) else {}
    for key in REQUIRED_PRE_TRADE_CONTROLS:
        if controls.get(key) is not True:
            failures.append(f"pre_trade_control_missing_or_failed:{key}")
    return not failures, failures


def build_strategy_competition_portfolio_audit(
    conn: sqlite3.Connection,
    *,
    trade_date: str = "",
    fixed_candidate_pool: Iterable[Any],
    alpha_model_cards: Optional[Dict[str, JsonDict]] = None,
    portfolio_constraints: Optional[JsonDict] = None,
    benchmark_contract: Optional[JsonDict] = None,
    independent_validator: Optional[JsonDict] = None,
    shadow_execution: Optional[JsonDict] = None,
    pre_trade_risk_controls: Optional[JsonDict] = None,
    output_dir: str | Path | None = None,
    operator_name: str = "strategy_competition_audit",
) -> JsonDict:
    """Build a hard audit artifact for unified competition and Top5 outputs.

    The audit is intentionally stricter than the legacy recommendation service:
    a recommendation can exist historically, but formal Top5/production use is
    blocked unless model cards, fixed candidate pool, portfolio construction,
    independent validation, shadow execution, and pre-trade controls all pass.
    """

    apply_professional_migrations(conn)
    pool = _normalize_pool(fixed_candidate_pool)
    cards = {str(k or "").lower(): dict(v) for k, v in (alpha_model_cards or {}).items() if isinstance(v, dict)}
    constraints = _merge_constraints(portfolio_constraints)
    recommendation = build_unified_system_recommendation(
        conn,
        trade_date=trade_date,
        top_stock_limit=int(constraints.get("top_stock_count") or 5),
    )
    competition_run_id = _new_id("comp")
    blocking: List[str] = []
    if not pool:
        blocking.append("fixed_candidate_pool_missing")

    reviews = {
        str(item.get("strategy") or "").lower(): item
        for item in recommendation.get("competition_pool") or []
        if isinstance(item, dict)
    }
    for strategy in pool:
        blocking.extend(_model_card_failures(strategy, cards.get(strategy) or {}))
        review = reviews.get(strategy)
        if not review:
            blocking.append(f"fixed_candidate_missing_from_recommendation:{strategy}")
            continue
        status = str(review.get("competition_status") or "").lower()
        if status in PROHIBITED_COMPETITION_STATUSES or review.get("competes_for_formal_top") is not True:
            blocking.append(f"fixed_candidate_not_formal_competition_eligible:{strategy}:{status}")

    top_strategy_names = [str(item.get("strategy") or "").lower() for item in recommendation.get("top_strategies") or []]
    for strategy in top_strategy_names:
        if strategy and strategy not in pool:
            blocking.append(f"top_strategy_not_in_fixed_candidate_pool:{strategy}")
    top_stocks = [dict(item) for item in recommendation.get("top_stocks") or [] if isinstance(item, dict)]
    required_count = int(constraints.get("top_stock_count") or 5)
    if len(top_stocks) != required_count:
        blocking.append(f"top5_count_mismatch:{len(top_stocks)}/{required_count}")

    top5_audit, portfolio_failures, risk_summary, cost_summary = _build_top5_audit(
        conn,
        top_stocks=top_stocks,
        constraints=constraints,
    )
    benchmark_contract_payload = _build_benchmark_contract(
        trade_date=str(trade_date or ""),
        top5_rows=top5_audit,
        risk_summary=risk_summary,
        constraints=constraints,
        provided_contract=benchmark_contract,
    )
    blocking.extend(portfolio_failures)

    independent_ok, independent_failures = _validate_independent_validator(independent_validator or {})
    normalized_shadow_execution = _normalize_shadow_execution_payload(shadow_execution or {})
    shadow_ok, shadow_failures = _validate_shadow_execution(normalized_shadow_execution)
    controls_ok, controls_failures = _validate_pre_trade_controls(pre_trade_risk_controls or {})
    blocking.extend(independent_failures)
    blocking.extend(shadow_failures)
    blocking.extend(controls_failures)

    competition_passed = not blocking
    production_allowed = competition_passed and independent_ok and shadow_ok and controls_ok
    result_status = "industry_benchmark_competition_passed" if production_allowed else "industry_benchmark_competition_blocked"
    ranking_contract = {
        "method": "unified_system_score_then_top_stock_consensus",
        "fixed_candidate_pool": pool,
        "top_strategy_limit": recommendation.get("policy", {}).get("top_strategy_limit"),
        "top_stock_limit": required_count,
        "no_posthoc_candidate_addition": True,
        "failed_or_research_only_candidate_banned": True,
    }
    artifact: JsonDict = {
        "artifact_version": "strategy_competition_portfolio_audit.v1",
        "competition_run_id": competition_run_id,
        "created_at": _now_text(),
        "operator_name": str(operator_name or ""),
        "trade_date": str(trade_date or ""),
        "result_status": result_status,
        "passed": production_allowed,
        "blocking_reasons": blocking,
        "fixed_candidate_pool": pool,
        "ranking_method_hash": _hash_payload(ranking_contract),
        "ranking_contract": ranking_contract,
        "alpha_model_cards": cards,
        "model_card_hashes": {strategy: _hash_payload(card) for strategy, card in cards.items()},
        "data_hashes": {strategy: str((cards.get(strategy) or {}).get("data_hash") or "") for strategy in pool},
        "code_hashes": {strategy: str((cards.get(strategy) or {}).get("code_hash") or "") for strategy in pool},
        "rule_hashes": {strategy: str((cards.get(strategy) or {}).get("rule_hash") or "") for strategy in pool},
        "recommendation_summary": {
            "passed": bool(recommendation.get("passed") is True),
            "blocking_reasons": list(recommendation.get("blocking_reasons") or []),
            "eligible_pool": [str(item.get("strategy") or "") for item in recommendation.get("eligible_pool") or []],
            "top_strategies": [str(item.get("strategy") or "") for item in recommendation.get("top_strategies") or []],
            "top_stocks": [str(item.get("ts_code") or "") for item in top_stocks],
        },
        "top5_portfolio_audit": top5_audit,
        "risk_summary": risk_summary,
        "cost_summary": cost_summary,
        "benchmark_contract": benchmark_contract_payload,
        "portfolio_constraints": constraints,
        "independent_validation": {
            **(independent_validator or {}),
            "passed": independent_ok,
            "blocking_reasons": independent_failures,
        },
        "shadow_execution": {
            **normalized_shadow_execution,
            "passed": shadow_ok,
            "blocking_reasons": shadow_failures,
        },
        "pre_trade_risk_controls": {
            **(pre_trade_risk_controls or {}),
            "passed": controls_ok,
            "blocking_reasons": controls_failures,
        },
        "formal_top_allowed": production_allowed,
        "production_candidate_allowed": production_allowed,
        "hard_boundaries": [
            "fixed_candidate_pool_required",
            "model_card_hypothesis_rule_data_code_hash_required",
            "failed_research_only_candidate_banned_from_top5",
            "top5_requires_source_weight_risk_cost_constraint_explainability",
            "independent_validator_required_for_any_promotion",
            "production_requires_shadow_execution_and_pre_trade_risk_controls",
        ],
    }
    artifact_path = ""
    if output_dir is not None:
        out = Path(output_dir)
        out.mkdir(parents=True, exist_ok=True)
        artifact_path = str(out / f"strategy_competition_portfolio_audit_{competition_run_id}.json")
        Path(artifact_path).write_text(json.dumps(artifact, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        artifact["artifact_path"] = artifact_path
        Path(artifact_path).write_text(json.dumps(artifact, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    _persist_audit(
        conn,
        artifact=artifact,
        artifact_path=artifact_path,
        independent_validator=independent_validator or {},
    )
    return artifact


def _build_top5_audit(
    conn: sqlite3.Connection,
    *,
    top_stocks: List[JsonDict],
    constraints: JsonDict,
) -> tuple[List[JsonDict], List[str], JsonDict, JsonDict]:
    failures: List[str] = []
    industry_weights: Dict[str, float] = {}
    rows: List[JsonDict] = []
    total_cost_bps = 0.0
    max_single = float(constraints.get("max_single_name_weight") or 0.0)
    max_industry = float(constraints.get("max_industry_weight") or 0.0)
    min_liquidity = float(constraints.get("min_liquidity_amount") or 0.0)
    max_cost = float(constraints.get("max_estimated_cost_bps") or 0.0)
    default_cost = float(constraints.get("predeclared_cost_bps") or 0.0)
    notional = float(constraints.get("portfolio_notional") or 0.0)
    max_single_risk_contribution = float(constraints.get("max_single_risk_contribution") or 0.0)
    facts_by_code = {
        str(stock.get("ts_code") or ""): _latest_stock_facts(conn, str(stock.get("ts_code") or ""))
        for stock in top_stocks
        if str(stock.get("ts_code") or "")
    }
    industry_by_code = _industry_by_code_from_facts(facts_by_code)
    series_by_code = _build_return_series_map(conn, top_stocks=top_stocks, constraints=constraints)
    risk_by_code = _build_risk_proxy_map(series_by_code=series_by_code, constraints=constraints)
    plan = _build_top5_weight_plan(
        top_stocks=top_stocks,
        facts_by_code=facts_by_code,
        risk_by_code=risk_by_code,
        series_by_code=series_by_code,
        industry_by_code=industry_by_code,
        constraints=constraints,
        max_single=max_single,
        max_industry=max_industry,
    )
    weight_by_code = plan.get("weights") if isinstance(plan.get("weights"), dict) else {}
    cash_weight = float(plan.get("cash_weight") or 0.0)
    risk_summary_payload = _covariance_risk_contribution_summary(
        weights=weight_by_code,
        series_by_code=series_by_code,
        industry_by_code=industry_by_code,
        facts_by_code=facts_by_code,
        constraints=constraints,
        min_volatility_floor=float(constraints.get("min_volatility_floor") or 0.01),
    )
    risk_share_by_code = (
        risk_summary_payload.get("share")
        if isinstance(risk_summary_payload.get("share"), dict)
        else {}
    )
    factor_summary = (
        risk_summary_payload.get("factor_exposure_summary")
        if isinstance(risk_summary_payload.get("factor_exposure_summary"), dict)
        else {}
    )
    size_factor = (
        factor_summary.get("size", {}).get("factor_by_code")
        if isinstance(factor_summary.get("size"), dict)
        and isinstance(factor_summary.get("size", {}).get("factor_by_code"), dict)
        else {}
    )
    liquidity_factor = (
        factor_summary.get("liquidity", {}).get("factor_by_code")
        if isinstance(factor_summary.get("liquidity"), dict)
        and isinstance(factor_summary.get("liquidity", {}).get("factor_by_code"), dict)
        else {}
    )
    for stock in top_stocks:
        ts_code = str(stock.get("ts_code") or "")
        facts = facts_by_code.get(ts_code) or _latest_stock_facts(conn, ts_code)
        industry = str(facts.get("industry") or "unknown")
        weight = float(weight_by_code.get(ts_code, 0.0) or 0.0)
        industry_weights[industry] = industry_weights.get(industry, 0.0) + weight
        signal_refs = [dict(item) for item in stock.get("signal_refs") or [] if isinstance(item, dict)]
        order_value = notional * weight
        cost_bps = _estimate_cost_bps(
            order_value=order_value,
            amount=float(facts.get("amount") or 0.0),
            base_bps=default_cost,
            max_bps=max_cost,
        )
        total_cost_bps += cost_bps * weight
        constraint_checks = {
            "single_name_weight": weight <= max_single,
            "liquidity_amount": float(facts.get("amount") or 0.0) >= min_liquidity,
            "estimated_cost_bps": cost_bps <= max_cost,
            "has_signal_refs": bool(signal_refs),
            "has_risk_exposure": bool(industry),
            "risk_contribution_limit": float(risk_share_by_code.get(ts_code, 0.0) or 0.0) <= max_single_risk_contribution,
            "size_factor_exposure_limit": bool(
                (factor_summary.get("size") or {}).get("within_limit", True)
                if isinstance(factor_summary.get("size"), dict)
                else True
            ),
            "liquidity_factor_exposure_limit": bool(
                (factor_summary.get("liquidity") or {}).get("within_limit", True)
                if isinstance(factor_summary.get("liquidity"), dict)
                else True
            ),
        }
        for key, passed in constraint_checks.items():
            if passed is not True:
                failures.append(f"top5_constraint_failed:{ts_code}:{key}")
        rows.append(
            {
                "ts_code": ts_code,
                "weight": round(weight, 6),
                "source": {
                    "contributing_strategies": list(stock.get("contributing_strategies") or []),
                    "signal_refs": signal_refs,
                    "final_stock_score": stock.get("final_stock_score"),
                    "consensus_count": stock.get("consensus_count"),
                },
                "risk": {
                    "industry": industry,
                    "single_name_weight": round(weight, 6),
                    "liquidity_amount": facts.get("amount"),
                    "turnover_rate": facts.get("turnover_rate"),
                    "pct_chg": facts.get("pct_chg"),
                    "volatility_proxy": round(float(risk_by_code.get(ts_code, 0.0) or 0.0), 6),
                    "risk_contribution_share": round(float(risk_share_by_code.get(ts_code, 0.0) or 0.0), 6),
                    "size_factor_exposure": round(float(size_factor.get(ts_code, 0.0) or 0.0), 6),
                    "liquidity_factor_exposure": round(float(liquidity_factor.get(ts_code, 0.0) or 0.0), 6),
                },
                "cost": {
                    "estimated_cost_bps": round(cost_bps, 6),
                    "cost_model": "predeclared_base_plus_participation_impact",
                    "estimated_order_value": round(order_value, 4),
                },
                "constraint_checks": constraint_checks,
            }
        )
    for industry, weight in industry_weights.items():
        if weight > max_industry:
            failures.append(f"industry_weight_above_cap:{industry}:{round(weight, 6)}/{max_industry}")
    for factor_name in ("size", "liquidity"):
        factor_payload = factor_summary.get(factor_name) if isinstance(factor_summary.get(factor_name), dict) else {}
        if factor_payload and factor_payload.get("within_limit") is not True:
            failures.append(
                "factor_exposure_above_cap:"
                f"{factor_name}:{round(float(factor_payload.get('portfolio_exposure') or 0.0), 6)}/{round(float(factor_payload.get('cap') or 0.0), 6)}"
            )
    risk_summary = {
        "industry_weights": {key: round(value, 6) for key, value in sorted(industry_weights.items())},
        "max_single_name_weight": max((float(row.get("weight") or 0.0) for row in rows), default=0.0),
        "top5_count": len(top_stocks),
        "weight_plan": {
            "method": str(plan.get("method") or "multifactor_hard_constraint_weighting_v4"),
            "cash_weight": round(cash_weight, 6),
            **(
                {
                    "industry_neutral_source": str(plan.get("industry_neutral_source") or ""),
                    "industry_neutral_tolerance": round(float(plan.get("industry_neutral_tolerance") or 0.0), 6),
                    "industry_neutral_targets": {
                        key: round(float(value), 6)
                        for key, value in sorted((plan.get("industry_neutral_targets") or {}).items())
                    },
                    "industry_neutral_deviation": {
                        key: round(float(value), 6)
                        for key, value in sorted((plan.get("industry_neutral_deviation") or {}).items())
                    },
                    "industry_neutral_within_tolerance": bool(plan.get("industry_neutral_within_tolerance") is True),
                }
                if isinstance(plan.get("industry_neutral_targets"), dict)
                else {}
            ),
        },
        "risk_budget": {
            "max_single_risk_contribution": max_single_risk_contribution,
            "risk_proxy_by_code": {code: round(float(value), 6) for code, value in sorted(risk_by_code.items())},
            "risk_contribution_share_by_code": {
                code: round(float(value), 6) for code, value in sorted(risk_share_by_code.items())
            },
            "risk_contribution_model": str(risk_summary_payload.get("covariance_model") or ""),
            "portfolio_variance": round(float(risk_summary_payload.get("portfolio_variance") or 0.0), 8),
            "base_shrinkage": round(float(risk_summary_payload.get("base_shrinkage") or 0.0), 6),
            "shrinkage_intensity": round(float(risk_summary_payload.get("shrinkage_intensity") or 0.0), 6),
            "same_industry_correlation_scale": round(float(risk_summary_payload.get("same_industry_correlation_scale") or 0.0), 6),
            "cross_industry_correlation_scale": round(float(risk_summary_payload.get("cross_industry_correlation_scale") or 0.0), 6),
            "factor_exposure_summary": {
                key: {
                    "portfolio_exposure": round(float((value or {}).get("portfolio_exposure") or 0.0), 6),
                    "cap": round(float((value or {}).get("cap") or 0.0), 6),
                    "within_limit": bool((value or {}).get("within_limit", True)),
                }
                for key, value in factor_summary.items()
                if isinstance(value, dict)
            },
        },
    }
    cost_summary = {
        "weighted_estimated_cost_bps": round(total_cost_bps, 6),
        "cost_model": "predeclared_base_plus_participation_impact",
        "max_estimated_cost_bps": max_cost,
    }
    if total_cost_bps > max_cost:
        failures.append(f"portfolio_cost_above_cap:{round(total_cost_bps, 6)}/{max_cost}")
    return rows, failures, risk_summary, cost_summary


def _persist_audit(
    conn: sqlite3.Connection,
    *,
    artifact: JsonDict,
    artifact_path: str,
    independent_validator: JsonDict,
) -> None:
    for strategy in artifact.get("fixed_candidate_pool") or []:
        card = artifact.get("alpha_model_cards", {}).get(strategy) or {}
        strategy_failures = [reason for reason in artifact.get("blocking_reasons") or [] if f":{strategy}" in str(reason)]
        conn.execute(
            """
            INSERT OR REPLACE INTO strategy_competition_registry (
                registry_id, strategy, alpha_id, status, model_card_json, hypothesis,
                rule_hash, data_hash, code_hash, evidence_manifest, eligible_for_competition,
                blocking_reasons_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                _new_id("reg"),
                strategy,
                str(card.get("alpha_id") or strategy),
                "eligible" if not strategy_failures else "blocked",
                canonical_json(card.get("model_card") or {}),
                str(card.get("hypothesis") or ""),
                str(card.get("rule_hash") or ""),
                str(card.get("data_hash") or ""),
                str(card.get("code_hash") or ""),
                str(card.get("evidence_manifest") or ""),
                1 if not strategy_failures else 0,
                canonical_json(strategy_failures),
                _now_text(),
            ),
        )
    conn.execute(
        """
        INSERT OR REPLACE INTO portfolio_competition_runs (
            competition_run_id, trade_date, fixed_candidate_pool_json, ranking_method_hash,
            recommendation_artifact_json, top5_symbols_json, portfolio_constraints_json,
            result_status, blocking_reasons_json, artifact_path, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            artifact["competition_run_id"],
            str(artifact.get("trade_date") or ""),
            canonical_json(artifact.get("fixed_candidate_pool") or []),
            str(artifact.get("ranking_method_hash") or ""),
            canonical_json(artifact.get("recommendation_summary") or {}),
            canonical_json((artifact.get("recommendation_summary") or {}).get("top_stocks") or []),
            canonical_json(artifact.get("portfolio_constraints") or {}),
            str(artifact.get("result_status") or ""),
            canonical_json(artifact.get("blocking_reasons") or []),
            str(artifact_path or artifact.get("artifact_path") or ""),
            _now_text(),
        ),
    )
    conn.execute(
        """
        INSERT OR REPLACE INTO portfolio_construction_audits (
            audit_id, competition_run_id, top5_audit_json, risk_summary_json,
            cost_summary_json, constraints_passed, blocking_reasons_json, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            _new_id("port_audit"),
            artifact["competition_run_id"],
            canonical_json(artifact.get("top5_portfolio_audit") or []),
            canonical_json(artifact.get("risk_summary") or {}),
            canonical_json(artifact.get("cost_summary") or {}),
            1 if not [r for r in artifact.get("blocking_reasons") or [] if str(r).startswith(("top5_", "industry_", "portfolio_cost_"))] else 0,
            canonical_json(artifact.get("blocking_reasons") or []),
            _now_text(),
        ),
    )
    validation = artifact.get("independent_validation") or {}
    conn.execute(
        """
        INSERT OR REPLACE INTO independent_validation_decisions (
            validation_id, competition_run_id, validator_name, validator_role, decision,
            conflict_of_interest_attested, reviewed_artifacts_json, blocking_reasons_json, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            _new_id("validation"),
            artifact["competition_run_id"],
            str(validation.get("validator_name") or independent_validator.get("validator_name") or ""),
            str(validation.get("validator_role") or independent_validator.get("validator_role") or ""),
            str(validation.get("decision") or independent_validator.get("decision") or "pending"),
            1 if validation.get("conflict_of_interest_attestation") is True else 0,
            canonical_json(validation.get("reviewed_artifacts") or independent_validator.get("reviewed_artifacts") or []),
            canonical_json(validation.get("blocking_reasons") or []),
            _now_text(),
        ),
    )
    conn.commit()
