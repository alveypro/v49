from __future__ import annotations

import math
from typing import Any

import pandas as pd

from src.primary_result_candidate_basket import (
    CONDITIONAL_MAX_INDUSTRY_WEIGHT,
    DEFAULT_MAX_HIGH_RISK_WEIGHT,
    DEFAULT_MAX_SINGLE_WEIGHT,
    TARGET_MAX_INDUSTRY_WEIGHT,
)


def _append_flag(current_flag: object, new_flag: str) -> str:
    current = str(current_flag or "").strip()
    if not current or current == "ok":
        return new_flag
    parts = [part.strip() for part in current.split(",") if part.strip()]
    if new_flag in parts:
        return current
    parts.append(new_flag)
    return ",".join(parts)


def _numeric_series(df: pd.DataFrame, column: str, default: float = 0.0) -> pd.Series:
    if column not in df.columns:
        return pd.Series(default, index=df.index, dtype=float)
    return pd.to_numeric(df[column], errors="coerce").fillna(default)


def _text_series(df: pd.DataFrame, column: str, default: str = "") -> pd.Series:
    if column not in df.columns:
        return pd.Series(default, index=df.index, dtype=str)
    return df[column].astype(str).fillna(default)


def _average_abs_correlation(return_series: list[float], selected_series: list[list[float]]) -> float:
    if not return_series or not selected_series:
        return 0.0
    current = pd.Series(return_series, dtype=float)
    correlations: list[float] = []
    for series in selected_series:
        other = pd.Series(series, dtype=float)
        pair = pd.concat([current, other], axis=1).dropna()
        if len(pair) < 5:
            continue
        corr = pair.iloc[:, 0].corr(pair.iloc[:, 1])
        if pd.notna(corr):
            correlations.append(abs(float(corr)))
    return float(sum(correlations) / len(correlations)) if correlations else 0.0


def diversify_top_candidates(df: pd.DataFrame, top_n: int) -> pd.DataFrame:
    candidate_pool = df.sort_values("final_score", ascending=False).reset_index(drop=True).copy()
    if candidate_pool.empty:
        return candidate_pool

    max_industry_slots = max(1, int(math.ceil(float(top_n) * TARGET_MAX_INDUSTRY_WEIGHT)))
    selected_rows: list[pd.Series] = []
    industry_counts: dict[str, int] = {}
    market_counts: dict[str, int] = {}
    area_counts: dict[str, int] = {}
    selected_return_series: list[list[float]] = []

    while len(selected_rows) < int(top_n) and not candidate_pool.empty:
        scored_pool = candidate_pool.copy()
        scored_pool["diversification_penalty"] = 0.0

        for idx, row in scored_pool.iterrows():
            industry = str(row.get("industry", "") or "").strip()
            market = str(row.get("market", "") or "").strip()
            area = str(row.get("area", "") or "").strip()
            penalty = 0.0
            if industry:
                penalty += industry_counts.get(industry, 0) * 9.0
            if market:
                penalty += market_counts.get(market, 0) * 3.5
            if area:
                penalty += area_counts.get(area, 0) * 1.5
            correlation_penalty = _average_abs_correlation(row.get("recent_returns", []), selected_return_series) * 14.0
            penalty += correlation_penalty
            scored_pool.at[idx, "diversification_penalty"] = penalty
            scored_pool.at[idx, "correlation_penalty"] = correlation_penalty

        def _industry_has_headroom(row: pd.Series) -> bool:
            industry = str(row.get("industry", "") or "").strip()
            if not industry:
                return True
            return industry_counts.get(industry, 0) < max_industry_slots

        eligible_mask = scored_pool.apply(_industry_has_headroom, axis=1)
        if bool(eligible_mask.any()):
            scored_pool = scored_pool[eligible_mask].copy()

        scored_pool["selection_score"] = scored_pool["final_score"] - scored_pool["diversification_penalty"]
        pick_idx = scored_pool["selection_score"].idxmax()
        picked = scored_pool.loc[pick_idx].copy()
        selected_rows.append(picked)

        industry = str(picked.get("industry", "") or "").strip()
        market = str(picked.get("market", "") or "").strip()
        area = str(picked.get("area", "") or "").strip()
        if industry:
            industry_counts[industry] = industry_counts.get(industry, 0) + 1
        if market:
            market_counts[market] = market_counts.get(market, 0) + 1
        if area:
            area_counts[area] = area_counts.get(area, 0) + 1
        selected_return_series.append(list(picked.get("recent_returns", []) or []))

        candidate_pool = candidate_pool.drop(index=pick_idx).reset_index(drop=True)

    if not selected_rows:
        return candidate_pool.head(int(top_n)).copy()

    return pd.DataFrame(selected_rows).reset_index(drop=True)


def annotate_selected_subset(df: pd.DataFrame) -> pd.DataFrame:
    selected = df.sort_values("final_score", ascending=False).reset_index(drop=True).copy()
    if selected.empty:
        return selected

    industry_counts: dict[str, int] = {}
    market_counts: dict[str, int] = {}
    area_counts: dict[str, int] = {}
    penalties: list[float] = []
    selection_scores: list[float] = []
    correlation_penalties: list[float] = []
    selected_return_series: list[list[float]] = []

    for _, row in selected.iterrows():
        industry = str(row.get("industry", "") or "").strip()
        market = str(row.get("market", "") or "").strip()
        area = str(row.get("area", "") or "").strip()
        penalty = 0.0
        if industry:
            penalty += industry_counts.get(industry, 0) * 9.0
        if market:
            penalty += market_counts.get(market, 0) * 3.5
        if area:
            penalty += area_counts.get(area, 0) * 1.5
        correlation_penalty = _average_abs_correlation(row.get("recent_returns", []), selected_return_series) * 14.0
        penalty += correlation_penalty
        penalties.append(penalty)
        correlation_penalties.append(correlation_penalty)
        selection_scores.append(float(row.get("final_score", 0.0)) - penalty)

        if industry:
            industry_counts[industry] = industry_counts.get(industry, 0) + 1
        if market:
            market_counts[market] = market_counts.get(market, 0) + 1
        if area:
            area_counts[area] = area_counts.get(area, 0) + 1
        selected_return_series.append(list(row.get("recent_returns", []) or []))

    selected["diversification_penalty"] = penalties
    selected["correlation_penalty"] = correlation_penalties
    selected["selection_score"] = selection_scores
    return selected


def assign_basket_weights(df: pd.DataFrame) -> pd.DataFrame:
    basket = df.copy()
    if basket.empty:
        return basket

    raw_strength = (basket["selection_score"] - basket["selection_score"].min() + 1.0).clip(lower=0.5)
    total_strength = float(raw_strength.sum()) or 1.0
    basket["basket_weight_pct"] = raw_strength / total_strength

    industry_caps: dict[str, float] = {}
    for idx, row in basket.iterrows():
        industry = str(row.get("industry", "") or "").strip()
        if industry not in industry_caps:
            industry_caps[industry] = 0.0
        cap = 0.38 if idx == 0 else 0.32
        current_weight = float(basket.at[idx, "basket_weight_pct"])
        allowed_weight = min(current_weight, cap - industry_caps[industry]) if industry else min(current_weight, cap)
        allowed_weight = max(allowed_weight, min(current_weight, 0.08))
        basket.at[idx, "basket_weight_pct"] = allowed_weight
        if industry:
            industry_caps[industry] += allowed_weight

    total_after_cap = float(basket["basket_weight_pct"].sum()) or 1.0
    basket["basket_weight_pct"] = basket["basket_weight_pct"] / total_after_cap

    roles = []
    for idx, row in basket.iterrows():
        weight = float(row.get("basket_weight_pct", 0.0))
        if idx == 0 or weight >= 0.24:
            roles.append("core")
        elif weight >= 0.16:
            roles.append("satellite")
        else:
            roles.append("tactical")
    basket["basket_role"] = roles
    rounded_weights = basket["basket_weight_pct"].round(4)
    if not rounded_weights.empty:
        rounded_weights.iloc[-1] = round(1.0 - float(rounded_weights.iloc[:-1].sum()), 4)
    basket["basket_weight_pct"] = rounded_weights
    return basket


def apply_portfolio_risk_overlay(df: pd.DataFrame) -> pd.DataFrame:
    basket = df.copy()
    if basket.empty:
        return basket

    max_single_weight = DEFAULT_MAX_SINGLE_WEIGHT
    max_high_risk_weight = DEFAULT_MAX_HIGH_RISK_WEIGHT
    max_industry_weight = TARGET_MAX_INDUSTRY_WEIGHT

    basket["risk_overlay_penalty"] = 0.0
    basket["basket_risk_flag"] = "ok"

    capped_weights = basket["basket_weight_pct"].astype(float).copy()
    for idx, row in basket.iterrows():
        weight = float(row.get("basket_weight_pct", 0.0))
        risk_level = str(row.get("risk_level", "medium"))
        penalty = 0.0
        flags: list[str] = []

        if weight > max_single_weight:
            penalty += (weight - max_single_weight) * 120.0
            flags.append("single_weight_capped")
            capped_weights.at[idx] = max_single_weight

        if risk_level == "high" and float(capped_weights.at[idx]) > max_high_risk_weight:
            penalty += (weight - max_high_risk_weight) * 90.0
            flags.append("high_risk_trimmed")
            capped_weights.at[idx] = min(float(capped_weights.at[idx]), max_high_risk_weight)

        basket.at[idx, "risk_overlay_penalty"] = penalty
        if flags:
            basket.at[idx, "basket_risk_flag"] = ",".join(flags)

    leftover = 1.0 - float(capped_weights.sum())
    if leftover > 0:
        headroom = pd.Series(max_single_weight, index=capped_weights.index) - capped_weights
        high_risk_mask = basket["risk_level"].astype(str).eq("high")
        headroom.loc[high_risk_mask] = max_high_risk_weight - capped_weights.loc[high_risk_mask]
        headroom = headroom.clip(lower=0.0)
        total_headroom = float(headroom.sum())
        if total_headroom > 0:
            capped_weights = capped_weights + headroom / total_headroom * leftover

    capped_weights = capped_weights.clip(upper=max_single_weight)
    total_weight = float(capped_weights.sum()) or 1.0
    basket["basket_weight_pct"] = capped_weights / total_weight

    industry_weights = basket.groupby("industry", dropna=False)["basket_weight_pct"].transform("sum")
    overweight_mask = industry_weights > max_industry_weight
    if overweight_mask.any():
        for idx in basket[overweight_mask].index:
            basket.at[idx, "risk_overlay_penalty"] = float(basket.at[idx, "risk_overlay_penalty"]) + 4.0
            current_flag = str(basket.at[idx, "basket_risk_flag"])
            basket.at[idx, "basket_risk_flag"] = (
                "industry_overweight" if current_flag == "ok" else f"{current_flag},industry_overweight"
            )

    style_vol_series = _numeric_series(basket, "style_volatility")
    style_mom_series = _numeric_series(basket, "style_relative_strength")
    liquidity_score_series = _numeric_series(basket, "liquidity_score", default=0.62)
    median_amount_series = _numeric_series(basket, "median_amount")
    latest_amount_series = _numeric_series(basket, "latest_amount")
    style_vol_exposure = float((basket["basket_weight_pct"].astype(float) * style_vol_series).sum())
    style_mom_exposure = float((basket["basket_weight_pct"].astype(float) * style_mom_series).sum())
    if style_vol_exposure > 0.035:
        hot_vol_mask = style_vol_series > float(style_vol_series.median())
        for idx in basket[hot_vol_mask].index:
            basket.at[idx, "risk_overlay_penalty"] = float(basket.at[idx, "risk_overlay_penalty"]) + 3.0
            basket.at[idx, "basket_risk_flag"] = _append_flag(basket.at[idx, "basket_risk_flag"], "high_vol_exposure")
    if style_mom_exposure > 0.03:
        hot_mom_mask = style_mom_series > float(style_mom_series.median())
        for idx in basket[hot_mom_mask].index:
            basket.at[idx, "risk_overlay_penalty"] = float(basket.at[idx, "risk_overlay_penalty"]) + 2.0
            basket.at[idx, "basket_risk_flag"] = _append_flag(basket.at[idx, "basket_risk_flag"], "momentum_crowded")

    if not liquidity_score_series.empty:
        median_amount_safe = median_amount_series.mask(median_amount_series <= 0.0)
        latest_support_ratio = (latest_amount_series / median_amount_safe).fillna(0.0)
        capacity_stretch_mask = (
            (basket["basket_weight_pct"].astype(float) >= 0.18)
            & (
                (liquidity_score_series < 0.58)
                | ((latest_amount_series > 0) & (latest_support_ratio < 0.65))
            )
        )
        for idx in basket[capacity_stretch_mask].index:
            liquidity_gap = max(0.58 - float(liquidity_score_series.at[idx]), 0.0)
            support_gap = max(0.65 - float(latest_support_ratio.at[idx]), 0.0)
            penalty = 3.0 + liquidity_gap * 14.0 + support_gap * 10.0
            basket.at[idx, "risk_overlay_penalty"] = float(basket.at[idx, "risk_overlay_penalty"]) + penalty
            basket.at[idx, "basket_risk_flag"] = _append_flag(
                basket.at[idx, "basket_risk_flag"],
                "liquidity_capacity_stretched",
            )

    basket["portfolio_weight_after_risk"] = basket["basket_weight_pct"].round(4)
    basket["risk_adjusted_score"] = (basket["selection_score"] - basket["risk_overlay_penalty"]).round(2)

    rounded_weights = basket["portfolio_weight_after_risk"].copy()
    if not rounded_weights.empty:
        rounded_weights.iloc[-1] = round(1.0 - float(rounded_weights.iloc[:-1].sum()), 4)
    basket["portfolio_weight_after_risk"] = rounded_weights
    return basket


def summarize_candidate_basket(df: pd.DataFrame) -> dict[str, Any]:
    if df.empty:
        return {
            "candidate_count": 0,
            "expected_basket_return": 0.0,
            "calibrated_basket_return": 0.0,
            "basket_win_rate": 0.0,
            "top_industry_weight": 0.0,
            "max_single_weight": 0.0,
            "high_risk_weight": 0.0,
            "weighted_liquidity_score": 0.0,
            "liquidity_capacity_weight": 0.0,
            "concentration_hhi": 0.0,
            "risk_pressure_score": 0.0,
        }

    weights = df["portfolio_weight_after_risk"].astype(float)
    expected_basket_return = float((weights * df["pred_return"].astype(float)).sum())
    calibrated_basket_return = float((weights * df["calibrated_avg_return"].astype(float)).sum())
    basket_win_rate = float((weights * df["calibrated_upside_win_rate"].astype(float)).sum())
    style_vol_series = _numeric_series(df, "style_volatility")
    style_mom_series = _numeric_series(df, "style_relative_strength")
    liquidity_score_series = _numeric_series(df, "liquidity_score", default=0.62)
    style_volatility_exposure = float((weights * style_vol_series).sum())
    style_momentum_exposure = float((weights * style_mom_series).sum())
    weighted_liquidity_score = float((weights * liquidity_score_series).sum()) if not liquidity_score_series.empty else 0.0
    high_risk_weight = float(weights[df["risk_level"].astype(str).eq("high")].sum())
    risk_flag_series = _text_series(df, "basket_risk_flag")
    liquidity_capacity_mask = risk_flag_series.str.contains("liquidity_capacity_stretched", na=False)
    liquidity_capacity_weight = float(weights[liquidity_capacity_mask].sum()) if len(liquidity_capacity_mask) else 0.0
    industry_weight_map = (
        df.assign(_weight=weights)
        .groupby("industry", dropna=False)["_weight"]
        .sum()
        .sort_values(ascending=False)
    )
    top_industry_weight = float(industry_weight_map.iloc[0]) if not industry_weight_map.empty else 0.0
    max_single_weight = float(weights.max()) if not weights.empty else 0.0
    concentration_hhi = float((weights ** 2).sum())
    overlay_penalty = float((weights * df["risk_overlay_penalty"].astype(float)).sum())
    diversification_penalty = float((weights * df["diversification_penalty"].astype(float)).sum())
    style_pressure = max(style_volatility_exposure - 0.035, 0.0) * 180.0 + max(style_momentum_exposure - 0.03, 0.0) * 120.0
    liquidity_pressure = max(0.62 - weighted_liquidity_score, 0.0) * 90.0 + liquidity_capacity_weight * 25.0
    risk_pressure_score = (
        overlay_penalty
        + diversification_penalty
        + max(top_industry_weight - TARGET_MAX_INDUSTRY_WEIGHT, 0.0) * 100.0
        + style_pressure
        + liquidity_pressure
    )

    return {
        "candidate_count": int(len(df)),
        "expected_basket_return": round(expected_basket_return, 4),
        "calibrated_basket_return": round(calibrated_basket_return, 4),
        "basket_win_rate": round(basket_win_rate, 4),
        "top_industry": str(industry_weight_map.index[0]) if not industry_weight_map.empty else "",
        "top_industry_weight": round(top_industry_weight, 4),
        "max_single_weight": round(max_single_weight, 4),
        "high_risk_weight": round(high_risk_weight, 4),
        "weighted_liquidity_score": round(weighted_liquidity_score, 4),
        "liquidity_capacity_weight": round(liquidity_capacity_weight, 4),
        "style_volatility_exposure": round(style_volatility_exposure, 4),
        "style_momentum_exposure": round(style_momentum_exposure, 4),
        "concentration_hhi": round(concentration_hhi, 4),
        "risk_pressure_score": round(risk_pressure_score, 2),
    }


def finalize_candidate_basket(df: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, Any]]:
    basket = assign_basket_weights(df)
    basket = apply_portfolio_risk_overlay(basket)
    summary = summarize_candidate_basket(basket)
    return basket, summary


def _basket_registration_pressure(summary: dict[str, Any]) -> tuple[float, float, float, float]:
    hard_overflow = max(float(summary.get("top_industry_weight", 0.0) or 0.0) - CONDITIONAL_MAX_INDUSTRY_WEIGHT, 0.0)
    target_overflow = max(float(summary.get("top_industry_weight", 0.0) or 0.0) - TARGET_MAX_INDUSTRY_WEIGHT, 0.0)
    risk_pressure = float(summary.get("risk_pressure_score", 0.0) or 0.0)
    score = -float(summary.get("calibrated_basket_return", summary.get("expected_basket_return", 0.0)) or 0.0)
    return (round(hard_overflow, 6), round(target_overflow, 6), round(risk_pressure, 6), round(score, 6))


def rebalance_candidate_basket(candidate_pool: pd.DataFrame, top_n: int) -> tuple[pd.DataFrame, dict[str, Any]]:
    working_pool = candidate_pool.sort_values("final_score", ascending=False).reset_index(drop=True).copy()
    selected = diversify_top_candidates(working_pool, int(top_n))
    if selected.empty:
        return selected, summarize_candidate_basket(selected)

    anchor_code = str(selected.iloc[0]["ts_code"]) if not selected.empty else ""
    selected = annotate_selected_subset(selected)
    selected, summary = finalize_candidate_basket(selected)
    max_industry_weight = TARGET_MAX_INDUSTRY_WEIGHT
    max_risk_pressure = 75.0

    for _ in range(max(2, int(top_n) * 2)):
        if summary["top_industry_weight"] <= max_industry_weight and summary["risk_pressure_score"] <= max_risk_pressure:
            break

        dominant_industry = str(summary.get("top_industry", "") or "").strip()
        selected_codes = set(selected["ts_code"].astype(str))
        replacement_pool = working_pool[~working_pool["ts_code"].astype(str).isin(selected_codes)].copy()
        if dominant_industry:
            replacement_pool = replacement_pool[replacement_pool["industry"].fillna("").astype(str) != dominant_industry]
        if replacement_pool.empty:
            break

        drop_pool = selected.copy()
        if dominant_industry:
            drop_pool = drop_pool[drop_pool["industry"].fillna("").astype(str) == dominant_industry]
        if drop_pool.empty:
            drop_pool = selected.copy()
        if anchor_code and drop_pool["ts_code"].astype(str).ne(anchor_code).any():
            drop_pool = drop_pool[drop_pool["ts_code"].astype(str) != anchor_code]

        best_candidate = None
        best_summary = None
        replacement_candidates = replacement_pool.sort_values("final_score", ascending=False).head(max(5, int(top_n) * 2))
        for drop_idx in drop_pool.sort_values("selection_score", ascending=True).index.tolist():
            for _, replacement in replacement_candidates.iterrows():
                trial = selected.drop(index=drop_idx).reset_index(drop=True)
                trial = pd.concat([trial, pd.DataFrame([replacement.copy()])], ignore_index=True)
                trial = annotate_selected_subset(trial)
                trial, trial_summary = finalize_candidate_basket(trial)
                trial_key = _basket_registration_pressure(trial_summary)
                if best_summary is None or trial_key < _basket_registration_pressure(best_summary):
                    best_candidate = trial
                    best_summary = trial_summary
        if best_candidate is None or best_summary is None:
            break
        if _basket_registration_pressure(best_summary) >= _basket_registration_pressure(summary):
            break
        selected = best_candidate
        summary = best_summary

    selected = selected.sort_values(["final_score", "selection_score"], ascending=[False, False]).reset_index(drop=True)
    return selected, summary


def assign_single_name_basket(df: pd.DataFrame) -> pd.DataFrame:
    basket = df.head(1).copy().reset_index(drop=True)
    if basket.empty:
        return basket
    basket["basket_weight_pct"] = 1.0
    basket["portfolio_weight_after_risk"] = 1.0
    basket["basket_role"] = "concentrated_top1"
    basket["basket_risk_flag"] = "concentrated_top1"
    basket["risk_overlay_penalty"] = basket.get("risk_overlay_penalty", 0.0)
    basket["risk_adjusted_score"] = basket["selection_score"]
    return basket
