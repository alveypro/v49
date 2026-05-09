from __future__ import annotations

import math
from typing import Any

import pandas as pd

from src.primary_result_candidate_basket import TARGET_MAX_INDUSTRY_WEIGHT

from .basket import (
    annotate_selected_subset,
    assign_single_name_basket,
    finalize_candidate_basket,
    rebalance_candidate_basket,
    summarize_candidate_basket,
)
from .scoring import build_candidate_frame


def safe_percentile(series: pd.Series, ascending: bool = True) -> pd.Series:
    ranked = series.rank(pct=True, method="average", ascending=ascending)
    return ranked.fillna(0.5)


def apply_cross_sectional_ranking(df: pd.DataFrame) -> pd.DataFrame:
    ranked = df.copy()
    ranked["prob_rank_pct"] = safe_percentile(ranked["direction_prob_up"], ascending=True)
    ranked["return_rank_pct"] = safe_percentile(ranked["pred_return"], ascending=True)
    ranked["calibrated_up_rate_rank_pct"] = safe_percentile(ranked["calibrated_upside_win_rate"], ascending=True)
    ranked["calibrated_return_rank_pct"] = safe_percentile(ranked["calibrated_avg_return"], ascending=True)
    ranked["confidence_rank_pct"] = safe_percentile(ranked["confidence"], ascending=True)
    ranked["agreement_rank_pct"] = safe_percentile(ranked["model_agreement"], ascending=True)
    ranked["dispersion_rank_pct"] = safe_percentile(ranked["prediction_dispersion"], ascending=False)
    ranked["signal_rank_pct"] = safe_percentile(ranked["signal_score"], ascending=True)
    ranked["robustness_rank_pct"] = safe_percentile(ranked["robustness_score"], ascending=True)

    ranked["cross_section_score"] = (
        ranked["prob_rank_pct"] * 0.18
        + ranked["return_rank_pct"] * 0.12
        + ranked["calibrated_up_rate_rank_pct"] * 0.16
        + ranked["calibrated_return_rank_pct"] * 0.14
        + ranked["confidence_rank_pct"] * 0.10
        + ranked["agreement_rank_pct"] * 0.10
        + ranked["dispersion_rank_pct"] * 0.06
        + ranked["signal_rank_pct"] * 0.06
        + ranked["robustness_rank_pct"] * 0.08
    ) * 100.0

    industry_col = ranked["industry"].fillna("").astype(str)
    valid_industry = industry_col.ne("")
    ranked["industry_rank_pct"] = 0.5
    if valid_industry.any():
        industry_scores = ranked.loc[valid_industry].groupby("industry")["cross_section_score"].rank(
            pct=True, method="average", ascending=True
        )
        ranked.loc[valid_industry, "industry_rank_pct"] = industry_scores

    ranked["final_score"] = (
        ranked["final_score"]
        + ranked["cross_section_score"] * 0.35
        + ranked["industry_rank_pct"] * 8.0
        + ranked["robustness_score"] * 0.22
    )
    return ranked


def watch_candidate_is_executable(row: pd.Series) -> bool:
    reason = str(row.get("reason", "") or "").strip()
    if not reason:
        return True
    lowered = reason.lower()
    blocked_markers = (
        "market_rule_blocked(",
        "limit_up",
        "limit-down",
        "limit_down",
        "risk_blocked",
    )
    return not any(marker in lowered for marker in blocked_markers)


def expand_preferred_pool_for_diversification(
    df: pd.DataFrame,
    preferred: pd.DataFrame,
    top_n: int,
) -> pd.DataFrame:
    if preferred.empty or len(preferred) >= len(df) or int(top_n) <= 1:
        return preferred

    preferred_industry = preferred["industry"].fillna("").astype(str).str.strip()
    industry_counts = preferred_industry.value_counts()
    if industry_counts.empty:
        return preferred

    dominant_industry = str(industry_counts.index[0] or "").strip()
    dominant_count = int(industry_counts.iloc[0])
    max_industry_slots = max(1, int(math.ceil(float(top_n) * TARGET_MAX_INDUSTRY_WEIGHT)))
    if not dominant_industry or dominant_count <= max_industry_slots:
        return preferred

    current_alt_count = int(preferred_industry.ne(dominant_industry).sum())
    required_alt_count = max(0, min(int(top_n), len(preferred)) - max_industry_slots)
    supplemental_needed = max(0, required_alt_count - current_alt_count)
    if supplemental_needed <= 0:
        return preferred

    watch_pool = df[~df["signal"].isin(["strong_buy", "buy"])].copy()
    if watch_pool.empty:
        return preferred
    executable_mask = watch_pool.apply(watch_candidate_is_executable, axis=1)
    if bool(executable_mask.any()):
        watch_pool = watch_pool[executable_mask].copy()
    if watch_pool.empty:
        return preferred
    watch_pool["industry"] = watch_pool["industry"].fillna("").astype(str).str.strip()
    watch_pool = watch_pool[watch_pool["industry"].ne("") & watch_pool["industry"].ne(dominant_industry)].copy()
    if watch_pool.empty:
        return preferred

    watch_pool = watch_pool.sort_values("final_score", ascending=False).reset_index(drop=True)
    diversified_watch = watch_pool.groupby("industry", sort=False).head(1)
    supplemental = pd.concat([diversified_watch, watch_pool], ignore_index=True)
    supplemental = supplemental.drop_duplicates(subset=["ts_code"]).head(int(supplemental_needed))
    if supplemental.empty:
        return preferred

    expanded = pd.concat([preferred, supplemental], ignore_index=True)
    expanded = expanded.drop_duplicates(subset=["ts_code"]).reset_index(drop=True)
    return expanded


def rank_candidate_frame(
    df: pd.DataFrame,
    top_n: int,
    *,
    selection_mode: str = "diversified",
) -> pd.DataFrame:
    if df.empty:
        return df
    preferred = df[df["signal"].isin(["strong_buy", "buy"])].copy()
    if preferred.empty:
        preferred = df.copy()
    elif selection_mode == "diversified":
        preferred = expand_preferred_pool_for_diversification(df, preferred, int(top_n))
    if selection_mode == "raw":
        preferred = preferred.sort_values("final_score", ascending=False).head(int(top_n)).reset_index(drop=True)
        preferred = annotate_selected_subset(preferred)
        preferred, basket_summary = finalize_candidate_basket(preferred)
    elif selection_mode == "top1":
        preferred = preferred.sort_values("final_score", ascending=False).head(1).reset_index(drop=True)
        preferred = annotate_selected_subset(preferred)
        preferred = assign_single_name_basket(preferred)
        basket_summary = summarize_candidate_basket(preferred)
    else:
        preferred, basket_summary = rebalance_candidate_basket(preferred, int(top_n))
    if not preferred.empty:
        max_score = preferred["final_score"].max()
        min_score = preferred["final_score"].min()
        span = max(max_score - min_score, 1e-9)
        preferred["score_percentile"] = ((preferred["final_score"] - min_score) / span).round(4)
    preferred["cross_section_score"] = preferred["cross_section_score"].round(2)
    preferred["industry_rank_pct"] = preferred["industry_rank_pct"].round(4)
    preferred["diversification_penalty"] = preferred["diversification_penalty"].round(2)
    preferred["selection_score"] = preferred["selection_score"].round(2)
    preferred["risk_overlay_penalty"] = preferred["risk_overlay_penalty"].round(2)
    preferred["basket_expected_return"] = basket_summary.get("expected_basket_return", 0.0)
    preferred["basket_calibrated_return"] = basket_summary.get("calibrated_basket_return", 0.0)
    preferred["basket_win_rate"] = basket_summary.get("basket_win_rate", 0.0)
    preferred["basket_risk_pressure_score"] = basket_summary.get("risk_pressure_score", 0.0)
    preferred["basket_guardrail_mode"] = "normal"
    preferred["basket_guardrail_reason"] = ""
    preferred["final_score"] = preferred["final_score"].round(2)
    preferred.insert(0, "rank", range(1, len(preferred) + 1))
    return preferred


def rank_candidates(
    results: list[dict[str, Any]],
    top_n: int,
    stock_basic_map: dict[str, dict[str, str]] | None = None,
    *,
    selection_mode: str = "diversified",
) -> pd.DataFrame:
    df = build_candidate_frame(results, stock_basic_map=stock_basic_map)
    if df.empty:
        return df
    df = apply_cross_sectional_ranking(df)
    return rank_candidate_frame(df, top_n, selection_mode=selection_mode)
