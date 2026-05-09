from __future__ import annotations

from typing import Any

import pandas as pd


def build_candidate_audit_rows(df: pd.DataFrame) -> list[dict[str, Any]]:
    if df.empty:
        return []

    selected_codes = set(df["ts_code"].astype(str)) if "rank" in df.columns else set()
    rows: list[dict[str, Any]] = []
    for _, row in df.iterrows():
        ts_code = str(row.get("ts_code", "") or "")
        final_score = float(row.get("final_score", 0.0) or 0.0)
        cross_section_score = float(row.get("cross_section_score", 0.0) or 0.0)
        diversification_penalty = float(row.get("diversification_penalty", 0.0) or 0.0)
        risk_overlay_penalty = float(row.get("risk_overlay_penalty", 0.0) or 0.0)
        selection_score = float(row.get("selection_score", final_score - diversification_penalty) or 0.0)
        selected = ts_code in selected_codes
        reasons: list[str] = []
        if selected:
            reasons.append("selected_for_basket")
        if diversification_penalty > 0:
            reasons.append("diversification_penalty_applied")
        if risk_overlay_penalty > 0:
            reasons.append("risk_overlay_applied")
        risk_flag = str(row.get("basket_risk_flag", "ok") or "ok")
        if risk_flag not in {"", "ok"}:
            reasons.append(f"risk_flag:{risk_flag}")
        if not reasons:
            reasons.append("score_only")
        rows.append({
            "ts_code": ts_code,
            "selected": selected,
            "rank": int(row.get("rank", 0) or 0),
            "final_score": round(final_score, 2),
            "cross_section_score": round(cross_section_score, 2),
            "diversification_penalty": round(diversification_penalty, 2),
            "risk_overlay_penalty": round(risk_overlay_penalty, 2),
            "selection_score": round(selection_score, 2),
            "basket_risk_flag": risk_flag,
            "selection_reason": ",".join(reasons),
        })
    return rows
