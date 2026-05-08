from __future__ import annotations

from typing import Any

import pandas as pd


def risk_penalty(risk_level: str) -> float:
    if risk_level == "high":
        return 15.0
    if risk_level == "medium":
        return 5.0
    return 0.0


def sample_quality_multiplier(sample_size: int) -> float:
    size = max(int(sample_size or 0), 0)
    if size >= 180:
        return 1.0
    if size <= 0:
        return 0.75
    return 0.75 + min(size, 180) / 180.0 * 0.25


def regime_score_components(regime_info: dict[str, Any] | None) -> tuple[float, str]:
    regime_info = regime_info or {}
    regime = str(regime_info.get("regime", "range") or "range")
    market_trend = str(regime_info.get("market_trend", "bearish") or "bearish")
    env_score = float(regime_info.get("environment_score", 0.5) or 0.5)
    bonus = env_score * 12.0
    if "trend" in regime and "volatile" not in regime:
        bonus += 6.0
    if "range" in regime:
        bonus -= 1.5
    if "extreme" in regime:
        bonus -= 5.0
    if "volatile" in regime:
        bonus -= 4.5
    if market_trend == "bullish":
        bonus += 2.0
    else:
        bonus -= 2.0
    return bonus, regime


def build_candidate_frame(
    results: list[dict[str, Any]],
    stock_basic_map: dict[str, dict[str, str]] | None = None,
) -> pd.DataFrame:
    stock_basic_map = stock_basic_map or {}
    rows: list[dict[str, Any]] = []
    for result in results:
        signal_result = result.get("signal_result", {}) or {}
        forecast_result = result.get("forecast_result", {}) or {}
        risk_info = result.get("risk_info", {}) or {}
        position_result = result.get("position_result", {}) or {}
        style_snapshot = result.get("style_snapshot", {}) or {}
        code = str(result.get("ts_code", ""))
        meta = stock_basic_map.get(code, {})
        regime_info = result.get("regime_info", {}) or {}

        signal = str(signal_result.get("signal", "watch"))
        research_signal = str(signal_result.get("research_signal", signal) or signal)
        execution_state = str(signal_result.get("execution_state", "tradeable") or "tradeable")
        signal_score = float(signal_result.get("score", 0.0))
        prob_up = float(forecast_result.get("direction_prob_up", forecast_result.get("direction_prob", 0.5)) or 0.5)
        pred_ret = float(forecast_result.get("pred_return", forecast_result.get("expected_return", 0.0)) or 0.0)
        confidence = float(forecast_result.get("confidence", 0.0) or 0.0)
        calibrated_up_rate = float(forecast_result.get("calibrated_upside_win_rate", prob_up) or prob_up)
        calibrated_avg_return = float(forecast_result.get("calibrated_avg_return", pred_ret) or pred_ret)
        calibration_sample_size = int(forecast_result.get("calibration_sample_size", 0) or 0)
        risk_level = str(risk_info.get("risk_level", "medium"))
        model_agreement = float(forecast_result.get("model_agreement", confidence) or confidence)
        dispersion = float(forecast_result.get("prediction_dispersion", 0.0) or 0.0)
        sample_quality = sample_quality_multiplier(calibration_sample_size)
        regime_bonus, regime_name = regime_score_components(regime_info)

        robustness_score = (
            calibrated_up_rate * 32.0
            + confidence * 18.0
            + model_agreement * 14.0
            + sample_quality * 10.0
            + regime_bonus * 0.8
            - min(dispersion, 0.25) * 36.0
            - risk_penalty(risk_level) * 0.6
        )
        final_score = signal_score
        final_score += prob_up * 26.0
        final_score += pred_ret * 220.0
        final_score += confidence * 18.0
        final_score += model_agreement * 10.0
        final_score -= min(dispersion, 0.25) * 28.0
        final_score += (calibrated_up_rate - 0.5) * 22.0
        final_score += calibrated_avg_return * 260.0
        final_score += sample_quality * 10.0
        final_score += regime_bonus
        final_score -= risk_penalty(risk_level)

        rows.append({
            "ts_code": code,
            "stock_name": meta.get("stock_name", ""),
            "industry": meta.get("industry", ""),
            "market": meta.get("market", ""),
            "area": meta.get("area", ""),
            "signal": signal,
            "research_signal": research_signal,
            "execution_state": execution_state,
            "signal_score": round(signal_score, 2),
            "direction_prob_up": round(prob_up, 4),
            "pred_return": round(pred_ret, 4),
            "confidence": round(confidence, 4),
            "calibrated_upside_win_rate": round(calibrated_up_rate, 4),
            "calibrated_avg_return": round(calibrated_avg_return, 4),
            "calibration_sample_size": calibration_sample_size,
            "calibration_quality": round(sample_quality, 4),
            "regime": regime_name,
            "regime_bonus": round(regime_bonus, 2),
            "model_agreement": round(model_agreement, 4),
            "prediction_dispersion": round(dispersion, 4),
            "robustness_score": round(robustness_score, 2),
            "risk_level": risk_level,
            "style_volatility": round(float(style_snapshot.get("hist_vol_20", 0.0) or 0.0), 4),
            "style_relative_strength": round(float(style_snapshot.get("rel_strength_index", 0.0) or 0.0), 4),
            "stop_loss": float(risk_info.get("stop_loss", 0.0) or 0.0),
            "take_profit": float(risk_info.get("take_profit", 0.0) or 0.0),
            "position_pct": float(position_result.get("position_pct", 0.0) or 0.0),
            "reason": signal_result.get("reason", ""),
            "final_score": round(final_score, 4),
        })
    return pd.DataFrame(rows)
