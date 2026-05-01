from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Tuple


def calculate_v8_final_score(*, v7_score: float, advanced_total_score: float, advanced_max_score: float, market_penalty: float) -> Dict[str, float]:
    max_score = float(advanced_max_score or 100.0)
    advanced_score = (float(advanced_total_score or 0.0) / max_score) * 100.0 if max_score > 0 else 0.0
    final_score = (0.9 * advanced_score + 0.1 * float(v7_score or 0.0)) * float(market_penalty)
    final_score = max(0.0, min(100.0, final_score))
    return {
        "final_score": round(final_score, 2),
        "advanced_score": round(advanced_score, 2),
        "v7_score": round(float(v7_score or 0.0), 2),
        "v7_weight": 0.1,
        "advanced_weight": 0.9,
        "market_penalty": float(market_penalty),
    }


def calculate_v8_star_rating(score: float) -> Tuple[int, float]:
    if score >= 75:
        return 5, 0.25
    if score >= 65:
        return 4, 0.20
    if score >= 55:
        return 3, 0.15
    if score >= 45:
        return 2, 0.10
    return 1, 0.05


def get_v8_grade_and_description(score: float, stars: int) -> Tuple[str, str]:
    star_str = "*" * int(stars)
    if score >= 80:
        return "SSS", f"{star_str} 王者机会！10大因子全面优异，重点配置25%"
    if score >= 75:
        return "SS", f"{star_str} 完美标的！高级因子表现卓越，建议配置25%"
    if score >= 70:
        return "S+", f"{star_str} 极佳机会！多维度强势，建议配置20-25%"
    if score >= 65:
        return "S", f"{star_str} 优质标的！各项指标良好，建议配置20%"
    if score >= 60:
        return "A+", f"{star_str} 良好机会！具备明显优势，建议配置15-20%"
    if score >= 55:
        return "A", f"{star_str} 合格标的！有潜力，建议配置15%"
    if score >= 50:
        return "B+", f"{star_str} 中等机会，可参与，建议配置10-15%"
    if score >= 45:
        return "B", f"{star_str} 观察标的，少量试探，建议配置10%"
    return "C", f"{star_str} 暂不推荐，等待更好机会"


def build_v8_evaluation_result(
    *,
    version: str,
    v7_result: Dict[str, Any],
    advanced_result: Dict[str, Any],
    market_status: Dict[str, Any],
    market_penalty: float,
    atr_stops: Dict[str, Any],
    timestamp: str | None = None,
) -> Dict[str, Any]:
    score = calculate_v8_final_score(
        v7_score=float(v7_result.get("final_score", 0.0) or 0.0),
        advanced_total_score=float(advanced_result.get("total_score", 0.0) or 0.0),
        advanced_max_score=float(advanced_result.get("max_score", 100.0) or 100.0),
        market_penalty=float(market_penalty),
    )
    star_rating, position_pct = calculate_v8_star_rating(score["final_score"])
    grade, description = get_v8_grade_and_description(score["final_score"], star_rating)
    return {
        "success": True,
        "final_score": score["final_score"],
        "grade": grade,
        "star_rating": star_rating,
        "position_suggestion": position_pct,
        "description": description,
        "v7_score": score["v7_score"],
        "advanced_score": score["advanced_score"],
        "v7_weight": score["v7_weight"],
        "advanced_weight": score["advanced_weight"],
        "v7_details": v7_result,
        "advanced_factors": advanced_result,
        "market_status": market_status,
        "atr_stops": atr_stops,
        "version": version,
        "timestamp": timestamp or datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
