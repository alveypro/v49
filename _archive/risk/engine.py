from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List


@dataclass
class DualRiskResult:
    market_risk: str
    system_risk: str
    risk_level: str
    triggered_rules: List[str]
    recommended_actions: List[str]
    evidence: Dict[str, Any]


def evaluate_market_risk(stats: Dict[str, Any], thresholds: Dict[str, Any]) -> tuple[str, List[str]]:
    triggered: List[str] = []
    win_rate = float(stats.get("win_rate", 0.0) or 0.0)
    max_dd = float(stats.get("max_drawdown", 0.0) or 0.0)
    density = float(stats.get("signal_density", 0.0) or 0.0)

    if win_rate < float(thresholds.get("win_rate_min", 0.0)):
        triggered.append("win_rate_breach")
    if max_dd > float(thresholds.get("max_drawdown_max", 1.0)):
        triggered.append("drawdown_breach")
    if density < float(thresholds.get("signal_density_min", 0.0)):
        triggered.append("signal_density_collapse")

    if "drawdown_breach" in triggered:
        return "red", triggered
    if triggered:
        return "orange", triggered
    return "green", triggered


def evaluate_system_risk(health: Dict[str, Any]) -> tuple[str, List[str]]:
    triggered: List[str] = []
    if not bool(health.get("db_reachable", True)):
        triggered.append("db_unreachable")
    if not bool(health.get("compile_ok", True)):
        triggered.append("compile_failed")
    stale_days = int(health.get("db_stale_days", 0) or 0)
    stale_limit = int(health.get("db_stale_limit", 3) or 3)
    if stale_days > stale_limit:
        triggered.append("db_stale")

    if any(x in triggered for x in ["db_unreachable", "compile_failed"]):
        return "red", triggered
    if triggered:
        return "yellow", triggered
    return "green", triggered


def combine_risk(
    market_stats: Dict[str, Any],
    thresholds: Dict[str, Any],
    system_health: Dict[str, Any],
) -> DualRiskResult:
    market_risk, market_rules = evaluate_market_risk(market_stats, thresholds)
    system_risk, system_rules = evaluate_system_risk(system_health)
    all_rules = market_rules + system_rules

    order = {"green": 0, "yellow": 1, "orange": 2, "red": 3}
    risk_level = market_risk if order[market_risk] >= order[system_risk] else system_risk

    if risk_level == "red":
        actions = ["stop automation", "manual review required"]
    elif risk_level == "orange":
        actions = ["reduce aggressiveness", "manual review required"]
    elif risk_level == "yellow":
        actions = ["review warnings before next run"]
    else:
        actions = ["keep current mode"]

    return DualRiskResult(
        market_risk=market_risk,
        system_risk=system_risk,
        risk_level=risk_level,
        triggered_rules=all_rules,
        recommended_actions=actions,
        evidence={"market_stats": market_stats, "system_health": system_health},
    )
