from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List


@dataclass(frozen=True)
class StrategyProfile:
    name: str
    default_score_threshold: int
    default_sample_size: int
    default_holding_days: int
    tier: str = "research"  # "production" | "canary" | "shadow" | "research" | "retired"
    stage: str = "research"
    role: str = ""
    ui_tag: str = "secondary"
    evidence_status: str = "research_only"


STRATEGIES: Dict[str, StrategyProfile] = {
    "v9": StrategyProfile(
        "v9",
        60,
        500,
        15,
        tier="canary",
        stage="canary_candidate",
        role="中线小流量候选；需通过统一 walk-forward 证据门",
        ui_tag="primary",
        evidence_status="canary_pending_gate",
    ),
    "stable": StrategyProfile(
        "stable",
        60,
        300,
        10,
        tier="shadow",
        stage="defensive_shadow_candidate",
        role="防守型影子候选；验证回撤控制价值，不直接贡献Top5",
        evidence_status="shadow_pending_gate",
    ),
    "v8": StrategyProfile(
        "v8",
        50,
        400,
        8,
        tier="shadow",
        stage="shadow_candidate",
        role="风控影子候选；不得直接生产执行",
        evidence_status="shadow_pending_gate",
    ),
    "combo": StrategyProfile(
        "combo",
        68,
        500,
        10,
        tier="shadow",
        stage="shadow_arbiter",
        role="统一决策影子仲裁器；依赖底层策略证据",
        evidence_status="shadow_pending_gate",
    ),
    "v5": StrategyProfile(
        "v5",
        60,
        500,
        5,
        tier="shadow",
        stage="baseline",
        role="历史基线；只用于对照，不作为生产主策略",
        evidence_status="baseline_only",
    ),
    "v4": StrategyProfile("v4", 60, 400, 5, tier="research", stage="benchmark", role="因子研究基座", evidence_status="research_only"),
    "v6": StrategyProfile("v6", 75, 400, 5, tier="research", stage="research", role="超短周期实验；不得进入生产", evidence_status="research_only"),
    "v7": StrategyProfile("v7", 60, 400, 8, tier="retired", stage="retired", role="过渡版本；冻结或诊断用", evidence_status="retired"),
    "ai": StrategyProfile("ai", 60, 500, 10, tier="research", stage="research", role="AI辅助研究；缺少生产级模型合同", evidence_status="research_only"),
    "ensemble_core": StrategyProfile(
        "ensemble_core",
        60,
        500,
        10,
        tier="research",
        stage="research_framework",
        role="组合级多alpha研究线",
        evidence_status="research_only",
    ),
}


def get_profile(strategy: str) -> StrategyProfile:
    key = (strategy or "").lower()
    if key not in STRATEGIES:
        raise KeyError(f"unknown strategy: {strategy}")
    return STRATEGIES[key]


def production_strategies() -> List[str]:
    return [k for k, v in STRATEGIES.items() if v.tier == "production"]


def experimental_strategies() -> List[str]:
    return [k for k, v in STRATEGIES.items() if v.tier in {"research", "retired"}]


def primary_strategies() -> List[str]:
    primaries = [k for k, v in STRATEGIES.items() if v.ui_tag == "primary"]
    return primaries or canary_strategies()


def production_candidate_strategies() -> List[str]:
    return canary_strategies() + shadow_strategies()


def canary_strategies() -> List[str]:
    return [k for k, v in STRATEGIES.items() if v.tier == "canary"]


def shadow_strategies() -> List[str]:
    return [k for k, v in STRATEGIES.items() if v.tier == "shadow"]


def research_strategies() -> List[str]:
    return [k for k, v in STRATEGIES.items() if v.tier == "research"]


def retired_strategies() -> List[str]:
    return [k for k, v in STRATEGIES.items() if v.tier == "retired"]


def all_strategy_names() -> List[str]:
    return list(STRATEGIES.keys())


def ui_primary_strategies() -> list[str]:
    return primary_strategies()
