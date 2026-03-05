from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List


@dataclass(frozen=True)
class StrategyProfile:
    name: str
    default_score_threshold: int
    default_sample_size: int
    default_holding_days: int
    tier: str = "experimental"     # "production" | "experimental"
    role: str = ""                 # 角色说明
    ui_tag: str = "secondary"


# ── 生产策略 (默认运行) ──────────────────────────────────────
# v9  中线主策略: 15日持仓, 综合量价+资金流+板块轮动
# v8  风控主策略: 8日持仓, 极致风控+机构行为识别
# v5  趋势启动补充: 5日短持仓, 启动确认+爆发力筛选
# combo 统一决策出口: 多策略投票融合, 最终输出

# ── 实验策略 (手动开关, 不影响主流程) ────────────────────────
# v4  研究基座: 与v5重叠高, 保留做因子研究
# v6  超短周期: 噪声大, 不适合中心默认
# v7  过渡版本: 功能被v8/v9覆盖
# ai  AI选股: 辅助工具, 不作为中心主策略

STRATEGIES: Dict[str, StrategyProfile] = {
    # ── 生产策略 ──
    "v9": StrategyProfile("v9", 60, 500, 15, tier="production", role="中线主策略", ui_tag="primary"),
    "v8": StrategyProfile("v8", 50, 400, 8,  tier="production", role="风控主策略", ui_tag="primary"),
    "v5": StrategyProfile("v5", 60, 500, 5,  tier="production", role="趋势启动补充", ui_tag="primary"),
    "combo": StrategyProfile("combo", 68, 500, 10, tier="production", role="统一决策出口", ui_tag="primary"),
    # ── 实验策略 ──
    "v4": StrategyProfile("v4", 60, 400, 5, tier="experimental", role="因子研究基座"),
    "v6": StrategyProfile("v6", 75, 400, 5, tier="experimental", role="超短周期实验"),
    "v7": StrategyProfile("v7", 60, 400, 8, tier="experimental", role="过渡版本"),
    "stable": StrategyProfile("stable", 60, 300, 10, tier="experimental", role="稳健实验"),
    "ai": StrategyProfile("ai", 60, 500, 10, tier="experimental", role="AI辅助选股"),
}


def get_profile(strategy: str) -> StrategyProfile:
    key = (strategy or "").lower()
    if key not in STRATEGIES:
        raise KeyError(f"unknown strategy: {strategy}")
    return STRATEGIES[key]


def production_strategies() -> List[str]:
    """生产策略列表 — 默认运行、默认验证、默认告警。"""
    return [k for k, v in STRATEGIES.items() if v.tier == "production"]


def experimental_strategies() -> List[str]:
    """实验策略列表 — 手动开关, 不进入默认流水线。"""
    return [k for k, v in STRATEGIES.items() if v.tier == "experimental"]


def all_strategy_names() -> List[str]:
    return list(STRATEGIES.keys())


# backward compat
def ui_primary_strategies() -> list[str]:
    return production_strategies()
