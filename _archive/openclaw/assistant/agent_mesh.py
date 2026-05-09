from dataclasses import dataclass
from typing import Dict, List


@dataclass(frozen=True)
class AgentSpec:
    agent_id: str
    layer: str
    role: str
    enabled: bool = True


_AGENTS: List[AgentSpec] = [
    AgentSpec("intent_router", "core", "问题意图识别"),
    AgentSpec("task_decomposer", "core", "任务拆解"),
    AgentSpec("skill_orchestrator", "core", "skills编排"),
    AgentSpec("context_builder", "core", "上下文组装"),
    AgentSpec("prompt_architect", "core", "提示词构建"),
    AgentSpec("query_rewriter", "core", "问句重写"),
    AgentSpec("tool_planner", "core", "工具调用计划"),
    AgentSpec("response_synth", "core", "结果合成"),
    AgentSpec("confidence_estimator", "core", "置信度估计"),
    AgentSpec("fallback_router", "core", "回退路由"),
    AgentSpec("memory_short", "memory", "短期会话记忆"),
    AgentSpec("memory_long", "memory", "长期记忆"),
    AgentSpec("profile_agent", "memory", "用户画像"),
    AgentSpec("preference_agent", "memory", "风险偏好"),
    AgentSpec("history_reasoner", "memory", "历史行为提取"),
    AgentSpec("lesson_miner", "memory", "经验教训提炼"),
    AgentSpec("rule_updater", "memory", "规则更新"),
    AgentSpec("drift_detector", "memory", "偏移检测"),
    AgentSpec("forgetting_manager", "memory", "记忆衰减"),
    AgentSpec("memory_auditor", "memory", "记忆审计"),
    AgentSpec("market_regime_agent", "research", "市场状态识别"),
    AgentSpec("breadth_agent", "research", "市场宽度"),
    AgentSpec("stock_snapshot_agent", "research", "个股快照"),
    AgentSpec("trend_agent", "research", "趋势结构"),
    AgentSpec("flow_agent", "research", "资金量能"),
    AgentSpec("valuation_agent", "research", "估值判断"),
    AgentSpec("fundamental_agent", "research", "基本面分析"),
    AgentSpec("event_news_agent", "research", "事件新闻冲击"),
    AgentSpec("sector_rotation_agent", "research", "板块轮动"),
    AgentSpec("macro_policy_agent", "research", "宏观政策映射"),
    AgentSpec("signal_agent", "trading", "信号生成"),
    AgentSpec("alpha_agent", "trading", "因子打分"),
    AgentSpec("risk_gate_agent", "trading", "风险闸门"),
    AgentSpec("position_sizing_agent", "trading", "仓位计算"),
    AgentSpec("portfolio_agent", "trading", "组合约束"),
    AgentSpec("execution_agent", "trading", "执行动作"),
    AgentSpec("stoploss_agent", "trading", "止损策略"),
    AgentSpec("takeprofit_agent", "trading", "止盈策略"),
    AgentSpec("slippage_cost_agent", "trading", "交易成本估计"),
    AgentSpec("scenario_stress_agent", "trading", "压力测试"),
    AgentSpec("backtest_agent", "ops", "回测解释"),
    AgentSpec("ab_test_agent", "ops", "A/B实验"),
    AgentSpec("evolution_agent", "ops", "自进化评估"),
    AgentSpec("skill_sync_agent", "ops", "技能同步"),
    AgentSpec("skill_installer_agent", "ops", "技能安装"),
    AgentSpec("skill_safety_agent", "ops", "技能安全审查"),
    AgentSpec("quality_guard_agent", "ops", "回答质量守卫"),
    AgentSpec("monitoring_agent", "ops", "24小时监控"),
    AgentSpec("alert_agent", "ops", "告警通知"),
    AgentSpec("governance_agent", "ops", "权限审计治理"),
]

AGENT_VERSION = "mesh50-v1"


def list_agent_specs() -> List[Dict[str, str]]:
    return [
        {"agent_id": a.agent_id, "layer": a.layer, "role": a.role, "enabled": "1" if a.enabled else "0"}
        for a in _AGENTS
    ]


def count_agents() -> int:
    return len(_AGENTS)


def select_agents(question: str, route: str, confidence: float, max_agents: int = 10) -> List[str]:
    q = (question or "").lower()
    base = [
        "intent_router",
        "task_decomposer",
        "skill_orchestrator",
        "context_builder",
        "response_synth",
        "confidence_estimator",
    ]
    if route in ("stock_core", "hybrid_research"):
        base.extend(
            [
                "stock_snapshot_agent",
                "trend_agent",
                "flow_agent",
                "risk_gate_agent",
                "portfolio_agent",
                "execution_agent",
            ]
        )
    elif route == "skills":
        base.extend(["skill_sync_agent", "skill_installer_agent", "skill_safety_agent", "monitoring_agent"])
    elif route == "cross_discipline":
        base.extend(["macro_policy_agent", "history_reasoner", "lesson_miner"])
    else:
        base.extend(["fallback_router", "quality_guard_agent"])

    if any(k in q for k in ("回测", "胜率", "回撤", "参数")):
        base.extend(["backtest_agent", "ab_test_agent", "evolution_agent"])
    if any(k in q for k in ("新闻", "公告", "事件")):
        base.append("event_news_agent")
    if any(k in q for k in ("板块", "行业", "轮动")):
        base.append("sector_rotation_agent")
    if any(k in q for k in ("估值", "pe", "pb")):
        base.append("valuation_agent")
    if any(k in q for k in ("基本面", "财报", "roe")):
        base.append("fundamental_agent")
    if confidence < 60:
        base.extend(["fallback_router", "quality_guard_agent"])

    ordered: List[str] = []
    for aid in base:
        if aid not in ordered:
            ordered.append(aid)
    return ordered[: max(3, int(max_agents))]
