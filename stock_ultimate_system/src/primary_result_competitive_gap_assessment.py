from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from src.utils.project_paths import resolve_project_path


PRIMARY_RESULT_COMPETITIVE_GAP_ASSESSMENT_VERSION = "primary_result_competitive_gap_assessment.v1"


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _read_json(path: Path) -> dict[str, object]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _line_count(path: Path) -> int:
    if not path.exists():
        return 0
    return sum(1 for line in path.read_text(encoding="utf-8").splitlines() if line.strip())


def _exists(path: Path) -> bool:
    return path.exists() and path.stat().st_size >= 0


def _status(payload: dict[str, object]) -> str | None:
    value = payload.get("status")
    return str(value).strip() if value is not None else None


def _capability(
    *,
    capability_id: str,
    name: str,
    leader_standard: str,
    score: int,
    current_evidence: list[str],
    gap: str,
    next_action: str,
) -> dict[str, object]:
    return {
        "capability_id": capability_id,
        "name": name,
        "leader_standard": leader_standard,
        "score": int(max(0, min(score, 100))),
        "current_evidence": current_evidence,
        "gap": gap,
        "next_action": next_action,
    }


def build_primary_result_competitive_gap_assessment(
    *,
    exp_dir: str | Path = "data/experiments",
    artifacts_dir: str | Path = "artifacts",
    output_path: str | Path | None = None,
) -> tuple[int, dict[str, object]]:
    resolved_exp_dir = resolve_project_path(exp_dir)
    resolved_artifacts_dir = resolve_project_path(artifacts_dir)

    primary_result = _read_json(resolved_exp_dir / "primary_result_observation_latest.json")
    terminal = _read_json(resolved_exp_dir / "primary_result_terminal_latest.json")
    daily_closure = _read_json(resolved_exp_dir / "primary_result_daily_closure_latest.json")
    market_readiness = _read_json(resolved_exp_dir / "primary_result_market_data_readiness_latest.json")
    basket_pointer = _read_json(resolved_artifacts_dir / "primary_result_candidate_baskets" / "current.json")
    basket_observation = _read_json(resolved_artifacts_dir / "primary_result_candidate_baskets" / "observation_latest.json")
    baseline_pointer = _read_json(resolved_artifacts_dir / "baselines" / "current.json")
    production_readiness = _read_json(resolved_artifacts_dir / "primary_result_production_readiness" / "current.json")
    handoff_gate = _read_json(resolved_artifacts_dir / "primary_result_candidate_handoff_gate_latest.json")
    daily_scoreboard = _read_json(resolved_artifacts_dir / "primary_result_daily_operations_scoreboard_latest.json")
    feedback_loop = _read_json(resolved_exp_dir / "primary_result_feedback_loop_latest.json")
    performance_evidence = _read_json(resolved_artifacts_dir / "primary_result_performance_evidence_latest.json")
    promotion_gate = _read_json(resolved_artifacts_dir / "primary_result_promotion_readiness_gate_latest.json")

    primary_ledger_entries = _line_count(resolved_artifacts_dir / "primary_result_performance" / "ledger.jsonl")
    basket_ledger_entries = _line_count(resolved_artifacts_dir / "primary_result_candidate_baskets" / "performance_ledger.jsonl")
    baseline_active = bool(baseline_pointer.get("baseline_id"))
    terminal_outcome = str(terminal.get("terminal_outcome") or "").strip()
    observation_status = str(primary_result.get("observation_status") or "").strip()
    daily_closure_status = _status(daily_closure)
    basket_status = _status(basket_observation)
    scoreboard_status = str(daily_scoreboard.get("overall_status") or "").strip()
    handoff_decision = str(handoff_gate.get("decision") or "").strip()
    performance_evidence_status = str(performance_evidence.get("status") or "").strip()
    promotion_gate_decision = str(promotion_gate.get("decision") or "").strip()

    capabilities = [
        _capability(
            capability_id="data_quality",
            name="市场数据质量与覆盖",
            leader_standard="数据更新、基准覆盖、窗口覆盖和异常阻断每天自动运行，并形成可审计报告。",
            score=70 if market_readiness else 45,
            current_evidence=[
                f"market_data_readiness_status={_status(market_readiness) or 'missing'}",
                f"latest_update_log_present={bool(market_readiness.get('latest_update_log'))}",
            ],
            gap="当前已有 readiness 门禁，但正式窗口仍可能因交易日数据未形成而 blocked；复权、停牌、涨跌停、容量异常仍未完全制度化。",
            next_action="补充复权口径、停牌/涨跌停、容量与数据异常门禁，并纳入自动报告。",
        ),
        _capability(
            capability_id="primary_result_lifecycle",
            name="/stock 主结果生命周期",
            leader_standard="主结果从候选、审核、执行、观察、终局到绩效账本全链路自动闭合。",
            score=82 if observation_status == "observing" else 65,
            current_evidence=[
                f"observation_status={observation_status or 'missing'}",
                f"terminal_outcome={terminal_outcome or 'none'}",
                f"daily_closure_status={daily_closure_status or 'missing'}",
                f"candidate_handoff_decision={handoff_decision or 'missing'}",
            ],
            gap=(
                "candidate handoff gate 已建立；新候选与 lifecycle 不一致时会阻断 daily closure，防止对象错配。"
                if handoff_decision
                else "生命周期机制完整度较高，但当前主结果尚未形成 terminal outcome 和 performance ledger entry。"
            ),
            next_action=(
                "当 handoff_required 时先运行 lifecycle 并登记 current pointer，再允许 daily closure。"
                if handoff_decision
                else "等待正式窗口价格形成后，让 daily closure 自动完成 terminal 与 ledger 登记。"
            ),
        ),
        _capability(
            capability_id="automated_ops",
            name="自动化运营闭环",
            leader_standard="每日数据、主结果闭环、组合观察、健康检查均由调度驱动，失败和 blocked 都有报告。",
            score=88 if scoreboard_status in {"green", "yellow"} else 78 if daily_closure_status else 55,
            current_evidence=[
                f"daily_closure_report={bool(daily_closure)}",
                f"daily_closure_status={daily_closure_status or 'missing'}",
                f"basket_observation_status={basket_status or 'missing'}",
                f"daily_scoreboard_status={scoreboard_status or 'missing'}",
            ],
            gap=(
                "daily operations scoreboard 已建立；下一步差距不是再做看板，而是让它每天稳定积累 clean/blocked/failure 样本。"
                if scoreboard_status
                else "自动化 timer 已有，但尚缺统一运营总览，把各 timer 的产物合并成一张日终作战图。"
            ),
            next_action=(
                "保持 19:00 scoreboard 自动运行，并用红/黄原因驱动次日修复。"
                if scoreboard_status
                else "建立 daily operations scoreboard，汇总 update、primary closure、basket observation、healthcheck。"
            ),
        ),
        _capability(
            capability_id="candidate_basket",
            name="候选篮子组合治理",
            leader_standard="候选 TopN 不是列表，而是带权重、风险预算、历史快照、current pointer 和回滚能力的组合工件。",
            score=85 if basket_pointer.get("basket_id") else 45,
            current_evidence=[
                f"basket_current={basket_pointer.get('basket_id') or 'missing'}",
                f"basket_pointer_status={basket_pointer.get('status') or 'missing'}",
            ],
            gap="basket registry 已形成，但真实线上 basket 当前只有少量标的，尚未证明不同市场环境下的组合稳定性。",
            next_action="扩大候选生成质量验证，并把 basket performance ledger 连续积累到 20/60/120 个交易日。",
        ),
        _capability(
            capability_id="performance_evidence",
            name="长期绩效证据",
            leader_standard="主结果和组合都有连续样本的胜率、超额收益、回撤、失败分布和贡献归因。",
            score=82
            if performance_evidence_status == "ready"
            else 45 + min(primary_ledger_entries + basket_ledger_entries, 35)
            if performance_evidence_status == "accumulating"
            else 35 + min(primary_ledger_entries + basket_ledger_entries, 40),
            current_evidence=[
                f"primary_ledger_entries={primary_ledger_entries}",
                f"basket_ledger_entries={basket_ledger_entries}",
                f"performance_evidence_status={performance_evidence_status or 'missing'}",
            ],
            gap=(
                "performance evidence report 已建立；当前主要差距是连续 ledger 样本是否达到 20/60/120 证据地板。"
                if performance_evidence_status
                else "这是最大差距。机制已具备，但真实连续 ledger 样本不足，尚不能证明长期 alpha。"
            ),
            next_action=(
                "让 performance evidence 每日自动评估 20/60/120 样本，未达标前不做强结论。"
                if performance_evidence_status
                else "连续运行自动闭环，优先积累 20 个交易日，然后推进 60/120 日统计评估。"
            ),
        ),
        _capability(
            capability_id="release_baseline",
            name="Baseline、发布与生产就绪",
            leader_standard="任何策略/规则晋升都有 baseline、release decision、evidence bundle、不可变历史和 rollback。",
            score=85 if promotion_gate_decision == "promotion_review_allowed" else 76 if promotion_gate_decision in {"blocked", "freeze"} else 78 if baseline_active else 60,
            current_evidence=[
                f"baseline_active={baseline_active}",
                f"production_readiness_present={bool(production_readiness)}",
                f"promotion_gate_decision={promotion_gate_decision or 'missing'}",
            ],
            gap=(
                "promotion readiness gate 已建立；晋升现在受 performance evidence 与 review queue 双重约束。"
                if promotion_gate_decision
                else "baseline 机制已建立，但当前生产就绪仍依赖 terminal success 与绩效账本，尚未形成完整 ready 结论。"
            ),
            next_action=(
                "只有 promotion gate 允许进入评审后，才运行 release decision 与 baseline promotion。"
                if promotion_gate_decision
                else "在 terminal success 和 ledger entry 形成后运行 production readiness，并绑定 baseline 当前指针。"
            ),
        ),
        _capability(
            capability_id="learning_loop",
            name="失败归因与反馈学习",
            leader_standard="失败不是终点，而是自动进入归因、review queue、benchmark plan、release gate 和 baseline revalidation。",
            score=78
            if feedback_loop.get("status") in {"completed", "skipped"}
            else 70
            if _exists(resolved_exp_dir / "primary_result_failure_attribution_latest.json")
            or _exists(resolved_exp_dir / "primary_result_learning_feedback_latest.json")
            else 50,
            current_evidence=[
                f"failure_attribution_present={_exists(resolved_exp_dir / 'primary_result_failure_attribution_latest.json')}",
                f"learning_feedback_present={_exists(resolved_exp_dir / 'primary_result_learning_feedback_latest.json')}",
                f"feedback_loop_status={feedback_loop.get('status') or 'missing'}",
            ],
            gap=(
                "feedback loop 已建立；下一步差距是用真实 failed/weak-success terminal 样本持续验证归因质量。"
                if feedback_loop.get("status") in {"completed", "skipped"}
                else "反馈机制存在，但还没有被每日 terminal outcome 持续驱动。"
            ),
            next_action=(
                "保持 feedback loop 自动运行，并把 open review queue 作为治理待办处理。"
                if feedback_loop.get("status") in {"completed", "skipped"}
                else "让 terminal failed 自动触发 attribution 和 learning feedback，但继续禁止自动改策略。"
            ),
        ),
    ]

    average_score = round(sum(int(item["score"]) for item in capabilities) / len(capabilities), 2)
    critical_gaps = [
        item["capability_id"]
        for item in capabilities
        if int(item["score"]) < 60
    ]
    payload = {
        "assessment_version": PRIMARY_RESULT_COMPETITIVE_GAP_ASSESSMENT_VERSION,
        "generated_at": _utc_now_iso(),
        "benchmark_model": "industry_leader_public_capability_archetype",
        "truth_boundary": (
            "This assessment does not claim access to any competitor's private implementation. "
            "It compares current local evidence against a strict industry-leading capability model."
        ),
        "overall_score": average_score,
        "positioning": (
            "strong challenger" if average_score >= 75 else "credible challenger" if average_score >= 60 else "early challenger"
        ),
        "critical_gaps": critical_gaps,
        "capabilities": capabilities,
        "priority_actions": [
            "Accumulate primary and basket performance ledger entries for 20/60/120 trading days.",
            "Keep the daily operations scoreboard running and use red/yellow reasons as the operating repair queue.",
            "Add stronger market data quality gates for adjustment, suspension, limit-up/down, liquidity capacity, and anomaly handling.",
            "Keep terminal failed outcomes wired into attribution, learning feedback, and review queue without auto-changing production rules.",
        ],
    }
    if output_path:
        _write_json(resolve_project_path(output_path), payload)
    return (0 if average_score >= 60 else 1), payload
