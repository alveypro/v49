from __future__ import annotations

from typing import Any


_STAGE_LABELS = {
    "L1": "研究阶段",
    "L2": "候选阶段",
    "L3": "审核阶段",
    "L4": "执行准备阶段",
    "L5": "终局阶段",
}

_SURFACE_STATUS_LABELS = {
    "approved": "已对齐",
    "blocked": "待复核",
    "failed": "待复核",
    "partial_success": "待补齐",
    "running": "进行中",
    "completed": "已完成",
    "fresh": "已对齐",
    "stale": "待补齐",
}


def _text(value: object, fallback: str = "-") -> str:
    text = str(value or "").strip()
    return text if text else fallback


def _surface_status(value: object, fallback: str = "-") -> str:
    text = _text(value, fallback)
    return _SURFACE_STATUS_LABELS.get(text, text)


def _surface_candidate_code(value: object, fallback: str = "等待候选形成") -> str:
    text = _text(value, fallback)
    if text in {"暂无", "-", "对象暂缺"}:
        return fallback
    return text


def _surface_runtime_detail(value: object) -> str:
    text = _text(value, "")
    lowered = text.lower()
    if not text:
        return "当前候选链仍需补证。"
    if text.startswith("降级显示："):
        return "降级说明。"
    if "top_industry" in lowered:
        return "行业集中度约束待复核。"
    if "gate blocked" in lowered:
        return "候选门禁待复核。"
    if "batch_prediction_timeout" in lowered:
        return "候选结果仍带补证痕迹。"
    if "validation_skipped" in lowered:
        return "验证样本仍在补齐。"
    return text


def _surface_generation_reason(value: object) -> str:
    text = _text(value, "")
    lowered = text.lower()
    if not text or text in {"-", "完整候选结果", "formal_result", "formal", "completed"}:
        return ""
    if "batch_prediction_timeout" in lowered:
        return "当前结果带补证痕迹"
    if "validation_skipped" in lowered:
        return "当前结果仍需补齐验证样本"
    return "当前结果仍需补证"


def _stage_label(primary_result: dict[str, Any]) -> str:
    return _STAGE_LABELS.get(_text(primary_result.get("result_lifecycle_stage"), ""), "阶段待确认")


def _conclusion_tone(primary_result: dict[str, Any]) -> str:
    if _text(primary_result.get("disabled_reason"), ""):
        return "当前主结果已进入受限状态，禁止页面层继续正向放大"
    if _text(primary_result.get("invalid_reason"), ""):
        return "当前主结果已失效或终结，页面只允许展示制度结论"
    return "当前主结果以唯一 pointer 为准，页面不再自行推断第二结论"


def _conclusion_detail(
    primary_result: dict[str, Any],
    *,
    current_basket_pointer_status: str,
    current_basket_pointer_basket_id: str,
    current_basket_pointer_updated_at: str,
    latest_basket_attempt_status: str,
    latest_basket_attempt_generated_at: str,
    latest_basket_attempt_blocking_reason: str,
) -> str:
    result_id = _text(primary_result.get("result_id"), "primary:unavailable")
    ts_code = _text(primary_result.get("ts_code"), "对象暂缺")
    stock_name = _text(primary_result.get("stock_name"), "名称暂缺")
    stage_label = _stage_label(primary_result)
    run_id = _text(primary_result.get("run_id"), "run_id 暂缺")
    lifecycle_id = _text(primary_result.get("lifecycle_id"), "lifecycle_id 暂缺")
    as_of_date = _text(primary_result.get("as_of_date"), "as_of_date 暂缺")
    detail = (
        f"主结果 {result_id} ｜ {ts_code} {stock_name} ｜ {stage_label} ｜ "
        f"run {run_id} ｜ lifecycle {lifecycle_id} ｜ as_of_date {as_of_date}。"
    )
    disabled_reason = _text(primary_result.get("disabled_reason"), "")
    invalid_reason = _text(primary_result.get("invalid_reason"), "")
    if disabled_reason:
        detail += f" 当前阻断：{disabled_reason}。"
    elif invalid_reason:
        detail += f" 当前状态：{invalid_reason}。"
    else:
        detail += " 当前无制度级禁行结论。"
    if current_basket_pointer_status not in {"-", "unknown"}:
        detail += (
            f" 候选篮指针 {current_basket_pointer_status} ｜ {current_basket_pointer_basket_id} ｜ "
            f"{current_basket_pointer_updated_at}。"
        )
    if latest_basket_attempt_status == "blocked":
        detail += f" 最新候选篮尝试于 {latest_basket_attempt_generated_at} 被阻断：{latest_basket_attempt_blocking_reason}。"
    return detail


def _summary_lines(primary_result: dict[str, Any], *, health_status: str, backtest_conclusion: str) -> list[str]:
    sync_note = _surface_runtime_detail(primary_result.get("data_sync_note")).rstrip("。")
    lines = [
        f"主结果对象 {_text(primary_result.get('result_id'), 'primary:unavailable')} 是当前唯一业务结论入口。",
        f"当前阶段 {_stage_label(primary_result)}，数据同步说明：{sync_note}。",
        f"系统健康 {health_status}，研究回测结论 {backtest_conclusion}，它们只作为上下文，不覆盖主结果制度事实。",
    ]
    disabled_reason = _text(primary_result.get("disabled_reason"), "")
    invalid_reason = _text(primary_result.get("invalid_reason"), "")
    if disabled_reason:
        lines.append(f"制度阻断：{disabled_reason}")
    elif invalid_reason:
        lines.append(f"制度终局：{invalid_reason}")
    return lines


def build_primary_result_query_view(
    *,
    primary_result: dict[str, Any],
    health_status: str,
    backtest_conclusion: str,
    current_basket_pointer_status: str,
    current_basket_pointer_basket_id: str,
    current_basket_pointer_updated_at: str,
    latest_basket_attempt_status: str,
    latest_basket_attempt_generated_at: str,
    latest_basket_attempt_blocking_reason: str,
) -> dict[str, Any]:
    headline_tone = _conclusion_tone(primary_result)
    headline_detail = _conclusion_detail(
        primary_result,
        current_basket_pointer_status=current_basket_pointer_status,
        current_basket_pointer_basket_id=current_basket_pointer_basket_id,
        current_basket_pointer_updated_at=current_basket_pointer_updated_at,
        latest_basket_attempt_status=latest_basket_attempt_status,
        latest_basket_attempt_generated_at=latest_basket_attempt_generated_at,
        latest_basket_attempt_blocking_reason=latest_basket_attempt_blocking_reason,
    )
    return {
        "headline_tone": headline_tone,
        "headline_detail": headline_detail,
        "summary_lines": _summary_lines(
            primary_result,
            health_status=health_status,
            backtest_conclusion=backtest_conclusion,
        ),
        "primary_conclusion": {
            "result_id": _text(primary_result.get("result_id"), "primary:unavailable"),
            "ts_code": _text(primary_result.get("ts_code"), "对象暂缺"),
            "stock_name": _text(primary_result.get("stock_name"), "名称暂缺"),
            "stage_label": _stage_label(primary_result),
            "disabled_reason": _text(primary_result.get("disabled_reason"), ""),
            "invalid_reason": _text(primary_result.get("invalid_reason"), ""),
            "history_source_file": _text(primary_result.get("history_source_file"), ""),
            "history_generation_mode": _text(primary_result.get("history_generation_mode"), ""),
            "run_id": _text(primary_result.get("run_id"), ""),
            "lifecycle_id": _text(primary_result.get("lifecycle_id"), ""),
            "as_of_date": _text(primary_result.get("as_of_date"), ""),
        },
    }


def build_namespace_home_semantics(
    *,
    primary_result: dict[str, Any],
    candidate_artifact_status: dict[str, Any],
    prefilter_artifact_status: dict[str, Any],
    governance_release_readiness: dict[str, Any],
    market_snapshot: dict[str, Any],
    bt_diag: dict[str, Any],
    basket_summary: dict[str, Any],
    candidates_audit: dict[str, Any],
    evolution_status: dict[str, Any],
    evolution_registry: dict[str, Any],
    candidate_cards: list[dict[str, Any]],
    liquidity_capacity_state: str,
    current_basket_pointer_status: str,
    latest_basket_attempt_status: str,
    latest_basket_attempt_generated_at: str,
    latest_basket_attempt_blocking_reason: str,
    db_latest_trade_date: str,
) -> dict[str, Any]:
    return {
        "decision": _build_decision_semantics(primary_result=primary_result, market_snapshot=market_snapshot, bt_diag=bt_diag),
        "blocker": _build_blocker_semantics(
            candidate_artifact_status=candidate_artifact_status,
            evolution_status=evolution_status,
            latest_basket_attempt_status=latest_basket_attempt_status,
            latest_basket_attempt_generated_at=latest_basket_attempt_generated_at,
            latest_basket_attempt_blocking_reason=latest_basket_attempt_blocking_reason,
        ),
        "execution": _build_execution_semantics(
            candidate_artifact_status=candidate_artifact_status,
            prefilter_artifact_status=prefilter_artifact_status,
            governance_release_readiness=governance_release_readiness,
            current_basket_pointer_status=current_basket_pointer_status,
            latest_basket_attempt_status=latest_basket_attempt_status,
            db_latest_trade_date=db_latest_trade_date,
        ),
        "evidence": _build_evidence_semantics(
            candidate_cards=candidate_cards,
            basket_summary=basket_summary,
            candidates_audit=candidates_audit,
            evolution_status=evolution_status,
            candidate_artifact_status=candidate_artifact_status,
            liquidity_capacity_state=liquidity_capacity_state,
        ),
        "governance": _build_governance_semantics(
            evolution_status=evolution_status,
            evolution_registry=evolution_registry,
        ),
    }


def _build_decision_semantics(*, primary_result: dict[str, Any], market_snapshot: dict[str, Any], bt_diag: dict[str, Any]) -> dict[str, str]:
    top_code = _surface_candidate_code(primary_result.get("ts_code"), "等待候选形成")
    top_name = _text(primary_result.get("stock_name"))
    signal = _text(primary_result.get("signal_level"))
    risk = _text(primary_result.get("risk_level"))
    regime = _text(market_snapshot.get("dominant_regime"))
    risk_preference = _text(market_snapshot.get("risk_preference"))
    conclusion = _text(bt_diag.get("结论"), "未评估")
    result_id = _text(primary_result.get("result_id"), "primary:unavailable")
    stage = _text(primary_result.get("result_lifecycle_stage"))
    return {
        "headline": f"{result_id} 是当前唯一主结论对象",
        "headline_subtitle": f"页面只读 {stage} 阶段主结果，市场状态仅作上下文",
        "top_candidate_code": top_code,
        "top_candidate_name": top_name,
        "top_candidate_signal": signal,
        "top_candidate_risk": risk,
        "market_regime": regime,
        "risk_preference": risk_preference,
        "strategy_conclusion": conclusion,
        "summary_line": f"{result_id} ｜ {top_code} {top_name} ｜ 信号 {signal} ｜ 风险 {risk} ｜ 上下文结论 {conclusion}",
    }


def _build_blocker_semantics(
    *,
    candidate_artifact_status: dict[str, Any],
    evolution_status: dict[str, Any],
    latest_basket_attempt_status: str,
    latest_basket_attempt_generated_at: str,
    latest_basket_attempt_blocking_reason: str,
) -> dict[str, str | bool]:
    runtime_status = _text(candidate_artifact_status.get("runtime_status"))
    runtime_stage_label = _text(candidate_artifact_status.get("runtime_stage_label"))
    runtime_detail = _text(candidate_artifact_status.get("runtime_detail"))
    runtime_results_ready = _text(candidate_artifact_status.get("runtime_results_ready"))
    runtime_elapsed_sec = _text(candidate_artifact_status.get("runtime_elapsed_sec"))
    capacity_gate = _text(evolution_status.get("latest_capacity_gate_status"))
    capacity_state = _text(evolution_status.get("latest_capacity_state"))
    capacity_profile = _text(evolution_status.get("latest_capacity_profile"))
    capacity_score = _text(evolution_status.get("latest_capacity_stress_score"), "0.0")
    feedback_gate = _text(evolution_status.get("latest_feedback_gate_status"))
    feedback_level = _text(evolution_status.get("latest_feedback_level"))
    feedback_window = _text(evolution_status.get("latest_feedback_window"))
    feedback_summary = _text(evolution_status.get("latest_feedback_summary"))
    if runtime_status == "failed":
        return {
            "has_blocker": True,
            "blocker_title": "候选主链待复核",
            "blocker_detail": _surface_runtime_detail(runtime_detail) or "候选主链仍需人工复核。",
            "blocker_level": "critical",
            "blocker_source": "runtime",
            "blocker_action_hint": "先恢复候选主链，再讨论任何推进动作。",
        }
    if runtime_status == "running":
        return {
            "has_blocker": True,
            "blocker_title": "候选主链处理中",
            "blocker_detail": f"已处理 {runtime_results_ready} 个结果 ｜ 已耗时 {runtime_elapsed_sec}s ｜ {_surface_runtime_detail(runtime_detail)}",
            "blocker_level": "warning",
            "blocker_source": "runtime",
            "blocker_action_hint": "等待候选链完成后再判断是否推进。",
        }
    if capacity_gate in {"reject", "容量阻断"} or capacity_state in {"stretched", "容量受限"}:
        return {
            "has_blocker": True,
            "blocker_title": "容量门禁阻断当前晋级",
            "blocker_detail": f"最差压力分 {capacity_score} ｜ 建议档位 {capacity_profile} ｜ 当前状态 {capacity_state}",
            "blocker_level": "critical",
            "blocker_source": "capacity",
            "blocker_action_hint": "保持收紧，不允许按当前档位继续放大。",
        }
    if feedback_gate in {"observe", "review", "复核观察"}:
        return {
            "has_blocker": True,
            "blocker_title": "观察回灌要求当前版本先复核",
            "blocker_detail": f"{feedback_level} ｜ {feedback_window} ｜ {feedback_summary}",
            "blocker_level": "warning",
            "blocker_source": "feedback",
            "blocker_action_hint": "先完成复核，再决定是否恢复推进。",
        }
    if latest_basket_attempt_status == "blocked":
        return {
            "has_blocker": True,
            "blocker_title": "候选篮待复核",
            "blocker_detail": f"{latest_basket_attempt_generated_at} ｜ {_surface_runtime_detail(latest_basket_attempt_blocking_reason)}",
            "blocker_level": "warning",
            "blocker_source": "basket_attempt",
            "blocker_action_hint": "先解决候选篮阻断原因，再更新执行判断。",
        }
    return {
        "has_blocker": False,
        "blocker_title": "当前无显式阻断",
        "blocker_detail": "主链、门禁和候选篮未出现优先级更高的阻断条件。",
        "blocker_level": "info",
        "blocker_source": "none",
        "blocker_action_hint": "按当前执行资格判断推进。",
    }


def _build_execution_semantics(
    *,
    candidate_artifact_status: dict[str, Any],
    prefilter_artifact_status: dict[str, Any],
    governance_release_readiness: dict[str, Any],
    current_basket_pointer_status: str,
    latest_basket_attempt_status: str,
    db_latest_trade_date: str,
) -> dict[str, str]:
    runtime_status = _text(candidate_artifact_status.get("runtime_status"))
    generated_at = _text(candidate_artifact_status.get("generated_at"))
    basket_generated_at = _text(candidate_artifact_status.get("basket_generated_at"))
    generation_reason = _text(candidate_artifact_status.get("generation_reason"))
    freshness_status = _text(prefilter_artifact_status.get("freshness_status"))
    ready_for_release = bool(governance_release_readiness.get("ready_for_release", False))
    decision_action = "继续观察"
    decision_action_reason = "默认保持观察，避免在证据不足时过早推进。"
    execution_eligibility = "仅研究可见"
    execution_eligibility_reason = "当前默认仍以研究判断为主。"
    if runtime_status in {"running", "failed"}:
        decision_action_reason = "候选主链尚未稳定闭合。"
        execution_eligibility_reason = "运行链未完成，不应进入执行判断。"
    elif latest_basket_attempt_status == "blocked":
        decision_action = "进入复核"
        decision_action_reason = "候选篮最近一次尝试被阻断。"
        execution_eligibility = "不可执行"
        execution_eligibility_reason = "候选篮未通过当前约束。"
    elif current_basket_pointer_status == "approved" and ready_for_release:
        decision_action = "允许推进"
        decision_action_reason = "当前生效篮子已通过现有主门禁。"
        execution_eligibility = "可执行"
        execution_eligibility_reason = "当前结果已具备进入下一步的基础条件。"
    decision_validity_status = "fresh"
    decision_validity_label = "当前判断与最近产物一致"
    if freshness_status in {"stale", "blocked"}:
        decision_validity_status = "stale"
        decision_validity_label = "当前候选产物仍待补齐到最新交易日"
    generation_reason_label = _surface_generation_reason(generation_reason)
    if generation_reason_label:
        decision_validity_status = "partial"
        decision_validity_label = generation_reason_label
    return {
        "decision_action": decision_action,
        "decision_action_reason": decision_action_reason,
        "execution_eligibility": execution_eligibility,
        "execution_eligibility_reason": execution_eligibility_reason,
        "decision_validity_status": decision_validity_status,
        "decision_validity_label": decision_validity_label,
        "decision_valid_until_label": generated_at,
        "candidate_generated_at": generated_at,
        "basket_generated_at": basket_generated_at,
        "db_latest_trade_date": db_latest_trade_date,
    }


def _build_evidence_semantics(
    *,
    candidate_cards: list[dict[str, Any]],
    basket_summary: dict[str, Any],
    candidates_audit: dict[str, Any],
    evolution_status: dict[str, Any],
    candidate_artifact_status: dict[str, Any],
    liquidity_capacity_state: str,
) -> dict[str, str]:
    top_card = candidate_cards[0] if len(candidate_cards) >= 1 else {}
    second_card = candidate_cards[1] if len(candidate_cards) >= 2 else {}
    top_score = float(top_card.get("final_score", 0.0) or 0.0)
    second_score = float(second_card.get("final_score", 0.0) or 0.0)
    score_gap = top_score - second_score
    weighted_liquidity_score = float(basket_summary.get("weighted_liquidity_score", 0.0) or 0.0)
    liquidity_capacity_weight = float(basket_summary.get("liquidity_capacity_weight", 0.0) or 0.0)
    capacity_profile = _text(evolution_status.get("latest_capacity_profile"))
    capacity_stress_score = _text(evolution_status.get("latest_capacity_stress_score"), "0.0")
    runtime_stage_label = _text(candidate_artifact_status.get("runtime_stage_label"))
    runtime_results_ready = _text(candidate_artifact_status.get("runtime_results_ready"))
    runtime_elapsed_sec = _text(candidate_artifact_status.get("runtime_elapsed_sec"))
    runtime_detail = _text(candidate_artifact_status.get("runtime_detail"))
    runtime_status = _text(candidate_artifact_status.get("runtime_status"))
    audit_rows = candidates_audit.get("rows", []) if isinstance(candidates_audit, dict) else []
    if not isinstance(audit_rows, list):
        audit_rows = []
    top_code = _text(top_card.get("ts_code"), "")
    top_audit = next((row for row in audit_rows if _text((row or {}).get("ts_code"), "") == top_code), {})
    selection_reason = _text((top_audit or {}).get("selection_reason"), "")
    diversification_penalty = _text((top_audit or {}).get("diversification_penalty"), "")
    risk_overlay_penalty = _text((top_audit or {}).get("risk_overlay_penalty"), "")
    evidence_confidence_label = "观察中"
    if runtime_status == "completed":
        evidence_confidence_label = "已验证"
    elif runtime_status == "failed":
        evidence_confidence_label = "待补证"
    return {
        "top_candidate_advantage_reason": (
            f"{top_card.get('ts_code', '暂无')} 当前领先第二名 {score_gap:.2f} 分" if second_card else "当前只有单一主候选，缺少可比较的第二名。"
        ),
        "top_candidate_gap_reason": (
            f"第二名 {second_card.get('ts_code', '-')} 当前未能超过主候选的综合分与信号强度" if second_card else "暂无第二名差异说明。"
        ),
        "top_candidate_audit_summary": (
            f"{selection_reason} ｜ 分散惩罚 {diversification_penalty or '-'} ｜ 风险惩罚 {risk_overlay_penalty or '-'}"
            if top_audit
            else "当前 audit 摘要缺失，临时使用综合分、信号和风险等级判断。"
        ),
        "top_candidate_score": f"{top_score:.2f}",
        "second_candidate_code": _text(second_card.get("ts_code")),
        "second_candidate_name": _text(second_card.get("stock_name")),
        "second_candidate_score": f"{second_score:.2f}",
        "score_gap": f"{score_gap:.2f}",
        "capacity_summary_note": (
            f"当前建议档位 {capacity_profile} ｜ 最差压力分 {capacity_stress_score} ｜ "
            f"加权流动性分 {weighted_liquidity_score:.2f} ｜ 承载受限权重 {liquidity_capacity_weight:.1%}"
        ),
        "capacity_state": liquidity_capacity_state,
        "capacity_profile": capacity_profile,
        "capacity_stress_score": capacity_stress_score,
        "weighted_liquidity_score": f"{weighted_liquidity_score:.2f}",
        "liquidity_capacity_weight": f"{liquidity_capacity_weight:.1%}",
        "runtime_summary_note": f"{runtime_stage_label} ｜ 已处理 {runtime_results_ready} ｜ 已耗时 {runtime_elapsed_sec}s ｜ {runtime_detail}",
        "runtime_stage_label": runtime_stage_label,
        "runtime_results_ready": runtime_results_ready,
        "runtime_elapsed_sec": runtime_elapsed_sec,
        "evidence_confidence_label": evidence_confidence_label,
    }


def _build_governance_semantics(*, evolution_status: dict[str, Any], evolution_registry: dict[str, Any]) -> dict[str, Any]:
    feedback_status = _text(evolution_status.get("latest_feedback_gate_status"))
    feedback_level = _text(evolution_status.get("latest_feedback_level"))
    feedback_window = _text(evolution_status.get("latest_feedback_window"))
    feedback_summary = _text(evolution_status.get("latest_feedback_summary"))
    capacity_status = _text(evolution_status.get("latest_capacity_gate_status"))
    capacity_state = _text(evolution_status.get("latest_capacity_state"))
    capacity_profile = _text(evolution_status.get("latest_capacity_profile"))
    capacity_stress_score = _text(evolution_status.get("latest_capacity_stress_score"), "0.0")
    history_rows = evolution_status.get("history", []) or []
    if not isinstance(history_rows, list):
        history_rows = []
    gate_overall_status = "通过"
    gate_overall_reason = "当前未发现优先级更高的治理阻断。"
    governance_block_effect = "维持当前推进节奏"
    governance_block_effect_reason = "当前门禁未触发更强的收紧动作。"
    if capacity_status in {"reject", "容量阻断"}:
        gate_overall_status = "阻断"
        gate_overall_reason = "容量门禁未通过。"
        governance_block_effect = "当前版本保持收紧，不允许按当前档位放大或晋级"
        governance_block_effect_reason = f"建议档位 {capacity_profile} ｜ 最差压力分 {capacity_stress_score}"
    elif feedback_status in {"observe", "review", "复核观察"}:
        gate_overall_status = "观察"
        gate_overall_reason = "反馈门要求当前版本先复核。"
        governance_block_effect = "当前版本保持观察，不直接晋级"
        governance_block_effect_reason = f"{feedback_level} ｜ {feedback_window} ｜ {feedback_summary}"
    governance_recent_timeline: list[dict[str, str]] = []
    source_rows = history_rows[:3] if history_rows else list((evolution_registry.get("history", []) or [])[-3:])
    for item in source_rows:
        gates = (item.get("gates", {}) or {})
        capacity_gate = (gates.get("capacity_pressure", {}) or {})
        governance_recent_timeline.append(
            {
                "created_at": _text(item.get("created_at")),
                "action": _text(item.get("action")),
                "reason": _text(item.get("reason")),
                "result": _text(
                    item.get("capacity_gate_status")
                    or item.get("feedback_gate_status")
                    or ("阻断" if capacity_gate.get("passed") is False else "观察" if _text(item.get("action")) == "observe" else "通过")
                ),
            }
        )
    return {
        "gate_overall_status": gate_overall_status,
        "gate_overall_reason": gate_overall_reason,
        "gate_feedback_status": feedback_status,
        "gate_feedback_level": feedback_level,
        "gate_feedback_window": feedback_window,
        "gate_feedback_summary": feedback_summary,
        "gate_capacity_status": capacity_status,
        "gate_capacity_state": capacity_state,
        "gate_capacity_profile": capacity_profile,
        "gate_capacity_stress_score": capacity_stress_score,
        "governance_block_effect": governance_block_effect,
        "governance_block_effect_reason": governance_block_effect_reason,
        "governance_recent_timeline": governance_recent_timeline,
    }
