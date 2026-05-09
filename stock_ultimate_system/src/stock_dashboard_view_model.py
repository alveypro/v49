from __future__ import annotations

from typing import Any


def _text(value: object) -> str:
    return str(value or "").strip()


def _join_parts(parts: list[object]) -> str:
    normalized = [_text(part) for part in parts]
    return " · ".join(part for part in normalized if part and part != "-")


def _is_missing(value: object) -> bool:
    text = _text(value)
    return not text or text == "-"


def _display_status_label(value: object) -> str:
    text = _text(value)
    if not text or text == "-":
        return "待确认"
    normalized = text.lower()
    mapping = {
        "manual_review": "人工复核",
        "blocked": "已阻断",
        "conditional": "受控放行",
        "running": "运行中",
        "running_daily_research": "每日研究运行中",
        "completed": "已完成",
        "done": "已完成",
        "up_to_date": "已更新",
        "pending_window": "受控等待",
        "ready_for_data_check": "等待数据门检查",
        "unknown": "待确认",
        "pass": "通过",
        "failed": "失败",
        "yellow": "黄色观察",
        "partial_success": "待补齐",
    }
    return mapping.get(normalized, text)


def _display_home_status_label(value: object) -> str:
    text = _text(value)
    if not text or text == "-":
        return "待确认"
    normalized = text.lower()
    softened = {
        "failed": "待复核",
        "失败": "待复核",
        "partial_success": "待补齐",
        "部分成功": "待补齐",
        "blocked": "待补齐",
        "已阻断": "待补齐",
        "degraded": "待补齐",
        "stale": "待补齐",
    }
    return softened.get(normalized, softened.get(text, _display_status_label(text)))


def _display_candidate_name(value: object) -> str:
    text = _text(value)
    if not text or text in {"-", "未提供名称"}:
        return ""
    return text


def _display_candidate_score(value: object) -> str:
    text = _text(value)
    if not text or text == "-":
        return ""
    return text


def _display_health_score_label(value: object) -> str:
    text = _text(value)
    if not text or text == "-":
        return "健康评分待生成"
    score_text = text.split("/", 1)[0].strip()
    try:
        score_value = float(score_text)
    except Exception:
        return f"健康分 {text}"
    if score_value == 0:
        return "健康评分待生成"
    return f"健康分 {text}"


def _display_terminal_status(*, disabled_reason: str, invalid_reason: str) -> str:
    if _text(disabled_reason):
        return "当前存在制度阻断"
    if _text(invalid_reason):
        return _text(invalid_reason)
    return "终局结论暂缺"


def _display_research_surface_status(value: object) -> str:
    text = _text(value)
    if not text or text == "-":
        return "待确认"
    normalized = text.lower()
    mapping = {
        "approved": "已对齐",
        "completed": "已完成",
        "up_to_date": "已更新",
        "partial_success": "待补齐",
        "blocked": "待复核",
        "failed": "待复核",
        "running": "运行中",
        "observe_only": "观察中",
        "hold_observation": "继续观察",
    }
    return mapping.get(normalized, _display_status_label(text))


def _display_runtime_detail(value: object) -> str:
    text = _text(value)
    if not text or text == "-":
        return "候选链路待补齐"
    lowered = text.lower()
    if "top_industry" in lowered:
        return "行业集中度约束待复核"
    if "gate blocked" in lowered or lowered == "blocked":
        return "候选链路待复核"
    if "failed" in lowered:
        return "候选链路待复核"
    if "partial_success" in lowered:
        return "候选链路待补齐"
    return text


def _display_attempt_reason(value: object) -> str:
    text = _text(value)
    if not text or text == "-":
        return "等待复核候选链路"
    lowered = text.lower()
    if "gate blocked" in lowered or lowered == "blocked":
        return "门禁待复核"
    if "top_industry" in lowered:
        return "行业集中度待复核"
    if "failed" in lowered:
        return "候选链路待复核"
    return text


def _display_observation_label(value: object) -> str:
    text = _text(value)
    if not text or text == "-":
        return "等待形成"
    return text


def _display_blocker_title(value: object) -> str:
    text = _text(value)
    if not text or text == "-":
        return "等待复核"
    if text.startswith("候选主链失败于"):
        return "候选主链待复核"
    if text.startswith("候选主链仍在"):
        return "候选主链处理中"
    if text == "最新候选篮尝试被阻断":
        return "候选篮待复核"
    return text


def _display_blocker_detail(value: object) -> str:
    text = _text(value)
    if not text or text == "-":
        return "等待补齐研究证据后再复核。"
    lowered = text.lower()
    if "top_industry" in lowered:
        return "行业集中度约束待复核。"
    if "gate blocked" in lowered:
        return "当前门禁未通过，需先完成研究复核。"
    if "failed" in lowered:
        return "候选链路仍需复核，不宜直接推进。"
    return text


def _display_decision_validity(value: object) -> str:
    text = _text(value)
    if not text or text == "-":
        return "当前判断待复核"
    lowered = text.lower()
    if "batch_prediction_timeout" in lowered:
        return "当前结果带补证痕迹"
    return text


def _display_candidate_focus_code(value: object) -> str:
    text = _text(value)
    if not text or text in {"-", "暂无"}:
        return "等待候选形成"
    return text


def _is_waiting_candidate_focus(value: object) -> bool:
    return _display_candidate_focus_code(value) == "等待候选形成"


def _display_current_candidate_summary(top_code: object, candidate_name: object = "") -> str:
    focus_code = _display_candidate_focus_code(top_code)
    if focus_code == "等待候选形成":
        return "当前优先候选 等待候选形成"
    name = _display_candidate_name(candidate_name)
    return f"当前优先候选 {focus_code}" if not name else f"当前优先候选 {focus_code} {name}"


def _display_runtime_stage_label(value: object) -> str:
    text = str(value or "").strip()
    lowered = text.lower()
    if not text or text == "-":
        return "候选主链待复核"
    if "失败" in text or lowered == "failed":
        return "候选主链待复核"
    if "运行" in text or lowered == "running":
        return "候选主链处理中"
    return text


def _display_guardrail_mode(value: object) -> str:
    text = str(value or "").strip()
    lowered = text.lower()
    if not text or text == "-":
        return "常规"
    if lowered == "validation_skipped":
        return "样本待补齐"
    if lowered == "auto_runtime_budget":
        return "自动预算约束"
    return text


def _display_guardrail_reason(value: object) -> str:
    text = str(value or "").strip()
    lowered = text.lower()
    if not text or text == "-":
        return "当前未触发额外防守约束"
    if lowered == "auto_runtime_budget":
        return "自动运行预算约束仍在生效"
    if "validation_skipped" in lowered:
        return "验证样本仍在补齐"
    return text


def _display_generation_reason(value: object) -> str:
    text = str(value or "").strip()
    lowered = text.lower()
    if not text or text == "-":
        return "当前结果仍需补证"
    if "batch_prediction_timeout" in lowered:
        return "当前结果仍带补证痕迹"
    if "validation_skipped" in lowered:
        return "验证样本仍在补齐"
    return text


def _display_progress_status(value: object, *, floor: int = 20) -> str:
    text = _text(value)
    if not text or text == "-":
        return "样本积累中"
    if text == f"0/{floor}":
        return "样本积累中"
    return text


def _display_progress_note(progress: object, needed: object, *, floor: int = 20) -> str:
    progress_text = _text(progress)
    needed_text = _text(needed) or str(floor)
    if not progress_text or progress_text == f"0/{floor}":
        return f"首批 {needed_text} 个正式样本仍在积累"
    return f"还差 {needed_text} 个干净样本"


def _compact_date_key(value: object) -> str:
    text = str(value or "").strip()
    if len(text) >= 10 and text[4] == "-" and text[7] == "-":
        return text[:10].replace("-", "")
    if len(text) >= 10 and text[4] == "." and text[7] == ".":
        return text[:10].replace(".", "")
    if len(text) >= 8 and text[:8].isdigit():
        return text[:8]
    return ""


def _display_trade_date(value: object, fallback: str = "-") -> str:
    text = str(value or "").strip()
    if len(text) >= 10 and text[4] == "-" and text[7] == "-":
        return text[:10]
    if len(text) >= 8 and text[:8].isdigit():
        return f"{text[:4]}-{text[4:6]}-{text[6:8]}"
    return text or fallback


def _display_timestamp(value: object, fallback: str = "-") -> str:
    text = str(value or "").strip()
    if not text:
        return fallback
    if "T" in text and len(text) >= 19:
        return text[:19].replace("T", " ")
    return text


def _display_missing(value: object, fallback: str) -> str:
    text = str(value or "").strip()
    return text if text and text != "-" else fallback


def build_stock_runtime_view_model(
    *,
    effective_update_status: dict[str, Any],
    prefilter_artifact_status: dict[str, Any],
    automation_health: dict[str, Any],
    governance_recommended_action: str,
    cockpit_model: dict[str, Any],
    candidate_artifact_status: dict[str, Any],
    current_basket_pointer_status: str,
    current_basket_pointer_updated_at: str,
    current_basket_pointer_basket_id: str,
    latest_basket_attempt_status: str,
    latest_basket_attempt_generated_at: str,
    latest_basket_attempt_blocking_reason: str,
    observation_wait_status: dict[str, Any],
    daily_closure_latest: dict[str, Any],
) -> dict[str, str | bool]:
    update_status_label = _display_home_status_label(effective_update_status.get("status", "-"))
    update_stage_label = _display_status_label(effective_update_status.get("stage", "-"))
    prefilter_freshness_label = {
        "fresh": "预筛已对齐",
        "degraded": "预筛待补齐",
        "blocked": "预筛待补齐",
        "stale": "预筛待补齐",
    }.get(str(prefilter_artifact_status.get("freshness_status", "unknown") or "unknown"), "预筛待确认")
    automation_health_label = f'自动链路 {_display_home_status_label(automation_health.get("label", "待确认"))}'
    governance_recommended_action_label = _display_status_label(governance_recommended_action)
    promotion_decision = cockpit_model.get("promotion_decision")
    promotion_decision_label = "晋级锁定" if str(promotion_decision or "").strip().lower() == "blocked" else _display_status_label(promotion_decision)
    db_latest_trade_date = _display_trade_date(effective_update_status.get("db_latest", "-"), "等待同步")
    candidate_generated_at_label = _display_timestamp(candidate_artifact_status.get("generated_at", "-"), "待生成")
    candidate_basket_generated_at_label = _display_timestamp(candidate_artifact_status.get("basket_generated_at", "-"), "待生成")
    candidate_pointer_updated_label = _display_timestamp(current_basket_pointer_updated_at, "-")
    observation_window_start_raw = (
        daily_closure_latest.get("window_start")
        or (observation_wait_status.get("observation_window", {}) or {}).get("start_date")
        or (observation_wait_status.get("observation_window", {}) or {}).get("started_at")
        or "-"
    )
    observation_window_start_label = _display_trade_date(observation_window_start_raw, "等待形成")
    observation_generated_at_label = _display_timestamp(
        observation_wait_status.get("generated_at") or daily_closure_latest.get("generated_at") or "-",
        "待确认",
    )
    observation_current_date_key = _compact_date_key(
        observation_wait_status.get("current_date", "") or daily_closure_latest.get("window_start", "")
    )
    db_latest_trade_date_key = _compact_date_key(effective_update_status.get("db_latest", ""))
    observation_timeline_stale = bool(
        observation_current_date_key
        and db_latest_trade_date_key
        and observation_current_date_key < db_latest_trade_date_key
    )
    observation_timeline_label = _display_observation_label(
        f"{observation_window_start_label}（旧观察窗口）" if observation_timeline_stale else observation_window_start_label
    )
    candidate_timeline_label = (
        f"{candidate_generated_at_label}（候选未跟上最新数据）"
        if str(prefilter_artifact_status.get("freshness_status", "")) in {"stale", "blocked"}
        else candidate_generated_at_label
    )
    timeline_consistency_note = (
        f"数据库最新交易日 {db_latest_trade_date}，候选产物 {candidate_generated_at_label}，"
        f"篮子摘要 {candidate_basket_generated_at_label}，观察窗口 {observation_window_start_label}，"
        f"观察诊断生成于 {observation_generated_at_label}。"
    )
    if str(prefilter_artifact_status.get("freshness_status", "")) in {"stale", "blocked"}:
        timeline_consistency_note += " 候选链路未与最新交易日对齐。"
    if observation_timeline_stale:
        timeline_consistency_note += " 观察窗口诊断仍引用旧日期，不可被理解为当前数据日期。"
    current_basket_pointer_label = (
        f'当前生效篮子 {_display_research_surface_status(current_basket_pointer_status)} · {current_basket_pointer_updated_at} · {current_basket_pointer_basket_id}'
        if current_basket_pointer_status not in {"", "-", "unknown"}
        else ""
    )
    latest_basket_attempt_label = (
        f'最新篮子尝试 {_display_research_surface_status(latest_basket_attempt_status)} · {latest_basket_attempt_generated_at} · {_display_attempt_reason(latest_basket_attempt_blocking_reason)}'
        if latest_basket_attempt_status == "blocked"
        else ""
    )
    return {
        "update_status_label": update_status_label,
        "update_stage_label": update_stage_label,
        "prefilter_freshness_label": prefilter_freshness_label,
        "automation_health_label": automation_health_label,
        "governance_recommended_action_label": governance_recommended_action_label,
        "promotion_decision_label": promotion_decision_label,
        "db_latest_trade_date": db_latest_trade_date,
        "candidate_generated_at_label": candidate_generated_at_label,
        "candidate_basket_generated_at_label": candidate_basket_generated_at_label,
        "candidate_pointer_updated_label": candidate_pointer_updated_label,
        "observation_window_start_label": observation_window_start_label,
        "observation_generated_at_label": observation_generated_at_label,
        "observation_timeline_stale": observation_timeline_stale,
        "observation_timeline_label": observation_timeline_label,
        "candidate_timeline_label": candidate_timeline_label,
        "timeline_consistency_note": timeline_consistency_note,
        "current_basket_pointer_label": current_basket_pointer_label,
        "latest_basket_attempt_label": latest_basket_attempt_label,
    }


def build_stock_home_view_model(
    *,
    headline_tone: str,
    headline_detail: str,
    current_basket_pointer_label: str,
    latest_basket_attempt_label: str,
    current_basket_pointer_status: str,
    current_basket_pointer_basket_id: str,
    latest_basket_attempt_status: str,
    latest_basket_attempt_blocking_reason: str,
    health_status: str,
    health_score: str,
    top_code: str,
    candidate_name: str,
    candidate_generated_at: str,
    generation_mode_label: str,
    update_status_label: str,
    update_stage_label: str,
    candidate_count: int,
    candidate_score: str,
    candidate_timeline_label: str,
    run_freshness: str,
    db_latest_trade_date: str,
    observation_timeline_label: str,
    prefilter_freshness_label: str,
    backtest_scope_label: str,
    governance_cycle_state: str,
    governance_recommended_action_label: str,
    governance_ready_for_release: bool,
    governance_fully_release_ready: bool,
    result_id: str,
    stage_label: str,
    result_subject: str,
    dominant_regime: str,
    risk_preference: str,
    backtest_conclusion: str,
    avg_risk_pressure: str,
    disabled_reason: str,
    invalid_reason: str,
    blocker_semantics: dict[str, Any],
    cockpit_model: dict[str, Any],
    promotion_decision_label: str,
) -> dict[str, Any]:
    primary_progress_label = _display_progress_status(cockpit_model.get("primary_progress"))
    basket_progress_label = _display_progress_status(cockpit_model.get("basket_progress"))
    candidate_score_label = _display_candidate_score(candidate_score)
    health_score_label = _display_health_score_label(health_score)
    terminal_status_label = _display_terminal_status(
        disabled_reason=disabled_reason,
        invalid_reason=invalid_reason,
    )
    candidate_subject = _join_parts([top_code, candidate_name]).replace(" · ", " ")
    if not candidate_subject:
        candidate_subject = "候选待形成"
    home_hero_facts = {
        "headline_tone": headline_tone,
        "headline_detail": headline_detail,
        "current_basket_pointer_label": current_basket_pointer_label,
        "latest_basket_attempt_label": latest_basket_attempt_label,
        "health_status": health_status,
        "health_score": health_score,
        "health_score_label": health_score_label,
        "top_code": top_code,
        "candidate_name": candidate_name,
        "candidate_generated_at": candidate_generated_at,
        "generation_mode_label": generation_mode_label,
        "update_status_label": update_status_label,
        "update_stage_label": update_stage_label,
    }
    primary_result_home_facts = {
        "result_id": result_id,
        "stage_label": stage_label,
        "result_subject": result_subject,
        "top_code": top_code,
        "dominant_regime": dominant_regime,
        "risk_preference": risk_preference,
        "backtest_conclusion": backtest_conclusion,
        "db_latest_trade_date": db_latest_trade_date,
        "candidate_generated_at": candidate_generated_at,
        "observation_timeline_label": observation_timeline_label,
        "avg_risk_pressure": avg_risk_pressure,
        "disabled_reason": disabled_reason,
        "invalid_reason": invalid_reason,
        "blocker_title": str(blocker_semantics.get("blocker_title", "")) if blocker_semantics.get("has_blocker") else "",
        "blocker_detail": str(blocker_semantics.get("blocker_detail", "")) if blocker_semantics.get("has_blocker") else "",
    }
    control_strip_cards = [
        {
            "label": "研究状态",
            "value": health_status,
            "sub_lines": [health_score_label],
        },
        {
            "label": "自动链路",
            "value": update_status_label,
            "sub_lines": [f"{update_stage_label} · {run_freshness} · 数据最新交易日 {db_latest_trade_date}"],
        },
        {
            "label": "候选覆盖",
            "value": f"{candidate_count} 标的",
            "sub_lines": [
                _join_parts(
                    [
                        _display_current_candidate_summary(top_code, candidate_name),
                        candidate_score_label,
                        f"候选产物 {candidate_timeline_label}",
                        generation_mode_label,
                    ]
                ),
                current_basket_pointer_label,
                latest_basket_attempt_label,
            ],
        },
        {
            "label": "时间一致性",
            "value": db_latest_trade_date,
            "sub_lines": [
                f"候选产物 {candidate_generated_at} · 观察窗口 {observation_timeline_label} · {prefilter_freshness_label}"
            ],
        },
        {
            "label": "回测口径",
            "value": backtest_conclusion,
            "sub_lines": [backtest_scope_label],
        },
        {
            "label": "治理主链",
            "value": _display_status_label(governance_cycle_state),
            "sub_lines": [
                f"{governance_recommended_action_label} · 发布就绪 {'是' if governance_ready_for_release else '否'} · 完整就绪 {'是' if governance_fully_release_ready else '否'}"
            ],
        },
    ]
    basket_dual_track_rows = []
    if current_basket_pointer_label:
        basket_dual_track_rows.append({"tone": "pointer", "text": current_basket_pointer_label})
    if latest_basket_attempt_label:
        basket_dual_track_rows.append({"tone": "attempt", "text": latest_basket_attempt_label})
    validation_basket_kpis = []
    if current_basket_pointer_label:
        validation_basket_kpis.append(
            {
                "label": "当前生效篮子",
                "value": _display_research_surface_status(current_basket_pointer_status),
                "sub": current_basket_pointer_basket_id,
            }
        )
    if latest_basket_attempt_label:
        validation_basket_kpis.append(
            {
                "label": "最新篮子尝试",
                "value": _display_research_surface_status(latest_basket_attempt_status),
                "sub": _display_attempt_reason(latest_basket_attempt_blocking_reason),
            }
        )
    external_decision_spine = {
        "decision_status": (
            "继续观察"
            if primary_progress_label == "样本积累中" and basket_progress_label == "样本积累中"
            else _display_missing(cockpit_model.get("decision_status"), "状态待复核")
        ),
        "decision_reason": (
            "证据还不够"
            if primary_progress_label == "样本积累中" and basket_progress_label == "样本积累中"
            else _display_missing(cockpit_model.get("decision_reason"), "等待证据补齐")
        ),
        "decision_summary": (
            "可看观察名单，暂不行动"
            if primary_progress_label == "样本积累中" and basket_progress_label == "样本积累中"
            else _display_missing(cockpit_model.get("decision_summary"), "闭合动作保持锁定")
        ),
        "next_check": (
            "等下一批更新"
            if primary_progress_label == "样本积累中" and basket_progress_label == "样本积累中"
            else _display_missing(cockpit_model.get("next_check"), "待确认")
        ),
        "primary_progress": primary_progress_label,
        "primary_needed": _display_progress_note(cockpit_model.get("primary_progress"), cockpit_model.get("primary_needed")),
        "basket_progress": basket_progress_label,
        "basket_needed": _display_progress_note(cockpit_model.get("basket_progress"), cockpit_model.get("basket_needed")),
        "promotion_decision_label": "未开放行动" if promotion_decision_label == "晋级锁定" else promotion_decision_label,
        "boundary": "仅供观察，不构成买卖建议。",
        "current_conclusion_sentence": (
            "当前结论：继续观察，暂不行动"
            if primary_progress_label == "样本积累中" and basket_progress_label == "样本积累中"
            else f"当前结论：{_display_missing(cockpit_model.get('decision_status'), '状态待复核')}，暂不行动"
        ),
        "primary_result_sentence": f"主结果对象：{result_subject or '对象暂缺'}，{stage_label or '阶段待确认'}，{terminal_status_label}",
        "candidate_basket_sentence": f"候选篮第一：{candidate_subject}，仅为观察候选，不覆盖主结果",
        "risk_status_sentence": (
            f"风险状态：系统健康{health_status or '待确认'}，"
            f"行动门{'未开放行动' if promotion_decision_label == '晋级锁定' else promotion_decision_label or '待确认'}"
        ),
    }
    return {
        "home_hero_facts": home_hero_facts,
        "primary_result_home_facts": primary_result_home_facts,
        "control_strip_cards": control_strip_cards,
        "basket_dual_track_rows": basket_dual_track_rows,
        "validation_basket_kpis": validation_basket_kpis,
        "external_decision_spine": external_decision_spine,
        "command_pointer_sentence": f" {current_basket_pointer_label}。" if current_basket_pointer_label else "",
        "command_attempt_sentence": f" {latest_basket_attempt_label}。" if latest_basket_attempt_label else "",
    }


def build_stock_candidate_diagnostics_view_model(
    *,
    basket_validation_summary: dict[str, Any],
    basket_validation_variants: dict[str, Any],
    candidate_basket_feedback: dict[str, Any],
    evolution_status: dict[str, Any],
    basket_summary: dict[str, Any],
    candidate_artifact_status: dict[str, Any],
    best_variant_label: str,
) -> dict[str, Any]:
    rebalance_dates = int(basket_validation_summary.get("rebalance_dates", 0) or 0)
    validation_available = rebalance_dates > 0
    candidate_feedback_level = str(candidate_basket_feedback.get("feedback_level", "-") or "-")
    candidate_feedback_window = str(candidate_basket_feedback.get("window_label", "-") or "-")
    candidate_feedback_summary = str(candidate_basket_feedback.get("summary_note", "-") or "-")
    candidate_feedback_changes = int(candidate_basket_feedback.get("change_total", 0) or 0)
    evolution_capacity_state = str(evolution_status.get("latest_capacity_state", "-") or "-")
    evolution_capacity_profile = str(evolution_status.get("latest_capacity_profile", "-") or "-")
    evolution_capacity_gate_status = str(evolution_status.get("latest_capacity_gate_status", "-") or "-")
    evolution_capacity_stress_score = str(evolution_status.get("latest_capacity_stress_score", "0.0") or "0.0")
    weighted_liquidity_score = float(basket_summary.get("weighted_liquidity_score", 0.0) or 0.0)
    liquidity_capacity_weight = float(basket_summary.get("liquidity_capacity_weight", 0.0) or 0.0)
    liquidity_capacity_state = "可放大"
    if liquidity_capacity_weight >= 0.18 or weighted_liquidity_score < 0.58:
        liquidity_capacity_state = "放大量受限"
    elif liquidity_capacity_weight > 0 or weighted_liquidity_score < 0.68:
        liquidity_capacity_state = "放大需观察"
    candidate_runtime_status = str(candidate_artifact_status.get("runtime_status", "-") or "-")
    candidate_runtime_status_label = (
        "运行中"
        if candidate_runtime_status == "running"
        else "已完成"
        if candidate_runtime_status == "completed"
        else "失败"
        if candidate_runtime_status == "failed"
        else candidate_runtime_status
    )
    candidate_runtime_updated_label = _display_timestamp(candidate_artifact_status.get("runtime_updated_at", "-"), "待更新")

    def _fmt_validation_pct(value: object) -> str:
        if not validation_available:
            return "-"
        try:
            return f"{float(value or 0.0):.2%}"
        except Exception:
            return "-"

    return {
        "rebalance_dates": rebalance_dates,
        "weighted_liquidity_score": weighted_liquidity_score,
        "liquidity_capacity_weight": liquidity_capacity_weight,
        "liquidity_capacity_state": liquidity_capacity_state,
        "candidate_feedback_window": candidate_feedback_window,
        "candidate_feedback_level": candidate_feedback_level,
        "candidate_feedback_summary": candidate_feedback_summary,
        "candidate_feedback_changes": candidate_feedback_changes,
        "candidate_runtime_stage_label": _display_runtime_stage_label(candidate_artifact_status.get("runtime_stage_label", "-")),
        "candidate_runtime_status_label": _display_research_surface_status(candidate_runtime_status_label),
        "candidate_runtime_detail": _display_runtime_detail(candidate_artifact_status.get("runtime_detail", "-")),
        "candidate_runtime_results_ready": str(candidate_artifact_status.get("runtime_results_ready", "-") or "-"),
        "candidate_runtime_skipped": str(candidate_artifact_status.get("runtime_skipped_count", "-") or "-"),
        "candidate_runtime_updated_label": candidate_runtime_updated_label,
        "candidate_runtime_elapsed": str(candidate_artifact_status.get("runtime_elapsed_sec", "-") or "-"),
        "best_variant_label": best_variant_label,
        "diversified_avg_excess_return_5d_label": _fmt_validation_pct(
            (basket_validation_variants.get("diversified", {}) or {}).get("avg_excess_return_5d", 0.0)
        ),
        "raw_avg_excess_return_5d_label": _fmt_validation_pct(
            (basket_validation_variants.get("raw", {}) or {}).get("avg_excess_return_5d", 0.0)
        ),
        "top1_avg_excess_return_5d_label": _fmt_validation_pct(
            (basket_validation_variants.get("top1", {}) or {}).get("avg_excess_return_5d", 0.0)
        ),
        "avg_basket_return_5d_label": _fmt_validation_pct(basket_validation_summary.get("avg_basket_return_5d", 0.0)),
        "avg_excess_return_5d_label": _fmt_validation_pct(basket_validation_summary.get("avg_excess_return_5d", 0.0)),
        "basket_win_rate_5d_label": _fmt_validation_pct(basket_validation_summary.get("basket_win_rate_5d", 0.0)),
        "avg_top1_return_5d_label": _fmt_validation_pct(basket_validation_summary.get("avg_top1_return_5d", 0.0)),
        "evolution_capacity_gate_status": evolution_capacity_gate_status,
        "evolution_capacity_state": evolution_capacity_state,
        "evolution_capacity_profile": evolution_capacity_profile,
        "evolution_capacity_stress_score": evolution_capacity_stress_score,
    }


def build_stock_validation_view_model(
    *,
    rebalance_dates: int,
    basket_validation_summary: dict[str, Any],
    expected_basket_return: float,
    risk_pressure_score: object,
    liquidity_capacity_state: str,
    weighted_liquidity_score: float,
    liquidity_capacity_weight: float,
    candidate_feedback_window: str,
    candidate_feedback_level: str,
    candidate_feedback_summary: str,
    candidate_feedback_changes: object,
    candidate_runtime_stage_label: str,
    candidate_runtime_status_label: str,
    candidate_runtime_detail: str,
    candidate_runtime_results_ready: str,
    candidate_runtime_skipped: str,
    candidate_runtime_updated_label: str,
    candidate_runtime_elapsed: str,
    guardrail_mode_label: str,
    guardrail_reasons_label: str,
    generation_mode_label: str,
    generation_reason: str,
    validation_basket_kpis: list[dict[str, str]],
    skipped_count: object,
    best_variant_label: str,
    strategy_mode_label: str,
    strategy_strictness_label: str,
    strategy_weak_market_action_label: str,
    diversified_avg_excess_return_5d_label: str,
    raw_avg_excess_return_5d_label: str,
    top1_avg_excess_return_5d_label: str,
    avg_basket_return_5d_label: str,
    avg_excess_return_5d_label: str,
    basket_win_rate_5d_label: str,
    avg_top1_return_5d_label: str,
) -> dict[str, Any]:
    return {
        "display_contract": {
            "title": "历史篮子验证",
            "detail": "最近几次调仓的篮子表现，用来验证当前候选逻辑是否真实优于全池平均",
        },
        "kpi_rows": [
            [
                {"label": "验证次数", "value": str(rebalance_dates), "sub": "最近滚动回放样本数"},
                {"label": "5日平均篮子收益", "value": avg_basket_return_5d_label, "sub": "候选篮子未来 5 日平均收益"},
                {"label": "5日平均超额", "value": avg_excess_return_5d_label, "sub": "相对 universe 平均的超额收益"},
                {"label": "5日篮子胜率", "value": basket_win_rate_5d_label, "sub": "历史验证里为正收益的比例"},
            ],
            [
                {"label": "Top1平均5日收益", "value": avg_top1_return_5d_label, "sub": "只拿第一名时的平均表现"},
                {"label": "当前篮子预期收益", "value": f"{expected_basket_return:.2%}", "sub": "模型层当前组合预期"},
                {"label": "当前风险压力", "value": str(risk_pressure_score), "sub": "组合集中度与风控压力摘要"},
            ],
            [
                {"label": "放大量状态", "value": liquidity_capacity_state, "sub": "根据组合流动性质量与承载权重判断当前篮子能否继续放大"},
                {"label": "加权流动性分", "value": f"{weighted_liquidity_score:.2f}", "sub": "越低说明当前组合更多依赖成交支撑不足的标的"},
                {"label": "承载受限权重", "value": f"{liquidity_capacity_weight:.1%}", "sub": "被标记为 liquidity_capacity_stretched 的组合权重占比"},
            ],
            [
                {"label": "最新回灌窗口", "value": candidate_feedback_window, "sub": "候选篮观察窗口标签"},
                {"label": "最新反馈级别", "value": candidate_feedback_level, "sub": candidate_feedback_summary},
                {"label": "建议变更数", "value": str(candidate_feedback_changes), "sub": "候选篮回灌触发的受控变更建议"},
            ],
            [
                {"label": "候选主链阶段", "value": candidate_runtime_stage_label, "sub": f"{candidate_runtime_status_label} ｜ {candidate_runtime_detail}"},
                {"label": "已处理结果数", "value": candidate_runtime_results_ready, "sub": f"跳过 {candidate_runtime_skipped} ｜ 最近更新 {candidate_runtime_updated_label}"},
                {"label": "已耗时", "value": f"{candidate_runtime_elapsed}s", "sub": "候选主链运行时长，便于识别是否卡在批量预测"},
            ],
            [
                {"label": "Guardrail 模式", "value": _display_guardrail_mode(guardrail_mode_label), "sub": _display_guardrail_reason(guardrail_reasons_label)},
                {"label": "候选结果类型", "value": generation_mode_label, "sub": _display_generation_reason(generation_reason)},
                *validation_basket_kpis,
                {"label": "跳过样本数", "value": str(skipped_count), "sub": "因历史长度/数据问题跳过的标的数量"},
                {"label": "A/B 最优版本", "value": best_variant_label, "sub": "按 5 日平均超额粗排的当前领先版本"},
            ],
            [
                {"label": "自动策略模式", "value": strategy_mode_label, "sub": "系统当前学到的篮子执行方式"},
                {"label": "自动策略强度", "value": strategy_strictness_label, "sub": "候选阈值当前收紧程度"},
                {"label": "弱市动作", "value": strategy_weak_market_action_label, "sub": "弱市场下的自动降级动作"},
            ],
            [
                {"label": "分散篮子 5日超额", "value": diversified_avg_excess_return_5d_label, "sub": "当前主版本相对 universe 的平均超额"},
                {"label": "原始排序 5日超额", "value": raw_avg_excess_return_5d_label, "sub": "不做组合再平衡时的平均超额"},
                {"label": "只取第一名 5日超额", "value": top1_avg_excess_return_5d_label, "sub": "只拿第一名时相对 universe 的平均超额"},
            ],
        ],
    }


def build_stock_summary_view_model(
    *,
    health_status: str,
    backtest_conclusion: str,
    top_code: str,
    candidate_name: str,
    health_score: str,
    candidate_generated_at: str,
    backtest_scope_label: str,
    basket_dual_track_rows: list[dict[str, str]],
    execution_semantics: dict[str, Any],
    blocker_semantics: dict[str, Any],
    governance_semantics: dict[str, Any],
    evidence_semantics: dict[str, Any],
    evolution_capacity_gate_status: str,
    evolution_capacity_state: str,
    evolution_capacity_profile: str,
    evolution_capacity_stress_score: str,
    liquidity_capacity_state: str,
    governance_cycle_state_label: str,
    governance_decision_label: str,
    governance_recommended_action_label: str,
    governance_audit_status_label: str,
    governance_ready_for_release: bool,
    governance_fully_release_ready: bool,
    previous_stable_run_id: str,
    governance_operator_message: str,
    summary_lines: list[str],
    ai_explainer: dict[str, Any] | None = None,
) -> dict[str, Any]:
    recent_timeline = (governance_semantics.get("governance_recent_timeline") or [{}])[0]
    health_score_label = _display_health_score_label(health_score)
    evidence_chips = [
        health_score_label,
        f"候选生成 {candidate_generated_at}",
        f"数据 {str(execution_semantics.get('db_latest_trade_date', '-'))}",
    ]
    return {
        "display_contract": {
            "conclusion_title": f"{health_status} · {backtest_conclusion}",
            "conclusion_sub": _display_current_candidate_summary(top_code, candidate_name),
            "evidence_title": health_score_label,
            "evidence_sub": f"回测口径 {backtest_scope_label}",
            "boundary_title": "研究参考",
            "boundary_sub": "不构成收益保证；等待期以第一屏行动锁为准。",
            "next_title": str(execution_semantics.get("decision_action", "-")),
            "next_sub": str(execution_semantics.get("decision_action_reason", "-")),
        },
        "evidence_chips": evidence_chips,
        "basket_dual_track_rows": basket_dual_track_rows,
        "kpi_rows": [
            [
                {"label": "当前唯一动作", "value": str(execution_semantics.get("decision_action", "-")), "sub": str(execution_semantics.get("decision_action_reason", "-"))},
                {"label": "当前是否可执行", "value": str(execution_semantics.get("execution_eligibility", "-")), "sub": str(execution_semantics.get("execution_eligibility_reason", "-"))},
                {
                    "label": "判断有效期",
                    "value": _display_decision_validity(execution_semantics.get("decision_validity_label", "-")),
                    "sub": f"候选 {str(execution_semantics.get('candidate_generated_at', '-'))} ｜ 数据 {str(execution_semantics.get('db_latest_trade_date', '-'))}",
                },
            ],
            [
                {
                    "label": "当前阻塞点",
                    "value": _display_blocker_title(blocker_semantics.get("blocker_title", "-")),
                    "sub": _display_blocker_detail(blocker_semantics.get("blocker_detail", "-")),
                },
                {"label": "门禁总览", "value": str(governance_semantics.get("gate_overall_status", "-")), "sub": str(governance_semantics.get("gate_overall_reason", "-"))},
                {"label": "阻断后果", "value": str(governance_semantics.get("governance_block_effect", "-")), "sub": str(governance_semantics.get("governance_block_effect_reason", "-"))},
            ],
            [
                {"label": "为什么是它", "value": str(evidence_semantics.get("score_gap", "-")), "sub": str(evidence_semantics.get("top_candidate_advantage_reason", "-"))},
                {"label": "排序证据", "value": str(evidence_semantics.get("evidence_confidence_label", "-")), "sub": str(evidence_semantics.get("top_candidate_audit_summary", "-"))},
                {
                    "label": "最近治理轨迹",
                    "value": _display_research_surface_status(recent_timeline.get("result", "-")),
                    "sub": str(recent_timeline.get("reason", "暂无治理轨迹")),
                },
            ],
            [
                {"label": "容量门禁", "value": evolution_capacity_gate_status, "sub": f"{evolution_capacity_state} ｜ 建议档位 {evolution_capacity_profile}"},
                {"label": "最差压力分", "value": evolution_capacity_stress_score, "sub": "多规模与成本扰动场景下的最差退化评分"},
                {"label": "放大边界", "value": liquidity_capacity_state, "sub": "首页验证层看到的当前篮子放大量状态"},
            ],
            [
                {"label": "治理状态", "value": governance_cycle_state_label, "sub": f"决策 {governance_decision_label}"},
                {"label": "建议动作", "value": governance_recommended_action_label, "sub": f"审核 {governance_audit_status_label}"},
                {"label": "发布就绪", "value": "是" if governance_ready_for_release else "否", "sub": f"完整就绪 {'是' if governance_fully_release_ready else '否'} ｜ 稳定版本 {previous_stable_run_id}"},
            ],
        ],
        "governance_operator_message": governance_operator_message,
        "summary_lines": summary_lines,
        "ai_explainer": dict(ai_explainer or {}),
    }


def build_stock_actions_view_model(
    *,
    candidate_generated_at: str,
    basket_generated_at: str,
    candidate_source_label: str,
    generation_mode_label: str,
    dominant_regime: str,
    risk_preference: str,
    avg_risk_pressure: str,
    basket_dual_track_rows: list[dict[str, str]],
) -> dict[str, Any]:
    return {
        "detail": (
            f"仅作观察。来源 {candidate_source_label}，更新 {candidate_generated_at}"
        ),
        "metric_chips": [
            f"来源 {candidate_source_label}",
            f"市场状态 {dominant_regime}",
            f"风险偏好 {risk_preference}",
            f"风险压力 {avg_risk_pressure}",
        ],
        "basket_dual_track_rows": basket_dual_track_rows,
    }


def build_stock_overview_chrome_view_model(
    *,
    home_hero_facts: dict[str, Any],
    health_tag: str,
    top1_label: str,
    top1_signal: str,
    top1_risk: str,
    candidate_score: str,
    run_freshness: str,
    update_stage_label: str,
    update_status_label: str,
    candidate_name: str,
    result_subject: str = "",
    db_latest_trade_date: str,
    timeline_consistency_note: str,
    view_title: str,
    view_subtitle: str,
    command_pointer_sentence: str,
    command_attempt_sentence: str,
) -> dict[str, Any]:
    candidate_score_label = _display_candidate_score(candidate_score)
    candidate_focus_subtitle = _join_parts([candidate_name, top1_signal, top1_risk])
    candidate_meta_chips = [top1_signal, top1_risk]
    if candidate_score_label:
        candidate_meta_chips.append(f"Score {candidate_score_label}")
    generation_mode_chip = _text(home_hero_facts.get("generation_mode_label", "-"))
    if generation_mode_chip and generation_mode_chip != "-":
        candidate_meta_chips.append(generation_mode_chip)
    top_focus_code = _display_candidate_focus_code(home_hero_facts.get("top_code", "暂无"))
    waiting_candidate_focus = _is_waiting_candidate_focus(home_hero_facts.get("top_code", "暂无"))
    spotlight_eyebrow = "优先标的"
    spotlight_title = str(home_hero_facts.get("top_code", "暂无"))
    spotlight_candidate_name = candidate_name
    spotlight_description = "优先看信号方向、风险等级、仓位建议和依据的一致性，不建议只按综合分决策。"
    if waiting_candidate_focus:
        spotlight_eyebrow = "当前对象"
        spotlight_title = result_subject or "对象待确认"
        spotlight_candidate_name = ""
        spotlight_description = "候选链尚未闭合前，只展示当前 formal 主结果对象，不提前放大候选判断。"
    return {
        "hero_side": {
            "health_status": str(home_hero_facts.get("health_status", "-")),
            "health_score": str(home_hero_facts.get("health_score", "-")),
            "health_score_label": str(
                home_hero_facts.get(
                    "health_score_label",
                    _display_health_score_label(home_hero_facts.get("health_score", "-")),
                )
            ),
            "health_tag": health_tag,
            "top_code": str(home_hero_facts.get("top_code", "暂无")),
            "candidate_name": str(home_hero_facts.get("candidate_name", "-")),
            "candidate_generated_at": str(home_hero_facts.get("candidate_generated_at", "-")),
            "generation_mode_label": str(home_hero_facts.get("generation_mode_label", "-")),
            "update_status_label": str(home_hero_facts.get("update_status_label", "-")),
            "update_stage_label": str(home_hero_facts.get("update_stage_label", "-")),
        },
        "command_focus": {
            "title": top1_label,
            "detail": (
                f"当前优先候选 {top_focus_code}，信号 {top1_signal}，风险 {top1_risk}"
                f"{f'，综合分 {candidate_score_label}' if candidate_score_label else ''}。"
                f"候选文件时间 {str(home_hero_facts.get('candidate_generated_at', '-'))}，当前结果类型 {str(home_hero_facts.get('generation_mode_label', '-'))}。"
                f"{command_pointer_sentence}{command_attempt_sentence}"
            ),
        },
        "command_runtime": {
            "title": run_freshness,
            "detail": f"最近任务阶段 {update_stage_label}，数据库更新状态 {update_status_label}。",
        },
        "view_banner": {
            "view_title": view_title,
            "view_subtitle": view_subtitle,
            "focus_code": _display_candidate_focus_code(home_hero_facts.get("top_code", "暂无")),
            "focus_subtitle": candidate_focus_subtitle,
            "timeline_title": db_latest_trade_date,
            "timeline_detail": timeline_consistency_note,
        },
        "spotlight": {
            "eyebrow": spotlight_eyebrow,
            "top_code": spotlight_title,
            "candidate_name": spotlight_candidate_name,
            "description": spotlight_description,
            "meta_chips": candidate_meta_chips,
        },
    }


def build_stock_overview_kpi_view_model(
    *,
    health_status: str,
    health_score: str,
    update_status_label: str,
    execution_semantics: dict[str, Any],
    cockpit_model: dict[str, Any],
    governance_semantics: dict[str, Any],
) -> dict[str, Any]:
    health_score_label = _display_health_score_label(health_score)
    return {
        "kpi_rows": [[
            {
                "label": "系统状态",
                "value": health_status,
                "sub": f"{health_score_label} ｜ 自动链路 {update_status_label}",
            },
            {
                "label": "推进决策",
                "value": str(execution_semantics.get("decision_action", "-")),
                "sub": str(execution_semantics.get("decision_action_reason", "-")),
            },
            {
                "label": "证据进度",
                "value": _display_missing(cockpit_model.get("primary_progress"), "0/20"),
                "sub": (
                    f"主结果 {_display_missing(cockpit_model.get('primary_needed'), '20')} 待补 ｜ "
                    f"候选篮 {_display_missing(cockpit_model.get('basket_progress'), '0/20')}"
                ),
            },
            {
                "label": "治理门禁",
                "value": str(governance_semantics.get("gate_overall_status", "-")),
                "sub": str(governance_semantics.get("gate_overall_reason", "-")),
            },
        ]]
    }


def build_stock_overview_disclosure_view_model(
    *,
    update_status_label: str,
    update_stage_label: str,
) -> dict[str, str]:
    operations_detail = f"当前更新 {update_status_label}，阶段 {update_stage_label}。"
    if update_status_label in {"待补齐", "待复核"}:
        operations_detail = f"当前链路{update_status_label}，折叠区只保留内部复核入口，不参与首页正式判断。"
    return {
        "system_summary_title": "运行、候选、预筛、治理指标已下沉",
        "system_summary_detail": "需要核对运营指标时再展开；外部判断以上方证据路径为准。",
        "operations_title": "内部复核面已收起",
        "operations_detail": operations_detail,
    }


def build_stock_top1_view_model(
    *,
    top1: dict[str, Any],
    top1_signal: str,
    top1_risk: str,
) -> dict[str, str]:
    return {
        "top_code": _display_candidate_focus_code(top1.get("ts_code", "暂无")),
        "stock_name": _display_candidate_name(top1.get("stock_name", "")),
        "signal": top1_signal,
        "risk": top1_risk,
        "final_score": _display_candidate_score(top1.get("final_score", "-")),
    }


def build_stock_candidate_focus_view_model(
    *,
    top1: dict[str, Any],
    top1_signal: str,
    top1_risk: str,
    candidate_cards: list[dict[str, Any]],
    candidate_index: int,
) -> dict[str, Any]:
    position_pct_raw = str(top1.get("position_pct", "0") or "0").strip()
    candidate_position_label = "-"
    if position_pct_raw not in {"", "-"}:
        try:
            candidate_position_label = f"{float(position_pct_raw):.1%}"
        except Exception:
            candidate_position_label = position_pct_raw
    total_candidates = max(len(candidate_cards), 1)
    prev_index = max(0, candidate_index - 1)
    next_index = min(max(len(candidate_cards) - 1, 0), candidate_index + 1)
    quick_links = [
        {
            "label": f"Top{idx + 1}",
            "ts_code": str(card.get("ts_code", "-")),
            "index": idx,
            "active": idx == candidate_index,
        }
        for idx, card in enumerate(candidate_cards[:3])
    ]
    return {
        "top_code": _display_candidate_focus_code(top1.get("ts_code", "暂无")),
        "signal": top1_signal,
        "risk": top1_risk,
        "final_score": str(top1.get("final_score", "-")),
        "candidate_position_label": candidate_position_label,
        "current_index": candidate_index,
        "current_position_label": f"候选 {candidate_index + 1} / {total_candidates}",
        "total_candidates": total_candidates,
        "prev_index": prev_index,
        "next_index": next_index,
        "quick_links": quick_links,
    }


def build_stock_candidate_compare_view_model(
    *,
    candidate_cards: list[dict[str, Any]],
) -> dict[str, Any]:
    if len(candidate_cards) < 2:
        return {"available": False}
    top_card = candidate_cards[0]
    next_card = candidate_cards[1]
    top_score = float(top_card.get("final_score", "0") or 0.0)
    next_score = float(next_card.get("final_score", "0") or 0.0)
    return {
        "available": True,
        "top_card": {
            "ts_code": str(top_card.get("ts_code", "-")),
            "stock_name": str(top_card.get("stock_name", "")),
            "signal": str(top_card.get("signal", "-")),
            "risk_level": str(top_card.get("risk_level", "-")),
            "final_score": str(top_card.get("final_score", "-")),
            "pred_return": str(top_card.get("pred_return", "-")),
        },
        "next_card": {
            "ts_code": str(next_card.get("ts_code", "-")),
            "stock_name": str(next_card.get("stock_name", "")),
            "signal": str(next_card.get("signal", "-")),
            "risk_level": str(next_card.get("risk_level", "-")),
            "final_score": str(next_card.get("final_score", "-")),
            "pred_return": str(next_card.get("pred_return", "-")),
        },
        "score_gap": f"{top_score - next_score:.1f}",
    }


def build_stock_links_view_model(
    *,
    daily_md_href: str,
    daily_md_download_href: str,
    health_csv_href: str,
    health_csv_download_href: str,
    leaderboard_href: str,
    leaderboard_download_href: str,
    candidates_md_href: str,
    candidates_md_download_href: str,
    candidates_csv_href: str,
    candidates_csv_download_href: str,
    latest_report_href: str,
    latest_report_download_href: str,
    evolution_report_href: str,
) -> dict[str, Any]:
    return {
        "title": "快速入口",
        "detail": "报告、CSV、候选列表、回测文档",
        "entries": [
            {
                "title": "每日研究原文",
                "desc": "查看完整机器研究报告",
                "actions": [
                    {"label": "查看", "href": daily_md_href, "target_blank": True},
                    {"label": "下载", "href": daily_md_download_href, "target_blank": False},
                ],
            },
            {
                "title": "健康趋势 CSV",
                "desc": "追踪健康分与告警变化",
                "actions": [
                    {"label": "查看", "href": health_csv_href, "target_blank": True},
                    {"label": "下载", "href": health_csv_download_href, "target_blank": False},
                ],
            },
            {
                "title": "回测排行榜 CSV",
                "desc": "查看策略绩效排序",
                "actions": [
                    {"label": "查看", "href": leaderboard_href, "target_blank": True},
                    {"label": "下载", "href": leaderboard_download_href, "target_blank": False},
                ],
            },
            {
                "title": "候选股 Markdown",
                "desc": "阅读候选股文本结论",
                "actions": [
                    {"label": "查看", "href": candidates_md_href, "target_blank": True},
                    {"label": "下载", "href": candidates_md_download_href, "target_blank": False},
                ],
            },
            {
                "title": "候选股 CSV",
                "desc": "导出候选池与分数明细",
                "actions": [
                    {"label": "查看", "href": candidates_csv_href, "target_blank": True},
                    {"label": "下载", "href": candidates_csv_download_href, "target_blank": False},
                ],
            },
            {
                "title": "最新回测报告",
                "desc": "打开最新策略回测详情",
                "actions": [
                    {"label": "查看", "href": latest_report_href, "target_blank": True},
                    {"label": "下载", "href": latest_report_download_href, "target_blank": False},
                ],
            },
            {
                "title": "机制迭代摘要",
                "desc": "查看当前优先整改方向与最近晋级动作",
                "actions": [
                    {"label": "查看", "href": evolution_report_href, "target_blank": False},
                ],
            },
        ],
    }


def build_stock_guide_view_model() -> dict[str, Any]:
    return {
        "title": "指标释义与使用建议",
        "detail": "第一次看这个系统，建议先看这块",
        "items": [
            "健康评分：系统运行稳定性，越高越稳；低于 70 建议先排错再扩仓研究。",
            "夏普比率：单位风险收益，长期为负说明策略质量需优化。",
            "最大回撤：历史最深亏损幅度，数值越大风险越高。",
            "综合分：信号强度、上涨概率、预测收益、风险惩罚的合成分。",
            "建议仓位/止损止盈：是模型与风控给出的研究参考值，需结合你的风险偏好执行。",
            "候选建议：用于“研究优先级”排序，不是保证盈利的自动买入指令。",
        ],
    }


def build_stock_charts_view_model(
    *,
    health_chart_html: str,
    backtest_equity_html: str,
    backtest_drawdown_html: str,
    backtest_chart_html: str,
    backtest_map_chart_html: str,
    candidate_map_chart_html: str,
    candidate_chart_html: str,
) -> dict[str, Any]:
    return {
        "title": "图形监控",
        "detail": "从趋势图和分布图快速判断系统状态，而不是只看文本",
        "chart_html_blocks": [
            health_chart_html,
            backtest_equity_html,
            backtest_drawdown_html,
            backtest_chart_html,
            backtest_map_chart_html,
            candidate_map_chart_html,
            candidate_chart_html,
        ],
    }


def build_stock_opportunities_view_model(
    *,
    top_code: str,
    candidate_name: str,
    top1_signal: str,
    top1_risk: str,
    candidate_count: int,
    candidate_generated_at: str,
    generation_mode_label: str,
    basket_dual_track_rows: list[dict[str, str]],
    candidate_cards: list[dict[str, str]],
    candidate_hrefs: list[str],
) -> dict[str, Any]:
    return {
        "display_contract": {
            "conclusion_title": top_code,
            "conclusion_sub": f"{candidate_name} · {top1_signal} · {top1_risk}",
            "evidence_title": f"{candidate_count} 个候选",
            "evidence_sub": f"生成 {candidate_generated_at} · 类型 {generation_mode_label}",
            "boundary_title": "研究优先级",
            "boundary_sub": "候选卡只用于下钻顺序，不代表可交易或可闭合样本。",
            "next_title": "下钻候选详情",
            "next_sub": "先核对风险、证据和第一屏行动锁。",
        },
        "basket_dual_track_rows": basket_dual_track_rows,
        "cards": [
            {
                **card,
                "rank": str(idx + 1),
                "href": candidate_hrefs[idx] if idx < len(candidate_hrefs) else "#",
            }
            for idx, card in enumerate(candidate_cards[:5])
        ],
    }


def build_stock_prefilter_view_model(
    *,
    prefilter_artifact_status: dict[str, Any],
    db_latest_trade_date: str,
    top_candidates: list[dict[str, Any]],
    exclusion_summary_rows: list[dict[str, Any]],
    top_exclusion_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    prefilter_has_nonstandard_code = False
    prefilter_has_missing_name = False

    prefilter_table_rows: list[dict[str, str]] = []
    for idx, row in enumerate(top_candidates, start=1):
        ts_code = str(row.get("ts_code", "-"))
        stock_name = str(row.get("stock_name", "") or "")
        is_standard_code = (
            len(ts_code) == 9 and ts_code[:6].isdigit() and ts_code[6] == "." and ts_code[7:] in {"SH", "SZ", "BJ"}
        )
        prefilter_has_nonstandard_code = prefilter_has_nonstandard_code or not is_standard_code
        prefilter_has_missing_name = prefilter_has_missing_name or not stock_name
        name_display = stock_name if stock_name else ("诊断样本，非正式A股代码" if not is_standard_code else "名称映射缺失")
        prefilter_table_rows.append(
            {
                "rank": str(idx),
                "ts_code": ts_code,
                "name_display": name_display,
                "is_standard_code": "1" if is_standard_code else "",
                "prefilter_score": _display_missing(row.get("prefilter_score", "-"), "待生成"),
                "prefilter_reason": _display_missing(row.get("prefilter_reason", "-"), "暂无入池理由，需回看 artifact"),
            }
        )

    exclusion_summary_table_rows = [
        {
            "rank": str(idx),
            "reason": str(row.get("reason", "-")),
            "count": str(row.get("count", "0")),
            "share_pct": str(row.get("share_pct", "0.0%")),
        }
        for idx, row in enumerate(exclusion_summary_rows, start=1)
    ]

    top_exclusion_table_model_rows: list[dict[str, str]] = []
    for idx, row in enumerate(top_exclusion_rows, start=1):
        ts_code = str(row.get("ts_code", "-"))
        stock_name = str(row.get("stock_name", "") or "")
        is_standard_code = (
            len(ts_code) == 9 and ts_code[:6].isdigit() and ts_code[6] == "." and ts_code[7:] in {"SH", "SZ", "BJ"}
        )
        prefilter_has_nonstandard_code = prefilter_has_nonstandard_code or not is_standard_code
        prefilter_has_missing_name = prefilter_has_missing_name or not stock_name
        name_display = stock_name if stock_name else ("诊断样本，非正式A股代码" if not is_standard_code else "名称映射缺失")
        top_exclusion_table_model_rows.append(
            {
                "rank": str(idx),
                "ts_code": ts_code,
                "name_display": name_display,
                "is_standard_code": "1" if is_standard_code else "",
                "reason": _display_missing(row.get("exclusion_reason_zh", "-"), "暂无出池原因，需回看 artifact"),
            }
        )

    prefilter_row_count = _display_missing(prefilter_artifact_status.get("row_count", "0"), "0")
    prefilter_trade_date = _display_missing(prefilter_artifact_status.get("trade_date", "-"), "等待真实交易日")
    prefilter_freshness_status = str(prefilter_artifact_status.get("freshness_status", "unknown") or "unknown")
    prefilter_freshness_note = str(prefilter_artifact_status.get("freshness_note", "预筛产物状态待确认") or "预筛产物状态待确认")
    prefilter_trade_date_label = (
        f"{prefilter_trade_date}（过期）" if prefilter_freshness_status in {"stale", "blocked"} else prefilter_trade_date
    )
    prefilter_top1 = _display_missing(prefilter_artifact_status.get("top1", "-"), "暂无正式Top1")
    prefilter_top1_reason = _display_missing(prefilter_artifact_status.get("top1_reason", "-"), "暂无入池理由，需回看 artifact")
    prefilter_top_exclusion_reason = _display_missing(prefilter_artifact_status.get("top_exclusion_reason", "-"), "暂无出池原因")
    prefilter_quality_label = "诊断样本，不作为对外结论" if prefilter_has_nonstandard_code else "正式A股代码口径"
    if prefilter_has_missing_name and not prefilter_has_nonstandard_code:
        prefilter_quality_label = "正式代码，名称映射待补齐"

    guard_notes = [
        "对外结论以第一屏受控等待驾驶舱为准",
        "本区只解释全市场预筛过程，不触发样本闭合、不写 ledger",
        "非标准代码会标记为诊断样本，不能进入正式推荐表达",
    ]
    return {
        "display_contract": {
            "conclusion_title": prefilter_quality_label,
            "conclusion_sub": f"Top1 {prefilter_top1}",
            "evidence_title": f'{prefilter_row_count} / {str(prefilter_artifact_status.get("market_symbol_count", "0"))}',
            "evidence_sub": f'预筛交易日 {prefilter_trade_date_label} · 数据最新交易日 {db_latest_trade_date} · 生成 {str(prefilter_artifact_status.get("generated_at", "-"))}',
            "boundary_title": "专家证据区",
            "boundary_sub": "不替代主结论，不触发样本闭合、不写 ledger。",
            "next_title": "回看证据驾驶舱",
            "next_sub": "先确认等待状态和真实市场数据门。",
        },
        "quality_label": prefilter_quality_label,
        "trade_date_label": prefilter_trade_date_label,
        "freshness_note": prefilter_freshness_note,
        "top1": prefilter_top1,
        "top1_reason": prefilter_top1_reason,
        "top_exclusion_reason": prefilter_top_exclusion_reason,
        "guard_notes": guard_notes,
        "kpi_rows": [
            [
                {"label": "预筛总数", "value": prefilter_row_count, "sub": f'进入预筛结果表的标的数量 ｜ 覆盖率 {str(prefilter_artifact_status.get("pass_rate_pct", "0.0%"))}'},
                {"label": "最新交易日", "value": prefilter_trade_date_label, "sub": prefilter_freshness_note},
                {"label": "预筛Top1", "value": prefilter_top1, "sub": prefilter_top1_reason},
            ],
            [
                {"label": "全市场样本数", "value": str(prefilter_artifact_status.get("market_symbol_count", "0")), "sub": "当前交易日进入预筛诊断的股票数"},
                {"label": "出池数量", "value": str(prefilter_artifact_status.get("excluded_count", "0")), "sub": f'未进入当前预筛名单的股票数 ｜ 占比 {str(prefilter_artifact_status.get("excluded_rate_pct", "0.0%"))}'},
                {"label": "主要出池原因", "value": prefilter_top_exclusion_reason, "sub": f'涉及 {str(prefilter_artifact_status.get("top_exclusion_reason_count", "0"))} 只股票'},
            ],
        ],
        "top10_rows": prefilter_table_rows,
        "exclusion_rows": exclusion_summary_table_rows,
        "top_exclusion_rows": top_exclusion_table_model_rows,
        "generated_at": str(prefilter_artifact_status.get("generated_at", "-")),
        "top10_count": str(prefilter_artifact_status.get("top10_count", "0")),
    }


def build_stock_selection_funnel_view_model(
    *,
    market_sample_count: int,
    prefilter_pass_count: int,
    final_candidate_count: int,
    top_code: str,
    pass_rate_pct: str,
    top_exclusion_reason: str,
    top_exclusion_reason_count: str,
    configured_liquidity_min_turnover: str,
    effective_liquidity_min_turnover: str,
) -> dict[str, Any]:
    funnel_max = max(market_sample_count, prefilter_pass_count, final_candidate_count, 1)
    funnel_rows = [
        ("全市场样本", market_sample_count, "诊断基底"),
        ("预筛通过", prefilter_pass_count, "进入深度预测池"),
        ("最终候选", final_candidate_count, "进入今日候选板"),
        ("Top1", 1 if top_code and top_code not in {"-", "暂无"} else 0, _display_candidate_focus_code(top_code)),
    ]
    rendered_rows = [
        {
            "label": label,
            "value": str(value),
            "sub": sub,
            "width_pct": f"{max(6.0, (float(value) / float(funnel_max)) * 100.0 if funnel_max else 0.0):.2f}",
        }
        for label, value, sub in funnel_rows
    ]
    return {
        "display_contract": {
            "conclusion_title": f"{prefilter_pass_count} 进预筛，{final_candidate_count} 进候选",
            "conclusion_sub": f"Top1 {_display_candidate_focus_code(top_code)}",
            "evidence_title": f"{market_sample_count} → {prefilter_pass_count} → {final_candidate_count}",
            "evidence_sub": f"通过率 {pass_rate_pct} · 主要出池 {top_exclusion_reason}",
            "boundary_title": "漏斗解释",
            "boundary_sub": "解释筛选收缩，不替代主结果和证据驾驶舱。",
            "next_title": "检查异常收缩",
            "next_sub": "若样本过少，先查预筛口径和数据覆盖。",
        },
        "kpi_rows": [[
            {"label": "配置流动性门槛", "value": configured_liquidity_min_turnover, "sub": "原始配置值，不一定直接生效"},
            {"label": "实际流动性门槛", "value": effective_liquidity_min_turnover, "sub": "按市场分布校准后使用"},
            {"label": "预筛通过率", "value": pass_rate_pct, "sub": "全市场样本中进入深度预测池的比例"},
        ]],
        "metric_chips": [
            f"全市场样本 {market_sample_count}",
            f"预筛通过 {prefilter_pass_count}",
            f"最终候选 {final_candidate_count}",
            f"主要出池原因 {top_exclusion_reason} · {top_exclusion_reason_count} 只",
        ],
        "funnel_rows": rendered_rows,
    }
