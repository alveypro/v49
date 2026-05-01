from __future__ import annotations

from typing import Any, Callable, Dict

import pandas as pd
import streamlit as st


GATE_CODE_LABELS = {
    "gate_eval_error": "发布门禁校验失败",
    "validation_review_failed": "验证结果生成失败",
    "execution_rate_low": "历史执行率偏低",
    "avg_realized_return_weak": "历史已实现收益偏弱",
    "win_rate_low": "历史胜率偏低",
    "max_drawdown_high": "历史回撤偏高",
    "strategy_changed": "当前主策略发生变化",
    "governance_halt": "治理层要求暂停发布",
    "risk_block": "风险门禁阻断发布",
    "risk_review": "风险门禁要求人工复核",
    "risk_canary": "风险门禁仅允许灰度",
}


def _to_int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _to_float(value: Any) -> float | None:
    try:
        if value in (None, ""):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _nonempty_text(value: Any, fallback: str = "-") -> str:
    text = str(value or "").strip()
    return text or fallback


def _format_percent(value: Any, *, digits: int = 1) -> str:
    parsed = _to_float(value)
    if parsed is None:
        return "-"
    return f"{parsed:.{digits}f}%"


def _format_decimal(value: Any, *, digits: int = 2) -> str:
    parsed = _to_float(value)
    if parsed is None:
        return "-"
    return f"{parsed:.{digits}f}"


def _gate_code_label(code: Any) -> str:
    normalized = str(code or "").strip().lower()
    if not normalized:
        return ""
    return GATE_CODE_LABELS.get(normalized, normalized.replace("_", " "))


def _format_gate_group(title: str, codes: list[Any]) -> str | None:
    labels = [_gate_code_label(code) for code in codes if _gate_code_label(code)]
    if not labels:
        return None
    return f"{title}：{'；'.join(labels)}"


def _build_release_detail_payload(
    *,
    selected_row: Dict[str, Any],
    release_decision: str,
    gate_review: Dict[str, Any],
    canary_scope: Dict[str, Any],
    outcome_review: Dict[str, Any],
    override_audits: list[Dict[str, Any]],
    structured_count: int,
) -> Dict[str, Any]:
    return {
        "decision_date": str(selected_row.get("decision_date") or ""),
        "release_decision": str(release_decision or ""),
        "gate_passed": bool(gate_review.get("passed")),
        "gate_summary": str(gate_review.get("summary") or ""),
        "gate_codes": list(gate_review.get("gates") or []),
        "gate_blocking_codes": list(gate_review.get("blocking_gates") or []),
        "gate_review_codes": list(gate_review.get("review_gates") or []),
        "gate_canary_codes": list(gate_review.get("canary_gates") or []),
        "gate_metrics": gate_review.get("metrics") or {},
        "validation_review": gate_review.get("validation_review") or {},
        "validation_streak": gate_review.get("validation_streak") or {},
        "governance_review": gate_review.get("governance_review") or {},
        "canary_scope": canary_scope or {},
        "outcome_review": outcome_review or {},
        "override_audits": override_audits or [],
        "release_status": str(selected_row.get("release_status") or ""),
        "approved_by": str(selected_row.get("approved_by") or ""),
        "approved_at": str(selected_row.get("approved_at") or ""),
        "release_note": str(selected_row.get("release_note") or ""),
        "rollback_reason": str(selected_row.get("rollback_reason") or ""),
        "replaces_decision_date": str(selected_row.get("replaces_decision_date") or ""),
        "replaced_by_decision_date": str(selected_row.get("replaced_by_decision_date") or ""),
        "source_type": str(selected_row.get("source_type") or ""),
        "source_label": str(selected_row.get("source_label") or ""),
        "trade_date": str(selected_row.get("trade_date") or ""),
        "selected_count": _to_int(selected_row.get("selected_count")),
        "structured_count": _to_int(structured_count),
        "is_active": _to_int(selected_row.get("is_active")),
    }


def _summarize_release_detail(payload: Dict[str, Any]) -> Dict[str, Any]:
    decision_label_map = {
        "block": "阻断",
        "review": "人工复核",
        "canary": "灰度发布",
        "active": "正式生效",
    }
    status_label_map = {
        "draft": "草稿",
        "canary": "灰度",
        "active": "生效中",
        "rolled_back": "已回滚",
        "archived": "已归档",
    }
    validation_review = payload.get("validation_review") or {}
    validation_streak = payload.get("validation_streak") or {}
    governance_review = payload.get("governance_review") or {}
    canary_scope = payload.get("canary_scope") or {}
    outcome_review = payload.get("outcome_review") or {}
    override_audits = payload.get("override_audits") or []

    gate_lines = []
    summary = _nonempty_text(payload.get("gate_summary"), fallback="")
    if summary:
        gate_lines.append(summary)
    for item in (
        _format_gate_group("阻断原因", list(payload.get("gate_blocking_codes") or [])),
        _format_gate_group("人工复核原因", list(payload.get("gate_review_codes") or [])),
        _format_gate_group("灰度原因", list(payload.get("gate_canary_codes") or [])),
    ):
        if item:
            gate_lines.append(item)

    validation_lines = []
    if validation_review:
        validation_lines.append(
            "近期待验证表现："
            f"执行率 {_format_percent(validation_review.get('execution_rate'))}，"
            f"平均已实现收益 {_format_percent(validation_review.get('avg_realized_return_pct'))}，"
            f"胜率 {_format_percent(validation_review.get('win_rate'))}。"
        )
        validation_gate_line = _format_gate_group("验证关注项", list(validation_review.get("gates") or []))
        if validation_gate_line:
            validation_lines.append(validation_gate_line)
    streak = _to_int(validation_streak.get("consecutive_severe_runs"))
    if streak > 0:
        validation_lines.append(f"验证红线已连续触发 {streak} 次。")

    governance_lines = []
    governance_status = _nonempty_text(governance_review.get("status"), fallback="")
    governance_summary = _nonempty_text(governance_review.get("summary"), fallback="")
    if governance_status or governance_summary:
        governance_lines.append(
            f"治理复核：{governance_status or '已记录'}"
            f"{'；' + governance_summary if governance_summary else ''}"
        )
    governance_gate_line = _format_gate_group("治理关注项", list(governance_review.get("gates") or []))
    if governance_gate_line:
        governance_lines.append(governance_gate_line)

    canary_lines = []
    if canary_scope:
        buckets = "、".join(str(x) for x in (canary_scope.get("allowed_buckets") or []) if str(x).strip()) or "-"
        selected_codes = "、".join(str(x) for x in (canary_scope.get("selected_codes") or [])[:5] if str(x).strip()) or "-"
        canary_lines.append(
            f"灰度范围：队列 {buckets}，样本上限 {_to_int(canary_scope.get('sample_limit'))}，"
            f"时段 {_nonempty_text(canary_scope.get('window_start'))}-{_nonempty_text(canary_scope.get('window_end'))}。"
        )
        if selected_codes != "-":
            canary_lines.append(f"已选灰度样本：{selected_codes}")
        if str(canary_scope.get("scope_note") or "").strip():
            canary_lines.append(f"灰度备注：{str(canary_scope.get('scope_note') or '').strip()}")

    outcome_lines = []
    if outcome_review:
        for key in ("summary", "status", "conclusion"):
            text = str(outcome_review.get(key) or "").strip()
            if text:
                outcome_lines.append(f"发布后复盘：{text}")
                break
        execution_rate = outcome_review.get("execution_rate")
        realized_return = outcome_review.get("avg_realized_return_pct")
        if execution_rate not in (None, "") or realized_return not in (None, ""):
            outcome_lines.append(
                f"发布后结果：执行率 {_format_percent(execution_rate)}，"
                f"平均已实现收益 {_format_percent(realized_return)}。"
            )

    override_lines = []
    if override_audits:
        latest_override = override_audits[0]
        operator = _nonempty_text(latest_override.get("operator_name"))
        created_at = _nonempty_text(latest_override.get("created_at"))
        reason = _nonempty_text(latest_override.get("override_reason"))
        requested = _nonempty_text(latest_override.get("requested_decision"))
        override_lines.append(f"最近一次例外批准：{operator} 在 {created_at} 请求 {requested}，原因：{reason}。")

    relation_lines = []
    if str(payload.get("replaces_decision_date") or "").strip():
        relation_lines.append(f"本批次替换 {payload['replaces_decision_date']}。")
    if str(payload.get("replaced_by_decision_date") or "").strip():
        relation_lines.append(f"本批次后续被 {payload['replaced_by_decision_date']} 替换。")
    if str(payload.get("rollback_reason") or "").strip():
        relation_lines.append(f"回滚/归档原因：{payload['rollback_reason']}")
    if str(payload.get("release_note") or "").strip():
        relation_lines.append(f"发布说明：{payload['release_note']}")

    return {
        "overview": {
            "decision": decision_label_map.get(str(payload.get("release_decision") or "").lower(), "阻断"),
            "status": status_label_map.get(str(payload.get("release_status") or "").lower(), "草稿"),
            "source": (
                f"{_nonempty_text(payload.get('source_type'))}"
                f"{'/' + str(payload.get('source_label')) if str(payload.get('source_label') or '').strip() else ''}"
            ),
            "trade_date": _nonempty_text(payload.get("trade_date")),
            "selected_count": _to_int(payload.get("selected_count")),
            "structured_count": _to_int(payload.get("structured_count")),
            "gate_passed": "通过" if bool(payload.get("gate_passed")) else "未通过",
            "approved_by": _nonempty_text(payload.get("approved_by")),
            "approved_at": _nonempty_text(payload.get("approved_at")),
            "is_active": "是" if _to_int(payload.get("is_active")) == 1 else "否",
        },
        "gate_lines": gate_lines,
        "validation_lines": validation_lines,
        "governance_lines": governance_lines,
        "canary_lines": canary_lines,
        "outcome_lines": outcome_lines,
        "override_lines": override_lines,
        "relation_lines": relation_lines,
    }


def _default_feedback_status_for_action(action: str) -> str:
    action = str(action or "").strip().lower()
    return {
        "watch": "skipped",
        "hold": "skipped",
        "skip": "cancelled",
        "cancelled": "cancelled",
        "buy": "done",
        "switch": "done",
    }.get(action, "")


def _build_system_adopt_note(selected: pd.Series) -> str:
    system_action = str(selected.get("system_suggested_action") or "").strip()
    decision_reason = str(selected.get("decision_gate_reason") or "").strip()
    system_reason = str(selected.get("system_reason") or "").strip()
    parts = [f"采纳系统建议：{system_action or 'N/A'}"]
    if decision_reason:
        parts.append(f"分流原因：{decision_reason}")
    elif system_reason:
        parts.append(f"系统原因：{system_reason}")
    return "；".join(parts)


DECISION_PRESET_MAP = {
    "请选择": ("", ""),
    "买入完成": ("buy", "done"),
    "观察跳过": ("watch", "skipped"),
    "淘汰取消": ("skip", "cancelled"),
    "换仓执行": ("switch", "done"),
    "持有观察": ("hold", "skipped"),
    "人工取消": ("cancelled", "cancelled"),
    "自定义": ("", ""),
}


def _preset_label_for_action_status(action: str, status: str) -> str:
    action = str(action or "").strip().lower()
    status = str(status or "").strip().lower()
    for label, (preset_action, preset_status) in DECISION_PRESET_MAP.items():
        if label == "自定义":
            continue
        if action == preset_action and status == preset_status:
            return label
    return "自定义" if action or status else "请选择"


def _is_actionable_batch(row: Dict[str, Any], structured_count: int) -> bool:
    release_status = str(row.get("release_status") or "").strip().lower()
    selected_count = int(row.get("selected_count") or 0)
    if release_status in {"archived", "rolled_back"}:
        return False
    if selected_count <= 0:
        return False
    if structured_count <= 0:
        return False
    return True


def render_airivo_today_execution_queues(
    *,
    db_path: str,
    runtime_snapshot: Dict[str, Any],
    safe_file_mtime: Callable[[str], float],
    latest_execution_queue_cached: Callable[..., tuple[pd.DataFrame, Dict[str, Any]]],
    bucket_feedback_rows: Callable[[pd.DataFrame], pd.DataFrame],
    feedback_bucket_summary: Callable[[pd.DataFrame], Dict[str, int]],
) -> None:
    db_mtime = safe_file_mtime(db_path)
    queue_rows, queue_meta = latest_execution_queue_cached(db_path, db_mtime, limit=120)
    if queue_rows is None or queue_rows.empty:
        st.info("今日结构化执行队列尚未生成。请先运行日报流程或生成 overnight decision。")
        return
    queue_rows = bucket_feedback_rows(queue_rows)
    counts = feedback_bucket_summary(queue_rows)
    st.markdown("### 今日结构化执行队列")
    st.caption(
        f"决策日 {queue_meta.get('decision_date') or 'N/A'} | 交易日 {queue_meta.get('trade_date') or 'N/A'} | "
        f"风险门禁 {queue_meta.get('risk_level') or 'N/A'} | 来源 {queue_meta.get('source_type') or 'N/A'}"
        f"{'/' + str(queue_meta.get('source_label') or '') if queue_meta.get('source_label') else ''}。"
        "这不是 pending 总表，而是系统已经裁决后的今日任务分发。"
    )

    h1, h2, h3, h4 = st.columns(4)
    h1.metric("直接执行", counts.get("direct_execute", 0))
    h2.metric("观察等待", counts.get("observe", 0))
    h3.metric("自动淘汰", counts.get("auto_reject", 0))
    h4.metric("人工确认", counts.get("manual_review", 0))

    risk_level = str(queue_meta.get("risk_level") or "").upper()
    if risk_level in {"RED", "BLOCK", "STOP"}:
        st.error("当前风险门禁不允许把任何队列直接升级为生产执行。今日队列仅用于研究复核。")
    elif risk_level in {"YELLOW", "REVIEW"}:
        st.warning("当前风险门禁为复核模式。直接执行队列也应视为人工复核建议，而不是自动买入。")
    else:
        st.success("当前是可生产口径。系统已经先完成首轮裁决，你只需要处理少数异常项。")

    queue_specs = [
        ("direct_execute", "系统直接执行", "这些是今日最接近执行清单的标的，但仍不代表已成交。"),
        ("observe", "系统观察等待", "系统建议继续观察，不占用人工逐条点选。"),
        ("auto_reject", "系统自动淘汰", "系统认为今天不该做，保留原因即可。"),
        ("manual_review", "需要人工确认", "只有这些异常项应该消耗你的注意力。"),
    ]
    for bucket, title, help_text in queue_specs:
        subset = queue_rows[queue_rows["decision_bucket"] == bucket].copy()
        with st.expander(f"{title}（{len(subset)}）", expanded=(bucket == "manual_review")):
            st.caption(help_text)
            if subset.empty:
                st.caption("当前无记录")
                continue
            preview = subset[
                [
                    "trade_date",
                    "ts_code",
                    "stock_name",
                    "strategy",
                    "planned_action",
                    "system_suggested_action",
                    "system_confidence",
                    "decision_gate_reason",
                ]
            ].rename(
                columns={
                    "trade_date": "执行日",
                    "ts_code": "代码",
                    "stock_name": "名称",
                    "strategy": "策略",
                    "planned_action": "计划动作",
                    "system_suggested_action": "系统建议",
                    "system_confidence": "置信度",
                    "decision_gate_reason": "原因",
                }
            )
            st.dataframe(preview, use_container_width=True, hide_index=True)

    if counts.get("manual_review", 0) > 0:
        st.caption("建议动作顺序：先处理“需要人工确认”，再进入执行中心做批量采纳。")
    else:
        st.caption("今日没有异常项。执行中心只需要确认观察/淘汰队列是否批量采纳即可。")


def render_airivo_batch_manager(
    *,
    db_path: str,
    safe_file_mtime: Callable[[str], float],
    recent_execution_batches_cached: Callable[..., pd.DataFrame],
    evaluate_batch_release_gate: Callable[..., Dict[str, Any]],
    has_role: Callable[[str], bool],
    guard_action: Callable[..., bool],
    append_action_audit: Callable[..., None],
    set_active_batch: Callable[..., tuple[bool, str]],
    set_canary_batch: Callable[..., tuple[bool, str]],
    archive_batch: Callable[..., tuple[bool, str]],
    load_queue_batch_rows: Callable[..., pd.DataFrame],
    compare_queue_batches: Callable[..., pd.DataFrame],
    get_canary_scope: Callable[..., Any],
    get_override_audits: Callable[..., Any],
    get_release_outcome_review: Callable[..., Any],
) -> None:
    db_mtime = safe_file_mtime(db_path)
    batches = recent_execution_batches_cached(db_path, db_mtime, limit=3)
    st.markdown("### 批次管理")
    st.caption("当前队列不再只是“最新一批”，而是有发布状态、审批记录、替换关系和回滚理由的受控批次。")
    if batches is None or batches.empty:
        st.info("当前还没有可管理的执行批次。")
        return
    active_row = batches[batches["is_active"] == 1].head(1)
    if active_row.empty:
        active_row = batches.head(1)
    active = active_row.iloc[0]
    status_label_map = {
        "draft": "草稿",
        "canary": "灰度",
        "active": "生效中",
        "rolled_back": "已回滚",
        "archived": "已归档",
    }
    active_status = status_label_map.get(str(active.get("release_status") or "").strip().lower(), "草稿")
    st.info(
        f"当前生效批次：{active['decision_date']} | source={active['source_type'] or 'N/A'}"
        f"{('/' + str(active['source_label'])) if str(active['source_label'] or '') else ''} | "
        f"trade_date={active['trade_date']} | picks={int(active['selected_count'] or 0)} | status={active_status}"
    )
    if str(active.get("approved_by") or "").strip() or str(active.get("approved_at") or "").strip():
        st.caption(
            f"审批记录：{str(active.get('approved_by') or '-') or '-'} @ {str(active.get('approved_at') or '-') or '-'}"
        )

    batch_records = batches.to_dict(orient="records")
    structured_count_map: Dict[str, int] = {}
    for row in batch_records:
        decision_date = str(row.get("decision_date") or "").strip()
        if not decision_date:
            continue
        try:
            structured_count_map[decision_date] = int(len(load_queue_batch_rows(db_path, decision_date)))
        except Exception:
            structured_count_map[decision_date] = 0

    actionable_rows = [
        row for row in batch_records
        if _is_actionable_batch(row, structured_count_map.get(str(row.get("decision_date") or ""), 0))
    ]
    historical_rows = [
        row for row in batch_records
        if not _is_actionable_batch(row, structured_count_map.get(str(row.get("decision_date") or ""), 0))
    ]

    display = batches.rename(
        columns={
            "decision_date": "批次",
            "trade_date": "交易日",
            "source_type": "来源",
            "source_label": "标签",
            "risk_level": "门禁",
            "selected_count": "推荐数",
            "release_status": "发布状态",
            "release_note": "发布说明",
            "rollback_reason": "回滚原因",
            "approved_by": "审批人",
            "approved_at": "审批时间",
            "replaces_decision_date": "替换批次",
            "replaced_by_decision_date": "被替换为",
            "is_active": "生效中",
            "activated_at": "生效时间",
            "created_at": "创建时间",
        }
    )
    display["生效中"] = display["生效中"].map(lambda x: "是" if int(x or 0) == 1 else "否")
    display["发布状态"] = display["发布状态"].fillna("").astype(str).str.lower().map(status_label_map).fillna("草稿")
    display["结构化队列数"] = display["批次"].map(lambda x: structured_count_map.get(str(x or ""), 0))

    if actionable_rows:
        st.caption("主发布流只保留真正可操作的批次：必须有推荐、有结构化执行队列、且未归档/未回滚。")
        actionable_df = display[display["批次"].isin([str(row.get("decision_date") or "") for row in actionable_rows])].copy()
        st.dataframe(actionable_df, use_container_width=True, hide_index=True)
    else:
        st.warning("当前没有可进入主发布流的批次。请先生成有效候选并形成结构化执行队列。")

    if historical_rows:
        with st.expander("历史 / 异常批次", expanded=False):
            hist_df = display[display["批次"].isin([str(row.get("decision_date") or "") for row in historical_rows])].copy()
            if not hist_df.empty:
                hist_df["退出主发布流原因"] = hist_df.apply(
                    lambda r: (
                        "已归档/已回滚" if str(r.get("发布状态") or "") in {"已归档", "已回滚"}
                        else "推荐数为0" if int(r.get("推荐数") or 0) <= 0
                        else "缺少结构化执行队列" if int(r.get("结构化队列数") or 0) <= 0
                        else "不满足主发布条件"
                    ),
                    axis=1,
                )
                st.dataframe(hist_df, use_container_width=True, hide_index=True)

    options = [
        f"{row['decision_date']} | {row['source_type']}/{row['source_label'] or '-'} | picks={int(row['selected_count'] or 0)} | queue={structured_count_map.get(str(row.get('decision_date') or ''), 0)} | {status_label_map.get(str(row.get('release_status') or '').strip().lower(), '草稿')}"
        for row in actionable_rows
    ]
    if not options:
        return
    selected = st.selectbox("选择一个批次", options, key="airivo_batch_selected")
    selected_batch = selected.split(" | ")[0]
    selected_row = next((row for row in actionable_rows if str(row.get("decision_date") or "") == selected_batch), {})
    selected_primary = str(selected_row.get("source_label") or "").strip().lower()
    gate_review = evaluate_batch_release_gate(db_path, selected_batch, current_primary=selected_primary)
    release_decision = str(gate_review.get("decision") or "block").strip().lower()
    decision_label_map = {
        "block": "阻断",
        "review": "人工复核",
        "canary": "灰度发布",
        "active": "正式生效",
    }
    can_govern = has_role("admin")
    operator_name = st.text_input(
        "审批/操作人",
        value=str(st.session_state.get("airivo_operator_name") or "system"),
        key="airivo_batch_operator",
        disabled=not can_govern,
    )
    st.session_state["airivo_operator_name"] = operator_name
    release_note = st.text_input("发布说明", value="", key="airivo_batch_release_note", disabled=not can_govern)
    override_reason = st.text_input("例外批准原因", value="", key="airivo_batch_override_reason", disabled=not can_govern)
    rollback_reason = st.text_input("回滚原因", value="", key="airivo_batch_rollback_reason", disabled=not can_govern)
    canary_buckets = st.multiselect(
        "灰度队列范围",
        options=["direct_execute", "observe"],
        default=["direct_execute", "observe"],
        key="airivo_canary_buckets",
        disabled=not can_govern,
    )
    canary_cols = st.columns(3)
    with canary_cols[0]:
        canary_sample_limit = int(st.number_input("灰度样本上限", min_value=1, max_value=20, value=2, step=1, key="airivo_canary_sample_limit", disabled=not can_govern))
    with canary_cols[1]:
        canary_window_start = st.text_input("灰度开始时段", value="09:35", key="airivo_canary_window_start", disabled=not can_govern)
    with canary_cols[2]:
        canary_window_end = st.text_input("灰度结束时段", value="10:15", key="airivo_canary_window_end", disabled=not can_govern)
    st.caption(f"发布裁决：{decision_label_map.get(release_decision, release_decision or '阻断')}")
    if release_decision == "active":
        st.success(str(gate_review.get("summary") or "当前批次可正式生效。"))
    elif release_decision == "canary":
        st.info(str(gate_review.get("summary") or "当前批次只允许灰度发布。"))
    elif release_decision == "review":
        st.warning(str(gate_review.get("summary") or "当前批次需要人工复核。"))
    else:
        st.error(str(gate_review.get("summary") or "当前批次未通过发布门禁。"))

    act_left, act_mid, act_right, act_far = st.columns([0.24, 0.22, 0.22, 0.32])
    with act_left:
        if st.button("批准并设为生效批次", key="airivo_activate_batch", use_container_width=True, disabled=(release_decision != "active") or (not can_govern)):
            if not guard_action("admin", "batch_activate", target=selected_batch, reason=release_note):
                return
            ok, msg = set_active_batch(
                db_path,
                selected_batch,
                approved_by=operator_name,
                release_note=release_note,
                rollback_reason=rollback_reason,
                current_primary=selected_primary,
            )
            if ok:
                append_action_audit("batch_activate", True, target=selected_batch, detail=msg, reason=release_note)
                st.success(msg)
                st.rerun()
            else:
                append_action_audit("batch_activate", False, target=selected_batch, detail=msg, reason=release_note)
                st.error(msg)
    with act_mid:
        if st.button("批准为灰度批次", key="airivo_canary_batch", use_container_width=True, disabled=(release_decision != "canary") or (not can_govern)):
            if not guard_action("admin", "batch_canary", target=selected_batch, reason=release_note):
                return
            ok, msg = set_canary_batch(
                db_path,
                selected_batch,
                approved_by=operator_name,
                release_note=release_note,
                current_primary=selected_primary,
                allowed_buckets=canary_buckets,
                sample_limit=canary_sample_limit,
                window_start=canary_window_start,
                window_end=canary_window_end,
            )
            if ok:
                append_action_audit(
                    "batch_canary",
                    True,
                    target=selected_batch,
                    detail=msg,
                    reason=release_note,
                    extra={"allowed_buckets": canary_buckets, "sample_limit": canary_sample_limit},
                )
                st.success(msg)
                st.rerun()
            else:
                append_action_audit("batch_canary", False, target=selected_batch, detail=msg, reason=release_note)
                st.error(msg)
    with act_right:
        if st.button("人工例外批准", key="airivo_override_activate_batch", use_container_width=True, disabled=(release_decision != "review") or (not str(override_reason or "").strip()) or (not can_govern)):
            if not guard_action("admin", "batch_override_activate", target=selected_batch, reason=override_reason):
                return
            ok, msg = set_active_batch(
                db_path,
                selected_batch,
                approved_by=operator_name,
                release_note=f"[override_review] {release_note}",
                rollback_reason=rollback_reason,
                current_primary=selected_primary,
                override_gate=True,
                override_reason=override_reason,
            )
            if ok:
                append_action_audit("batch_override_activate", True, target=selected_batch, detail=msg, reason=override_reason)
                st.success(msg)
                st.rerun()
            else:
                append_action_audit("batch_override_activate", False, target=selected_batch, detail=msg, reason=override_reason)
                st.error(msg)
    with act_far:
        if st.button("归档所选批次", key="airivo_archive_batch", use_container_width=True, disabled=int(selected_row.get("is_active") or 0) == 1 or (not can_govern)):
            if not guard_action("admin", "batch_archive", target=selected_batch, reason=rollback_reason):
                return
            ok, msg = archive_batch(
                db_path,
                selected_batch,
                operator_name=operator_name,
                archive_note=release_note or rollback_reason,
            )
            if ok:
                append_action_audit("batch_archive", True, target=selected_batch, detail=msg, reason=rollback_reason)
                st.success(msg)
                st.rerun()
            else:
                append_action_audit("batch_archive", False, target=selected_batch, detail=msg, reason=rollback_reason)
                st.error(msg)
        st.caption("正式生效只接受 active 级批次；canary 只做灰度；review 需要填写发布说明后才能做人工例外批准。")

    if len(batches) >= 2:
        current_batch = str(active["decision_date"] or "")
        previous_row = next((row for row in batches.to_dict(orient="records") if str(row.get("decision_date") or "") != current_batch), None)
        if previous_row:
            current_df = load_queue_batch_rows(db_path, current_batch)
            previous_df = load_queue_batch_rows(db_path, str(previous_row.get("decision_date") or ""))
            diff_df = compare_queue_batches(current_df, previous_df)
            with st.expander(f"批次对比：{current_batch} vs {previous_row.get('decision_date')}", expanded=False):
                if diff_df.empty:
                    st.caption("两批队列没有结构化差异。")
                else:
                    st.dataframe(diff_df, use_container_width=True, hide_index=True)

    if selected_row:
        canary_scope = get_canary_scope(db_path, selected_batch)
        override_audits = get_override_audits(db_path, selected_batch, limit=5)
        outcome_review = get_release_outcome_review(db_path, selected_batch)
        detail_payload = _build_release_detail_payload(
            selected_row=selected_row,
            release_decision=release_decision,
            gate_review=gate_review,
            canary_scope=canary_scope,
            outcome_review=outcome_review,
            override_audits=override_audits,
            structured_count=structured_count_map.get(selected_batch, 0),
        )
        detail_summary = _summarize_release_detail(detail_payload)
        with st.expander("所选批次发布详情", expanded=False):
            overview = detail_summary["overview"]
            st.markdown("#### 批次概览")
            top_cols = st.columns(4)
            top_cols[0].metric("发布结论", overview["decision"])
            top_cols[1].metric("当前状态", overview["status"])
            top_cols[2].metric("推荐数", overview["selected_count"])
            top_cols[3].metric("结构化队列", overview["structured_count"])

            meta_cols = st.columns(4)
            meta_cols[0].metric("门禁结果", overview["gate_passed"])
            meta_cols[1].metric("来源", overview["source"])
            meta_cols[2].metric("交易日", overview["trade_date"])
            meta_cols[3].metric("当前生效", overview["is_active"])

            st.caption(
                f"批次 {selected_batch} | 审批人 {_nonempty_text(overview['approved_by'])} | "
                f"审批时间 {_nonempty_text(overview['approved_at'])}"
            )

            st.markdown("#### 为什么是这个结论")
            if detail_summary["gate_lines"]:
                for line in detail_summary["gate_lines"]:
                    st.markdown(f"- {line}")
            else:
                st.caption("当前没有额外的门禁说明。")

            st.markdown("#### 历史验证与治理")
            combined_review_lines = detail_summary["validation_lines"] + detail_summary["governance_lines"]
            if combined_review_lines:
                for line in combined_review_lines:
                    st.markdown(f"- {line}")
            else:
                st.caption("当前没有额外的验证或治理提醒。")

            extra_lines = (
                detail_summary["canary_lines"]
                + detail_summary["outcome_lines"]
                + detail_summary["override_lines"]
                + detail_summary["relation_lines"]
            )
            if extra_lines:
                st.markdown("#### 发布动作与后续")
                for line in extra_lines:
                    st.markdown(f"- {line}")

            if st.checkbox("显示技术明细（调试）", value=False, key=f"airivo_batch_debug_{selected_batch}"):
                st.json(detail_payload)


def render_airivo_feedback_workbench(
    *,
    db_path: str,
    default_bucket: str,
    safe_file_mtime: Callable[[str], float],
    feedback_snapshot_cached: Callable[..., Dict[str, Any]],
    bucket_feedback_rows: Callable[[pd.DataFrame], pd.DataFrame],
    load_feedback_rows: Callable[..., pd.DataFrame],
    feedback_bucket_summary: Callable[[pd.DataFrame], Dict[str, int]],
    has_role: Callable[[str], bool],
    guard_action: Callable[..., bool],
    append_action_audit: Callable[..., None],
    update_feedback_row: Callable[..., tuple[bool, str]],
    apply_batch_feedback_action: Callable[..., tuple[bool, str]],
    refresh_realized_outcomes: Callable[..., tuple[bool, str]],
    preloaded_snapshot: Dict[str, Any] | None = None,
    preloaded_pending_rows: pd.DataFrame | None = None,
) -> None:
    db_mtime = safe_file_mtime(db_path)
    snapshot = preloaded_snapshot if isinstance(preloaded_snapshot, dict) else feedback_snapshot_cached(db_path, db_mtime)
    pending_rows = (
        preloaded_pending_rows.copy()
        if isinstance(preloaded_pending_rows, pd.DataFrame)
        else bucket_feedback_rows(load_feedback_rows(db_path, status_filter="pending", limit=200))
    )
    bucket_counts = feedback_bucket_summary(pending_rows)
    st.markdown("### 执行反馈工作台")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("待处理", int(snapshot.get("pending_rows", 0) or 0))
    c2.metric("已执行", int(snapshot.get("done_rows", 0) or 0))
    c3.metric("跳过", int(snapshot.get("skipped_rows", 0) or 0))
    c4.metric("结果回刷", int(snapshot.get("outcome_rows", 0) or 0))
    s1, s2, s3 = st.columns(3)
    s1.metric("系统建议", int(snapshot.get("system_suggested_rows", 0) or 0))
    s2.metric("人工覆盖", int(snapshot.get("human_override_rows", 0) or 0))
    s3.metric("缺覆盖原因", int(snapshot.get("missing_override_reason_rows", 0) or 0))
    if int(snapshot.get("missing_override_reason_rows", 0) or 0) > 0:
        st.warning("存在人工覆盖系统建议但未填写覆盖原因的记录。补齐前，这些样本不能用于严肃评估人工优于系统。")
    st.caption("只记录真实执行。没有买入价、未执行原因或操作人时，不要把 pending 改成完成。")

    q1, q2, q3, q4 = st.columns(4)
    q1.metric("需要人工确认", bucket_counts.get("manual_review", 0))
    q2.metric("系统直接执行", bucket_counts.get("direct_execute", 0))
    q3.metric("系统观察等待", bucket_counts.get("observe", 0))
    q4.metric("系统自动淘汰", bucket_counts.get("auto_reject", 0))
    st.caption("新的执行中心口径：人工只处理异常项；高置信度建议直接入执行/观察/淘汰队列。")

    control_left, control_mid, control_right = st.columns([0.45, 0.25, 0.30])
    with control_left:
        status_label = st.selectbox("记录范围", ["待处理 pending", "全部", "已执行 done", "已跳过 skipped", "已取消 cancelled"], index=0, key="airivo_feedback_status_filter")
    with control_mid:
        bucket_options = [
            ("manual_review", "仅异常项人工确认"),
            ("direct_execute", "仅系统直接执行"),
            ("observe", "仅系统观察等待"),
            ("auto_reject", "仅系统自动淘汰"),
            ("all", "显示当前范围全部"),
        ]
        default_bucket_index = next((idx for idx, item in enumerate(bucket_options) if item[0] == default_bucket), 0)
        bucket_label = st.selectbox("自动分流视图", [label for _, label in bucket_options], index=default_bucket_index, key="airivo_feedback_bucket_filter")
    with control_right:
        limit = st.number_input("显示条数", min_value=10, max_value=200, value=50, step=10, key="airivo_feedback_limit")

    filter_map = {
        "待处理 pending": "pending",
        "全部": "all",
        "已执行 done": "done",
        "已跳过 skipped": "skipped",
        "已取消 cancelled": "cancelled",
    }
    bucket_map = {label: value for value, label in bucket_options}
    selected_bucket = bucket_map.get(bucket_label, "manual_review")
    selected_status_filter = filter_map.get(status_label, "pending")
    if selected_status_filter == "pending":
        rows = pending_rows.copy().head(int(limit))
    else:
        rows = bucket_feedback_rows(load_feedback_rows(db_path, status_filter=selected_status_filter, limit=int(limit)))
    if selected_bucket != "all" and not rows.empty:
        rows = rows[rows["decision_bucket"] == selected_bucket].reset_index(drop=True)
    if rows.empty:
        st.info("当前范围没有执行反馈记录。")
    else:
        can_execute = has_role("operator")
        default_operator_name = str(st.session_state.get("airivo_operator_name") or "system")
        if selected_bucket == "manual_review":
            st.markdown("#### 异常项快速处理")
            st.caption("先用快速结论清掉大多数异常项；只有复杂分歧再进下面的逐条表单。")
            with st.form("airivo_manual_review_quick_defaults_form", clear_on_submit=False):
                quick_note = st.text_input(
                    "快速处理统一备注",
                    value=str(st.session_state.get("airivo_manual_review_quick_note") or "人工快速复核：本次不满足直接执行条件，已按结论处理。"),
                    key="airivo_manual_review_quick_note",
                    disabled=not can_execute,
                )
                quick_reason = st.text_input(
                    "快速人工理由",
                    value=str(st.session_state.get("airivo_manual_review_quick_reason") or "人工复核后不直接执行，转观察/取消。"),
                    key="airivo_manual_review_quick_reason",
                    disabled=not can_execute,
                )
                st.form_submit_button("更新快速处理默认值", use_container_width=True, disabled=not can_execute)
            quick_rows = rows.head(8)
            for review in quick_rows.itertuples(index=False):
                card = st.container(border=True)
                with card:
                    top_left, top_mid, top_right = st.columns([0.42, 0.38, 0.20])
                    with top_left:
                        st.markdown(f"**#{int(review.id)} {review.ts_code} {review.stock_name or ''}**")
                        st.caption(f"{review.trade_date} | system={getattr(review, 'system_suggested_action', '') or '-'} | planned={getattr(review, 'planned_action', '') or '-'}")
                    with top_mid:
                        st.caption(str(getattr(review, "decision_gate_reason", "") or "需要人工确认"))
                    with top_right:
                        pass
                    b1, b2, b3 = st.columns(3)
                    with b1:
                        if st.button("转观察", key=f"airivo_quick_watch_{int(review.id)}", use_container_width=True, disabled=not can_execute):
                            note = quick_note or "人工快速复核后转观察。"
                            override_reason = ""
                            if str(getattr(review, "system_suggested_action", "") or "").strip() not in {"watch", "hold"}:
                                override_reason = quick_reason or "人工复核后改为观察。"
                            if not guard_action("operator", "feedback_quick_watch", target=str(int(review.id)), reason=note):
                                return
                            ok, msg = update_feedback_row(
                                db_path,
                                int(review.id),
                                "watch",
                                "skipped",
                                note,
                                default_operator_name,
                                str(getattr(review, "system_suggested_action", "") or ""),
                                override_reason,
                            )
                            if ok:
                                append_action_audit("feedback_quick_watch", True, target=str(int(review.id)), detail=msg, reason=note, extra={"final_action": "watch", "execution_status": "skipped"})
                                st.success(msg)
                                st.rerun()
                            else:
                                append_action_audit("feedback_quick_watch", False, target=str(int(review.id)), detail=msg, reason=note, extra={"final_action": "watch", "execution_status": "skipped"})
                                st.error(msg)
                    with b2:
                        if st.button("本次放弃", key=f"airivo_quick_skip_{int(review.id)}", use_container_width=True, disabled=not can_execute):
                            note = quick_note or "人工快速复核后本次放弃。"
                            override_reason = ""
                            if str(getattr(review, "system_suggested_action", "") or "").strip() not in {"skip", "cancelled"}:
                                override_reason = quick_reason or "人工复核后本次取消。"
                            if not guard_action("operator", "feedback_quick_skip", target=str(int(review.id)), reason=note):
                                return
                            ok, msg = update_feedback_row(
                                db_path,
                                int(review.id),
                                "skip",
                                "cancelled",
                                note,
                                default_operator_name,
                                str(getattr(review, "system_suggested_action", "") or ""),
                                override_reason,
                            )
                            if ok:
                                append_action_audit("feedback_quick_skip", True, target=str(int(review.id)), detail=msg, reason=note, extra={"final_action": "skip", "execution_status": "cancelled"})
                                st.success(msg)
                                st.rerun()
                            else:
                                append_action_audit("feedback_quick_skip", False, target=str(int(review.id)), detail=msg, reason=note, extra={"final_action": "skip", "execution_status": "cancelled"})
                                st.error(msg)
                    with b3:
                        st.caption("复杂分歧走下方详情")

        display = rows.rename(
            columns={
                "id": "ID",
                "decision_date": "信号日",
                "trade_date": "执行日",
                "ts_code": "代码",
                "stock_name": "名称",
                "planned_action": "计划动作",
                "system_suggested_action": "系统建议",
                "system_confidence": "系统置信度",
                "system_reason": "系统原因",
                "final_action": "最终动作",
                "execution_status": "执行状态",
                "execution_note": "备注",
                "operator_name": "操作人",
                "human_override": "人工覆盖",
                "human_override_reason": "覆盖原因",
                "updated_at": "更新时间",
                "decision_bucket_label": "自动分流",
                "decision_gate_reason": "分流原因",
            }
        )
        display_cols = [
            "ID", "信号日", "执行日", "代码", "名称", "计划动作",
            "系统建议", "系统置信度", "自动分流", "分流原因",
            "最终动作", "执行状态", "备注", "操作人", "人工覆盖", "覆盖原因", "更新时间",
        ]
        display = display[[c for c in display_cols if c in display.columns]]
        st.dataframe(display, use_container_width=True, hide_index=True)

        option_labels = [
            f"#{int(r.id)} {r.trade_date} {r.ts_code} {r.stock_name or ''} | {getattr(r, 'decision_bucket_label', '') or '-'} | system={getattr(r, 'system_suggested_action', '') or '-'} | status={r.execution_status}"
            for r in rows.itertuples(index=False)
        ]
        selected_label = st.selectbox("选择一条候选处理", option_labels, key="airivo_feedback_selected_row")
        selected_pos = option_labels.index(selected_label)
        selected = rows.iloc[selected_pos]

        final_options = ["请选择", "buy", "watch", "skip", "switch", "hold", "cancelled"]
        status_options = ["请选择", "done", "skipped", "cancelled", "executed", "filled"]
        current_action = str(selected.get("final_action") or "")
        current_status = str(selected.get("execution_status") or "")
        system_action = str(selected.get("system_suggested_action") or "")
        system_status = str(selected.get("system_suggested_status") or "")
        system_confidence = str(selected.get("system_confidence") or "")
        system_reason = str(selected.get("system_reason") or "")
        system_default_action = system_action if system_action in final_options else ""
        system_default_status = _default_feedback_status_for_action(system_action)
        effective_action = current_action if current_action in final_options and current_action != "pending" else system_default_action
        effective_status = current_status if current_status in status_options and current_status != "pending" else system_default_status
        action_index = final_options.index(effective_action) if effective_action in final_options else 0
        status_index = status_options.index(effective_status) if effective_status in status_options else 0

        st.markdown("#### 系统建议")
        s1, s2, s3 = st.columns(3)
        s1.metric("建议动作", system_action or "未生成")
        s2.metric("建议状态", system_status or "未生成")
        s3.metric("置信度", system_confidence or "未生成")
        st.caption(f"自动分流：{selected.get('decision_bucket_label') or '需要人工确认'} | {selected.get('decision_gate_reason') or '无'}")
        if system_reason:
            st.info(system_reason)
        else:
            st.warning("这条记录还没有系统建议。请先运行反馈种子或刷新 pending 建议。")

        quick_adopt_allowed = bool(
            system_action in {"watch", "hold", "skip", "cancelled"}
            and system_default_status
            and str(selected.get("decision_bucket") or "") in {"manual_review", "observe", "auto_reject"}
        )
        default_operator_name = str(selected.get("operator_name") or st.session_state.get("airivo_operator_name") or "system")
        default_execution_note = str(selected.get("execution_note") or "").strip() or _build_system_adopt_note(selected)

        if quick_adopt_allowed:
            ql, qr = st.columns([0.72, 0.28])
            with ql:
                st.success(
                    f"这条记录可以一键采纳系统建议：`{system_action}` / `{system_default_status}`。"
                    " 只有你不同意系统结论时，才需要展开手工覆盖。"
                )
            with qr:
                if st.button(
                    "一键采纳系统建议",
                    type="primary",
                    key=f"airivo_accept_system_{int(selected['id'])}",
                    use_container_width=True,
                    disabled=not can_execute,
                ):
                    if not guard_action("operator", "feedback_accept_system", target=str(int(selected["id"])), reason=default_execution_note):
                        return
                    ok, msg = update_feedback_row(
                        db_path,
                        int(selected["id"]),
                        system_action,
                        system_default_status,
                        default_execution_note,
                        default_operator_name,
                        system_action,
                        "",
                    )
                    if ok:
                        append_action_audit(
                            "feedback_accept_system",
                            True,
                            target=str(int(selected["id"])),
                            detail=msg,
                            reason=default_execution_note,
                            extra={"final_action": system_action, "execution_status": system_default_status},
                        )
                        st.success(msg)
                        st.rerun()
                    else:
                        append_action_audit(
                            "feedback_accept_system",
                            False,
                            target=str(int(selected["id"])),
                            detail=msg,
                            reason=default_execution_note,
                            extra={"final_action": system_action, "execution_status": system_default_status},
                        )
                        st.error(msg)

        with st.expander("手工覆盖 / 复杂处理", expanded=not quick_adopt_allowed):
            preset_options = list(DECISION_PRESET_MAP.keys())
            preset_label = _preset_label_for_action_status(effective_action, effective_status)
            preset_index = preset_options.index(preset_label) if preset_label in preset_options else 0

            with st.form(f"airivo_feedback_edit_form_{int(selected['id'])}", clear_on_submit=False):
                edit_left, edit_right = st.columns(2)
                with edit_left:
                    decision_preset = st.selectbox(
                        "处理结论",
                        preset_options,
                        index=preset_index,
                        key=f"airivo_decision_preset_{int(selected['id'])}",
                        disabled=not can_execute,
                    )
                    operator_name = st.text_input("操作人", value=default_operator_name, key=f"airivo_operator_{int(selected['id'])}", disabled=not can_execute)
                with edit_right:
                    st.text_input("关联代码", value=str(selected.get("ts_code") or ""), disabled=True, key=f"airivo_code_{int(selected['id'])}")
                    st.caption("优先用业务结论处理；只有选“自定义”时才需要拆成动作和状态。")

                preset_action, preset_status = DECISION_PRESET_MAP.get(decision_preset, ("", ""))
                final_action = preset_action
                execution_status = preset_status

                if decision_preset == "自定义":
                    custom_left, custom_right = st.columns(2)
                    with custom_left:
                        final_action = st.selectbox(
                            "最终动作",
                            final_options,
                            index=action_index,
                            key=f"airivo_final_action_{int(selected['id'])}",
                            disabled=not can_execute,
                        )
                    with custom_right:
                        execution_status = st.selectbox(
                            "执行状态",
                            status_options,
                            index=status_index,
                            key=f"airivo_execution_status_{int(selected['id'])}",
                            disabled=not can_execute,
                        )

                selected_override = bool(system_action and final_action != "请选择" and final_action != system_action)
                human_override_reason = st.text_input(
                    "人工覆盖原因",
                    value=str(selected.get("human_override_reason") or ""),
                    placeholder="仅当人工最终动作不同于系统建议时必填，例如：盘口弱于预期、已有仓位冲突、价格超过计划区间。",
                    disabled=(not selected_override) or (not can_execute),
                    key=f"airivo_override_reason_{int(selected['id'])}",
                )
                if selected_override:
                    st.warning("人工最终动作不同于系统建议。保存前必须填写覆盖原因，这类样本会进入 20 日分歧统计。")

                execution_note = st.text_area(
                    "执行备注",
                    value=default_execution_note,
                    placeholder="例：09:35 以 xx.xx 买入；或：未执行，原因是高开超过计划价；或：切换到 xxxx，原因是流动性更好。",
                    height=90,
                    key=f"airivo_execution_note_{int(selected['id'])}",
                    disabled=not can_execute,
                )

                save_left, save_right = st.columns([0.35, 0.65])
                with save_left:
                    submitted = st.form_submit_button("保存这条反馈", type="primary", use_container_width=True, disabled=not can_execute)
                with save_right:
                    st.caption("验收口径：20 个交易日内不能断档；每条 pending 必须能解释执行、跳过或取消的真实原因。")

            if submitted:
                if not guard_action("operator", "feedback_save", target=str(int(selected["id"])), reason=execution_note):
                    return
                ok, msg = update_feedback_row(
                    db_path,
                    int(selected["id"]),
                    final_action,
                    execution_status,
                    execution_note,
                    operator_name,
                    system_action,
                    human_override_reason,
                )
                if ok:
                    append_action_audit("feedback_save", True, target=str(int(selected["id"])), detail=msg, reason=execution_note, extra={"final_action": final_action, "execution_status": execution_status})
                    st.success(msg)
                    st.rerun()
                else:
                    append_action_audit("feedback_save", False, target=str(int(selected["id"])), detail=msg, reason=execution_note, extra={"final_action": final_action, "execution_status": execution_status})
                    st.error(msg)

    refresh_left, refresh_right = st.columns([0.35, 0.65])
    with refresh_left:
        lookback_days = st.number_input("回刷窗口/天", min_value=5, max_value=365, value=120, step=5, key="airivo_outcome_lookback")
    with refresh_right:
        if st.button("回刷已执行结果", key="airivo_refresh_outcomes", disabled=not has_role("operator")):
            if not guard_action("operator", "refresh_outcomes", target="realized_outcomes", reason=f"lookback_days={int(lookback_days)}"):
                return
            ok, msg = refresh_realized_outcomes(db_path, lookback_days=int(lookback_days))
            if ok:
                append_action_audit("refresh_outcomes", True, target="realized_outcomes", detail=msg, reason=f"lookback_days={int(lookback_days)}")
                st.success(msg)
                st.rerun()
            else:
                append_action_audit("refresh_outcomes", False, target="realized_outcomes", detail=msg, reason=f"lookback_days={int(lookback_days)}")
                st.error(msg)


def render_airivo_execution_center(
    *,
    db_path: str,
    safe_file_mtime: Callable[[str], float],
    feedback_snapshot_cached: Callable[..., Dict[str, Any]],
    bucket_feedback_rows: Callable[[pd.DataFrame], pd.DataFrame],
    load_feedback_rows: Callable[..., pd.DataFrame],
    feedback_bucket_summary: Callable[[pd.DataFrame], Dict[str, int]],
    has_role: Callable[[str], bool],
    guard_action: Callable[..., bool],
    append_action_audit: Callable[..., None],
    apply_batch_feedback_action: Callable[..., tuple[bool, str]],
    render_feedback_workbench: Callable[..., None],
) -> None:
    db_mtime = safe_file_mtime(db_path)
    snapshot = feedback_snapshot_cached(db_path, db_mtime)
    rows = bucket_feedback_rows(load_feedback_rows(db_path, status_filter="pending", limit=200))
    bucket_counts = feedback_bucket_summary(rows)
    st.markdown("## 执行中心")
    st.caption("这里不研究策略，只把今日候选转成真实执行。系统先裁决，人工只处理异常项。")

    top1, top2, top3, top4 = st.columns(4)
    top1.metric("异常待确认", bucket_counts.get("manual_review", 0))
    top2.metric("可直接执行", bucket_counts.get("direct_execute", 0))
    top3.metric("观察等待", bucket_counts.get("observe", 0))
    top4.metric("自动淘汰", bucket_counts.get("auto_reject", 0))

    if bucket_counts.get("manual_review", 0) > 0:
        st.warning("当前仍有异常项需要人工确认。优先只处理这些记录，不要逐条检查全部候选。")
    else:
        st.success("当前没有异常待确认项，系统已替你完成首轮裁决。")

    preview_cols = st.columns(3)
    queue_specs = [
        ("需要人工确认", "manual_review", "优先处理数据缺失、换仓冲突、低置信度分歧。"),
        ("系统直接执行", "direct_execute", "高置信度 buy/switch 建议，直接进入执行清单。"),
        ("系统观察等待", "observe", "中高置信度 watch/hold 建议，不占用人工逐条点选。"),
    ]
    for col, (title, bucket, help_text) in zip(preview_cols, queue_specs):
        subset = rows[rows["decision_bucket"] == bucket].head(6)
        with col:
            st.markdown(f"### {title}")
            st.caption(help_text)
            if subset.empty:
                st.caption("当前无记录")
            else:
                preview = subset[["trade_date", "ts_code", "stock_name", "system_suggested_action", "system_confidence"]].rename(
                    columns={
                        "trade_date": "执行日",
                        "ts_code": "代码",
                        "stock_name": "名称",
                        "system_suggested_action": "建议",
                        "system_confidence": "置信度",
                    }
                )
                st.dataframe(preview, use_container_width=True, hide_index=True)

    reject_rows = rows[rows["decision_bucket"] == "auto_reject"].head(8)
    with st.expander("系统自动淘汰队列", expanded=False):
        st.caption("这些记录默认不占用人工决策时间，只保留可追溯原因。")
        if reject_rows.empty:
            st.info("当前没有自动淘汰记录。")
        else:
            reject_preview = reject_rows[["trade_date", "ts_code", "stock_name", "system_suggested_action", "decision_gate_reason"]].rename(
                columns={
                    "trade_date": "执行日",
                    "ts_code": "代码",
                    "stock_name": "名称",
                    "system_suggested_action": "建议",
                    "decision_gate_reason": "原因",
                }
            )
            st.dataframe(reject_preview, use_container_width=True, hide_index=True)

    st.markdown("### 批量动作")
    st.caption("只对不需要真实买入成交的队列开放批量采纳。直接执行队列仍然必须保留人工确认。")
    batch_left, batch_mid, batch_right = st.columns([0.28, 0.44, 0.28])
    with batch_left:
        batch_operator = st.text_input("批量操作人", key="airivo_batch_feedback_operator", value=str(st.session_state.get("airivo_operator_name") or "system"), disabled=not has_role("operator"))
        st.session_state["airivo_operator_name"] = batch_operator
    with batch_mid:
        batch_note = st.text_input("批量备注", key="airivo_batch_note", value="系统批量采纳，已完成首轮裁决确认", disabled=not has_role("operator"))
    with batch_right:
        if st.button("批量采纳观察队列", key="airivo_batch_accept_observe", use_container_width=True, disabled=not has_role("operator")):
            if not guard_action("operator", "batch_feedback_observe", target="observe", reason=batch_note):
                return
            ok, msg = apply_batch_feedback_action(db_path, bucket="observe", operator_name=batch_operator, execution_note=batch_note)
            if ok:
                append_action_audit("batch_feedback_observe", True, target="observe", detail=msg, reason=batch_note)
                st.success(msg)
                st.rerun()
            else:
                append_action_audit("batch_feedback_observe", False, target="observe", detail=msg, reason=batch_note)
                st.error(msg)
        if st.button("批量采纳淘汰队列", key="airivo_batch_accept_reject", use_container_width=True, disabled=not has_role("operator")):
            if not guard_action("operator", "batch_feedback_reject", target="auto_reject", reason=batch_note):
                return
            ok, msg = apply_batch_feedback_action(db_path, bucket="auto_reject", operator_name=batch_operator, execution_note=batch_note)
            if ok:
                append_action_audit("batch_feedback_reject", True, target="auto_reject", detail=msg, reason=batch_note)
                st.success(msg)
                st.rerun()
            else:
                append_action_audit("batch_feedback_reject", False, target="auto_reject", detail=msg, reason=batch_note)
                st.error(msg)

    st.divider()
    render_feedback_workbench(
        db_path=db_path,
        default_bucket="manual_review",
        preloaded_snapshot=snapshot,
        preloaded_pending_rows=rows,
    )
