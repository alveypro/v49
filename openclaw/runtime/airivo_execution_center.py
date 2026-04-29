from __future__ import annotations

from typing import Any, Callable, Dict

import pandas as pd
import streamlit as st


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
    st.dataframe(display, use_container_width=True, hide_index=True)

    options = [
        f"{row['decision_date']} | {row['source_type']}/{row['source_label'] or '-'} | picks={int(row['selected_count'] or 0)} | {status_label_map.get(str(row.get('release_status') or '').strip().lower(), '草稿')}"
        for row in batches.to_dict(orient="records")
    ]
    selected = st.selectbox("选择一个批次", options, key="airivo_batch_selected")
    selected_batch = selected.split(" | ")[0]
    selected_row = next((row for row in batches.to_dict(orient="records") if str(row.get("decision_date") or "") == selected_batch), {})
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
        with st.expander("所选批次发布详情", expanded=False):
            st.json(
                {
                    "decision_date": str(selected_row.get("decision_date") or ""),
                    "release_decision": release_decision,
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
                    "canary_scope": canary_scope,
                    "outcome_review": outcome_review,
                    "override_audits": override_audits,
                    "release_status": str(selected_row.get("release_status") or ""),
                    "approved_by": str(selected_row.get("approved_by") or ""),
                    "approved_at": str(selected_row.get("approved_at") or ""),
                    "release_note": str(selected_row.get("release_note") or ""),
                    "rollback_reason": str(selected_row.get("rollback_reason") or ""),
                    "replaces_decision_date": str(selected_row.get("replaces_decision_date") or ""),
                    "replaced_by_decision_date": str(selected_row.get("replaced_by_decision_date") or ""),
                }
            )


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
) -> None:
    db_mtime = safe_file_mtime(db_path)
    snapshot = feedback_snapshot_cached(db_path, db_mtime)
    pending_rows = bucket_feedback_rows(load_feedback_rows(db_path, status_filter="pending", limit=200))
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
    rows = bucket_feedback_rows(load_feedback_rows(db_path, status_filter=filter_map.get(status_label, "pending"), limit=int(limit)))
    if selected_bucket != "all" and not rows.empty:
        rows = rows[rows["decision_bucket"] == selected_bucket].reset_index(drop=True)
    if rows.empty:
        st.info("当前范围没有执行反馈记录。")
    else:
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
        action_index = final_options.index(current_action) if current_action in final_options and current_action != "pending" else 0
        status_index = status_options.index(current_status) if current_status in status_options and current_status != "pending" else 0

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

        edit_left, edit_right = st.columns(2)
        can_execute = has_role("operator")
        with edit_left:
            final_action = st.selectbox("最终动作", final_options, index=action_index, key=f"airivo_final_action_{int(selected['id'])}", disabled=not can_execute)
            operator_name = st.text_input("操作人", value=str(selected.get("operator_name") or ""), key=f"airivo_operator_{int(selected['id'])}", disabled=not can_execute)
        with edit_right:
            execution_status = st.selectbox("执行状态", status_options, index=status_index, key=f"airivo_execution_status_{int(selected['id'])}", disabled=not can_execute)
            st.text_input("关联代码", value=str(selected.get("ts_code") or ""), disabled=True, key=f"airivo_code_{int(selected['id'])}")

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
            value=str(selected.get("execution_note") or ""),
            placeholder="例：09:35 以 xx.xx 买入；或：未执行，原因是高开超过计划价；或：切换到 xxxx，原因是流动性更好。",
            height=90,
            key=f"airivo_execution_note_{int(selected['id'])}",
            disabled=not can_execute,
        )

        save_left, save_right = st.columns([0.35, 0.65])
        with save_left:
            if st.button("保存这条反馈", type="primary", key=f"airivo_save_feedback_{int(selected['id'])}", disabled=not can_execute):
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
        with save_right:
            st.caption("验收口径：20 个交易日内不能断档；每条 pending 必须能解释执行、跳过或取消的真实原因。")

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
    render_feedback_workbench(db_path=db_path, default_bucket="manual_review")
