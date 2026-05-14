from __future__ import annotations

from typing import Any, Callable

import streamlit as st


def render_qa_self_learning_panel(
    *,
    qa_assistant: Any,
    airivo_has_role: Callable[[str], bool],
    airivo_guard_action: Callable[[str, str, str, str], bool],
    airivo_append_action_audit: Callable[..., None],
    set_focus_once: Callable[..., None],
) -> None:
    with st.expander("ClawAlpha 自学习看板", expanded=False):
        col_brief_left, col_snapshot_left, col_snapshot_right = st.columns([1, 1, 1])
        with col_brief_left:
            if st.button("生成今日陪伴简报", key="openclaw_companion_brief"):
                try:
                    brief_text = qa_assistant.get_daily_companion_brief()
                except Exception as e:
                    brief_text = f"今日简报暂时生成失败：{e}"
                st.session_state.openclaw_qa_messages.append({"role": "assistant", "content": brief_text})
                set_focus_once(main_tab="智能交易助手", assistant_tab="OpenClaw问答")
                st.rerun()
        with col_snapshot_left:
            if st.button("创建策略快照", key="openclaw_create_snapshot"):
                try:
                    snap = qa_assistant.create_strategy_snapshot()
                    if snap.get("ok"):
                        st.success(f"已创建快照，ID={snap.get('snapshot_id')}")
                    else:
                        st.warning(f"创建快照失败：{snap.get('error', 'unknown')}")
                except Exception as e:
                    st.warning(f"创建快照失败：{e}")
        with col_snapshot_right:
            restore_id = st.text_input("回滚快照ID", key="openclaw_restore_snapshot_id", placeholder="例如 12")
            if st.button("执行回滚", key="openclaw_restore_snapshot", disabled=not airivo_has_role("admin")):
                if not airivo_guard_action("admin", "openclaw_restore_snapshot", target=str(restore_id or ""), reason="restore_strategy_snapshot"):
                    st.stop()
                try:
                    sid = int(str(restore_id).strip())
                    rst = qa_assistant.restore_strategy_snapshot(sid)
                    if rst.get("ok"):
                        airivo_append_action_audit("openclaw_restore_snapshot", True, target=str(sid), detail=f"restored_keys={rst.get('restored_keys', 0)}")
                        st.success(f"回滚成功：恢复 {rst.get('restored_keys', 0)} 项配置")
                    else:
                        airivo_append_action_audit("openclaw_restore_snapshot", False, target=str(sid), detail=str(rst.get('error', 'snapshot_not_found')))
                        st.warning(f"回滚失败：{rst.get('error', 'snapshot_not_found')}")
                except Exception as e:
                    airivo_append_action_audit("openclaw_restore_snapshot", False, target=str(restore_id or ""), detail=str(e))
                    st.warning(f"回滚失败：{e}")

        try:
            dashboard = qa_assistant.get_learning_dashboard()
        except Exception:
            dashboard = {}
        try:
            h_dashboard = qa_assistant.get_humanlike_dashboard()
        except Exception:
            h_dashboard = {}

        col_m1, col_m2, col_m3, col_m4 = st.columns(4)
        with col_m1:
            st.metric("学习卡片", int(dashboard.get("total_cards", 0)))
        with col_m2:
            st.metric("待补结果", int(dashboard.get("pending_cards", 0)))
        with col_m3:
            st.metric("硬规则", int(dashboard.get("hard_rules", 0)))
        with col_m4:
            st.metric("软规则", int(dashboard.get("soft_rules", 0)))
        if h_dashboard:
            col_h1, col_h2, col_h3 = st.columns(3)
            with col_h1:
                st.metric("工具栈状态", "OK" if h_dashboard.get("ok", False) else "ERR")
            with col_h2:
                st.metric("审计日志", int(h_dashboard.get("audit_count", 0)))
            with col_h3:
                st.metric("结果回填", int(h_dashboard.get("outcome_count", 0)))

        run_weekly = st.checkbox("同时执行周评估", value=False, key="openclaw_selflearn_weekly")
        if st.button("执行自学习评估", key="openclaw_selflearn_run", disabled=not airivo_has_role("admin")):
            if not airivo_guard_action("admin", "openclaw_selflearn_run", target="self_learning_cycle", reason=f"force_weekly={bool(run_weekly)}"):
                st.stop()
            with st.spinner("正在执行自学习评估..."):
                cycle = qa_assistant.run_self_learning_cycle(force_weekly=run_weekly)
            daily_score = ((cycle.get("daily") or {}).get("overall_score"))
            airivo_append_action_audit(
                "openclaw_selflearn_run",
                True,
                target="self_learning_cycle",
                detail=f"overall_score={daily_score if daily_score is not None else 'N/A'}",
                extra={"force_weekly": bool(run_weekly)},
            )
            st.success(f"评估完成：日评估总分={daily_score if daily_score is not None else 'N/A'}")
            st.session_state["openclaw_last_cycle"] = cycle

        last_daily = dashboard.get("last_daily") or {}
        if last_daily:
            st.caption(
                f"最近日评估：overall={last_daily.get('overall_score', 'N/A')} | "
                f"evaluated_cards={last_daily.get('evaluated_cards', 'N/A')}"
            )
        last_weekly = dashboard.get("last_weekly") or {}
        if last_weekly:
            st.caption(
                f"最近周评估：promoted={last_weekly.get('promoted_rules', 0)} | "
                f"downgraded={last_weekly.get('downgraded_rules', 0)}"
            )
        last_tracking = dashboard.get("last_tracking") or {}
        if last_tracking:
            st.caption(
                f"最近结果回填：processed={last_tracking.get('processed_cards', 0)} | "
                f"closed={last_tracking.get('closed_cards', 0)} | "
                f"skipped={last_tracking.get('skipped_cards', 0)}"
            )
