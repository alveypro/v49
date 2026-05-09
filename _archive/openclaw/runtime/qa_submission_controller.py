from __future__ import annotations

import os
from typing import Any, Callable

import streamlit as st


def render_qa_submission_controller(
    *,
    qa_assistant: Any,
    set_focus_once: Callable[..., None],
) -> None:
    user_q = st.chat_input("输入问题（如：688608还能加仓吗？给我仓位与风控计划）")
    if not user_q:
        return

    set_focus_once(main_tab="智能交易助手", assistant_tab="OpenClaw问答")
    st.session_state.openclaw_qa_messages.append({"role": "user", "content": user_q})

    with st.spinner("ClawAlpha 正在思考..."):
        qa_result = qa_assistant.answer(
            user_q,
            st.session_state.openclaw_qa_messages[-12:],
        )

    answer_text = qa_result.get("answer", "暂无回答")
    sources = qa_result.get("sources", [])
    route_used = qa_result.get("mode") or qa_result.get("route") or "unknown"
    conf_used = qa_result.get("confidence")
    agent_hits = qa_result.get("agent_hits") if isinstance(qa_result, dict) else []
    agent_ver = qa_result.get("agent_version") if isinstance(qa_result, dict) else ""
    show_meta = os.getenv("OPENCLAW_QA_SHOW_META", "0") == "1"
    if sources and show_meta:
        answer_text = f"{answer_text}\n\n参考数据源：{len(sources)}项"
    if show_meta:
        if conf_used is not None:
            answer_text += f"\n\n路由：`{route_used}` ｜ 置信度：`{conf_used}`"
        else:
            answer_text += f"\n\n路由：`{route_used}`"
        if agent_ver:
            answer_text += f"\n智能体网格：`{agent_ver}`"
        if isinstance(agent_hits, list) and agent_hits:
            answer_text += f"\n命中智能体：`{', '.join(agent_hits[:8])}`"
    learning_card_id = qa_result.get("learning_card_id")
    self_eval = qa_result.get("self_eval") if isinstance(qa_result, dict) else {}
    eval_score = (self_eval or {}).get("overall_score")
    eval_flag = (self_eval or {}).get("reflection_required")
    if show_meta:
        if learning_card_id:
            answer_text += f"\n\n学习卡片：`{learning_card_id}`"
        if eval_score is not None:
            answer_text += f"\n自评得分：{eval_score}"
        if eval_flag:
            answer_text += "\n自我反思：已触发规则修正。"

    st.session_state.openclaw_qa_messages.append({"role": "assistant", "content": answer_text})
    st.session_state.openclaw_last_qa_meta = {
        "learning_card_id": qa_result.get("learning_card_id"),
        "route": qa_result.get("route"),
        "confidence": qa_result.get("confidence"),
        "self_eval": qa_result.get("self_eval"),
    }
    st.rerun()
