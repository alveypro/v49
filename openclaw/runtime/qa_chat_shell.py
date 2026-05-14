from __future__ import annotations

import os
import uuid
from typing import Any, Callable, Type

import streamlit as st


QA_VERSION = "v2.2"
DEFAULT_WELCOME = (
    "我是 ClawAlpha v2.0 — 由 Kimi LLM 驱动的超级智能体。\n\n"
    "我擅长：股票投研、量化策略、风控分析、哲学/历史/概率/博弈论等跨学科思维。\n\n"
    "试试问我：**你是谁？** 或 **600519可以买吗？**"
)
RESET_WELCOME = "对话已清空。你可以继续问我策略、回测、个股问题。"


def render_qa_chat_shell(
    *,
    qa_assistant_cls: Type[Any],
    permanent_db_path: str,
    set_focus_once: Callable[..., None],
) -> Any:
    st.subheader("ClawAlpha 智能体 v2.0")
    st.markdown(
        """
        <style>
        /* QA chat visual polish */
        div[data-testid="stChatMessage"] {
            border: 1px solid #e8edf4;
            border-radius: 12px;
            padding: 6px 10px;
            margin-bottom: 8px;
            background: #ffffff;
        }
        div[data-testid="stChatMessageContent"] p,
        div[data-testid="stChatMessageContent"] li {
            font-size: 16px !important;
            line-height: 1.75 !important;
        }
        div[data-testid="stChatInput"] {
            margin-top: 10px;
        }
        div[data-testid="stChatInput"] input {
            min-height: 48px !important;
            border-radius: 12px !important;
            border: 1.5px solid #cfd8e6 !important;
            padding: 10px 14px !important;
            font-size: 15px !important;
        }
        div[data-testid="stChatInput"] input:focus {
            border-color: #2b6cb0 !important;
            box-shadow: 0 0 0 2px rgba(43, 108, 176, 0.16) !important;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )
    st.info("可提问示例：你是谁？| 600519可以买吗？| 量化交易的哲学本质 | 博弈论在投资中的应用")
    llm_on = os.getenv("OPENCLAW_QA_USE_LLM", "0") == "1"
    model_name = os.getenv("OPENAI_MODEL", "规则引擎")
    if llm_on:
        st.caption(f"当前模式：Kimi LLM 增强（{model_name}）+ 本地量化规则引擎")
    else:
        st.caption("当前模式：本地量化规则引擎（可在 .env 中开启 LLM）")

    if st.session_state.get("_qa_engine_version") != QA_VERSION:
        st.session_state.pop("openclaw_qa_assistant", None)
        st.session_state.pop("openclaw_qa_messages", None)
        st.session_state.pop("openclaw_qa_session_id", None)
        st.session_state["_qa_engine_version"] = QA_VERSION

    if "openclaw_qa_assistant" not in st.session_state:
        st.session_state.openclaw_qa_assistant = qa_assistant_cls(
            log_dir="logs/openclaw",
            db_path=permanent_db_path,
        )
    if "openclaw_qa_messages" not in st.session_state:
        st.session_state.openclaw_qa_messages = [{"role": "assistant", "content": DEFAULT_WELCOME}]
    if "openclaw_qa_session_id" not in st.session_state:
        st.session_state.openclaw_qa_session_id = f"stock-web-{uuid.uuid4().hex[:10]}"

    if st.button("清空对话", key="openclaw_qa_clear_history"):
        st.session_state.openclaw_qa_messages = [{"role": "assistant", "content": RESET_WELCOME}]
        st.session_state.openclaw_qa_session_id = f"stock-web-{uuid.uuid4().hex[:10]}"
        st.session_state.pop("openclaw_qa_assistant", None)
        set_focus_once(main_tab="智能交易助手", assistant_tab="OpenClaw问答")
        st.rerun()

    chat_history_box = st.container(height=520, border=False)
    with chat_history_box:
        for msg in st.session_state.openclaw_qa_messages[-30:]:
            with st.chat_message(msg["role"]):
                st.markdown(msg["content"])

    return st.session_state.openclaw_qa_assistant
