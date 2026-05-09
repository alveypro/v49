"""
Airivo Login Page
用户登录页面

功能：
- 用户名密码登录
- JWT Token认证
- 记住登录状态
- 错误提示
"""

import os
from pathlib import Path

import streamlit as st

from openclaw.services.airivo_auth_service import (
    AUTH_DB_PATH,
    AUTH_COOKIE_NAME,
    authenticate_user,
    init_auth_db,
    record_audit_log,
)


st.set_page_config(
    page_title="Airivo - 登录",
    page_icon="📊",
    layout="centered",
)


def render_login_page():
    """渲染登录页面"""
    # 初始化认证数据库
    init_auth_db(AUTH_DB_PATH)

    # 页面样式
    st.markdown(
        """
        <style>
        .login-container {
            max-width: 400px;
            margin: 0 auto;
            padding: 2rem;
        }
        .login-header {
            text-align: center;
            margin-bottom: 2rem;
        }
        .login-header h1 {
            color: #1f77b4;
            margin-bottom: 0.5rem;
        }
        .login-header p {
            color: #666;
            font-size: 0.9rem;
        }
        .stTextInput > div > div > input {
            padding: 0.75rem;
        }
        .stButton > button {
            width: 100%;
            padding: 0.75rem;
            font-size: 1rem;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    # 登录表单
    with st.container():
        st.markdown(
            """
            <div class="login-header">
                <h1>📊 Airivo</h1>
                <p>A股量化决策系统</p>
            </div>
            """,
            unsafe_allow_html=True,
        )

        with st.form("login_form", clear_on_submit=False):
            username = st.text_input("用户名", placeholder="请输入用户名", autocomplete="username")
            password = st.text_input("密码", type="password", placeholder="请输入密码", autocomplete="current-password")
            
            col1, col2 = st.columns([3, 1])
            with col1:
                remember_me = st.checkbox("记住登录状态", value=True)
            with col2:
                pass

            submitted = st.form_submit_button("登录", type="primary", use_container_width=True)

            if submitted:
                if not username or not password:
                    st.error("请输入用户名和密码")
                    return

                # 认证用户
                success, message, token = authenticate_user(username, password, AUTH_DB_PATH)

                if success:
                    # 记录审计日志
                    record_audit_log(
                        action="login",
                        username=username,
                        success=True,
                        db_path=AUTH_DB_PATH,
                    )

                    # 存储Token到Cookie
                    try:
                        st.context.cookies[AUTH_COOKIE_NAME] = token
                        if remember_me:
                            st.context.cookies[AUTH_COOKIE_NAME]["max-age"] = 86400 * 7  # 7天
                        else:
                            st.context.cookies[AUTH_COOKIE_NAME]["max-age"] = 86400  # 1天
                        st.context.cookies[AUTH_COOKIE_NAME]["path"] = "/"
                        st.context.cookies[AUTH_COOKIE_NAME]["httponly"] = True
                        st.context.cookies[AUTH_COOKIE_NAME]["samesite"] = "Lax"
                    except Exception as e:
                        st.warning(f"Cookie设置失败: {e}")

                    st.success("登录成功，正在跳转...")
                    st.rerun()
                else:
                    # 记录审计日志
                    record_audit_log(
                        action="login",
                        username=username,
                        success=False,
                        db_path=AUTH_DB_PATH,
                        detail=message,
                    )
                    st.error(message)


if __name__ == "__main__":
    render_login_page()
