"""
Airivo Login Page
用户登录页面

功能：
- 用户名密码登录
- JWT Token认证
- 通过Cookie传递Token给主应用
- 错误提示
"""

import time

import streamlit as st

from openclaw.services.airivo_auth_middleware import (
    get_auth_debug_snapshot,
    get_current_user,
    get_user_from_gateway_headers,
    should_show_auth_debug_for_user,
)
from openclaw.services.airivo_auth_service import (
    AUTH_COOKIE_NAME,
    AUTH_DB_PATH,
    authenticate_user,
    init_auth_db,
    record_audit_log,
)


st.set_page_config(
    page_title="Airivo - 登录",
    page_icon="📊",
    layout="centered",
)


APP_ENTRY_PATH = "/app"
APP_REDIRECT_TS_KEY = "_airivo_last_app_redirect_ts"
APP_REDIRECT_DEBOUNCE_SECONDS = 1.5
APP_REDIRECT_COUNT_KEY = "_airivo_app_redirect_count"
APP_REDIRECT_WINDOW_START_KEY = "_airivo_app_redirect_window_start"
APP_REDIRECT_WINDOW_SECONDS = 10.0
APP_REDIRECT_MAX_COUNT = 4


def redirect_to_main_app() -> None:
    """统一跳转到主功能入口，优先使用Streamlit路由。"""
    now = time.time()
    last_ts = float(st.session_state.get(APP_REDIRECT_TS_KEY, 0.0) or 0.0)
    if now - last_ts < APP_REDIRECT_DEBOUNCE_SECONDS:
        st.stop()
    window_start = float(st.session_state.get(APP_REDIRECT_WINDOW_START_KEY, 0.0) or 0.0)
    count = int(st.session_state.get(APP_REDIRECT_COUNT_KEY, 0) or 0)
    if window_start <= 0 or now - window_start > APP_REDIRECT_WINDOW_SECONDS:
        st.session_state[APP_REDIRECT_WINDOW_START_KEY] = now
        st.session_state[APP_REDIRECT_COUNT_KEY] = 1
    else:
        st.session_state[APP_REDIRECT_COUNT_KEY] = count + 1

    if int(st.session_state.get(APP_REDIRECT_COUNT_KEY, 0) or 0) > APP_REDIRECT_MAX_COUNT:
        st.error("检测到主页面重定向循环，已暂停自动跳转，请点击下方按钮手动进入。")
        st.link_button("手动进入主功能系统", f"{APP_ENTRY_PATH}/", use_container_width=True)
        st.stop()

    st.session_state[APP_REDIRECT_TS_KEY] = now
    try:
        st.switch_page("v49_app.py")
    except Exception:
        st.markdown(
            f'<meta http-equiv="refresh" content="0;url={APP_ENTRY_PATH}/">',
            unsafe_allow_html=True,
        )
        st.link_button("进入主功能系统", f"{APP_ENTRY_PATH}/", use_container_width=True)
    st.stop()


def render_login_page():
    """渲染登录页面"""
    init_auth_db(AUTH_DB_PATH)

    # 如果已经通过Nginx会话认证，不应再次卡在应用内登录页。
    gateway_user = get_user_from_gateway_headers()
    if gateway_user is not None:
        st.session_state["airivo_gateway_user"] = gateway_user
        st.session_state["user"] = str(gateway_user.get("sub", ""))
        st.session_state["authenticated"] = True

    # 仅当鉴权中间件也能确认登录态时才自动跳转，避免 login<->v49 循环跳转。
    current_user = get_current_user()
    if current_user is not None:
        st.session_state["user"] = str(current_user.get("sub", st.session_state.get("user", "用户")))
        st.session_state["authenticated"] = True
        st.success(f"欢迎回来，{st.session_state.get('user', '用户')}！")
        redirect_to_main_app()
    elif st.session_state.get("authenticated"):
        # 清理仅存在于session_state但不可验证的残留状态。
        st.session_state.pop("authenticated", None)
        st.session_state.pop("user", None)
        st.session_state.pop("airivo_gateway_user", None)
        st.session_state.pop(APP_REDIRECT_COUNT_KEY, None)
        st.session_state.pop(APP_REDIRECT_WINDOW_START_KEY, None)

    debug_user = current_user or gateway_user
    if should_show_auth_debug_for_user(debug_user):
        with st.expander("Auth Debug (Admin)", expanded=False):
            st.caption("仅管理员可见；不展示 token 明文。")
            st.json(get_auth_debug_snapshot(), expanded=False)

    st.markdown(
        """
        <style>
        .stApp [baseHeader] { visibility: hidden; }
        .stAppDeployButton { display: none; }
        .stMainMenu { display: none; }
        .login-header { text-align: center; margin-bottom: 2rem; }
        .login-header h1 { color: #1f77b4; margin-bottom: 0.5rem; }
        .login-header p { color: #666; font-size: 0.9rem; }
        </style>
        """,
        unsafe_allow_html=True,
    )

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
            username = st.text_input("用户名", placeholder="请输入用户名")
            password = st.text_input("密码", type="password", placeholder="请输入密码")
            remember_me = st.checkbox("记住登录状态", value=True)
            submitted = st.form_submit_button("登录", type="primary", use_container_width=True)

            if submitted:
                if not username or not password:
                    st.error("请输入用户名和密码")
                    return

                success, message, token = authenticate_user(username, password, AUTH_DB_PATH)

                if success:
                    record_audit_log(action="login", username=username, success=True, db_path=AUTH_DB_PATH)
                    st.session_state[AUTH_COOKIE_NAME] = token
                    st.session_state["user"] = username
                    st.session_state["authenticated"] = True
                    st.session_state.pop(APP_REDIRECT_COUNT_KEY, None)
                    st.session_state.pop(APP_REDIRECT_WINDOW_START_KEY, None)
                    max_age = 604800 if remember_me else 86400

                    try:
                        st.context.cookies[AUTH_COOKIE_NAME] = token
                        st.context.cookies[AUTH_COOKIE_NAME]["path"] = "/"
                        st.context.cookies[AUTH_COOKIE_NAME]["max-age"] = max_age
                    except Exception:
                        pass

                    st.success("登录成功！正在跳转...")
                    redirect_to_main_app()
                else:
                    record_audit_log(action="login", username=username, success=False, db_path=AUTH_DB_PATH, detail=message)
                    st.error(message)


if __name__ == "__main__":
    render_login_page()
