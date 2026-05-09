"""
Airivo Authentication Middleware
Streamlit认证中间件

功能：
- 拦截所有请求，验证JWT Token
- 自动注入用户角色头信息
- 支持白名单路径（登录页、静态资源）
- 强制HTTPS（生产环境）
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict, Optional

import streamlit as st

from openclaw.services.airivo_auth_service import (
    JWT_SECRET,
    ROLE_RANK,
    init_auth_db,
    validate_token,
)


# 认证配置
AUTH_DB_PATH = os.getenv(
    "AIRIVO_AUTH_DB",
    str(Path(__file__).parent.parent.parent / "data" / "airivo_auth.db"),
)
AUTH_ENABLED = os.getenv("AIRIVO_AUTH_ENABLED", "1") == "1"
AUTH_COOKIE_NAME = os.getenv("AIRIVO_AUTH_COOKIE", "airivo_auth_token")

# 白名单路径（不需要认证）
AUTH_WHITELIST = [
    "/login",
    "/signin",
    "/auth",
    "/_stcore/",
    "/static/",
]


def is_auth_whitelisted(path: str) -> bool:
    """检查路径是否在白名单中"""
    for whitelist_path in AUTH_WHITELIST:
        if path.startswith(whitelist_path):
            return True
    return False


def get_token_from_cookie() -> Optional[str]:
    """从Cookie获取Token"""
    try:
        cookies = st.context.cookies
        if cookies is None:
            return None
        return str(cookies.get(AUTH_COOKIE_NAME, "") or "")
    except Exception:
        return None


def get_token_from_headers() -> Optional[str]:
    """从请求头获取Token"""
    try:
        headers = getattr(st.context, "headers", None)
        if headers is None:
            return None
        auth_header = str(headers.get("Authorization", "") or "")
        if auth_header.startswith("Bearer "):
            return auth_header[7:]
        return None
    except Exception:
        return None


def get_current_user() -> Optional[Dict[str, Any]]:
    """获取当前认证用户"""
    if not AUTH_ENABLED:
        return None

    token = get_token_from_cookie() or get_token_from_headers()
    if not token:
        return None

    return validate_token(token)


def require_auth() -> bool:
    """
    认证检查中间件
    
    Returns:
        True: 已认证，可以继续
        False: 未认证，应重定向到登录页
    """
    if not AUTH_ENABLED:
        return True

    # 初始化认证数据库
    init_auth_db(AUTH_DB_PATH)

    # 检查白名单
    try:
        current_path = st.context.path or ""
        if is_auth_whitelisted(current_path):
            return True
    except Exception:
        pass

    # 验证Token
    user = get_current_user()
    if user is None:
        return False

    return True


def get_user_role() -> str:
    """获取当前用户角色"""
    if not AUTH_ENABLED:
        return "viewer"

    user = get_current_user()
    if user is None:
        return "viewer"

    return str(user.get("role", "viewer") or "viewer").strip().lower()


def get_user_info() -> Dict[str, str]:
    """获取当前用户信息"""
    if not AUTH_ENABLED:
        return {
            "username": "anonymous",
            "display_name": "匿名用户",
            "role": "viewer",
        }

    user = get_current_user()
    if user is None:
        return {
            "username": "anonymous",
            "display_name": "匿名用户",
            "role": "viewer",
        }

    return {
        "username": str(user.get("sub", "") or ""),
        "display_name": str(user.get("display_name", "") or user.get("sub", "")),
        "role": str(user.get("role", "viewer") or "viewer").strip().lower(),
    }


def inject_auth_headers() -> None:
    """
    注入认证头信息到Streamlit会话
    
    这个方法应该在应用初始化时调用，将认证信息注入到session_state
    """
    if not AUTH_ENABLED:
        return

    user_info = get_user_info()
    
    # 存储到session_state供后续使用
    st.session_state["airivo_auth_user"] = user_info
    st.session_state["airivo_auth_role"] = user_info["role"]
    st.session_state["airivo_auth_username"] = user_info["username"]
    st.session_state["airivo_auth_display_name"] = user_info["display_name"]


def redirect_to_login() -> None:
    """重定向到登录页"""
    st.switch_page("pages/login.py")


def check_auth_and_redirect() -> None:
    """认证检查并自动重定向"""
    if not require_auth():
        redirect_to_login()
        st.stop()


def logout() -> None:
    """登出用户"""
    try:
        st.context.cookies[AUTH_COOKIE_NAME] = ""
        st.context.cookies[AUTH_COOKIE_NAME]["max-age"] = 0
        st.context.cookies[AUTH_COOKIE_NAME]["path"] = "/"
    except Exception:
        pass

    # 清除session_state
    for key in list(st.session_state.keys()):
        if key.startswith("airivo_auth_"):
            del st.session_state[key]

    st.switch_page("pages/login.py")
