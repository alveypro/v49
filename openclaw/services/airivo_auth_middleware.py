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
import time
import json
import logging
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
GATEWAY_SESSION_USER_KEY = "airivo_gateway_user"
_LOGIN_REDIRECT_TS_KEY = "_airivo_last_login_redirect_ts"
_LOGIN_REDIRECT_DEBOUNCE_SECONDS = float(os.getenv("AIRIVO_LOGIN_REDIRECT_DEBOUNCE_SECONDS", "1.5"))
_AUTH_REDIRECT_WINDOW_SECONDS = float(os.getenv("AIRIVO_AUTH_REDIRECT_WINDOW_SECONDS", "10"))
_AUTH_REDIRECT_MAX_COUNT = int(os.getenv("AIRIVO_AUTH_REDIRECT_MAX_COUNT", "4"))
_AUTH_REDIRECT_COUNT_KEY = "_airivo_auth_redirect_count"
_AUTH_REDIRECT_WINDOW_START_KEY = "_airivo_auth_redirect_window_start"
_AUTH_LAST_DECISION_KEY = "_airivo_auth_last_decision"
_AUTH_DECISION_LOG_PATH = os.getenv(
    "AIRIVO_AUTH_DECISION_LOG_PATH",
    "/tmp/airivo_auth_decision.jsonl",
)
_logger = logging.getLogger(__name__)

# 白名单路径（不需要认证）
AUTH_WHITELIST = [
    "/login",
    "/signin",
    "/auth",
    "/_stcore/",
    "/static/",
]


def _header_value(headers: Any, key: str) -> str:
    if headers is None:
        return ""
    value = headers.get(key)
    if value is None:
        value = headers.get(key.lower())
    if value is None:
        value = headers.get(key.upper())
    return str(value or "").strip()


def _is_truthy(value: Any) -> bool:
    text = str(value or "").strip().lower()
    return text in {"1", "true", "yes", "on", "y"}


def _query_param_value(key: str) -> str:
    try:
        query_params = getattr(st, "query_params", None)
        if query_params is None:
            return ""
        value = query_params.get(key, "")
        if isinstance(value, list):
            return str(value[0] if value else "").strip()
        return str(value or "").strip()
    except Exception:
        return ""


def _safe_path_text() -> str:
    try:
        return str(st.context.path or "").strip()
    except Exception:
        return ""


def _append_auth_decision_log(payload: Dict[str, Any]) -> None:
    try:
        if not _AUTH_DECISION_LOG_PATH:
            return
        parent = Path(_AUTH_DECISION_LOG_PATH).parent
        parent.mkdir(parents=True, exist_ok=True)
        line = json.dumps(payload, ensure_ascii=False)
        with Path(_AUTH_DECISION_LOG_PATH).open("a", encoding="utf-8") as file_obj:
            file_obj.write(line + "\n")
    except Exception as exc:
        _logger.debug("append auth decision log failed: %s", exc)


def _masked_user(user: Optional[Dict[str, Any]]) -> str:
    if not user:
        return ""
    sub = str(user.get("sub", "") or "").strip()
    if len(sub) <= 2:
        return sub
    return f"{sub[0]}***{sub[-1]}"


def get_user_from_gateway_headers() -> Optional[Dict[str, Any]]:
    """从Nginx注入的会话头中解析用户信息。"""
    try:
        headers = getattr(st.context, "headers", None)
        username = _header_value(headers, "X-Airivo-Username")
        if not username:
            return None
        role = _header_value(headers, "X-Airivo-Role").lower() or "viewer"
        if role not in ROLE_RANK:
            role = "viewer"
        display_name = _header_value(headers, "X-Airivo-Display-Name") or username
        user_info = {
            "sub": username,
            "role": role,
            "display_name": display_name,
            "source": "gateway_header",
        }
        st.session_state[GATEWAY_SESSION_USER_KEY] = user_info
        return user_info
    except Exception:
        return None


def _get_user_from_gateway_session() -> Optional[Dict[str, Any]]:
    try:
        cached = st.session_state.get(GATEWAY_SESSION_USER_KEY)
        if not isinstance(cached, dict):
            return None
        username = str(cached.get("sub", "") or "").strip()
        if not username:
            return None
        role = str(cached.get("role", "viewer") or "viewer").strip().lower()
        if role not in ROLE_RANK:
            role = "viewer"
        display_name = str(cached.get("display_name", "") or username)
        return {
            "sub": username,
            "role": role,
            "display_name": display_name,
            "source": str(cached.get("source", "gateway_session") or "gateway_session"),
        }
    except Exception:
        return None


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
        if cookies is not None:
            cookie_token = str(cookies.get(AUTH_COOKIE_NAME, "") or "").strip()
            if cookie_token:
                return cookie_token

        # Streamlit某些部署环境下无法稳定写入cookie，回退到session_state避免跳转循环。
        return str(st.session_state.get(AUTH_COOKIE_NAME, "") or "").strip() or None
    except Exception:
        return str(st.session_state.get(AUTH_COOKIE_NAME, "") or "").strip() or None


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

    gateway_user = get_user_from_gateway_headers()
    if gateway_user is not None:
        return gateway_user

    gateway_session_user = _get_user_from_gateway_session()
    if gateway_session_user is not None:
        return gateway_session_user

    token = get_token_from_cookie() or get_token_from_headers()
    if not token:
        return None

    return validate_token(token)


def get_auth_debug_snapshot() -> Dict[str, Any]:
    """返回不含敏感信息的鉴权诊断快照。"""
    headers = getattr(st.context, "headers", None)
    current_path = _safe_path_text()

    cookie_token = get_token_from_cookie()
    header_token = get_token_from_headers()
    session_token = str(st.session_state.get(AUTH_COOKIE_NAME, "") or "").strip()
    gateway_header_user = get_user_from_gateway_headers()
    gateway_session_user = _get_user_from_gateway_session()
    current_user = get_current_user()

    return {
        "auth_enabled": bool(AUTH_ENABLED),
        "path": current_path,
        "whitelist_hit": bool(is_auth_whitelisted(current_path)),
        "gateway_header_username_present": bool(_header_value(headers, "X-Airivo-Username")),
        "gateway_header_role_present": bool(_header_value(headers, "X-Airivo-Role")),
        "gateway_header_user_ok": bool(gateway_header_user),
        "gateway_session_user_ok": bool(gateway_session_user),
        "cookie_token_present": bool(cookie_token),
        "authorization_header_token_present": bool(header_token),
        "session_token_present": bool(session_token),
        "current_user_ok": bool(current_user),
        "current_user_source": str((current_user or {}).get("source", "")),
        "current_user_sub": str((current_user or {}).get("sub", "")),
        "current_user_role": str((current_user or {}).get("role", "")),
        "login_redirect_debounce_seconds": _LOGIN_REDIRECT_DEBOUNCE_SECONDS,
        "redirect_window_seconds": _AUTH_REDIRECT_WINDOW_SECONDS,
        "redirect_max_count": _AUTH_REDIRECT_MAX_COUNT,
        "redirect_count_in_window": int(st.session_state.get(_AUTH_REDIRECT_COUNT_KEY, 0) or 0),
        "last_decision": st.session_state.get(_AUTH_LAST_DECISION_KEY, {}),
    }


def should_show_auth_debug_for_user(user: Optional[Dict[str, Any]]) -> bool:
    """仅管理员在显式开启 auth_debug 时展示诊断信息。"""
    debug_enabled = _is_truthy(os.getenv("AIRIVO_AUTH_DEBUG", "0")) or _is_truthy(_query_param_value("auth_debug"))
    if not debug_enabled or user is None:
        return False
    role = str(user.get("role", "viewer") or "viewer").strip().lower()
    return role == "admin"


def resolve_auth_decision() -> Dict[str, Any]:
    """
    统一输出鉴权决策，便于调试与线上审计。
    decision:
      - allow
      - allow_whitelist
      - redirect_login
    """
    current_path = _safe_path_text()
    user = get_current_user()
    if not AUTH_ENABLED:
        return {
            "decision": "allow",
            "reason": "auth_disabled",
            "path": current_path,
            "user_source": "",
            "user_masked": "",
        }

    if is_auth_whitelisted(current_path):
        return {
            "decision": "allow_whitelist",
            "reason": "whitelisted_path",
            "path": current_path,
            "user_source": str((user or {}).get("source", "")),
            "user_masked": _masked_user(user),
        }

    if user is None:
        return {
            "decision": "redirect_login",
            "reason": "no_valid_auth_context",
            "path": current_path,
            "user_source": "",
            "user_masked": "",
        }

    return {
        "decision": "allow",
        "reason": "authenticated",
        "path": current_path,
        "user_source": str(user.get("source", "")),
        "user_masked": _masked_user(user),
    }


def _auth_redirect_circuit_breaker_open() -> bool:
    now = time.time()
    window_start = float(st.session_state.get(_AUTH_REDIRECT_WINDOW_START_KEY, 0.0) or 0.0)
    count = int(st.session_state.get(_AUTH_REDIRECT_COUNT_KEY, 0) or 0)
    if window_start <= 0 or now - window_start > _AUTH_REDIRECT_WINDOW_SECONDS:
        st.session_state[_AUTH_REDIRECT_WINDOW_START_KEY] = now
        st.session_state[_AUTH_REDIRECT_COUNT_KEY] = 0
        return False
    return count >= _AUTH_REDIRECT_MAX_COUNT


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

    decision = resolve_auth_decision()
    st.session_state[_AUTH_LAST_DECISION_KEY] = decision
    _append_auth_decision_log(
        {
            "ts": int(time.time()),
            "decision": decision["decision"],
            "reason": decision["reason"],
            "path": decision["path"],
            "user_source": decision["user_source"],
            "user_masked": decision["user_masked"],
        }
    )
    return decision["decision"] != "redirect_login"


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
    now = time.time()
    last_ts = float(st.session_state.get(_LOGIN_REDIRECT_TS_KEY, 0.0) or 0.0)
    if now - last_ts < _LOGIN_REDIRECT_DEBOUNCE_SECONDS:
        return
    st.session_state[_LOGIN_REDIRECT_TS_KEY] = now
    st.switch_page("pages/login.py")


def check_auth_and_redirect() -> None:
    """认证检查并自动重定向"""
    if not require_auth():
        now = time.time()
        window_start = float(st.session_state.get(_AUTH_REDIRECT_WINDOW_START_KEY, 0.0) or 0.0)
        if window_start <= 0 or now - window_start > _AUTH_REDIRECT_WINDOW_SECONDS:
            st.session_state[_AUTH_REDIRECT_WINDOW_START_KEY] = now
            st.session_state[_AUTH_REDIRECT_COUNT_KEY] = 1
        else:
            st.session_state[_AUTH_REDIRECT_COUNT_KEY] = int(st.session_state.get(_AUTH_REDIRECT_COUNT_KEY, 0) or 0) + 1

        if _auth_redirect_circuit_breaker_open():
            st.error("检测到登录重定向循环，已自动熔断。请联系管理员并提供 Auth Debug 信息。")
            st.stop()
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
    st.session_state.pop(AUTH_COOKIE_NAME, None)
    st.session_state.pop("authenticated", None)
    st.session_state.pop("user", None)
    st.session_state.pop(GATEWAY_SESSION_USER_KEY, None)
    st.session_state.pop(_LOGIN_REDIRECT_TS_KEY, None)
    st.session_state.pop(_AUTH_REDIRECT_WINDOW_START_KEY, None)
    st.session_state.pop(_AUTH_REDIRECT_COUNT_KEY, None)
    st.session_state.pop(_AUTH_LAST_DECISION_KEY, None)

    st.switch_page("pages/login.py")
