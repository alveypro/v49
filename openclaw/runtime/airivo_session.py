from __future__ import annotations

from datetime import datetime
import json
from pathlib import Path
from typing import Any, Dict, Optional

import streamlit as st


def request_headers() -> Dict[str, str]:
    try:
        headers = getattr(st.context, "headers", None)
        if headers is None:
            return {}
        return {str(k): str(v) for k, v in dict(headers).items()}
    except Exception:
        return {}


def session_meta(role_rank: Dict[str, int]) -> Dict[str, str]:
    headers = request_headers()
    role = str(headers.get("X-Airivo-Role", "viewer") or "viewer").strip().lower()
    if role not in role_rank:
        role = "viewer"
    return {
        "username": str(headers.get("X-Airivo-Username", "") or "").strip(),
        "display_name": str(headers.get("X-Airivo-Display-Name", "") or headers.get("X-Airivo-Username", "") or "").strip(),
        "role": role,
    }


def has_role(required_role: str, role_rank: Dict[str, int]) -> bool:
    current = session_meta(role_rank).get("role", "viewer")
    return role_rank.get(current, 0) >= role_rank.get(str(required_role or "viewer").strip().lower(), 0)


def append_action_audit(
    *,
    action: str,
    ok: bool,
    role_rank: Dict[str, int],
    audit_log_path: str,
    fallback_log_path: str,
    target: str = "",
    detail: str = "",
    reason: str = "",
    extra: Optional[Dict[str, Any]] = None,
) -> None:
    session = session_meta(role_rank)
    payload = {
        "ts": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "action": str(action or "").strip(),
        "ok": bool(ok),
        "username": session.get("username", ""),
        "display_name": session.get("display_name", ""),
        "role": session.get("role", "viewer"),
        "target": str(target or "").strip(),
        "detail": str(detail or "").strip(),
        "reason": str(reason or "").strip(),
        "extra": extra or {},
    }
    for raw_path in [audit_log_path, fallback_log_path]:
        try:
            path = Path(raw_path)
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(payload, ensure_ascii=False) + "\n")
            return
        except Exception:
            continue


def guard_action(
    *,
    required_role: str,
    action: str,
    role_rank: Dict[str, int],
    audit_log_path: str,
    fallback_log_path: str,
    target: str = "",
    reason: str = "",
) -> bool:
    if has_role(required_role, role_rank):
        return True
    session = session_meta(role_rank)
    msg = (
        f"当前账号角色={session.get('role', 'viewer')}，无权执行 {action}。"
        f" 该动作至少需要 {required_role}。"
    )
    append_action_audit(
        action=action,
        ok=False,
        role_rank=role_rank,
        audit_log_path=audit_log_path,
        fallback_log_path=fallback_log_path,
        target=target,
        detail="permission_denied",
        reason=reason or msg,
        extra={"required_role": required_role},
    )
    st.error(msg)
    return False
