"""
Airivo Authentication Service
企业级用户认证和权限管理服务

功能：
- 用户注册、登录、登出
- JWT Token认证
- 基于角色的访问控制(RBAC)
- 密码加密存储
- 会话管理
- 审计日志
"""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import secrets
import sqlite3
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import jwt


# JWT配置
JWT_SECRET = os.getenv("AIRIVO_JWT_SECRET", secrets.token_hex(32))
JWT_ALGORITHM = "HS256"
JWT_EXPIRY_HOURS = int(os.getenv("AIRIVO_JWT_EXPIRY_HOURS", "24"))

# 密码哈希配置
PASSWORD_SALT_ROUNDS = 10000

# 角色定义
ROLE_RANK = {"viewer": 1, "operator": 2, "admin": 3}
ROLE_LABELS = {
    "viewer": "查看者",
    "operator": "操作员",
    "admin": "管理员",
}

# 默认管理员账户（首次启动时创建）
DEFAULT_ADMIN = {
    "username": "admin",
    "password": "Airivo@2026",  # 首次登录后必须修改
    "display_name": "系统管理员",
    "role": "admin",
}


def _hash_password(password: str, salt: str = None) -> Tuple[str, str]:
    """使用PBKDF2哈希密码"""
    if salt is None:
        salt = secrets.token_hex(16)
    pwd_hash = hashlib.pbkdf2_hmac(
        "sha256",
        password.encode("utf-8"),
        salt.encode("utf-8"),
        PASSWORD_SALT_ROUNDS,
    )
    return pwd_hash.hex(), salt


def _verify_password(password: str, stored_hash: str, salt: str) -> bool:
    """验证密码"""
    pwd_hash, _ = _hash_password(password, salt)
    return hmac.compare_digest(pwd_hash, stored_hash)


def _generate_token(username: str, role: str, display_name: str) -> str:
    """生成JWT Token"""
    payload = {
        "sub": username,
        "role": role,
        "display_name": display_name,
        "iat": datetime.utcnow(),
        "exp": datetime.utcnow() + timedelta(hours=JWT_EXPIRY_HOURS),
    }
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)


def _decode_token(token: str) -> Optional[Dict[str, Any]]:
    """解码JWT Token"""
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
        return payload
    except (jwt.ExpiredSignatureError, jwt.InvalidTokenError):
        return None


def init_auth_db(db_path: str) -> None:
    """初始化认证数据库"""
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(path))
    try:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                password_salt TEXT NOT NULL,
                display_name TEXT NOT NULL,
                role TEXT NOT NULL DEFAULT 'viewer',
                is_active INTEGER NOT NULL DEFAULT 1,
                last_login TEXT,
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                updated_at TEXT NOT NULL DEFAULT (datetime('now'))
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS auth_audit_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL DEFAULT (datetime('now')),
                action TEXT NOT NULL,
                username TEXT,
                ip_address TEXT,
                user_agent TEXT,
                success INTEGER NOT NULL,
                detail TEXT,
                extra TEXT
            )
        """)

        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_users_username ON users(username)
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_audit_timestamp ON auth_audit_log(timestamp)
        """)

        # 创建默认管理员账户（如果不存在）
        cursor = conn.execute("SELECT COUNT(*) FROM users WHERE username = ?", (DEFAULT_ADMIN["username"],))
        if cursor.fetchone()[0] == 0:
            pwd_hash, salt = _hash_password(DEFAULT_ADMIN["password"])
            conn.execute(
                """INSERT INTO users (username, password_hash, password_salt, display_name, role)
                   VALUES (?, ?, ?, ?, ?)""",
                (
                    DEFAULT_ADMIN["username"],
                    pwd_hash,
                    salt,
                    DEFAULT_ADMIN["display_name"],
                    DEFAULT_ADMIN["role"],
                ),
            )
            conn.commit()

    finally:
        conn.close()


def authenticate_user(username: str, password: str, db_path: str) -> Tuple[bool, str, Optional[str]]:
    """
    用户认证
    
    Returns:
        (success, message, token)
    """
    conn = sqlite3.connect(db_path)
    try:
        cursor = conn.execute(
            "SELECT password_hash, password_salt, display_name, role, is_active FROM users WHERE username = ?",
            (username,),
        )
        row = cursor.fetchone()

        if row is None:
            return False, "用户名或密码错误", None

        pwd_hash, salt, display_name, role, is_active = row

        if not is_active:
            return False, "账户已被禁用", None

        if not _verify_password(password, pwd_hash, salt):
            return False, "用户名或密码错误", None

        # 更新最后登录时间
        conn.execute(
            "UPDATE users SET last_login = datetime('now'), updated_at = datetime('now') WHERE username = ?",
            (username,),
        )
        conn.commit()

        # 生成Token
        token = _generate_token(username, role, display_name)

        return True, "登录成功", token

    finally:
        conn.close()


def validate_token(token: str, db_path: str = None) -> Optional[Dict[str, Any]]:
    """验证Token并返回用户信息"""
    payload = _decode_token(token)
    if payload is None:
        return None

    # 检查用户是否仍然活跃
    if db_path is None:
        db_path = os.getenv("AIRIVO_AUTH_DB", str(Path(__file__).parent.parent.parent / "data" / "airivo_auth.db"))
    conn = sqlite3.connect(db_path)
    try:
        cursor = conn.execute(
            "SELECT is_active FROM users WHERE username = ?",
            (payload.get("sub"),),
        )
        row = cursor.fetchone()
        if row is None or not row[0]:
            return None
        return payload
    finally:
        conn.close()


def get_user_info(username: str, db_path: str) -> Optional[Dict[str, Any]]:
    """获取用户信息"""
    conn = sqlite3.connect(db_path)
    try:
        cursor = conn.execute(
            "SELECT username, display_name, role, is_active, last_login, created_at FROM users WHERE username = ?",
            (username,),
        )
        row = cursor.fetchone()
        if row is None:
            return None

        return {
            "username": row[0],
            "display_name": row[1],
            "role": row[2],
            "is_active": bool(row[3]),
            "last_login": row[4],
            "created_at": row[5],
        }
    finally:
        conn.close()


def list_users(db_path: str) -> List[Dict[str, Any]]:
    """列出所有用户"""
    conn = sqlite3.connect(db_path)
    try:
        cursor = conn.execute(
            "SELECT username, display_name, role, is_active, last_login, created_at FROM users ORDER BY created_at DESC"
        )
        users = []
        for row in cursor.fetchall():
            users.append({
                "username": row[0],
                "display_name": row[1],
                "role": row[2],
                "is_active": bool(row[3]),
                "last_login": row[4],
                "created_at": row[5],
            })
        return users
    finally:
        conn.close()


def create_user(
    username: str,
    password: str,
    display_name: str,
    role: str,
    db_path: str,
    created_by: str = "system",
) -> Tuple[bool, str]:
    """创建新用户"""
    if role not in ROLE_RANK:
        return False, f"无效的角色: {role}"

    if len(password) < 8:
        return False, "密码长度至少8位"

    conn = sqlite3.connect(db_path)
    try:
        pwd_hash, salt = _hash_password(password)
        conn.execute(
            """INSERT INTO users (username, password_hash, password_salt, display_name, role)
               VALUES (?, ?, ?, ?, ?)""",
            (username, pwd_hash, salt, display_name, role),
        )
        conn.commit()
        return True, f"用户 {username} 创建成功"
    except sqlite3.IntegrityError:
        return False, f"用户名 {username} 已存在"
    finally:
        conn.close()


def update_user_role(username: str, new_role: str, db_path: str, updated_by: str = "system") -> Tuple[bool, str]:
    """更新用户角色"""
    if new_role not in ROLE_RANK:
        return False, f"无效的角色: {new_role}"

    conn = sqlite3.connect(db_path)
    try:
        cursor = conn.execute("SELECT COUNT(*) FROM users WHERE username = ?", (username,))
        if cursor.fetchone()[0] == 0:
            return False, f"用户 {username} 不存在"

        conn.execute(
            "UPDATE users SET role = ?, updated_at = datetime('now') WHERE username = ?",
            (new_role, username),
        )
        conn.commit()
        return True, f"用户 {username} 角色已更新为 {ROLE_LABELS.get(new_role, new_role)}"
    finally:
        conn.close()


def toggle_user_active(username: str, db_path: str, updated_by: str = "system") -> Tuple[bool, str]:
    """切换用户激活状态"""
    conn = sqlite3.connect(db_path)
    try:
        cursor = conn.execute("SELECT is_active FROM users WHERE username = ?", (username,))
        row = cursor.fetchone()
        if row is None:
            return False, f"用户 {username} 不存在"

        new_status = 0 if row[0] else 1
        conn.execute(
            "UPDATE users SET is_active = ?, updated_at = datetime('now') WHERE username = ?",
            (new_status, username),
        )
        conn.commit()
        status_text = "已激活" if new_status else "已禁用"
        return True, f"用户 {username} {status_text}"
    finally:
        conn.close()


def change_password(username: str, old_password: str, new_password: str, db_path: str) -> Tuple[bool, str]:
    """修改密码"""
    if len(new_password) < 8:
        return False, "新密码长度至少8位"

    conn = sqlite3.connect(db_path)
    try:
        cursor = conn.execute(
            "SELECT password_hash, password_salt FROM users WHERE username = ?",
            (username,),
        )
        row = cursor.fetchone()
        if row is None:
            return False, "用户不存在"

        pwd_hash, salt = row
        if not _verify_password(old_password, pwd_hash, salt):
            return False, "原密码错误"

        new_hash, new_salt = _hash_password(new_password)
        conn.execute(
            "UPDATE users SET password_hash = ?, password_salt = ?, updated_at = datetime('now') WHERE username = ?",
            (new_hash, new_salt, username),
        )
        conn.commit()
        return True, "密码修改成功"
    finally:
        conn.close()


def record_audit_log(
    action: str,
    username: str,
    success: bool,
    db_path: str,
    ip_address: str = "",
    user_agent: str = "",
    detail: str = "",
    extra: Dict[str, Any] = None,
) -> None:
    """记录审计日志"""
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            """INSERT INTO auth_audit_log (action, username, ip_address, user_agent, success, detail, extra)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (
                action,
                username,
                ip_address,
                user_agent,
                1 if success else 0,
                detail,
                json.dumps(extra or {}, ensure_ascii=False),
            ),
        )
        conn.commit()
    finally:
        conn.close()


def get_audit_logs(db_path: str, limit: int = 100) -> List[Dict[str, Any]]:
    """获取审计日志"""
    conn = sqlite3.connect(db_path)
    try:
        cursor = conn.execute(
            """SELECT timestamp, action, username, ip_address, user_agent, success, detail, extra
               FROM auth_audit_log ORDER BY timestamp DESC LIMIT ?""",
            (limit,),
        )
        logs = []
        for row in cursor.fetchall():
            logs.append({
                "timestamp": row[0],
                "action": row[1],
                "username": row[2],
                "ip_address": row[3],
                "user_agent": row[4],
                "success": bool(row[5]),
                "detail": row[6],
                "extra": json.loads(row[7]) if row[7] else {},
            })
        return logs
    finally:
        conn.close()
