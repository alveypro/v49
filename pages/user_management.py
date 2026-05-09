"""
Airivo User Management Page
用户管理页面（仅admin可访问）

功能：
- 用户列表查看
- 创建新用户
- 修改用户角色
- 启用/禁用用户
- 审计日志查看
"""

import os
from pathlib import Path

import pandas as pd
import streamlit as st

from openclaw.services.airivo_auth_middleware import (
    check_auth_and_redirect,
    get_user_info,
    logout,
)
from openclaw.services.airivo_auth_service import (
    AUTH_DB_PATH,
    ROLE_LABELS,
    ROLE_RANK,
    change_password,
    create_user,
    get_audit_logs,
    list_users,
    record_audit_log,
    toggle_user_active,
    update_user_role,
)


st.set_page_config(
    page_title="Airivo - 用户管理",
    page_icon="👥",
    layout="wide",
)


def render_user_management():
    """渲染用户管理页面"""
    # 认证检查
    check_auth_and_redirect()

    # 权限检查
    user_info = get_user_info()
    if user_info.get("role") != "admin":
        st.error("仅管理员可访问此页面")
        st.stop()

    # 页面标题
    st.title("👥 用户管理")

    # 顶部操作栏
    col1, col2, col3 = st.columns([1, 1, 1])
    with col1:
        st.caption(f"当前用户: {user_info.get('display_name', user_info.get('username', ''))} ({ROLE_LABELS.get(user_info.get('role', ''), user_info.get('role', ''))})")
    with col3:
        if st.button("退出登录", type="secondary", use_container_width=True):
            logout()

    # 标签页
    tab_users, tab_create, tab_audit = st.tabs(["用户列表", "创建用户", "审计日志"])

    with tab_users:
        render_user_list()

    with tab_create:
        render_create_user()

    with tab_audit:
        render_audit_logs()


def render_user_list():
    """渲染用户列表"""
    st.subheader("用户列表")

    users = list_users(AUTH_DB_PATH)
    if not users:
        st.info("暂无用户")
        return

    # 转换为DataFrame
    df = pd.DataFrame(users)
    df["角色"] = df["role"].map(lambda r: ROLE_LABELS.get(r, r))
    df["状态"] = df["is_active"].map({True: "✅ 活跃", False: "❌ 禁用"})
    df = df[["username", "display_name", "角色", "状态", "last_login", "created_at"]]
    df.columns = ["用户名", "显示名称", "角色", "状态", "最后登录", "创建时间"]

    st.dataframe(df, use_container_width=True, hide_index=True)

    # 用户操作
    st.markdown("---")
    st.subheader("用户操作")

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("**修改角色**")
        selected_user = st.selectbox("选择用户", [u["username"] for u in users], key="role_change_user")
        new_role = st.selectbox("新角色", list(ROLE_LABELS.keys()), format_func=lambda r: ROLE_LABELS[r], key="role_change_role")

        if st.button("更新角色", key="update_role_btn"):
            current_user = get_user_info()
            success, message = update_user_role(selected_user, new_role, AUTH_DB_PATH, current_user["username"])
            if success:
                st.success(message)
                record_audit_log(
                    action="update_user_role",
                    username=current_user["username"],
                    success=True,
                    db_path=AUTH_DB_PATH,
                    detail=f"{selected_user}: {new_role}",
                )
                st.rerun()
            else:
                st.error(message)

    with col2:
        st.markdown("**启用/禁用用户**")
        toggle_user = st.selectbox("选择用户", [u["username"] for u in users], key="toggle_user")
        
        if st.button("切换状态", key="toggle_status_btn"):
            current_user = get_user_info()
            success, message = toggle_user_active(toggle_user, AUTH_DB_PATH, current_user["username"])
            if success:
                st.success(message)
                record_audit_log(
                    action="toggle_user_active",
                    username=current_user["username"],
                    success=True,
                    db_path=AUTH_DB_PATH,
                    detail=toggle_user,
                )
                st.rerun()
            else:
                st.error(message)

    # 修改密码
    st.markdown("---")
    st.subheader("修改密码")

    col1, col2, col3 = st.columns(3)
    with col1:
        pwd_user = st.selectbox("选择用户", [u["username"] for u in users], key="pwd_user")
    with col2:
        new_pwd = st.text_input("新密码", type="password", key="new_pwd")
    with col3:
        confirm_pwd = st.text_input("确认密码", type="password", key="confirm_pwd")

    if st.button("修改密码", key="change_pwd_btn"):
        if not new_pwd or not confirm_pwd:
            st.error("请输入新密码")
        elif new_pwd != confirm_pwd:
            st.error("两次输入的密码不一致")
        else:
            current_user = get_user_info()
            success, message = change_password(pwd_user, "", new_pwd, AUTH_DB_PATH)
            if success:
                st.success(message)
                record_audit_log(
                    action="change_password",
                    username=current_user["username"],
                    success=True,
                    db_path=AUTH_DB_PATH,
                    detail=f"为用户 {pwd_user} 修改密码",
                )
            else:
                st.error(message)


def render_create_user():
    """渲染创建用户表单"""
    st.subheader("创建新用户")

    with st.form("create_user_form"):
        col1, col2 = st.columns(2)
        with col1:
            new_username = st.text_input("用户名", placeholder="请输入用户名")
            new_display_name = st.text_input("显示名称", placeholder="请输入显示名称")
        with col2:
            new_password = st.text_input("密码", type="password", placeholder="至少8位")
            new_role = st.selectbox("角色", list(ROLE_LABELS.keys()), format_func=lambda r: ROLE_LABELS[r])

        submitted = st.form_submit_button("创建用户", type="primary")

        if submitted:
            if not new_username or not new_password or not new_display_name:
                st.error("请填写所有必填字段")
                return

            current_user = get_user_info()
            success, message = create_user(
                username=new_username,
                password=new_password,
                display_name=new_display_name,
                role=new_role,
                db_path=AUTH_DB_PATH,
                created_by=current_user["username"],
            )

            if success:
                st.success(message)
                record_audit_log(
                    action="create_user",
                    username=current_user["username"],
                    success=True,
                    db_path=AUTH_DB_PATH,
                    detail=new_username,
                )
                st.rerun()
            else:
                st.error(message)


def render_audit_logs():
    """渲染审计日志"""
    st.subheader("审计日志")

    col1, col2 = st.columns([1, 3])
    with col1:
        log_limit = st.number_input("显示条数", min_value=10, max_value=1000, value=100, step=10)

    logs = get_audit_logs(AUTH_DB_PATH, limit=int(log_limit))
    if not logs:
        st.info("暂无审计日志")
        return

    df = pd.DataFrame(logs)
    df["成功"] = df["success"].map({True: "✅", False: "❌"})
    df = df[["timestamp", "action", "username", "成功", "detail"]]
    df.columns = ["时间", "操作", "用户", "结果", "详情"]

    st.dataframe(df, use_container_width=True, hide_index=True)


if __name__ == "__main__":
    render_user_management()
