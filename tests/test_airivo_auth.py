"""
Airivo Authentication System Tests
测试认证系统的各个组件
"""

import os
import sys
import tempfile
from pathlib import Path

# 添加项目路径
sys.path.insert(0, str(Path(__file__).parent))

from openclaw.services.airivo_auth_service import (
    ROLE_RANK,
    authenticate_user,
    change_password,
    create_user,
    get_audit_logs,
    get_user_info,
    init_auth_db,
    list_users,
    record_audit_log,
    toggle_user_active,
    update_user_role,
    validate_token,
)


def test_auth_system():
    """测试认证系统"""
    print("=" * 60)
    print("Airivo Authentication System Tests")
    print("=" * 60)

    # 创建临时数据库
    with tempfile.TemporaryDirectory() as tmpdir:
        test_db = os.path.join(tmpdir, "test_auth.db")

        print("\n1. 初始化认证数据库...")
        init_auth_db(test_db)
        print("✅ 数据库初始化成功")

        print("\n2. 测试默认管理员账户...")
        success, message, token = authenticate_user("admin", "Airivo@2026", test_db)
        assert success, f"管理员登录失败: {message}"
        assert token is not None, "Token为空"
        print(f"✅ 管理员登录成功，Token: {token[:20]}...")

        print("\n3. 验证Token...")
        user_info = validate_token(token, test_db)
        assert user_info is not None, "Token验证失败"
        assert user_info["sub"] == "admin", "用户名不匹配"
        assert user_info["role"] == "admin", "角色不匹配"
        print(f"✅ Token验证成功: {user_info['sub']} ({user_info['role']})")

        print("\n4. 测试错误登录...")
        success, message, token = authenticate_user("admin", "wrong_password", test_db)
        assert not success, "错误密码应该登录失败"
        print(f"✅ 错误登录被拒绝: {message}")

        print("\n5. 创建新用户...")
        success, message = create_user(
            username="test_user",
            password="TestPass123",
            display_name="测试用户",
            role="viewer",
            db_path=test_db,
        )
        assert success, f"创建用户失败: {message}"
        print(f"✅ 用户创建成功: {message}")

        print("\n6. 测试新用户登录...")
        success, message, token = authenticate_user("test_user", "TestPass123", test_db)
        assert success, f"新用户登录失败: {message}"
        print(f"✅ 新用户登录成功")

        print("\n7. 列出所有用户...")
        users = list_users(test_db)
        assert len(users) >= 2, "用户数量不正确"
        print(f"✅ 用户列表: {[u['username'] for u in users]}")

        print("\n8. 更新用户角色...")
        success, message = update_user_role("test_user", "operator", test_db, "admin")
        assert success, f"更新角色失败: {message}"
        print(f"✅ 角色更新成功: {message}")

        print("\n9. 切换用户状态...")
        success, message = toggle_user_active("test_user", test_db, "admin")
        assert success, f"切换状态失败: {message}"
        print(f"✅ 状态切换成功: {message}")

        print("\n10. 测试禁用用户登录...")
        success, message, token = authenticate_user("test_user", "TestPass123", test_db)
        assert not success, "禁用用户应该无法登录"
        print(f"✅ 禁用用户登录被拒绝: {message}")

        print("\n11. 重新启用用户...")
        success, message = toggle_user_active("test_user", test_db, "admin")
        assert success, f"重新启用失败: {message}"
        print(f"✅ 用户重新启用: {message}")

        print("\n12. 修改密码...")
        success, message = change_password("test_user", "TestPass123", "NewPass456", test_db)
        assert success, f"修改密码失败: {message}"
        print(f"✅ 密码修改成功: {message}")

        print("\n13. 测试新密码登录...")
        success, message, token = authenticate_user("test_user", "NewPass456", test_db)
        assert success, f"新密码登录失败: {message}"
        print(f"✅ 新密码登录成功")

        print("\n14. 记录审计日志...")
        record_audit_log(
            action="test_action",
            username="admin",
            success=True,
            db_path=test_db,
            detail="测试操作",
        )
        print("✅ 审计日志记录成功")

        print("\n15. 获取审计日志...")
        logs = get_audit_logs(test_db, limit=10)
        assert len(logs) > 0, "审计日志为空"
        print(f"✅ 审计日志数量: {len(logs)}")
        for log in logs[:3]:
            print(f"   - {log['timestamp']}: {log['action']} by {log['username']} ({'✅' if log['success'] else '❌'})")

        print("\n" + "=" * 60)
        print("所有测试通过！✅")
        print("=" * 60)


if __name__ == "__main__":
    test_auth_system()
