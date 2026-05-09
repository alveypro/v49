# Airivo 权限控制系统部署指南

## 概述

本文档详细说明如何部署和使用 Airivo 企业级权限控制系统。该系统解决了"当前账号不是 admin"的权限限制问题，实现了完整的用户认证、角色管理和权限控制。

## 系统架构

### 核心组件

1. **认证服务** (`openclaw/services/airivo_auth_service.py`)
   - 用户管理（创建、查询、更新、删除）
   - JWT Token 生成和验证
   - 密码加密存储（PBKDF2）
   - 审计日志记录

2. **认证中间件** (`openclaw/services/airivo_auth_middleware.py`)
   - 请求拦截和认证检查
   - Token 解析和用户信息注入
   - 白名单路径管理
   - 会话管理

3. **登录页面** (`pages/login.py`)
   - 用户登录界面
   - Cookie 存储 Token
   - 记住登录状态

4. **用户管理页面** (`pages/user_management.py`)
   - 用户列表查看
   - 创建新用户
   - 角色分配
   - 审计日志查看

### 角色体系

| 角色 | 权限级别 | 说明 |
|------|---------|------|
| viewer | 1 | 查看者 - 只能查看数据和执行队列 |
| operator | 2 | 操作员 - 可以执行扫描、回测等操作 |
| admin | 3 | 管理员 - 完全权限，包括批次治理、例外批准、灰度发布 |

## 部署步骤

### 1. 环境准备

```bash
# 安装依赖
pip install PyJWT

# 复制配置示例
cp .env.auth.example .env
```

### 2. 配置环境变量

编辑 `.env` 文件：

```bash
# 启用认证
AIRIVO_AUTH_ENABLED=1

# 设置强随机JWT密钥（生产环境必须修改）
AIRIVO_JWT_SECRET=$(openssl rand -hex 32)

# JWT过期时间（小时）
AIRIVO_JWT_EXPIRY_HOURS=24

# 数据库路径
AIRIVO_AUTH_DB=./data/airivo_auth.db

# 审计日志路径
AIRIVO_AUTH_AUDIT_LOG=./logs/airivo_auth_audit.jsonl
AIRIVO_ACTION_AUDIT_LOG=./logs/airivo_action_audit.jsonl
```

### 3. 更新 Nginx 配置

使用更新后的 Nginx 配置文件：

```bash
# 备份原配置
cp /etc/nginx/sites-available/airivo.online.streamlit.conf /etc/nginx/sites-available/airivo.online.streamlit.conf.bak

# 部署新配置
cp deploy/nginx/airivo.online.streamlit.conf /etc/nginx/sites-available/airivo.online.streamlit.conf

# 测试配置
nginx -t

# 重载 Nginx
systemctl reload nginx
```

### 4. 初始化数据库

首次启动应用时，系统会自动创建认证数据库和默认管理员账户：

- **默认管理员账户**：
  - 用户名：`admin`
  - 密码：`Airivo@2026`
  - 角色：`admin`

⚠️ **重要**：首次登录后请立即修改默认密码！

### 5. 启动应用

```bash
# 启动 Streamlit 应用
streamlit run v49_app.py --server.port 8501
```

## 使用指南

### 首次登录

1. 访问 `http://airivo.online/login`
2. 使用默认管理员账户登录：
   - 用户名：`admin`
   - 密码：`Airivo@2026`
3. 登录后立即修改密码

### 用户管理

1. 以 admin 身份登录
2. 访问用户管理页面（侧边栏或导航菜单）
3. 可以执行以下操作：
   - 创建新用户
   - 修改用户角色
   - 启用/禁用用户
   - 查看审计日志

### 创建新用户

1. 进入"用户管理" -> "创建用户"标签页
2. 填写用户信息：
   - 用户名（唯一）
   - 显示名称
   - 密码（至少8位）
   - 角色（viewer/operator/admin）
3. 点击"创建用户"

### 权限说明

#### Viewer（查看者）
- ✅ 查看执行队列
- ✅ 查看数据状态
- ✅ 查看策略结果
- ❌ 开始扫描
- ❌ 批次治理
- ❌ 例外批准
- ❌ 灰度发布

#### Operator（操作员）
- ✅ 所有 Viewer 权限
- ✅ 开始扫描
- ✅ 执行回测
- ✅ 数据更新
- ❌ 批次治理
- ❌ 例外批准
- ❌ 灰度发布

#### Admin（管理员）
- ✅ 所有 Operator 权限
- ✅ 批次治理
- ✅ 例外批准
- ✅ 灰度发布
- ✅ 用户管理
- ✅ 系统配置

### 审计日志

系统自动记录以下操作：

- 用户登录/登出
- 角色变更
- 用户创建/删除
- 密码修改
- 权限操作（通过 `guard_action`）

查看审计日志：
1. 以 admin 身份登录
2. 进入"用户管理" -> "审计日志"标签页
3. 可以筛选和查看最近的操作记录

## 安全建议

### 生产环境配置

1. **JWT密钥**：
   ```bash
   # 生成强随机密钥
   openssl rand -hex 32
   ```

2. **HTTPS**：
   - 必须启用 HTTPS
   - 配置 SSL 证书
   - 强制 HTTPS 重定向

3. **密码策略**：
   - 最小长度：8位
   - 建议包含大小写字母、数字、特殊字符
   - 定期修改密码

4. **会话管理**：
   - 合理设置 JWT 过期时间
   - 启用"记住登录"功能时注意安全风险
   - 定期清理过期会话

5. **访问控制**：
   - 限制 IP 访问（如可能）
   - 启用失败登录锁定
   - 监控异常登录行为

### 数据库安全

```bash
# 设置数据库文件权限
chmod 600 ./data/airivo_auth.db

# 定期备份
cp ./data/airivo_auth.db ./data/airivo_auth.db.backup.$(date +%Y%m%d)
```

## 故障排除

### 问题1：无法登录

**症状**：输入正确的用户名密码后仍然提示错误

**解决方案**：
1. 检查认证数据库是否存在：`ls -la ./data/airivo_auth.db`
2. 检查数据库权限：`chmod 644 ./data/airivo_auth.db`
3. 查看审计日志：`tail -f ./logs/airivo_auth_audit.jsonl`
4. 重置管理员密码（见下方）

### 问题2：权限不生效

**症状**：登录后仍然显示"当前账号不是 admin"

**解决方案**：
1. 清除浏览器 Cookie
2. 重新登录
3. 检查用户角色：访问用户管理页面查看
4. 确认 `.env` 中 `AIRIVO_AUTH_ENABLED=1`

### 问题3：Nginx 配置问题

**症状**：登录后跳转失败或页面加载异常

**解决方案**：
1. 检查 Nginx 配置：`nginx -t`
2. 确认 Cookie 传递配置正确
3. 查看 Nginx 错误日志：`tail -f /var/log/nginx/error.log`

### 重置管理员密码

如果忘记管理员密码，可以通过以下方式重置：

```python
# 在 Python 环境中执行
import sys
sys.path.insert(0, '/path/to/2026Qlin')
from openclaw.services.airivo_auth_service import change_password

success, message = change_password(
    username="admin",
    old_password="",  # 直接重置
    new_password="NewSecurePassword123",
    db_path="./data/airivo_auth.db"
)
print(f"{success}: {message}")
```

## 升级说明

### 从旧版本升级

如果之前使用的是无认证版本：

1. 所有现有功能保持不变
2. 首次启动会自动创建认证数据库
3. 默认管理员账户自动创建
4. 现有数据不会受影响

### 回滚方案

如果需要临时禁用认证：

```bash
# 编辑 .env 文件
AIRIVO_AUTH_ENABLED=0

# 重启应用
```

## 技术支持

如遇到问题，请提供以下信息：

1. 应用日志：`./logs/` 目录下的日志文件
2. 审计日志：`./logs/airivo_auth_audit.jsonl`
3. Nginx 错误日志：`/var/log/nginx/error.log`
4. 环境变量配置（脱敏后）

## 版本历史

- **v1.0** (2026-05-09)
  - 初始版本
  - JWT Token 认证
  - 三级角色体系
  - 用户管理界面
  - 审计日志
