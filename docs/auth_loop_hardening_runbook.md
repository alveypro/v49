# Airivo Auth Loop Hardening Runbook

## 1) 目标与范围

- 目标：消除 `login <-> v49` 循环跳转，并在异常时自动熔断，避免浏览器 `history.pushState` 风暴。
- 范围：`pages/login.py`、`openclaw/services/airivo_auth_middleware.py`、`v49_app.py`。
- 非目标：本次不改 Nginx/OpenResty 登录网关实现，不改数据库模型。

## 2) 设计原则（对齐行业实践）

- 单一决策出口：鉴权由 `resolve_auth_decision()` 统一判定，避免多处 if 分叉导致状态不一致。
- 故障隔离：重定向循环触发阈值后熔断，不再继续自动跳转。
- 可观测优先：记录鉴权决策 JSONL，且管理员可见 `Auth Debug` 面板。
- 最小暴露：诊断信息不输出 token 明文，用户名打码。

## 3) 上线前检查（必须逐项通过）

- 代码静态检查：
  - `python3 -m py_compile openclaw/services/airivo_auth_middleware.py pages/login.py v49_app.py`
- 线上部署验证：
  - `DEPLOY_PASS=*** bash tools/deploy_auth_to_release.sh`
  - `DEPLOY_PASS=*** bash tools/verify_airivo_system.sh`
- 路由校验：
  - `https://airivo.online/login?auth_debug=1`
  - `https://airivo.online/app?auth_debug=1`

## 4) 灰度与放量

- 阶段A（管理员验证）：
  - 仅管理员带 `auth_debug=1` 登录验证。
  - 连续 10 次登录/刷新不出现循环。
- 阶段B（小流量）：
  - 开放给内部 5-10 用户。
  - 观察 30-60 分钟。
- 阶段C（全量）：
  - 关闭 `auth_debug` 显示（去 query 参数或环境变量）。

## 5) 验收标准（必须量化）

- 浏览器端不再出现 `Attempt to use history.pushState() more than 100 times per 10 seconds`。
- `Auth Debug` 中 `last_decision.decision` 在正常路径上应稳定为 `allow` 或 `allow_whitelist`。
- 若异常出现，系统展示熔断提示，不产生持续自动跳转。
- 决策日志存在且持续写入：`AIRIVO_AUTH_DECISION_LOG_PATH`（默认 `/tmp/airivo_auth_decision.jsonl`）。

## 6) 快速排障指引

- 现象：登录后立即被踢回登录页
  - 看 `Auth Debug`：
    - `gateway_header_user_ok=false` 且 `cookie_token_present=false` 且 `session_token_present=false`
      - 结论：无有效认证上下文；优先排查网关头和 cookie 注入。
    - `gateway_header_username_present=true` 但 `current_user_ok=false`
      - 结论：头注入异常或角色字段非法；排查网关映射与 header 透传。
    - `redirect_count_in_window` 持续增长并触发熔断
      - 结论：存在双向重定向条件，检查 login 页自动跳转条件和 path 白名单。

## 7) 回滚条件与动作

- 任一条件触发即回滚：
  - 核心登录失败率显著上升（例如 >5% 持续 10 分钟）。
  - 熔断提示大面积出现（例如管理员反馈 >3 个独立账号复现）。
- 回滚动作：
  - 执行 `tools/deploy_auth_to_release.sh` 生成的远程备份恢复流程，回到上一稳定版本。
  - 回滚后仍保留日志用于根因分析。

## 8) 后续增强（下一迭代）

- 在 Nginx/OpenResty 侧增加请求级 `trace_id`，并透传到应用日志。
- 将决策日志接入集中式日志平台（ELK/Loki），做异常告警。
- 增加针对 auth 流程的端到端自动化回归（Playwright）。

## 9) 已落地的告警脚本接入

- 脚本路径：`tools/auth_decision_alert.py`
- 功能：从 `AIRIVO_AUTH_DECISION_LOG_PATH`（默认 `/tmp/airivo_auth_decision.jsonl`）读取日志，检测窗口内 `redirect_login` 风暴并返回退出码。
- 退出码：
  - `0`：正常
  - `2`：告警（可直接用于 cron/systemd/监控平台）

### 手工执行

- `python3 tools/auth_decision_alert.py --window-seconds 300 --redirect-threshold 12 --consecutive-threshold 4`
- JSON 输出：`python3 tools/auth_decision_alert.py --json`

### Cron 示例（每分钟检查）

- `* * * * * /usr/bin/python3 /opt/openclaw/current/tools/auth_decision_alert.py --window-seconds 300 --redirect-threshold 12 --consecutive-threshold 4 >> /var/log/airivo_auth_alert.log 2>&1`

### Systemd 定时任务建议

- 建议创建 `oneshot` service + `timer`，将退出码 `2` 接入你现有的告警系统（企业微信/飞书/PagerDuty）。
- 阈值建议从保守值开始（300s 窗口内 12 次，连续 4 次），根据真实流量再调优。
- 仓库已提供可直接安装脚本：`tools/install_auth_alert_timer.sh`
  - `DEPLOY_PASS=*** bash tools/install_auth_alert_timer.sh`
