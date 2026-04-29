# airivo.online 问题诊断与修复指南

## 一、Bad message format

### 可能来源
- **Telegram Bot API**：`sendMessage` 返回 400 时，`description` 类似 `Bad Request: message text is empty` / `message is too long` / `can't parse entities`
- **airivo-ai-bridge**：`Invalid JSON` 或 `messages required`（400）
- **云端 LLM API**：消息格式不符合 OpenAI 规范

### 排查步骤
1. 查看 Telegram 桥接日志：
   ```bash
   tail -100 /opt/airivo/app/logs/openclaw/telegram_bridge.launchd.err.log
   tail -100 /opt/airivo/app/logs/openclaw/telegram_bridge.launchd.log
   ```
2. 搜索 `RuntimeError`, `telegram api error`, `400` 等关键词
3. 若为 Telegram 发送失败，常见原因：
   - 回复内容为空
   - 内容含 Telegram 不接受的字符（控制字符、异常 Unicode）
   - 单条消息过长（需分片，默认 MAX_REPLY_CHARS=3500）

### 建议修复（telegram_bridge_bot.py）
- 发送前对 `text` 做清洗：去除 `\x00` 等控制字符，空串不发送
- 捕获 `_api_call` 返回的 `description`，记录并可选地尝试重试/截断

---

## 二、策略系统一直重启

### 可能原因
1. **Streamlit 进程崩溃**：异常退出 → launchd/systemd KeepAlive 自动拉起 → 形成重启循环
2. **端口冲突**：`start_v49_full.sh` 会检测 8501 是否被占用，若已有进程则 sleep 30s 等待，不会暴力重启
3. **内存不足**：大扫描/回测导致 OOM
4. **数据更新失败**：`scripts_run_daily.sh` 中 `REQUIRE_FRESH_DB=1` 时，若 DB 过期会直接 exit
5. **Tushare 限频/网络**：数据更新失败 → 下游任务失败 → 进程退出

### 排查步骤
```bash
# 查看 Streamlit 错误日志
tail -200 /opt/airivo/app/logs/v49.streamlit.launchd.err.log
# 或 Linux systemd
journalctl -u v49-streamlit -n 200 --no-pager

# 查看 launchd 状态（macOS）
launchctl print gui/$(id -u)/com.airivo.v49.streamlit | grep -E "state|pid|exit"
```

### 建议措施
- 将 `REQUIRE_FRESH_DB` 设为 `0` 或在 DB 过期时 fallback 到旧数据，避免直接退出
- 增加 Streamlit 稳定参数（已有）：`server.runOnSave false`, `server.fileWatcherType none`
- 对大扫描加超时/内存限制，或改为后台任务
- 检查 `openclaw/scripts_run_daily.sh` 中 `AUTO_UPDATE_DB`、`REQUIRE_FRESH_DB`、`MAX_DB_STALE_TRADE_DAYS` 的配置

---

## 三、股票池分析问题 & 最新价格未更新

### 数据流
- **最新价格来源**：`daily_trading_data` 表，按 `trade_date DESC` 取最新
- **数据更新入口**：
  - `scripts/openclaw_data_update.sh` → `openclaw/update_db_calendar.py`
  - 或 `openclaw/scripts_run_daily.sh` 内 `AUTO_UPDATE_DB=1` 时自动调用

### 更新规则（update_db_calendar.py）
- 使用上交所交易日历，仅在交易日后 `close_hour + delay_hours`（默认 17:00）之后才认为当日数据就绪
- 非交易日的预期日期为最近交易日
- 需配置 `TUSHARE_TOKEN`（环境变量或 tushare_token.txt）

### 调度（launchd）
- `com.airivo.openclaw.data-update`：周一至周五 17:10 执行
- **注意**：`RunAtLoad` 为 `false`，首次部署不会立即更新，需手动触发一次或改为 `true`

### 排查步骤
```bash
# 1. 检查数据更新是否成功
tail -50 /opt/airivo/app/logs/openclaw/data_update.launchd.log
cat  /opt/airivo/app/logs/openclaw/data_update.last_ok

# 2. 检查 DB 最新交易日
sqlite3 /opt/airivo/data/permanent_stock_database.db \
  "SELECT MAX(trade_date) FROM daily_trading_data;"

# 3. 手动触发一次更新（在项目根目录）
OPENCLAW_ROOT=/opt/airivo/app ./scripts/openclaw_data_update.sh
```

### 若最新价格仍不更新
- 确认 Tushare 积分足够、限频未触发
- 确认服务器时区为 Asia/Shanghai，否则 `_cn_now()` 可能算错“今日”
- 检查 `OPENCLAW_TRADE_CLOSE_HOUR`、`OPENCLAW_DATA_READY_DELAY_HOURS` 是否与预期一致

---

## 四、airivo.online 部署结构速查

| 组件 | 路径/服务 | 说明 |
|------|----------|------|
| v49 Streamlit | `start_v49_full.sh` → 8501 | 主策略 UI |
| Stock API (5101) | `openclaw-stockapi.service` | 主入口问答/股票智能体后端 |
| 数据更新 | `scripts/openclaw_data_update.sh` | 周一~五 17:10 |
| Telegram 桥接 | `deploy_stock/telegram_bridge_bot.py` | 需 TELEGRAM_BOT_TOKEN |
| Nginx 反向代理 | `deploy/nginx/airivo.online.streamlit.conf` | 代理 8501 |
| DB | `/opt/airivo/data/permanent_stock_database.db` | 主数据库 |

### 口径说明

- `openclaw-qa-stock.service` 不是当前 5101 的正式服务名，不再作为主入口运维检查依据。
- 当前 5101 正式服务名为：`openclaw-stockapi.service`
- `/stock`、`/T12`、`/apex` 为其他系统入口，本节不以它们的服务名作为 `airivo.online` 主入口运维裁决依据。

---

## 五、建议立即执行的检查

```bash
# 在 airivo.online 服务器上执行
cd /opt/airivo/app

# 1. 数据更新状态
ls -la logs/openclaw/data_update.last_ok
sqlite3 /opt/airivo/data/permanent_stock_database.db "SELECT MAX(trade_date) FROM daily_trading_data;"

# 2. Telegram 错误
tail -50 logs/openclaw/telegram_bridge.launchd.err.log

# 3. Streamlit 稳定性
tail -100 logs/v49.streamlit.launchd.err.log
```
