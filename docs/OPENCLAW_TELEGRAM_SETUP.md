# OpenClaw Telegram Setup

目标：通过 Telegram Bot 与 OpenClaw 对话并安排工作（本地协作模式）。

## 1) 在 .env 增加配置

```bash
TELEGRAM_BOT_TOKEN=123456789:your_bot_token
# 先留空，后面通过脚本自动发现 chat_id
# TELEGRAM_ALLOWED_CHAT_IDS=

# 推荐：本地 OpenClaw 协作模式
OPENCLAW_CLOUD_BRAIN_ONLY=0
```

## 2) 发现 chat_id

先给你的 bot 发一条消息（例如 `/start`），然后运行：

```bash
bash tools/openclaw_telegram_get_chat_id.sh
```

把输出里的建议值写回 `.env`，例如：

```bash
TELEGRAM_ALLOWED_CHAT_IDS=123456789,-1009876543210
```

## 3) 启动 Telegram Bridge

```bash
bash tools/openclaw_telegram_run.sh
```

常用命令（在 Telegram 聊天里发）：
- `/help`
- `/health`
- `/clear`
- `/deep`
- `/action`

## 4) 最小验收

1. 给 bot 发：`/health`，应收到 bridge 在线回复。  
2. 发业务指令：`请给我今天v49优化重点三条`。  
3. 回复内容应包含可执行结论，而不是空泛模板。  

## Troubleshooting

1. 提示 `missing TELEGRAM_BOT_TOKEN`：`.env` 未配置或拼写错误。  
2. 机器人无回复：先确认你已给 bot 发过消息，再检查 `TELEGRAM_ALLOWED_CHAT_IDS` 是否包含当前聊天。  
3. 回答质量差：先跑 `bash tools/openclaw_reset_runtime.sh`，确认 `mode=agent_llm`。  
