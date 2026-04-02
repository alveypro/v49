# Agent Single Mainline (OC + WAWA)

## 目标
- 只保留一条主线，不再多 Bot/多入口混跑。
- `oc` 只负责 v49 流水线执行。
- `wawa` 只负责通用助手对话（非股票），股票问题走本地 v49 快照增强分析。

## 唯一入口
- 入口：`deploy_stock/telegram_bridge_bot.py`
- 启动脚本：`tools/openclaw_single_mainline.sh`

## 路由规则
1. `oc ...`：进入 OC 执行链（`oc daily|oc audit|oc go|oc approve|oc reject`）。
2. `wawa ...`：进入 WAWA 链。
3. 无前缀消息：默认进入 WAWA 链。
4. WAWA 非股票问题：强制 `general_only` 路由（不会再落到股票端点）。
5. WAWA 股票问题：读取本地 DB 快照后回答（必须显式 `as_of_trade_date`）。

## 防串线配置
- `OPENCLAW_NON_STOCK_ALLOW_STOCK_FALLBACK=0`
- `OPENCLAW_NON_STOCK_EMERGENCY_FALLBACK=0`
- `OPENCLAW_STOCK_CONCEPT_CLOUD_FIRST=1`
- `OPENCLAW_CLOUD_BRAIN_ONLY=0`（默认混合；若要严格只云端可设为 `1`）

## 运行
```bash
bash tools/openclaw_single_mainline.sh
```

## 自检
1. 发送：`wawa 人类该怎么活着`
2. 发送：`wawa 688608 怎么样`
3. 发送：`oc daily`

预期：
- 第 1 条不应出现仓位/止损模板。
- 第 2 条应包含本地快照和 `as_of_trade_date` 语境。
- 第 3 条输出流水线 JSON 结果。
