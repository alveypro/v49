# ClawAlpha 统一入口方案（OC + WAWA）

## 目标
- 前台只保留一个智能体入口：`ClawAlpha`
- 后端分成两条模式：
  - `oc`：严格执行流（daily/audit/go/approve/reject）
  - `wawa`：自由研究流（agent llm）
- 股票问答必须先读本地/服务器真实 v49 数据快照再回答
- 全系统只允许一个 Telegram token 消费者（单活）

## 推荐架构
- Web/Telegram 统一入口：`ClawAlpha`
- Router 规则：
  - 命令前缀是 `oc` -> 走执行流
  - 命令前缀是 `wawa` 或默认自然问答 -> 走研究流
  - 股票问题（含 6 位代码）在 `wawa` 流中先做 DB 快照注入，再交给 LLM

## 路由规则模板
```text
if text startswith "oc ":
  route = OC_EXEC
elif text startswith "wawa ":
  route = WAWA_RESEARCH
else:
  route = WAWA_RESEARCH

if route == WAWA_RESEARCH and contains_stock_code(text):
  snapshot = load_v49_snapshot_from_db(ts_code)
  prompt = "基于真实快照回答" + snapshot + user_text
else:
  prompt = "自由研究助手模式" + user_text
```

## OC 执行流（严格）
- 保留命令：
  - `oc daily`
  - `oc audit`
  - `oc go`
  - `oc approve`
  - `oc reject`
- 审批仍必须是 patch-only（有 diff 才算）
- 失败默认 stop，不自动放行

## WAWA 研究流（自由）
- 使用 agent llm 直接回答，不套“仓位模板”
- 股票问题时启用真实快照注入（DB + 最新日报提示）
- 允许观点和自然表达，但不允许伪造字段/伪造执行结果

## 单活原则（非常重要）
- 一个 Telegram bot token 只能由一个 bridge 进程消费。
- 若出现 `409 Conflict`，表示还有其他实例在抢 token。

### 标准生产建议
1. 只在服务器启动 bridge。
2. 本地停掉 bridge 与 stock-agent（或至少停 bridge）。
3. 新 token 只配置在服务器，不在本地保存。

## 你的当前基线（已核验）
- `stock_basic`: 5464
- `industry_count`: 110
- `daily_trading_data`: 5,171,996
- `db_size`: 2.293 GB
- `latest_trade_date`: 20260213
- `active_on_latest`: 4988

## 中国交易日判断（2026-02-21 时点）
- 春节休市区间内，节前最后交易日是 `2026-02-13`
- 所以 `latest_trade_date=20260213` 是合理的

## 切换完成后的验收
1. 发 `/health`：应稳定返回
2. 发 `wawa 人类的追求是什么`：应自然对话，不应出现仓位模板
3. 发 `wawa 688608 怎么样`：应包含本地快照字段并给出 `as_of_trade_date`
4. 发 `oc daily`：应返回结构化执行结果，不应混入 wawa 语气

