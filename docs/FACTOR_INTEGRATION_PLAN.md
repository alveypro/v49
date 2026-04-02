# Tushare 因子增强接入说明（v49）

## 目标
- 在不重写 v4~v9 主评分器的前提下，引入可解释的因子加分层。
- 使用 `auto_evolve.py` 每日补充高价值因子表。
- 用 AB 脚本验证“增强前/增强后”收益质量差异。

## 已接入模块
- 因子加分引擎：`data/factor_bonus.py`
- 数据更新入口：`auto_evolve.py`
- 主界面加分接入：`终极量价暴涨系统_v49.0_长期稳健版.py`
- 助手加分接入：`trading_assistant.py`
- AB 回测脚本：`scripts/factor_ab_test.py`

## 数据表（自动创建）
- `stk_factor_pro_daily`：个股技术因子日快照
- `idx_factor_pro_daily`：指数技术因子日快照
- `cyq_perf_daily`：筹码胜率/成本分布日快照
- `moneyflow_dc_daily`：东财个股资金流日快照
- `moneyflow_ind_dc_daily`：东财行业资金流日快照
- `moneyflow_mkt_dc_daily`：东财市场资金流日快照
- `stk_auction_daily`：集合竞价日快照
- `hk_hold_daily`：港股通持股日快照
- `hm_detail_daily`：沪深港通十大成交明细
- `share_float_events`：限售股解禁事件
- `repurchase_events`：股份回购事件
- `broker_recommend_events`：券商研报评级事件
- `stk_surv_events`：股东户数/调研类事件
- `cyq_chips_daily`：筹码分布明细（按股票抓取）

说明：表结构由 `to_sql` 自动推断，按 `trade_date` 做日覆盖写入。

## 关键环境变量
- `OPENCLAW_ENABLE_ADV_FACTORS=1`：是否拉取高级因子（默认开启）
- `OPENCLAW_ENABLE_TUSHARE_PLUS=1`：是否拉取增强接口（默认开启）
- `OPENCLAW_FACTOR_BONUS_ENABLED=1`：是否启用因子加分（默认开启）
- `UPDATE_DAYS=120`：数据补齐窗口（建议至少 90）
- `CYQ_CHIPS_SYMBOL_LIMIT=300`：每次抓取筹码分布的股票上限（`0` 表示全量）

## 运行建议
1. 日更补齐：
   - `AUTO_EVOLVE_PHASE=data_only UPDATE_DAYS=120 python3 auto_evolve.py`
2. 因子 AB 测试：
   - `python3 scripts/factor_ab_test.py --lookback-days 180 --hold-days 5 --top-k 20`
3. 上线判据：
   - 增强版 `avg_return_delta_pct > 0`
   - 增强版 `win_rate_delta_pct >= 0`
   - 增强版 `sharpe_delta > 0`

## 风险控制
- 若 Tushare 端临时限流或字段变化，更新函数会返回失败结果但不阻断主流程。
- 若高级因子表缺失，加分引擎会自动降级为 0 加分，不影响原策略执行。
