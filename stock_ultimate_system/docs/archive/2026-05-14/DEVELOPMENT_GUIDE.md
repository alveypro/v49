# 股票算法智能体系统开发文档

## 0. 当前执行口径

本文件保留为系统结构和基础模块说明。

自即日起，涉及系统成熟度提升、8分以上顶级化建设、治理主线、证据主线、发布主线的开发，统一以：

- `docs/STRICT_CONTINUATION_EXECUTION_STANDARD.md`
- `docs/INDUSTRY_FIRST_CANDIDATE_SYSTEM_EXECUTION_STANDARD.md`
- `docs/STOCK_SINGLE_FORMAL_CHAIN_EXECUTION_STANDARD.md`
- `docs/TOP_TIER_EXECUTION_STANDARD.md`

作为主执行规范。

如本文件与该规范冲突，优先以 `STRICT_CONTINUATION_EXECUTION_STANDARD.md` 为准。

当前阶段额外冻结要求：

- `airivo.online/stock` 是唯一正式股票系统
- `apex` 只能作为内部试验 / 预发布 / 旁路验证环境
- 凡不能强化 `/stock` 唯一正式主链的开发，本阶段一律不做

## 1. 系统目标

本系统是一个面向中国股市的多智能体股票算法平台，目标包括：
- 数据获取与治理
- 因子构建
- 市场状态识别
- 多模型预测
- 信号融合
- 风险控制
- 仓位管理
- 回测评估
- 自动进化

## 2. 核心主链

```text
Data Agent
-> Feature Agent
-> Regime Agent
-> Forecast Agent
-> Signal Agent
-> Risk Agent
-> Position Agent
-> Execution Agent
-> Backtest Agent
-> Evolution Agent
```

## 3. 开发顺序

### 3.1 基础设施
- 完成 config、logs、data、models、reports 目录
- 配置加载器 ConfigLoader
- 日志系统 LoggerManager
- 序列化工具与路径工具

### 3.2 数据层
- DataFetcher 对接 tushare/akshare
- DataCleaner 统一字段和缺失值处理
- TradeCalendarManager 统一交易日历
- DataStorage 管理 parquet/csv/pickle

### 3.3 规则层
- MarketRuleEngine 统一中国股市交易约束
- PriceLimitRules 管理涨跌停
- SecurityFilter 过滤 ST、停牌、低流动性标的

### 3.4 特征层
- 趋势特征
- 动量特征
- 波动率特征
- 量价特征
- 市场环境特征
- 风险特征

### 3.5 标签层
- direction label
- return label
- range label
- trade success label

### 3.6 模型层
- classical: logistic, random forest, lightgbm, xgboost
- deep: lstm, transformer
- ensemble: weighted average, majority vote, regime weighted

### 3.7 决策层
- FactorScorer 打分
- SignalFusionEngine 融合
- EntryExitPlanner 生成进出场方案

### 3.8 风控层
- 止损
- 止盈
- 波动率过滤
- 回撤保护
- 流动性过滤

### 3.9 仓位与执行
- PositionSizer
- PortfolioManager
- OrderManager
- TradeSimulator

### 3.10 回测与评估
- EventDrivenBacktester
- PerformanceTracker
- PerformanceMetrics
- StabilityAnalyzer
- StrategyExplainer
- ReportGenerator

### 3.11 进化层
- HyperparameterTuner
- ModelSelector
- FactorEvolution

## 4. 目录说明

- `src/agents/` 负责智能体编排
- `src/data_engine/` 负责数据获取、清洗、存储、对齐
- `src/rules/` 负责市场约束
- `src/features/` 负责因子工程
- `src/models_engine/` 负责模型训练与推理
- `src/signal_engine/` 负责信号融合
- `src/risk_engine/` 负责风险管理
- `src/portfolio/` 负责仓位与组合
- `src/execution/` 负责订单与成交仿真
- `src/backtest_engine/` 负责事件驱动回测
- `src/evaluation/` 负责指标与报告
- `src/evolution/` 负责选模、调参、因子淘汰
- `src/pipeline/` 负责总流程串联

## 5. 关键接口设计

### 5.1 预测接口
- 输入：单只股票特征表最后一行或最近窗口
- 输出：方向概率、预期收益率、置信度、各模型组件结果

### 5.2 信号接口
- 输入：forecast_result, factor_score, regime_info, risk_info
- 输出：signal, score, explanation, entry_plan

### 5.3 风险接口
- 输入：行情、预测结果、账户状态
- 输出：allow_trade, risk_level, stop_loss, take_profit

### 5.4 仓位接口
- 输入：signal_result, risk_result, account_info
- 输出：position_pct, scaling_plan

## 6. 后续真实接入建议

### 6.1 数据接入
把 `DataFetcher` 中的 stub 替换为：
- tushare 日线接口
- akshare 指数与板块接口
- 本地缓存增量更新机制

### 6.2 模型接入
把 `LSTMModel` 和 `TransformerModel` 的占位实现替换为真实 torch 模型。

### 6.3 回测真实化
补充：
- 涨停买不到
- 跌停卖不掉
- T+1
- 停牌不可成交
- 印花税和佣金
- 分批建仓 / 分批止盈

## 7. 交付标准

系统最低应满足：
- `run_train.py` 能正常训练至少一个模型
- `run_predict.py` 能输出单股信号
- `run_backtest.py` 能输出基础回测结果
- 报告模块能导出 markdown 或 html
