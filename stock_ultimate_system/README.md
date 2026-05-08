# Stock Ultimate System

A-股多智能体股票算法交易系统，基于 Qlib 数据引擎 + 多模型集成 + 事件驱动回测。

## 架构

```
Data Agent → Feature Agent → Regime Agent → Forecast Agent
→ Signal Agent → Risk Agent → Position Agent → Execution Agent
→ Backtest Agent → Evolution Agent
```

## 核心模块

| 模块 | 说明 |
|------|------|
| `src/data_engine/` | Qlib数据获取、清洗、合并、存储 |
| `src/features/` | 趋势/动量/波动率/量价/市场环境/风险因子 |
| `src/models_engine/` | LightGBM, XGBoost, RF, Logistic, LSTM, Transformer |
| `src/signal_engine/` | 因子打分、信号融合、进出场规划 |
| `src/risk_engine/` | 止损、止盈、回撤保护、风险过滤 |
| `src/portfolio/` | 仓位计算、组合管理 |
| `src/execution/` | 订单管理、成交仿真 |
| `src/backtest_engine/` | 事件驱动回测引擎 |
| `src/evaluation/` | 绩效指标、稳定性分析、策略解释、报告生成 |
| `src/evolution/` | 超参调优(Optuna)、模型选择、因子衰减监控 |
| `src/rules/` | A股交易规则(涨跌停、ST过滤、T+1) |
| `src/visualization/` | 净值曲线、月度收益、滚动指标图表 |

## 快速开始

```bash
cd stock_ultimate_system
pip install -r requirements.txt

# 训练模型
python run_train.py

# 单股预测
python run_predict.py --code 000001.SZ

# 批量预测
python run_predict.py --batch 000001.SZ 600036.SH 000002.SZ

# 运行回测
python run_backtest.py --stocks 000001.SZ 600036.SH

# 运行进化优化
python run_evolve.py

# 网格批量回测（带profile模板）
python run_grid_backtest.py --profile short --stocks 000001.SZ

# 每日研究流水线（short->medium）
python run_daily_research.py --stocks 000001.SZ

# 治理审计（检查是否达到实验门禁）
python run_governance_audit.py

# 生成正式实验产物包（补齐治理所需文件）
python run_experiment_artifact_bundle.py

# 一键治理主链（daily research -> artifact bundle -> governance audit）
python run_governance_cycle.py

# 生成每日可买清单快照（固定数量，如5只）
python run_buylist_snapshot.py --target-count 5

# 完整演示
python main.py
```

## 数据源

- **主要**: Qlib (`~/.qlib/qlib_data/cn_data`)
- **备用**: 内置 stub 数据（自动降级）

首次使用需下载 Qlib 数据:
```bash
python -m qlib.run.get_data qlib_data --target_dir ~/.qlib/qlib_data/cn_data --region cn
```

## 配置

所有配置在 `config/` 目录:
- `settings.yaml` — 全局设置
- `model_params.yaml` — 模型超参数
- `feature_params.yaml` — 特征参数
- `signal_rules.yaml` — 信号阈值
- `risk_rules.yaml` — 风险规则
- `market_rules.yaml` — A股交易规则
- `experiment_framework.example.yaml` — 正式实验体系模板（时间切分、门禁、冠军治理、产物规范）

## 实验治理

建议先阅读以下文档，再继续扩展研究或上线治理：

- `FIRST_PLACE_CHALLENGE_PROTOCOL.md`
- `FIRST_PLACE_20_OBSERVATION_SPRINT.md`
- `FIRST_PLACE_GAP_CLOSURE_REGISTER.md`
- `docs/GRID_BACKTEST_PRESETS.md`
- `docs/EXPERIMENT_REDESIGN_BLUEPRINT.md`
