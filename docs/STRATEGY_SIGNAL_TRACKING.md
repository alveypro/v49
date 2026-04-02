# 策略后效追踪（v4/v5/v6/v7）

目标：把“每天各策略选出的股票”转成可量化的后效评估，持续比较策略真实表现。

## 数据流
1. `oc daily` 产生 `run_summary_*.json`
2. `tools/openclaw_partner_daily_run.sh` 自动调用：
   - `record`：写入当日信号（strategy/ts_code/score/rank）
   - `refresh`：计算 T+1/T+5/T+10/T+20 实际收益
   - `scoreboard`：输出当日策略对比看板（md + csv）
3. 产物写入 `logs/openclaw/`：
   - `strategy_scoreboard_YYYYmmdd_HHMMSS.md`
   - `strategy_scoreboard_YYYYmmdd_HHMMSS.csv`

## 表结构
- `strategy_signal_tracking`：原始信号台账
- `strategy_signal_performance`：已实现后效（含基准超额）

## 指标解释
- `win_rate_pct`：收益为正的样本占比
- `avg_ret_pct`：平均收益率
- `median_ret_pct`：中位收益率
- `avg_excess_ret_pct`：相对基准（000001.SH）平均超额收益

## 关键规则
- 只记录真实 `run_summary` 里的选股；无选股不会伪造样本。
- 若当前窗口内暂无可评估样本，也会生成空看板文件（保证产物稳定）。
- T+N 使用交易日序列，不使用自然日。

## 手工命令（排查用）
```bash
python3 openclaw/strategy_tracking_cli.py record --run-summary logs/openclaw/run_summary_xxx.json
python3 openclaw/strategy_tracking_cli.py refresh --lookback-days 240
python3 openclaw/strategy_tracking_cli.py scoreboard --lookback-days 120 --output-dir logs/openclaw
```

## 常见现象
- `no_performance_rows`：通常是信号太新，还没走完 T+N；不是故障。
- `inserted=0`：同一 `run_id + strategy + ts_code` 已存在，去重生效。
