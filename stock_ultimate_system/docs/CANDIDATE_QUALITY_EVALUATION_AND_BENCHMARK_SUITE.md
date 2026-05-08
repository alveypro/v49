# 候选股质量评估口径与 Benchmark Suite 冻结文档

## 1. 文档定位

本文档用于冻结 `candidate_quality_evaluation` 的正式评估口径与 benchmark suite。

它不讨论模型细节，不讨论展示样式，也不讨论“这批票看起来像不像强票”，只定义四件事：

- 候选股质量必须用哪些指标衡量
- benchmark suite 必须包含哪些固定对照
- 什么样本可以进入正式评估，什么样本必须剔除
- 正式质量报告必须产出哪些固定字段

执行优先级：

1. [STRICT_CONTINUATION_EXECUTION_STANDARD.md](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/docs/STRICT_CONTINUATION_EXECUTION_STANDARD.md:1)
2. [INDUSTRY_FIRST_CANDIDATE_SYSTEM_EXECUTION_STANDARD.md](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/docs/INDUSTRY_FIRST_CANDIDATE_SYSTEM_EXECUTION_STANDARD.md:1)
3. 本文档
4. 其他候选评估说明、临时分析、口头判断

---

## 2. 冻结目标

### 2.1 唯一目标

`用固定口径回答：当前候选股系统是否真的在持续产出顶级水准的候选股票。`

### 2.2 明确禁止

以下做法在正式评估中一律禁止：

- 只看单批次收益
- 只看 top1 漂亮案例
- 不区分样本内与样本外
- 不区分 market regime
- 因为“这次行情特殊”就临时改口径
- 把候选质量和页面展示效果混在一起评价

---

## 3. 评估对象与样本边界

### 3.1 正式评估对象

正式评估对象只包括：

- `/stock` 正式主链产生的候选列表
- 进入正式评估窗口的候选版本
- 受 formal main-chain authenticity baseline 保护的候选输出

以下对象不进入正式评估主结果：

- `apex` 内部验证候选
- 临时实验输出
- 不带 `run_id / result_id / artifact_ids / source_scope` 的手工样本
- 无法回放的历史截图、临时 CSV、人工摘录

### 3.2 样本窗口

正式评估必须同时固定三类窗口：

- `20` 交易日短窗
- `60` 交易日中窗
- `120` 交易日长窗

任何正式质量结论都必须同时给出这三类窗口，不允许只给单窗结果。

### 3.3 样本分层

正式评估必须同时保留以下分层：

- `top1`
- `top3`
- `top5`
- `top10`

不允许只看总体 basket 平均值。

### 3.4 Market Regime 分层

正式评估必须固定四类 regime：

- `bull`
- `bear`
- `range`
- `high_vol`

任一候选质量报告都必须至少回答：

- 总体表现
- 四类 regime 下的表现
- regime 间性能差异是否可接受

---

## 4. 正式指标口径

### 4.1 主指标

以下指标是正式主指标，必须固定输出：

- `top1_avg_return`
- `top3_avg_return`
- `top5_avg_return`
- `top10_avg_return`
- `top1_avg_excess_return`
- `top3_avg_excess_return`
- `top5_avg_excess_return`
- `top10_avg_excess_return`
- `win_rate`
- `payoff_ratio`
- `max_drawdown`
- `return_drawdown_ratio`
- `signal_coverage`
- `ranking_consistency_score`
- `signal_decay_5d`
- `signal_decay_20d`

### 4.2 辅指标

以下指标是正式辅指标，必须和主指标一起输出：

- `false_positive_rate`
- `missed_winner_rate`
- `top_bucket_turnover`
- `industry_concentration`
- `capacity_risk_summary`
- `signal_drought_days`
- `failure_attribution_distribution`

### 4.3 指标解释约束

所有主指标和辅指标必须遵守：

- 指标定义固定
- 计算窗口固定
- 不因某次结果难看而临时改算法
- 不允许一个字段在不同报告中换含义

---

## 5. Benchmark Suite 冻结

### 5.1 Benchmark Suite 必含对象

正式 benchmark suite 必须同时包含以下对照：

1. `universe_equal_weight`
2. `benchmark_index_proxy`
3. `top1_only`
4. `top3_equal_weight`
5. `top5_equal_weight`
6. `top10_equal_weight`
7. `previous_formal_baseline`

### 5.2 每个 benchmark 的作用

#### `universe_equal_weight`

作用：
- 回答“候选系统是否至少优于不筛选 universe”

#### `benchmark_index_proxy`

作用：
- 回答“候选系统是否优于普通市场暴露”

#### `top1_only`

作用：
- 回答“第一名候选是否真的具备头部质量”

#### `top3_equal_weight / top5_equal_weight / top10_equal_weight`

作用：
- 回答“排序扩展后，质量如何衰减”
- 回答“系统强在 top1 还是强在整个排序带”

#### `previous_formal_baseline`

作用：
- 回答“当前版本是否真的优于上一个正式基线”

### 5.3 明确禁止的 benchmark 行为

以下行为一律禁止：

- 每次版本评估时临时换 benchmark
- 为了让结果更好看剔除不利 benchmark
- 只和最弱对照比，不和上个正式基线比

---

## 6. 正式评估产物

### 6.1 必须产出的文件

每次正式 `candidate_quality_evaluation` 必须产出至少三类产物：

- `candidate_quality_summary.json`
- `candidate_quality_benchmark_table.csv`
- `candidate_quality_failure_attribution.json`

### 6.2 Summary 必须包含的字段

`candidate_quality_summary.json` 必须至少包含：

- `evaluation_id`
- `run_id`
- `source_scope`
- `evaluation_window`
- `benchmark_suite_version`
- `sample_count`
- `top1/top3/top5/top10` 主指标摘要
- `regime_breakdown`
- `pass_or_fail`
- `blocking_reasons`

### 6.3 Benchmark Table 必须包含的列

`candidate_quality_benchmark_table.csv` 必须至少包含：

- `bucket`
- `window`
- `benchmark_name`
- `avg_return`
- `avg_excess_return`
- `win_rate`
- `max_drawdown`
- `signal_coverage`
- `ranking_consistency_score`

### 6.4 Failure Attribution 必须包含的字段

`candidate_quality_failure_attribution.json` 必须至少包含：

- `false_positive_cases`
- `missed_winner_cases`
- `rank_too_low_cases`
- `risk_gate_blocked_but_later_strong_cases`
- `evidence_insufficient_cases`
- `regime_mismatch_cases`

---

## 7. 正式门禁口径

### 7.1 不允许晋级的情形

以下任一成立，候选版本不得进入“顶级候选股能力提升”正向结论：

- 样本数不足
- 只有单窗结果
- 缺少 previous formal baseline 对照
- 缺少 regime breakdown
- 缺少 failure attribution
- 主链真实性基线未通过

### 7.2 可以被判断为“质量提升”的最低条件

只有以下条件同时成立，才允许说“本版本候选质量提升”：

- 相对 `previous_formal_baseline` 在至少两个窗口中取得稳定优势
- 在 `top1` 与 `top3` 至少一层保持超额优势
- 没有因 regime 切换出现不可解释的系统性塌陷
- failure attribution 没显示出新引入的大类退化

---

## 8. 第一批落地任务

本文件冻结后，下一步只允许做以下事情：

1. 建立 `candidate_quality_evaluation` 正式入口
2. 固化 benchmark suite 数据结构与版本字段
3. 生成第一版正式质量报告产物
4. 为该入口补正式集成测试

在这四步完成前，不允许宣称“候选股质量评估体系已建成”。
