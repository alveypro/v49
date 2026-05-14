# 实验体系重设计蓝图

## 1. 文档目标

本文档重新设计 `stock_ultimate_system` 的实验体系，使当前系统从“工程化研究流水线”升级为“可审计、可复现、可晋级治理”的正式实验平台。

本文档不替代现有实现，而是定义未来实验系统应遵守的上层约束。

---

## 2. 当前系统的客观判断

当前系统已经具备以下优点：

- 有完整主链：数据、特征、状态识别、预测、信号、风控、回测、进化、候选、日报、看板。
- 有一定抗失真机制：`walk_forward`、`replay`、失败降级、健康评分、流动性过滤。
- 已有实验追踪器、排行榜、冠军版本治理、控制面输出。

但从正式实验体系角度，仍有四类关键不足：

- 实验边界不够硬：搜索窗、验证窗、重放窗、上线观察窗没有形成强制隔离。
- 晋级规则不够严：冠军晋级主要依赖 `walk_forward_score` 和稳定性，交易约束和风险门禁不够强。
- 评分体系仍偏启发式：候选排序强依赖人工加权配方。
- 审计闭环不完整：实验元数据、研究池快照、数据版本、运行参数、状态流转还不够标准化。

---

## 3. 新实验体系的设计原则

### 3.1 原则一：研究、验证、上线必须严格隔离

任何参数搜索、模型选择、阈值优化都只能发生在研究阶段。

验证阶段只允许：

- 固定参数
- 固定模型集
- 固定数据处理逻辑
- 固定评分器

上线观察阶段只允许：

- 不改参数
- 不改特征
- 不改评分配方
- 只观察真实运行稳定性

### 3.2 原则二：冠军不是“当前最好”，而是“通过门禁后仍最稳”

冠军晋级必须是多门禁机制，而不是单分数排序。

### 3.3 原则三：所有实验都必须可复现

一次实验至少应能回答：

- 用了什么数据
- 用了什么股票池
- 用了什么参数
- 用了什么随机种子
- 用了什么冠军基线
- 在什么时间执行
- 为什么晋级或失败

### 3.4 原则四：候选排序必须逐步从手工经验转向可校准学习器

规则可以保留，但应从“直接决定最终分数”退化为“提供排序特征”。

---

## 4. 新实验体系总架构

建议将实验体系分为 6 层：

1. `Hypothesis Layer`
   - 定义实验假设、变量、固定项、退出条件。
2. `Dataset Layer`
   - 定义数据版本、研究池快照、时间切分、股票池来源。
3. `Search Layer`
   - 在研究窗内执行参数搜索、模型选择、候选配方比较。
4. `Validation Layer`
   - 在独立样本外窗口执行冻结验证。
5. `Governance Layer`
   - 对候选版本做晋级/观察/拒绝/回滚判定。
6. `Production Observation Layer`
   - 用只读方式观察生产链表现，不允许即时调参。

---

## 5. 新实验分层

### 5.1 Level 0：Smoke

目标：

- 校验链路是否跑通
- 校验配置是否可用
- 校验数据结构是否完整

特点：

- 可用短窗
- 可用单票或小池
- 不参与冠军治理

当前对应：

- `short` profile

### 5.2 Level 1：Research

目标：

- 研究方向是否有效
- 变量变化是否产生稳定改善

允许：

- 网格搜索
- 分层采样
- 启发式筛选
- 失败重试

不允许：

- 直接升 production champion

当前对应：

- `medium` 和部分 `long`

### 5.3 Level 2：Validation

目标：

- 在独立样本外窗口验证研究结论

要求：

- 固定参数
- 固定模型集
- 固定评分器
- 禁止继续搜索

必须输出：

- 样本外收益
- 样本外风险
- 市场状态覆盖
- 成本敏感性
- 容量/流动性影响

### 5.4 Level 3：Champion Evaluation

目标：

- 判断候选版本是否可晋级为 staging champion

要求：

- 必须通过所有门禁
- 必须有清晰晋级原因
- 必须记录被比较的当前冠军基线

### 5.5 Level 4：Staging Observation

目标：

- 用生产同等链路运行，但不影响真实生产决策

要求：

- 只读观察
- 不改参数
- 不改启用模型
- 不改候选评分器

### 5.6 Level 5：Production Champion

目标：

- 成为生产环境默认冠军

要求：

- 完整通过研究、验证、治理、观察四层
- 有回滚计划
- 有最近表现监控

---

## 6. 时间切分重设计

### 6.1 必须使用四段式时间窗

对于任何正式实验，建议统一为：

- `train_window`
- `search_window`
- `validation_window`
- `observation_window`

定义：

- `train_window`：训练模型和基础特征统计
- `search_window`：搜索参数和筛选策略
- `validation_window`：完全样本外验证
- `observation_window`：冠军观察期

### 6.2 禁止同窗 replay

如果 `replay_start_date/replay_end_date` 与搜索窗完全重合，则该 replay 不得计为样本外验证。

### 6.3 必须做滚动验证

建议至少支持：

- anchored expanding window
- rolling window
- regime segmented window

### 6.4 必须报告覆盖度

每次实验应输出：

- bull / bear / range / high-vol 的覆盖情况
- 单票样本数
- 多股票池样本数
- 成本后有效交易数

---

## 7. 指标体系重设计

### 7.1 一级指标：战略有效性

- `walk_forward_score`
- `trade_objective_mean`
- `trade_objective_stability`
- `cost_adjusted_total_return`
- `cost_adjusted_sharpe`

### 7.2 二级指标：交易质量

- `max_drawdown`
- `calmar_ratio`
- `win_rate`
- `total_trades`
- `turnover_proxy`
- `low_liquidity_block_rate`
- `slippage_sensitivity`

### 7.3 三级指标：实验可信度

- `sample_count`
- `fold_count`
- `pool_count`
- `regime_coverage_score`
- `parameter_sensitivity_score`
- `ranking_consistency_score`

### 7.4 四级指标：生产适配度

- `runtime_sec`
- `candidate_generation_degraded`
- `daily_health_score`
- `automation_failure_rate`

---

## 8. 新冠军门禁设计

冠军判定必须是门禁，而不是线性打分。

建议至少四层门禁：

### 8.1 Gate A：统计门禁

必须满足：

- `walk_forward_score >= threshold`
- `trade_objective_stability >= threshold`
- `fold_count >= threshold`
- `sample_count >= threshold`

### 8.2 Gate B：交易门禁

必须满足：

- 成本后收益为正
- 最大回撤未超过限制
- 有效交易数达到最低要求
- 流动性拦截率不超过阈值

### 8.3 Gate C：稳健性门禁

必须满足：

- 在不同时间窗不过度失真
- 在不同市场状态不完全失效
- 参数微调后结果不崩塌

### 8.4 Gate D：生产门禁

必须满足：

- 候选生成未频繁 degraded
- 运行时延可接受
- 控制面健康评分达标
- 有清晰回滚路径

### 8.5 冠军动作

候选版本只允许以下四类动作：

- `reject`
- `observe`
- `promote_to_staging`
- `promote_to_production`

不建议继续使用过于宽泛的 `rollback` 作为候选动作名；回滚应是生产态动作，而非研究态评审动作。

---

## 9. 冠军状态流转重设计

建议新增三套冠军：

- `research_champion`
- `staging_champion`
- `production_champion`

状态流转：

`research_candidate -> research_champion -> staging_champion -> production_champion`

回滚流转：

`production_champion -> previous_production_champion`

要求：

- 研究冠军不允许直接改写生产参数
- staging 冠军只用于观察，不参与真实交易
- production 冠军必须有历史可追溯性

---

## 10. 候选排序器重设计

### 10.1 当前问题

当前候选排序依赖手工权重：

- `prob_up`
- `pred_return`
- `confidence`
- `model_agreement`
- `dispersion`
- `sample_quality`
- `regime_bonus`
- `risk_penalty`

这种方式可解释，但容易把研究者偏好硬编码进候选结果。

### 10.2 新设计

候选排序改为两阶段：

#### 阶段一：规则过滤

只做硬性约束：

- 市场规则
- 流动性门槛
- 风险禁止条件
- 异常数据剔除

#### 阶段二：校准排序器

输入特征：

- 方向概率
- 预期收益
- 置信度
- 校准胜率
- 校准收益
- 样本量质量
- 模型一致性
- 预测分歧
- 市场状态标签
- 风险特征

输出：

- `candidate_rank_score`
- `candidate_rank_band`
- `candidate_rank_uncertainty`

### 10.3 实施建议

先不移除当前手工分数，而是：

- 保留 `final_score_legacy`
- 新增 `candidate_rank_score_v2`
- 在看板中并排展示
- 用 A/B 验证替换旧版

---

## 11. 实验追踪重设计

每次正式实验必须记录以下元数据：

- `experiment_id`
- `hypothesis_id`
- `experiment_level`
- `git_commit`
- `created_at`
- `operator`
- `random_seed`
- `sampling_mode`
- `config_snapshot`
- `runtime_overrides`
- `stock_pool_snapshot`
- `research_pool_meta`
- `data_version`
- `train_window`
- `search_window`
- `validation_window`
- `observation_window`
- `baseline_champion_version`
- `candidate_version`
- `governance_action`

此外，所有 leaderboard 不应只展示“最新”，还必须有不可变历史快照。

---

## 12. 实验产物标准化

每次实验至少输出：

### 12.1 结构化产物

- `experiment_manifest.json`
- `config_snapshot.json`
- `research_pool_snapshot.csv`
- `search_results.csv`
- `validation_results.csv`
- `governance_decision.json`

### 12.2 人类可读产物

- `experiment_summary.md`
- `risk_review.md`
- `candidate_ranking_review.md`

### 12.3 诊断产物

- `signal_logs.csv`
- `regime_coverage.json`
- `sensitivity_report.json`
- `slippage_stress.csv`

---

## 13. 控制面重设计

控制面不应只展示结果，还要展示实验合法性。

建议看板新增以下区域：

- 当前冠军的实验层级
- 当前冠军是否通过全部门禁
- 当前 replay 是否独立样本外
- 当前候选排序器版本
- 当前生产冠军与研究冠军是否一致
- 当前实验是否存在参数污染风险

---

## 14. 推荐实施顺序

### Phase 1：立刻执行

- 强制 `validation_window` 独立于 `search_window`
- 在治理层引入 `reject / observe / promote_to_staging / promote_to_production`
- 补充实验元数据与研究池快照

### Phase 2：中期执行

- 引入三套冠军状态
- 引入多门禁治理
- 增加成本/容量/状态覆盖度指标

### Phase 3：后期执行

- 将候选排序升级为学习型校准排序器
- 建立正式实验矩阵与假设管理
- 建立生产观察与自动回滚联动

---

## 15. 成功标准

实验体系升级完成后，应满足以下标准：

- 任一冠军版本都能追溯完整实验路径
- 任一晋级决定都能解释为门禁通过结果
- 任一验证都能证明其为真正样本外
- 任一候选排序都能说明其版本、来源与可信度
- 任一生产冠军都可以在 1 分钟内回滚到上一个稳定冠军

如果上述五条不能满足，则系统仍属于“研究工程系统”，还不属于“顶级实验平台”。
