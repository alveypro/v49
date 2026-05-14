# 行业第一候选股系统下一阶段执行纲要

## 1. 文档定位

本文档是 `stock_ultimate_system` 在 `/stock` 唯一正式主链收口、主链真实性护栏完成本周期硬化之后，进入下一阶段的正式执行文档。

它不讨论愿景包装，不做融资叙事，不把“系统更复杂”误写成“系统更强”，只回答三件事：

- 行业第一候选股系统的目标如何被可量化定义
- 下一阶段必须建设哪些能力，系统才配得上追求行业第一
- 哪些事情现在明确不做，避免再次偏回低价值扩张

执行优先级：

1. [STRICT_CONTINUATION_EXECUTION_STANDARD.md](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/docs/STRICT_CONTINUATION_EXECUTION_STANDARD.md:1)
2. 本文档
3. [STOCK_TWO_WEEK_EXECUTION_CHECKLIST.md](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/docs/STOCK_TWO_WEEK_EXECUTION_CHECKLIST.md:1)
3. [CANDIDATE_QUALITY_EVALUATION_AND_BENCHMARK_SUITE.md](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/docs/CANDIDATE_QUALITY_EVALUATION_AND_BENCHMARK_SUITE.md:1)
4. [TOP_TIER_EXECUTION_STANDARD.md](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/docs/TOP_TIER_EXECUTION_STANDARD.md:1)
5. [MAIN_CHAIN_PRODUCTION_ACTION_BASELINE.md](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/docs/MAIN_CHAIN_PRODUCTION_ACTION_BASELINE.md:1)
6. 其他设计说明、阶段总结、口头判断

---

## 2. 总目标的可量化定义

### 2.1 唯一总目标

下一阶段唯一总目标冻结为：

`持续、可验证、可复利地产出行业顶级水准的候选股票。`

这一定义有四个硬约束：

- `持续`
  不是单批次漂亮结果，而是跨时间、跨市场状态、跨回放窗口的稳定输出。
- `可验证`
  候选质量必须能被固定口径、固定样本、固定基准验证。
- `可复利`
  胜利样本、失败样本、漏检样本、错排样本都必须能反哺系统。
- `顶级水准`
  必须对标可比较基准，而不是只对自己的旧版本自我安慰。

### 2.2 顶级候选股系统的真实定义

达到“行业第一候选股系统”不等于：

- 偶尔抓到几个大牛股
- 页面上看起来更完整
- 候选数量更多
- 解释文字更多
- 单次回测更漂亮

达到“行业第一候选股系统”至少要求以下条件同时成立：

- 候选排序在样本外长期优于固定对照基准
- 候选进入理由、排位理由、排除理由可解释
- 不同 market regime 下的表现边界可解释
- 失败样本能被系统化回收，而不是只靠人工记忆
- 系统改动不会破坏 formal main-chain authenticity baseline

### 2.3 下一阶段硬指标

下一阶段必须建立并固定以下指标，不允许只看总收益：

- `top1 / top3 / top5 / top10` 候选收益与命中质量
- 排名分层收益曲线
- 样本外收益稳定性
- 胜率
- 盈亏比
- 最大回撤
- 收益回撤比
- 信号覆盖率
- 信号衰减速度
- bull / bear / range / high-vol 四类环境表现
- 错排样本率
- 漏检样本率
- 失败归因分布

### 2.4 下一阶段完成标志

只有以下条件同时成立，才允许说“下一阶段完成”：

- 候选股质量有固定评估框架，不再靠临时解读
- 候选生成链已结构解耦，支持稳定迭代而不污染正式主链
- 主链真实性正式回归基线继续全绿
- 失败样本反哺机制已形成固定产物，而不是零散复盘
- 发布链和策略评估链同时进入正式日常流程

---

## 3. 下一阶段必须做的能力建设

### 3.1 能力一：候选股质量评估体系

#### 当前问题

当前系统已经比较擅长保证：

- `/stock` 主链真
- pointer 真
- registry 真
- lifecycle evidence 真
- deploy / rollback / verification 不撒谎

但这还不等于“候选股质量被正式证明为顶级”。

当前缺的是：

- 候选质量的统一评估框架
- 固定对照基准
- 固定分层统计
- 固定失败归因视角

#### 必须完成

- 建立正式 `candidate_quality_evaluation` 入口
- 固定 `top1 / top3 / top5 / top10` 分层口径
- 固定 benchmark suite
- 固定 OOS / observation / promotion 三段评估口径
- 固定 bull / bear / range / high-vol regime 分层口径
- 固定错排 / 漏检 / false-positive / false-negative 归因口径

#### 完成标志

- 任一候选列表都能生成统一质量报告
- 任一候选排序变化都能被量化解释
- 不再需要人工临时拼表回答“这批候选到底强不强”

本能力项的正式口径冻结见：

- [CANDIDATE_QUALITY_EVALUATION_AND_BENCHMARK_SUITE.md](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/docs/CANDIDATE_QUALITY_EVALUATION_AND_BENCHMARK_SUITE.md:1)

### 3.2 能力二：候选生成链结构解耦

#### 当前问题

当前 `/stock` 正式主链真实性已较硬，但候选能力本身的迭代链还不够干净。

如果候选生成、评分、排序、入选、证据、页面消费还粘在一起，会导致：

- 候选模型难以做严肃 A/B 对比
- 研究和正式主链耦合过紧
- 页面层或补跑链更容易重新污染主链

#### 必须完成

- 拆出 `candidate query / scoring / ranking / admission / evidence` 五层
- 形成候选专用 registry / report / replay 产物边界
- 页面和 API 只消费稳定 query service，不直接拼研究散文件
- 正式主链与候选实验链共享事实源约束，但不共享脏状态

#### 完成标志

- 候选机制能独立升级，不必顺带改动正式页面主链
- 新评分方法可以在不污染 `/stock` 正式主链的前提下并行对比
- 候选质量提升成为正式工程动作，不再靠大脚本局部加逻辑

### 3.3 能力三：失败样本反哺闭环

#### 当前问题

没有系统化失败样本反哺，就不可能成为行业第一。

普通系统只积累成功案例；顶级系统更重视：

- 应进未进
- 进了但排低
- 排高但失败
- 被风险门禁挡掉但后续走强
- 证据不足未晋级但后续验证有效

#### 必须完成

- 建立 `candidate_failure_attribution_ledger`
- 建立 `missed_winner_ledger`
- 建立 `false_positive_ledger`
- 建立固定的“失败原因 taxonomy”
- 把失败样本直接回流到评分、排序、过滤、治理阈值评估

#### 完成标志

- 系统能固定产出“为什么错”而不是只会说“这次没做好”
- 失败样本可以被回放、聚类、统计，而不是停留在口头复盘
- 候选质量提升开始体现出复利，而不是一次次重来

### 3.4 能力四：回归分层提速

#### 当前问题

现在主链正式回归基线已经很强，但再往下一阶段推进，如果所有开发都默认全量跑，会拖慢迭代节奏。

#### 必须完成

- 把回归测试分层成：
  - `fast`
  - `integration`
  - `main-chain-authenticity`
  - `candidate-quality`
  - `recovery`
  - `deploy-verification`
- 固定日常开发、候选机制迭代、正式发布三种测试命令集

#### 完成标志

- 日常改动不需要每次全量跑到极慢
- 正式发布时仍有完整门禁
- 测试分层服务研发效率，而不是破坏门禁强度

### 3.5 能力五：正式发布链产品化

#### 当前问题

deploy / verification / readiness / rollback 已经较硬，但仍偏“资深维护者可驾驭”，还不够产品化。

#### 必须完成

- 固化标准发布 runbook
- 固化标准 rollback runbook
- 固化标准 candidate-quality verification run
- 让 release evidence、strategy evidence、readiness evidence 成套关联

#### 完成标志

- 发布不是靠记忆和经验，而是靠固定产物链
- 任何一次版本升级，都能回答“候选质量变强了还是只是页面变了”

---

## 4. 明确不做的事

### 4.1 不做的叙事扩张

下一阶段禁止把主线再次带回：

- 多正式面叙事
- `/apex` 并列产品化
- 泛平台化、泛生态化、泛 AI 营销化

`/stock` 继续是唯一正式业务主链。`apex` 继续只是内部验证环境。

### 4.2 不做的低价值工程

以下工作不再作为主任务：

- 继续无限补同层 provenance 负例
- 大范围 UI 润色
- 与候选股质量无关的模块重命名
- 不改变候选质量、却显著增加复杂度的包装性抽象

### 4.3 不做的假进展

以下不算下一阶段进展：

- 测试数继续上涨但候选质量评估框架没有建立
- 页面更复杂但排序逻辑没有更清晰
- 文档更多但失败样本反哺没有形成闭环
- 指标更多但没有固定 benchmark 对照

### 4.4 不允许的越界

任何会削弱以下任一条件的开发，直接视为越界：

- `/stock` 唯一正式主链
- formal main-chain authenticity baseline
- fail-closed 原则
- deploy / rollback / verification 诚实性

---

## 5. 第一批开发任务列表

下一阶段第一批任务按以下顺序推进，不允许跳步。

1. 冻结候选股质量评估指标口径与 benchmark suite
2. 建立正式 `candidate_quality_evaluation` 产物
3. 拆分候选生成链的 query / scoring / ranking / admission / evidence 边界
4. 建立失败样本 ledger 与归因 taxonomy
5. 固化回归测试分层与正式发布测试命令

不允许把页面美化、叙事增强、入口扩张排到这五项之前。

---

## 6. 阶段关闭判断

下一阶段是否完成，不看“功能是不是更多”，只看三条：

- 候选股质量是否被统一框架量化证明
- 候选机制是否已经结构化到可稳定迭代
- 失败样本是否已经进入固定反哺闭环

如果这三条没有同时成立，下一阶段就不算完成。
下一阶段 `/stock` 正式产品面的重构，必须同时遵守：
- [STOCK_UI_SECOND_STAGE_RECONSTRUCTION_CHECKLIST.md](./STOCK_UI_SECOND_STAGE_RECONSTRUCTION_CHECKLIST.md)
