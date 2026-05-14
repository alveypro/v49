# 后续继续推进严格开发规范

## 1. 文档定位

本文档是 `stock_ultimate_system` 后续 90 天继续推进的严格执行规范。

它不讨论愿景，不做宣传，不做泛化路线图，只定义四件事：

- 后续开发的唯一主目标是什么
- 后续开发必须按什么顺序推进
- 哪些工作允许做，哪些工作禁止做
- 每一阶段什么才算真实完成

自本文档生效起，凡涉及以下事项，统一以本文档为一线执行标准：

- 主结果治理
- 事实源收口
- 看板重构
- registry 建设
- 发布校验
- 运行回放
- 策略证据补强

如本文档与其他说明性文档冲突，执行优先级如下：

1. `docs/STRICT_CONTINUATION_EXECUTION_STANDARD.md`
2. `docs/archive/2026-05-14/INDUSTRY_FIRST_CANDIDATE_SYSTEM_EXECUTION_STANDARD.md`
3. `docs/archive/2026-05-14/STOCK_SINGLE_FORMAL_CHAIN_EXECUTION_STANDARD.md`
4. `docs/FORMAL_HOSTING_BOUNDARY_AND_ACTIVATION_BLOCKERS.md`
5. `docs/archive/2026-05-14/TOP_TIER_EXECUTION_STANDARD.md`
6. `docs/archive/2026-05-14/DEVELOPMENT_GUIDE.md`

---

## 2. 正式定位冻结

上一阶段 `/stock` 单一正式主链收口已于 `2026-05-01` 关闭。相关阶段归档见：

- `docs/archive/2026-05-14/STOCK_SINGLE_FORMAL_CHAIN_EXECUTION_STANDARD.md`
- `docs/FORMAL_HOSTING_BOUNDARY_AND_ACTIVATION_BLOCKERS.md`
- `docs/archive/2026-05-14/first_real_stock_scoped_activation_admission_gate.md`

后续开发默认回到本文档主线执行；凡涉及 `/stock` 与 `apex` 正式身份、deploy baseline、verification 收口、formal activation 历史边界时，仍必须参考这些归档文档，但不再把它们当作当前阶段继续阻断下一阶段的主线文档。

当前阶段冻结口径为：

`把 airivo.online/stock 钉成唯一正式主链，把 apex 降为内部试验环境；凡不能强化这件事的开发，一律不做。`

当前系统的对外正式定位冻结为：

`A 股研究决策与主结果治理平台`

英文定位冻结为：

`research-to-governance platform for A-share decision systems`

达到更高证据强度前，禁止对外宣称为：

- 顶级自动交易平台
- 成熟大资金实盘放大量化平台
- 已完成行业头部级实盘验证的平台

原因只有一条：

当前系统最强的资产是：

- 研究链路
- 主结果治理
- 可追溯证据
- 发布与回滚纪律雏形

当前系统最弱的资产仍然是：

- 交易执行规模化验证
- 长周期实盘放大证据
- 连续样本外策略优势证明

---

## 2.1 下一阶段主线冻结

主链真实性与正式主链收口在本周期已达到可防守状态。自本阶段起，后续开发主线冻结为：

`持续、可验证、可复利地产出行业顶级水准的候选股票。`

对应的下一阶段执行文档为：

- `docs/archive/2026-05-14/INDUSTRY_FIRST_CANDIDATE_SYSTEM_EXECUTION_STANDARD.md`

后续凡涉及：

- 候选股质量评估
- 候选生成链结构解耦
- 失败样本反哺
- 正式回归分层
- 发布链产品化

统一优先参考该文档，而不是继续把“主链真实性补护栏”当作唯一主线。

其中，候选股质量评估口径与 benchmark suite 的正式冻结文档为：

- `docs/archive/2026-05-14/CANDIDATE_QUALITY_EVALUATION_AND_BENCHMARK_SUITE.md`

本阶段接下来两周的正式执行清单为：

- `docs/archive/2026-05-14/STOCK_TWO_WEEK_EXECUTION_CHECKLIST.md`

---

## 3. 总体执行原则

### 3.1 唯一主目标

未来 90 天的唯一主目标不是继续堆功能，而是把系统从：

`复杂但脆`

推进到：

`复杂但可控`

### 3.2 先治根，再做表现层

在以下问题未解决前，页面美化、叙事增强、模块命名扩张一律不得作为高优先级工作：

- 多份 `latest` 共同决定一个主结论
- `/stock` 主结果无唯一指针
- 看板读路径依赖散落文件拼装
- 关键 artifact 不可回放
- 发布后没有自动 verification artifact

### 3.3 fail closed 原则

凡主结果证据链不完整、pointer 不唯一、artifact 缺失、状态冲突，系统必须默认：

- 不自动补脑
- 不自动推断为可推进
- 不自动展示“看起来合理”的正向结论

而必须明确显示：

- 证据不足
- 当前阻断
- 需要人工复核

### 3.4 latest 降级原则

`latest` 允许存在，但只允许充当：

- 索引入口
- 最近成功产物快捷访问入口
- 面向人类操作的 convenience alias

`latest` 不允许继续承担：

- 主事实源
- 生命周期判断主依据
- 发布是否允许的最高依据
- UI 主结果真相来源

### 3.5 单一主结论原则

`/stock` 首页只能有一个 primary conclusion。

禁止以下做法：

- 多个卡片共同“协商”最终结论
- 页面层混合多个时间点工件后给出看似一致的判断
- 业务主结论由显示层逻辑反推

### 3.6 治理只读原则

`/T12` 是治理只读镜像，不是第二业务工作台，不是策略操作台，不是总控台。

后续开发中，凡会让 `/T12` 获得写回、推进、替代 `/stock` 主结论能力的改动，一律视为越界。

---

## 4. 90 天推进顺序

未来 90 天开发只允许按 `P0 -> P1 -> P2` 顺序推进，不允许跳段宣布完成。

### P0：真相源收口

周期：第 1-30 天

目标：

- 让 `/stock` 主结果只来自唯一 pointer
- 让 `latest` 彻底退出主事实源地位
- 让主结果对象拥有可追溯的统一身份

必须完成：

- 建立 `current_result_pointer`
- 统一 `result_id / run_id / artifact_ids / lifecycle_id / as_of_date`
- `unified_result_builder` 改为先读 pointer 再读链路工件
- `dashboard_context` 不再直接用多份 `latest` 拼主结论
- 任一主结果缺证据时明确 fail closed

完成标志：

- `/stock` 主结论只来自一个 pointer
- 任一主结果可追溯到唯一 `result_id`
- 任一主结果可追溯到明确 `run_id`
- 任一主结果可定位完整 artifact chain

### P1：读模型与索引层重构

周期：第 31-60 天

目标：

- 把系统从“大脚本拼装”推进为“可维护读模型”
- 把 registry 从辅助记录推进为主索引层

必须完成：

- 拆分 `run_dashboard.py`
- 建立 `query service / view model / render` 三层
- 引入 `result_registry`
- 引入 `run_registry`
- 扩展 `artifact_registry` 元数据
- `/`、`/stock`、`/T12` 统一消费 query service

完成标志：

- `run_dashboard.py` 明显降体量并按职责分层
- `/T12` 不再读业务散文件拼细节
- registry 覆盖研究、候选、审核、观察、终局五类主工件

### P2：连续证据与发布校验补强

周期：第 61-90 天

目标：

- 把“有制度”推进成“有连续证据”
- 把发布通过推进成“发布可验证、可回放、可追责”

必须完成：

- 发布后自动生成 verification artifact
- 支持 30 天任意日主结果回放
- 固化 `20/60/120` 交易日统计
- 补齐 failure attribution ledger
- 补齐 signal drought diagnostics
- 补齐 bull / bear / range / high-vol 分环境统计

完成标志：

- 发布后有自动 verification 记录
- 任意一天主结果和证据链可回放
- failure / blocked / degraded 都有标准化记录

---

## 5. 当前阶段禁止项

未来 90 天内，以下工作默认禁止作为主任务：

- 大规模 UI 改版
- 增加新首页叙事层
- 扩张 `/T12` 为控制台
- 引入新的“总平台主工作台”概念
- 再新增一批只增命名不增硬事实的新模块
- 用新文档覆盖老问题而不改代码读路径
- 把“测试数量很多”当作“生产可靠性已完成”

如果确有必要做，必须同时满足：

- 不影响 `P0/P1/P2` 主线
- 不新增事实源混乱
- 不稀释主结果唯一性

---

## 6. 模块级改造要求

### 6.1 主结果层

以下模块属于 P0 一级改造范围：

- `src/unified_result_builder.py`
- `src/dashboard_context.py`
- `src/first_place_evidence_cockpit.py`
- `src/primary_result_candidate_handoff_gate.py`

改造要求：

- 不再直接以多份 `*_latest.json` 推断主结果
- 先取 pointer，再按 `result_id` 拉事实链
- 所有降级显示都必须显式标注 degraded 原因

### 6.2 看板层

以下模块属于 P1 一级改造范围：

- `run_dashboard.py`
- `src/dashboard_operations.py`
- `src/dashboard_reports.py`
- `src/main_site_home.py`
- `src/t12_governance_summary.py`

改造要求：

- handler 不做业务裁决
- query service 不输出 HTML
- render 层不重写制度事实
- `/stock` 只读一个 primary conclusion view model

### 6.3 registry 层

以下模块属于 P0-P1 一级改造范围：

- `src/artifact_registry.py`
- 新增 `src/result_registry.py`
- 新增 `src/run_registry.py`

最低字段要求：

- `result_id`
- `run_id`
- `artifact_id`
- `artifact_type`
- `lifecycle_stage`
- `created_at`
- `producer`
- `config_hash`
- `data_snapshot_id`
- `code_revision`
- `parent_artifact_ids`

### 6.4 发布层

以下模块属于 P2 一级改造范围：

- `scripts/run_stock_release_pipeline.py`
- `scripts/check_release_gates.py`
- `scripts/build_release_evidence_bundle.py`

改造要求：

- 发布后必须自动生成 verification artifact
- verification 至少覆盖：
  - 路由
  - primary result API
  - scope 边界
  - pointer 完整性
  - evidence 完整性

---

## 7. 每阶段 Definition of Done

### 7.1 P0 完成定义

只有同时满足以下条件，P0 才算完成：

- `/stock` 主结果来自唯一 pointer
- 页面不再以多份 `latest` 推断主事实
- 关键主结果缺证据时 fail closed
- `result_id/run_id/artifact_ids` 已贯通

### 7.2 P1 完成定义

只有同时满足以下条件，P1 才算完成：

- 看板入口完成职责拆分
- query service 已成为三层 scope 的统一读入口
- registry 已覆盖五类主工件
- `/T12` 继续保持只读治理镜像

### 7.3 P2 完成定义

只有同时满足以下条件，P2 才算完成：

- 发布后 verification artifact 自动生成
- 30 天内任意一天可回放主结果与证据链
- 样本外连续统计已固定输出
- blocked / degraded / failed 有统一记录

---

## 8. 强制验收指标

未来 90 天只盯以下 6 个指标，禁止额外发散叙事：

1. `/stock` 主结果是否只来自唯一 `current_result_pointer`
2. `latest` 是否已从主事实源降级为索引入口
3. `run_dashboard.py` 是否完成职责拆分并显著缩减单体复杂度
4. registry 是否覆盖研究、候选、审核、观察、终局五类主工件
5. 发布后是否自动生成 verification artifact
6. 30 天内是否能回放任意一天的主结果和证据链

只有这 6 项持续推进，系统才会真正升维。

---

## 9. 周节奏要求

后续推进按周执行，不允许“想到哪做到哪”。

### 每周一

- 确认本周只做一个主目标
- 明确关联到 `P0/P1/P2` 哪一项
- 明确本周禁止做什么

### 每周三

- 检查是否引入新的 `latest` 依赖
- 检查是否新增事实源绕路
- 检查是否出现 UI 层反推制度事实

### 每周五

- 提交本周验收结果
- 只回答三个问题：
  - 主事实源是否更硬了
  - 证据链是否更连续了
  - 运行与发布是否更可验证了

如果三个问题都不能回答“是”，则本周工作不算高质量推进。

---

## 10. 变更准入规则

后续任何较大开发，在编码前都必须明确写出：

- 改动属于 `P0/P1/P2` 哪一阶段
- 改动解决哪一个验收指标
- 改动是否新增或减少 `latest` 读路径
- 改动是否影响 `/stock` 主结果唯一性
- 改动是否影响 `/T12` 只读边界

无法回答以上问题的改动，不进入主分支优先级。

---

## 11. 例外处理规则

只有以下两类情况允许临时例外：

- 紧急修复生产阻断
- 紧急修复错误主结论

即使是例外修复，也必须满足：

- 事后补 pointer / registry / evidence 记录
- 不把临时兼容逻辑升级为长期事实源
- 不以“先跑通”为理由永久保留结构性脏路径

---

## 12. 最终执行结论

未来 90 天，系统开发不再以“功能更多”为成功标准，而只以下列结果为成功标准：

- 主事实源唯一
- 读路径可控
- 证据链连续
- 发布可验证
- 历史可回放

后续所有开发、评审、取舍、优先级判断，必须严格服从这条主线。
`/stock` 正式研究终端的产品面重构，必须遵守：
- [STOCK_UI_SECOND_STAGE_RECONSTRUCTION_CHECKLIST.md](./archive/2026-05-14/STOCK_UI_SECOND_STAGE_RECONSTRUCTION_CHECKLIST.md)
