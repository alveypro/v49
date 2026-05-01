# Airivo 专业化系统升级开发蓝图

版本：`v1.0`  
日期：`2026-04-30`  
状态：`强制执行`  
性质：`专业化升级唯一开发蓝图`  
适用范围：`/Users/mac/2026Qlin` 主线仓内所有后续开发、重构、测试、发布、回滚、验收活动

## 0. 文档地位

本文件是 Airivo 从“准生产系统”升级为“专业级系统”的唯一开发蓝图。

自本文件生效起，后续与以下主题直接相关的开发，必须围绕本文件开展：

- 信号事实链
- 决策事实链
- 执行事实链
- 发布事实链
- 架构分层
- 核心表设计
- 服务边界
- CI 与自动验证
- 回滚与审计

本文件与以下文档共同构成主线约束体系：

- [MAINLINE_MANDATE.md](/Users/mac/2026Qlin/docs/MAINLINE_MANDATE.md:1)
- [AIRIVO_UNIQUE_MAINLINE_DELIVERY_STANDARD.md](/Users/mac/2026Qlin/docs/AIRIVO_UNIQUE_MAINLINE_DELIVERY_STANDARD.md:1)
- [AIRIVO_30_DAY_EXECUTION_PLAN.md](/Users/mac/2026Qlin/docs/AIRIVO_30_DAY_EXECUTION_PLAN.md:1)

裁决规则：

1. 若本文件与代码现状冲突，以本文件为后续开发方向。
2. 若本文件与上位主线文档冲突，以上位主线文档为准。
3. 未经显式修订，不得绕开本文件另起第二套“专业化方案”。

## 1. 目标定义

本阶段不追求一步到位建设“重型平台”，只追求四个专业化目标落地：

1. 同一信号可追溯到 `data_version`、`code_version`、`param_version`
2. 同一决策可追溯到放行理由、门状态、审批记录
3. 同一执行结果可追溯到订单、成交、滑点、偏差原因
4. 同一发布可追溯到验证动作、发布动作、回滚动作

只要这四个目标没有成立，系统就仍然处于“准生产”而非“专业级”。

## 2. 现状判断

当前系统的真实状态定义为：

- 已具备研究层、部分决策层、部分治理层能力
- 尚未建立完整的权威执行层
- 尚未建立统一事实链
- 尚未彻底完成 UI、服务、编排、研究逻辑的职责分离

因此，本阶段开发的首要任务不是继续堆叠页面和功能，而是：

- 先立权威事实账本
- 再补自动验证
- 再做主入口减债

## 3. 顶级设计原则

后续所有开发必须同时遵守以下原则：

1. 单一事实源
2. 事件优先于状态
3. 页面只消费事实，不定义事实
4. 编排负责流程，服务负责事实，策略负责研究
5. 所有关键输出必须可追溯、可审计、可回滚
6. 先账本，后智能
7. 删除耦合优先于增加功能

执行解释：

- 不允许页面层直接定义“最终真相”
- 不允许同一类事实分散落在多套 latest 文件、临时 JSON、页面状态里
- 不允许发布、回滚、审批只留自然语言、不留结构化记录

## 4. 本阶段范围

### 4.1 必做范围

- 建立四条权威事实链
- 建立三版本体系
- 建立关键服务边界
- 建立最小可用执行事实层
- 建立 CI 与自动验证主链
- 启动 `v49_app.py` 核心职责抽离

### 4.2 明确不做

本阶段不作为硬目标推进以下事项：

- 全面微服务化
- Kafka / Airflow / K8s 等重基础设施改造
- 多账户多券商统一交易中台
- 全量历史模块一次性重写
- 以“页面更丰富”为导向的 UI 扩张

## 5. 目标架构

后续主线架构统一为五层：

### 5.1 数据层

负责：

- 原始数据接入
- 清洗与标准化
- 快照与版本
- 数据质量检查
- 面向研究和决策的服务化读取

当前主要依托：

- `openclaw/services/market_data_service.py`
- `openclaw/services/airivo_dashboard_snapshot_service.py`

本阶段新增：

- `openclaw/services/data_version_service.py`
- `openclaw/services/data_quality_service.py`

### 5.2 研究层

负责：

- 扫描
- 回测
- 参数实验
- 实验结果冻结
- 研究产物证据化

当前主要依托：

- `backtest/`
- `openclaw/research/`
- `openclaw/adapters/v49_adapter.py`
- `openclaw/runtime/v49_handlers.py`

### 5.3 决策层

负责：

- 风险门
- 发布门
- 灰度
- 审批
- 回滚决策
- 决策审计

当前主要依托：

- `openclaw/services/airivo_batch_service.py`
- `openclaw/services/airivo_feedback_service.py`
- `openclaw/runtime/airivo_execution_center.py`

本阶段新增：

- `openclaw/services/decision_service.py`

### 5.4 执行层

负责：

- 订单事实
- 成交事实
- 执行状态机
- 滑点归因
- 未成交与偏差原因

本阶段新增：

- `openclaw/services/execution_order_service.py`
- `openclaw/services/execution_fill_service.py`
- `openclaw/services/execution_analytics_service.py`

### 5.5 治理层

负责：

- CI
- health gate
- release gate
- 审计
- 发布
- 回滚
- 验证记录

当前主要依托：

- `tools/governance_gate.py`
- `tools/openclaw_health_gate.py`
- `tools/release_gate.sh`

本阶段新增：

- `openclaw/services/release_event_service.py`
- `.github/workflows/ci.yml`

## 6. 四条权威事实链

### 6.1 信号事实链

目标：任何候选结果都能回答以下问题：

1. 何时生成
2. 哪个策略生成
3. 基于什么数据版本
4. 基于什么代码版本
5. 基于什么参数版本
6. 来自哪个父运行

权威表：

- `signal_runs`
- `signal_items`

约束：

- 扫描、回测、参数实验都必须先写 `signal_runs`
- 结果明细必须归属到某个 `run_id`
- 页面层不允许直接落地“最终候选事实”

### 6.2 决策事实链

目标：任何 approve、reject、canary、rollback 都能重放。

权威表：

- `decision_events`
- `decision_snapshot`

约束：

- 任何决策动作都必须先写事件，再更新快照
- 审批理由必须包含 `reason_codes + note`
- 风险门状态和发布门状态必须随决策一起落账

### 6.3 执行事实链

目标：任何执行结果都能追到订单、成交、滑点和偏差原因。

权威表：

- `execution_orders`
- `execution_fills`
- `execution_attribution`

约束：

- 订单和成交必须拆表
- 不允许把执行事实只写成备注型文本
- 未成交、部分成交、撤单、拒单必须结构化记录

### 6.4 发布事实链

目标：任何发布和回滚都能追到验证动作和结果。

权威表：

- `release_events`
- `release_validations`

约束：

- 任何正式发布必须写入 `release_events`
- 发布验证必须结构化记录，不允许只看终端输出
- 回滚必须成为一类正式 release event

## 7. 三版本体系

所有进入正式链路的 `run`、`decision`、`release` 必须带以下三种版本：

### 7.1 `data_version`

最小组成：

- 关键表最新交易日
- 关键表最新日期摘要
- 数据快照 hash

建议格式：

`trade_date:20260430|max_daily=20260430|max_moneyflow=20260430|db_hash=xxxx`

### 7.2 `code_version`

最小组成：

- git commit sha
- dirty flag

建议格式：

`git:abcd1234:dirty0`

### 7.3 `param_version`

最小组成：

- canonical json
- sha256 hash

建议格式：

`param:sha256:xxxx`

裁决：

- 没有三版本的结果，不得进入正式候选、正式审批、正式发布链

## 8. 核心表设计

### 8.1 `signal_runs`

最小字段：

- `run_id`
- `run_type`
- `strategy`
- `trade_date`
- `data_version`
- `code_version`
- `param_version`
- `parent_run_id`
- `status`
- `artifact_path`
- `summary_json`
- `created_at`

### 8.2 `signal_items`

最小字段：

- `run_id`
- `ts_code`
- `score`
- `rank_idx`
- `reason_codes`
- `raw_payload_json`

### 8.3 `decision_events`

最小字段：

- `decision_id`
- `decision_type`
- `based_on_run_id`
- `risk_gate_state`
- `release_gate_state`
- `approval_reason_codes`
- `approval_note`
- `operator_name`
- `decision_payload_json`
- `created_at`

### 8.4 `decision_snapshot`

最小字段：

- `decision_id`
- `decision_status`
- `effective_trade_date`
- `selected_count`
- `active_flag`
- `updated_at`

### 8.5 `execution_orders`

最小字段：

- `order_id`
- `decision_id`
- `ts_code`
- `side`
- `target_qty`
- `decision_price`
- `submitted_price`
- `submitted_at`
- `status`
- `cancel_reason`
- `broker_ref`
- `source_type`

### 8.6 `execution_fills`

最小字段：

- `fill_id`
- `order_id`
- `fill_price`
- `fill_qty`
- `fill_time`
- `fill_fee`
- `fill_slippage_bp`
- `venue`

### 8.7 `execution_attribution`

最小字段：

- `order_id`
- `decision_price`
- `submit_price`
- `avg_fill_price`
- `close_price`
- `delay_sec`
- `fill_ratio`
- `slippage_bp`
- `miss_reason_code`
- `updated_at`

### 8.8 `release_events`

最小字段：

- `release_id`
- `release_type`
- `code_version`
- `config_version`
- `operator_name`
- `gate_result`
- `payload_json`
- `created_at`

### 8.9 `release_validations`

最小字段：

- `release_id`
- `validation_type`
- `validation_status`
- `validation_output_path`
- `created_at`

## 9. ID 与状态规范

### 9.1 ID 规范

- `run_id`: `run_{type}_{strategy}_{yyyymmdd}_{hhmmss}_{shortuuid}`
- `decision_id`: `dec_{yyyymmdd}_{hhmmss}_{shortuuid}`
- `order_id`: `ord_{yyyymmdd}_{hhmmss}_{shortuuid}`
- `fill_id`: `fill_{yyyymmdd}_{hhmmss}_{shortuuid}`
- `release_id`: `rel_{yyyymmdd}_{hhmmss}_{shortuuid}`

### 9.2 reason code 规范

本阶段先冻结以下标准值：

- `risk_pass`
- `risk_warn_override`
- `freshness_warn_override`
- `canary_only`
- `execution_quality_low`
- `signal_consensus_weak`
- `rollback_after_validation`
- `manual_override`
- `capacity_limit_hit`
- `data_quality_warn`

### 9.3 执行状态机规范

统一使用以下状态：

- `created`
- `submitted`
- `partial_fill`
- `filled`
- `cancelled`
- `rejected`
- `expired`
- `manual_override`

裁决：

- 不允许页面、脚本、服务私自发明同义状态名

## 10. 目录落点方案

本阶段新增目录与文件建议冻结如下：

- `openclaw/services/lineage_service.py`
- `openclaw/services/data_version_service.py`
- `openclaw/services/data_quality_service.py`
- `openclaw/services/decision_service.py`
- `openclaw/services/execution_order_service.py`
- `openclaw/services/execution_fill_service.py`
- `openclaw/services/execution_analytics_service.py`
- `openclaw/services/release_event_service.py`
- `openclaw/orchestration/signal_run_orchestrator.py`
- `openclaw/orchestration/decision_orchestrator.py`
- `openclaw/orchestration/release_orchestrator.py`
- `scripts/migrations/001_lineage.sql`
- `scripts/migrations/002_decision.sql`
- `scripts/migrations/003_execution.sql`
- `scripts/migrations/004_release.sql`
- `.github/workflows/ci.yml`

## 11. 文件级改造优先级

### 11.1 第一优先级

- `v49_app.py`
- `openclaw/runtime/airivo_execution_center.py`
- `openclaw/adapters/v49_adapter.py`
- `openclaw/runtime/v49_handlers.py`
- `openclaw/services/airivo_batch_service.py`
- `tools/release_gate.sh`

### 11.2 第二优先级

- `openclaw/services/airivo_feedback_service.py`
- `openclaw/services/airivo_dashboard_snapshot_service.py`
- `tools/openclaw_health_gate.py`
- `backtest/engine.py`

### 11.3 改造原则

- 页面保留展示职责
- service 负责读写事实
- orchestration 负责流程编排
- adapter 负责对旧系统做稳定接口封装

## 12. 90 天实施计划

### Phase 1: D1-D30 立账本

目标：

- 建立 `signal`、`decision`、`release` 三类主表
- 给扫描、回测、审批、发布统一补 ID 与三版本

必须完成：

- 落地四类主表中的前三类核心表
- 在 `V49Adapter` 和 `v49_handlers` 中补 `run_id`
- 在执行中心中补 `decision_id`
- 在发布门中补 `release_id`

验收标准：

- 任一候选结果可追溯三版本
- 任一审批动作有结构化记录
- 任一发布动作有验证记录

### Phase 2: D31-D60 补执行闭环

目标：

- 建立执行事实层
- 建立执行偏差归因层

必须完成：

- 落地 `execution_orders`
- 落地 `execution_fills`
- 落地 `execution_attribution`
- 统一执行状态机
- 把执行质量纳入日报与 gate 证据

验收标准：

- 任一执行结果可追到订单和成交
- 任一未成交有结构化原因
- 滑点和成交率可被统计

### Phase 3: D61-D90 固化验证与减债

目标：

- 建立 CI 主链
- 抽离主入口核心职责
- 固化发布与回滚证据

必须完成：

- 新增 CI workflow
- 自动跑关键 `pytest`
- 自动跑 `governance gate`
- 自动跑最小 smoke / health checks
- 从 `v49_app.py` 抽离至少 30% 核心逻辑

验收标准：

- PR 有自动验证
- 主入口职责显著下降
- 四条事实链连通

## 13. CI 与验证标准

本阶段至少建立一个正式 `ci.yml`，覆盖以下验证：

- `pytest`
- `tools/governance_gate.py`
- 核心 import smoke
- 关键 schema / migration smoke

建议分层：

- `fast checks`
- `core checks`
- `ops checks`

裁决：

- 没有自动验证的主线改动，不应被视为稳定改动

## 14. 禁止事项

后续围绕本蓝图开发时，明确禁止：

- 在页面层直接定义主事实
- 继续把关键能力堆进 `v49_app.py`
- 再引入第二套审批口径
- 再引入第二套执行状态命名
- 再用 latest 文件承担唯一主事实源
- 以临时脚本替代正式审计链
- 为追求速度跳过版本链和事件链

## 15. 开发前必答问题

任何后续开发开始前，必须回答：

1. 这次改动属于哪一条事实链？
2. 这次改动是否强化了单一事实源？
3. 这次改动是落在 UI、service、orchestration、adapter 的哪一层？
4. 这次改动是否补强了追溯、审计、验证、回滚中的至少一项？
5. 这次改动会不会继续增加 `v49_app.py` 的职责？

任意两项回答不清，不应进入主线开发。

## 16. 本蓝图的后续使用方式

从本文件生效起，后续开发应统一采用以下工作方式：

1. 先引用本蓝图确定开发所属阶段与所属事实链
2. 再产出对应模块的细化设计
3. 再按文件级优先级落地开发
4. 再按本蓝图验收标准验收

后续若需新增细化文档，必须注明：

- 归属本蓝图哪一章
- 是否新增事实表、状态、reason code、service 边界
- 是否改变现有验收标准

未经批准，不得用新增细化文档替代本蓝图。

## 17. 最终裁决

本蓝图冻结的不是“某个理想平台”，而是 Airivo 从当前代码现实出发、走向专业级系统的唯一可执行路径。

后续开发优先级必须统一收敛到四件事：

- 信号可追溯
- 决策可追溯
- 执行可追溯
- 发布可追溯

凡是不直接服务这四件事、却继续增加复杂度的开发，原则上都视为偏航。
