# Airivo 30 Day Execution Plan

版本：`v1.0`  
日期：`2026-04-29`  
状态：`强制执行`  
性质：`唯一 30 天主线执行计划`

## 0. 文档定位
本文件是 `AIRIVO_UNIQUE_MAINLINE_DELIVERY_STANDARD.md` 的唯一配套执行计划。

目标不是讨论，而是把未来 30 天拆成可执行任务、明确顺序、明确验收、明确禁止事项。

30 天内，不以“功能更丰富”为目标，只以以下目标为准：

- 主线唯一
- 事实唯一
- 路径唯一
- 发布唯一
- daily outputs 不断

## 1. 30 天目标
30 天结束时，必须达到：

1. `2026Qlin` 成为唯一生产主线仓
2. `/stock`、`/T12`、`/` 成为唯一正式矩阵
3. `/stock` 每日候选链不断
4. `/T12` 每日 5 票链不断
5. governance audit 不断
6. execution feedback 闭环不断
7. 发布/回滚链只认主线仓
8. `M-agent2026`、`T12Agent` 不再定义生产真相

## 2. 四阶段计划

### Phase 1: 主线冻结
时间：`D1-D3`

必须完成：

- 把唯一标准文档落仓
- 把本执行计划落仓
- 确认 `2026Qlin` 为唯一生产主线仓
- 冻结 `/`、`/stock`、`/T12` 职责
- 冻结 `T12` 五票规则
- 冻结生产策略池：`v5/v8/v9/combo`
- 冻结研究策略池：`v4/v6/v7/stable/ai`

交付物：

- `docs/AIRIVO_UNIQUE_MAINLINE_DELIVERY_STANDARD.md`
- `docs/AIRIVO_30_DAY_EXECUTION_PLAN.md`
- `docs/AIRIVO_SCOPE_AND_OWNERSHIP.md`

验收标准：

- 主线仓无歧义
- 三正式作用域无歧义
- 必保留能力清单无歧义

禁止事项：

- 新开入口
- 新开主线仓
- 在 `M-agent2026` 写新生产逻辑
- 在 `T12Agent` 写新产品定义逻辑

### Phase 2: 事实层收敛
时间：`D4-D14`

目标：统一 candidate、buylist、governance、execution 四条事实链。

必须完成：

- 在主线仓建立 `product/`、`governance/`、`platform/registry/` 骨架
- 建立 `run_registry`
- 建立 `result_registry`
- 建立 `artifact_registry`
- 建立 `audit_registry`
- 明确 `/stock` 候选池唯一事实源
- 明确 `/T12` buylist 唯一事实源
- 明确 governance audit 唯一事实源
- 明确 execution feedback 唯一事实源

迁入任务：

- 从 `M-agent2026` 迁入 candidate selection / buylist snapshot / primary result lifecycle
- 从 `T12Agent` 迁入 governance gate / task truth source / feedback 模板 / automation 规范

交付物：

- `product/stock_pipeline/`
- `product/t12_pipeline/`
- `product/primary_result/`
- `platform/registry/`
- `governance/`
- `docs/AIRIVO_TRUTH_SOURCE_MAP.md`

验收标准：

- 能回答 `/stock` 候选从哪里来
- 能回答 `/T12` 五票从哪里来
- 能回答 governance audit 从哪里来
- 能回答 execution feedback 回写到哪里
- latest 不再承担主事实职责

### Phase 3: 运行层统一
时间：`D15-D23`

目标：统一 deploy、service、health、smoke、rollback。

必须完成：

- 冻结正式服务名
- 冻结正式端口与 upstream
- 冻结 nginx 路由标准
- 冻结 systemd/launchd 标准
- 冻结 preflight / smoke / verify / rollback 标准
- 建立 daily freshness checks

交付物：

- `docs/AIRIVO_SERVICE_TOPOLOGY.md`
- `docs/AIRIVO_DEPLOY_AND_ROLLBACK_RUNBOOK.md`
- `docs/AIRIVO_DAILY_HEALTHCHECK_STANDARD.md`
- `tools/production/` 统一化脚本
- `tools/diagnostics/` 统一化脚本

验收标准：

- 发布只认主线仓
- 回滚只认主线仓
- `/`、`/stock`、`/T12` 可单独 smoke
- daily outputs freshness 可统一检查

### Phase 4: 展示层统一
时间：`D24-D30`

目标：统一平台感知，但不破坏职责分层。

必须完成：

- 导航统一
- 术语统一
- 主站职责减肥
- `/stock` 与 `/T12` 边界复检
- `/apex` 降级或明确为兼容入口

交付物：

- `docs/AIRIVO_INFORMATION_ARCHITECTURE_FINAL.md`
- `docs/AIRIVO_LABELS_AND_TERMS_STANDARD.md`
- `docs/AIRIVO_APEX_RETIREMENT_OR_NAMESPACE_PLAN.md`

验收标准：

- 用户能清楚识别三个正式作用域
- 主站不输出业务主结论
- `/stock` 不承载治理主摘要
- `/T12` 不变成控制台

## 3. 逐周计划

### Week 1
目标：冻结主线与规则

任务：

- 写入唯一标准文档
- 写入 30 天执行计划
- 冻结生产/研究策略分级
- 冻结三正式作用域
- 清单化必保留能力
- 清单化禁止事项

### Week 2
目标：冻结事实源

任务：

- 建立 registry 骨架
- 明确 candidate/buylist/governance/execution 四条主链
- 整理现有 latest 清单
- 标注哪些 latest 为索引，哪些必须退役
- 开始迁入 `M-agent2026` 与 `T12Agent` 核心能力

### Week 3
目标：冻结运行链

任务：

- 统一服务拓扑
- 统一部署脚本
- 统一健康检查
- 统一回滚 runbook
- 统一 error log 检查

### Week 4
目标：冻结产品感知

任务：

- 统一导航与术语
- 做三正式作用域验收
- 处理 `/apex` 过渡方案
- 输出最终阶段验收报告

## 4. 每周验收问题
每周结束必须能回答：

1. 现在唯一生产主线仓是不是已经固定？
2. `/stock` 每日候选是否稳定产出？
3. `/T12` 每日 5 票是否稳定产出？
4. governance audit 是否稳定刷新？
5. execution feedback 闭环是否仍然完整？
6. 本周是否又制造了第二套口径？

任意两项回答不清，本周不算完成。

## 5. 风险控制

### 5.1 高风险事项

- 在迁移过程中中断 `/stock` 候选产出
- 在迁移过程中中断 `/T12` buylist 产出
- 发布链与回滚链混乱
- 同步保留旧链又叠加新链，导致双真相
- `/apex` 继续被当作第四产品

### 5.2 风险应对

- 所有迁移先保 daily outputs，再做结构优化
- 所有新链路必须能回退到旧稳态
- 所有切换前先做 preflight
- 所有切换后立即 smoke
- 所有治理规则先冻结，再谈重构

## 6. 每日执行要求
每天推进前必须回答：

1. 今天这项工作是在减少分叉，还是增加分叉？
2. 今天这项工作是在统一事实源，还是制造第二事实源？
3. 今天这项工作是否影响 `/stock` 候选链？
4. 今天这项工作是否影响 `/T12` 五票链？
5. 今天这项工作是否有回滚路径？

答不清，不做。

## 7. 30 天结束的唯一完成标准
30 天结束，必须同时成立：

- 主线仓唯一
- 正式矩阵唯一
- `/stock` 每日候选不断
- `/T12` 每日五票不断
- governance audit 不断
- execution feedback 闭环不断
- deploy / rollback 标准化
- 历史仓不再定义生产真相

如果只是目录更整齐、页面更统一，但上述 8 条未成立，则视为失败。

## 8. 最终执行要求
本计划是主线标准的唯一执行文件。  
任何新增计划、需求、页面、脚本、迁移工作都必须说明它属于本计划哪个阶段、哪个周、哪个交付物。

不能归属的工作，不得进入主线。
