# STOCK PRIMARY RESULT CUTOVER PLAN

## Current Stage

`/stock` canonical 主结果当前处于默认切换阶段。

- 真实运行链路继续保留 canonical 输入与 ViewModel 构造
- 默认渲染已经切到 canonical
- legacy 路径继续保留，但仅作为迁移期 fallback，不再承接新特性
- 当前重点转入运行稳定化、元信息固化与 legacy 退场准备

## Cutover Gates

默认切换到 canonical 的门槛已经满足，当前继续执行以下稳定性门禁：

- 结构门禁：canonical ViewModel 与渲染顺序稳定
- parity 门禁：旧路径与 canonical 在核心语义上保持可接受一致性
- 边界门禁：canonical 不混入 `/T12` 治理摘要，也不混入主站平台叙事
- 集成门禁：现有 `/stock` 页面基础渲染不退化
- fallback 门禁：切换后 legacy 仍可立即回退
- 运行时门禁：source / runtime_mode / fallback_reason / contract_version 稳定输出

## Allowed Differences

以下差异属于允许的 canonical 规范化：

- 缺失值从旧文案收敛为 `暂缺 / 待补充 / 降级说明`
- 历史噪声字段被白名单过滤
- 同步说明与来源时间缺失时统一落到 canonical 占位词

## Disallowed Regressions

以下差异视为不可接受退化：

- 主结论丢失或被解释层压过
- 治理摘要内容进入 `/stock`
- 主站平台叙事进入 `/stock`
- 结论、解释、边界说明顺序失稳
- `/stock` 页面出现 `/T12` 治理模块标识

## Fallback Principle

- canonical 开关必须集中控制
- 默认保留旧路径 fallback
- 若发现语义退化或运行异常，立即关闭 canonical 开关并回退旧路径

## Legacy Off-Ramp

旧路径仅在以下条件全部满足后才允许下线：

- parity 基线长期稳定
- 关键集成测试稳定通过
- canonical 已作为默认主路径运行一段稳定周期
- fallback 不再承担实际回退职责
