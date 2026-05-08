# STOCK PRIMARY RESULT RUNTIME OBSERVABILITY SPEC

## Purpose

本文件定义 `/stock` canonical 单轨运行下的最小运行观测面。

## Required Runtime Metadata

以下字段必须稳定存在：

- `stock_primary_result_source`
- `stock_primary_result_runtime_mode`
- `stock_primary_result_fallback_reason`
- `stock_primary_result_has_problem_fallback`
- `stock_primary_result_is_canonical`
- `stock_primary_result_render_contract_version`

## Stable Values In Single-Track Mode

单轨 canonical 运行下，期望值为：

- `stock_primary_result_source = canonical`
- `stock_primary_result_runtime_mode = canonical`
- `stock_primary_result_fallback_reason = none`
- `stock_primary_result_has_problem_fallback = false`
- `stock_primary_result_is_canonical = true`

## Blocking Release Conditions

以下情况属于必须阻断发布的问题：

- runtime metadata 字段缺失
- `fallback_reason != none`
- `stock_primary_result_has_problem_fallback = true`
- `stock_primary_result_runtime_mode != canonical`
- canonical 内容质量或布局契约失稳

## Stability Signals

可认为单轨运行仍稳定的信号：

- runtime metadata 长期稳定为 canonical 单轨值
- canonical-only readiness 持续通过
- layout contract / content quality 持续通过
- `/stock` 仍不混入 `/T12` 治理摘要与主站叙事
