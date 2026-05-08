# STOCK PRIMARY RESULT LEGACY REMOVAL PREFLIGHT

## Purpose

本文件记录删除 legacy 路径前最后一轮预检的完成条件。

当前该预检已执行完成，文档转为留档用途。

## Required Checks

- 默认 canonical 模式下问题型 fallback 为零
- canonical-only readiness 持续通过
- content quality / layout contract / runtime metadata 持续通过
- legacy 不再承接任何默认职责
- 默认路径不存在对 legacy 输出结构的隐性依赖

## Zero-Tolerance Rule

默认 canonical 模式下，以下问题型 fallback 必须为零：

- `invalid_runtime_mode`

若出现问题型 fallback，则不得进入 legacy 删除阶段。

## Evidence Sources

预检证据至少来自以下部分：

- runtime metadata
- canonical-only readiness 测试
- legacy decommission readiness 测试
- legacy removal preflight 测试
- content quality / layout contract 测试

## Result

- removal preflight 已完成
- legacy 删除已执行
- `/stock` 已进入 canonical 单轨运行
