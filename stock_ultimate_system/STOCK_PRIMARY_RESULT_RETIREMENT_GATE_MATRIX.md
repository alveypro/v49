# STOCK PRIMARY RESULT RETIREMENT GATE MATRIX

## Purpose

本矩阵记录 legacy 删除前用于判断是否可退场的门禁条件。

当前 legacy 删除已执行，文档转为历史留档。

判断维度：

- `fallback_reason`
- 是否允许出现
- 是否阻断 legacy 下线
- 是否需要修复

## Matrix

| fallback_reason | 默认 canonical 模式允许出现 | 仅 legacy/shadow 模式允许 | 是否阻断 legacy 下线 | 是否需要修复 |
| --- | --- | --- | --- | --- |
| `none` | 是 | 是 | 否 | 否 |
| `explicit_legacy_mode` | 否 | 是 | 是 | 否 |
| `shadow_mode_uses_legacy_render` | 否 | 是 | 是 | 否 |
| `invalid_runtime_mode` | 否 | 否 | 是 | 是 |

## Canonical Default Rules

默认 canonical 模式下，以下 reason 绝不应出现：

- `explicit_legacy_mode`
- `shadow_mode_uses_legacy_render`
- `invalid_runtime_mode`

默认 canonical 模式下，期望值应为：

- `fallback_reason = none`

## Historical Meaning

legacy 退场前的直接信号：

- 默认 canonical 模式长期只出现 `none`
- `explicit_legacy_mode` 不再承担实际运行职责
- `shadow_mode_uses_legacy_render` 不再承担迁移职责
- `invalid_runtime_mode` 长期为零

legacy 退场前的阻断信号：

- 默认 canonical 模式仍出现任何非 `none` reason
- `invalid_runtime_mode` 仍有发生
- 仍需要显式切回 legacy 或 shadow 来维持稳定
