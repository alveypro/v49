# STOCK PRIMARY RESULT FALLBACK REASON SPEC

## Purpose

本文件记录 legacy 退场前使用过的 `fallback_reason` 受控语义集合。

当前 `/stock` 已进入 canonical 单轨运行，文档转为历史留档。

## Allowed Values

历史上 `fallback_reason` 允许以下值：

- `none`
- `explicit_legacy_mode`
- `shadow_mode_uses_legacy_render`
- `invalid_runtime_mode`

## Semantic Classes

### Defensive Fallback

- `explicit_legacy_mode`
  说明：人为显式切到 legacy，属于受控回退
- `shadow_mode_uses_legacy_render`
  说明：影子模式下继续走 legacy 渲染，属于迁移期受控行为

### Problem Fallback

- `invalid_runtime_mode`
  说明：运行模式配置异常，系统被迫回退到 legacy

### Canonical Default

- `none`
  说明：当前未发生 fallback，canonical 为正常主路径

## Historical Meaning

这些 reason 曾用于判断 legacy 是否可下线。

- `invalid_runtime_mode`

以下 reason 若仍被常态使用，也不应下线 legacy：

- `explicit_legacy_mode`
- `shadow_mode_uses_legacy_render`

以下条件长期满足时，可支持 legacy 退场判断：

- `fallback_reason=none` 为默认常态
- `invalid_runtime_mode` 长期为零
- `explicit_legacy_mode` 与 `shadow_mode_uses_legacy_render` 不再承担实际运行职责

## Current State

- 当前单轨运行下，`fallback_reason` 固定为 `none`
- 不再存在常驻 legacy / shadow / fallback_legacy 运行路径
