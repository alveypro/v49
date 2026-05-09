# STOCK PRIMARY RESULT BENCHMARK REGISTRY

## Purpose

本文件为 `/stock` benchmark / golden set 提供版本化 registry。

## Registry

### Core Samples

- `B001_NORMAL`
  - 类别：正常态
  - 核心约束：结论层清晰、解释层受控、边界层轻量、无污染项
  - 阻断级退化：主结论失稳、顺序失稳

- `B002_EMPTY`
  - 类别：空态
  - 核心约束：占位词受控、边界层不膨胀
  - 阻断级退化：出现自由文本空态

- `B003_DEGRADED`
  - 类别：降级态
  - 核心约束：统一落到 `降级说明`
  - 阻断级退化：降级文案漂移

- `B004_DISABLED_INVALID`
  - 类别：禁用/失效态
  - 核心约束：禁用解释和失效解释稳定存在
  - 阻断级退化：失效术语漂移

### Extended Samples

- `B101_NOISY_INPUT`
  - 类别：噪声字段
  - 核心约束：噪声字段不污染主结果

- `B102_GOVERNANCE_POLLUTED`
  - 类别：治理污染
  - 核心约束：治理摘要词汇不进入 `/stock`

- `B103_MAIN_SITE_POLLUTED`
  - 类别：主站叙事污染
  - 核心约束：主站品牌叙事不进入 `/stock`

## Gate Levels

- 阻断级：B001, B002, B003, B004
- 观察级：B101, B102, B103

## Test Mapping

当前 registry 与 `tests/test_stock_primary_result_benchmarks.py` 一一对应。
