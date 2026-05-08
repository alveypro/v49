# STOCK_PRIMARY_RESULT_BENCHMARK_DIFF_SPEC.md

## /stock Benchmark Diff 规范

### 1. 目标

本文件定义两份 `/stock` benchmark report 之间的最小比较结构，用于持续比较、发布前核对和长期演进留档。

### 2. 比较维度

固定比较以下字段：

- `benchmark_version`
- `registry_version`
- `sample_total`
- `core_sample_total`
- `extended_sample_total`
- `blocking_total`
- `observation_total`
- `render_contract_version`
- `runtime_observability_version`
- `has_blocking_regression`

### 3. 变化分类

- `blocking_regressions`
  - `has_blocking_regression` 从 `false` 变为 `true`
  - `blocking_total` 增加
- `observation_changes`
  - `observation_total` 变化
  - 版本字段变化
  - 非阻断但需观察的计数变化
- `enhancements`
  - `sample_total` 增加
  - `core_sample_total` 增加
  - `extended_sample_total` 增加
  - 且未触发阻断级退化

### 4. 最小 diff 输出结构

- `base_benchmark_version`
- `target_benchmark_version`
- `change_total`
- `has_blocking_regression`
- `blocking_regressions`
- `observation_changes`
- `enhancements`

每个变更项最小字段：

- `field`
- `before`
- `after`
- `classification`

### 5. 阻断语义

只要 diff 中 `has_blocking_regression=true`，该版本比较结果就不能被视为安全增强。
