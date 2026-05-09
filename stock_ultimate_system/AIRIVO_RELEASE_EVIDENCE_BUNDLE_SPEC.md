# AIRIVO_RELEASE_EVIDENCE_BUNDLE_SPEC.md

## 发布证据包规范

### 1. 目标

每轮发布至少生成一份最小 release evidence bundle，用于留存 `/stock` benchmark 结果、release gate 执行结果和版本状态摘要。

### 2. 最小组成

证据包至少包含：

- `stock_primary_result_benchmark_report.json`
- `stock_primary_result_benchmark_report.md`
- `release_gates.json`
- `release_evidence_bundle.json`

### 3. 最小 bundle 结构

- `evidence_bundle_version`
- `benchmark_report`
- `release_gate_result`
- `blocking_status_summary`

其中：

- `benchmark_report`
  - `benchmark_version`
  - `registry_version`
  - `render_contract_version`
  - `runtime_observability_version`
- `release_gate_result`
  - `status`
  - `gate_total`
  - `failed_total`
- `blocking_status_summary`
  - `has_blocking_regression`
  - `release_gate_failed`

### 4. 目的边界

release evidence bundle 用于发布留档与比较，不承载业务判断，不改变主站、`/stock`、`/T12` 的系统边界。
