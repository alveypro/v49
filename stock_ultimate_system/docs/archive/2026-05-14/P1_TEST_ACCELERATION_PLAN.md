# P1 测试提速计划

## 1. 文档定位

本文档不是 `P1` 功能计划，而是 `P1` 的前置工程计划。

唯一目标：

`在进入 P1 主改造前，先把 release pipeline 相关测试从“可验证但过慢”降到“可持续验证”。`

---

## 2. 当前问题

当前已观测到：

- 旧的 `test_run_stock_release_pipeline.py` 全文件回归耗时约 `930s`，现已拆分为 `fast / integration / e2e`
- 即使只跑 2 条端到端测试，也需要约 `150s`

这说明当前问题不是单条断言，而是：

- 测试职责耦合过多
- CLI 级 subprocess 覆盖过宽
- benchmark/report/gate/pipeline/evidence bundle 多层重复验证

如果不先处理，`P1` 的任何结构性改造都会被验证时间放大。

---

## 3. 提速原则

只遵守三条：

1. 不降低关键发布链覆盖
2. 优先拆职责，不优先做 mock 污染
3. 保留少量真实端到端，用更多函数级/复用级测试替代全链 CLI 重跑

---

## 4. 拆分策略

## A. 保留 2 条真实端到端 CLI 测试

只保留：

1. 默认发布入口缺 active pointer 时失败
2. 提供合格 `release_gates.json` 时可通过

作用：

- 验证真实 CLI 行为
- 验证发布入口默认严格

其余行为尽量下沉到函数级测试。

## B. 把 pipeline 复用逻辑留在函数级

继续使用：

- [test_release_pipeline_reuses_gate_results.py](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/tests/test_release_pipeline_reuses_gate_results.py:1)

负责覆盖：

- executed gate
- existing gate reuse
- active pointer policy mismatch
- blocking gate rejection

这类测试不应反复通过 CLI 完整重跑 benchmark/report。

## C. 把 evidence bundle 独立验证

继续使用：

- [test_release_evidence_bundle.py](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/tests/test_release_evidence_bundle.py:1)

负责验证：

- gate_policy 写入
- pointer_integrity 摘要写入
- previous stable release 与 rollback readiness 摘要

这类测试不应依赖完整 release pipeline。

## D. 把 benchmark/report 结果与 pipeline 拼装解耦

对 benchmark 自身行为，优先依赖：

- benchmark/report 专项测试

不要让 pipeline 测试再次承担 benchmark 正确性主验证。

pipeline 只需要验证：

- 是否正确消费 benchmark 产物
- 是否正确把状态写入 summary/manifest/bundle

---

## 5. 推荐测试分层

### Layer 1: Fast unit / function

目标时间：

- 单文件 `1-5s`

范围：

- release gate policy
- existing gate reuse
- evidence bundle mapping
- manifest hash wiring

### Layer 2: Medium integration

目标时间：

- 单文件 `5-20s`

范围：

- `build_stock_release_pipeline_summary(...)`
- artifact registry registration
- baseline promotion wiring

### Layer 3: Slow E2E CLI

目标时间：

- 总数 `<= 2`
- 总耗时尽量压到 `<= 180s`

范围：

- CLI 缺 active pointer 失败
- CLI 在合格 gate json 下通过

---

## 6. 具体改造项

## T1. 标注 slow test 角色

给以下测试明确角色说明：

- `test_run_stock_release_pipeline_requires_active_pointer_by_default`
- `test_run_stock_release_pipeline_accepts_existing_release_gates_with_active_pointer`

目的：

- 让后续维护者知道这两条是保留的真实 CLI 测试

## T2. 把其余 CLI 测试尽量下沉到函数级

优先迁移：

- stable summary
- previous stable reference
- baseline promotion metadata
- blocking diff summary

这些不必全部通过 CLI 再验证一次。

## T3. 给 release pipeline 测试增加分组标签

建议分成：

- `release_pipeline_fast`
- `release_pipeline_integration`
- `release_pipeline_e2e`

目的：

- 日常开发默认跑 fast/integration
- 发布前再跑 e2e

## T4. 记录目标耗时

验收目标：

- `test_run_stock_release_pipeline_fast.py`、`test_run_stock_release_pipeline_functional.py`、`test_run_stock_release_pipeline_integration.py`、`test_run_stock_release_pipeline_e2e.py` 分层承担原有职责
- 端到端慢测总时长下降到当前的一半以下
- 日常回归不依赖 15 分钟单文件测试

---

## 7. 不建议做的事

- 不要把所有慢测试都删掉
- 不要用过度 mock 替代发布链真实行为
- 不要把 benchmark 正确性塞回 pipeline 测试
- 不要在 `P1` 中边拆 dashboard 边顺手修测试速度

测试提速必须先独立完成。

---

## 8. 完成标准

只有同时满足以下条件，才算完成 `P1` 前置测试提速：

1. 保留 2 条真实 CLI 发布路径测试
2. 其余 pipeline 行为主要由函数级或中速集成测试覆盖
3. release pipeline 不再由单个 15 分钟级测试文件承载
4. 日常开发能用一组 `fast + integration` 在合理时间内完成验证

未满足前，不建议正式启动大规模 `P1` 读模型拆分。
