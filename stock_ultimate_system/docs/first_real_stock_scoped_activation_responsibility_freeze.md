# 第一次真实 `stock-scoped` Activation 责任冻结

## 1. 文档定位

本文档用于冻结第一次真实 `stock-scoped activation` 的责任分工。

它不讨论技术实现，只回答五件事：

- 谁批准
- 谁执行
- 谁对 rollback 有最终裁决权
- 在什么时间窗口执行
- 在什么时间窗口内允许触发 rollback

本文档的目标不是补流程感，而是防止第一次真实 activation 被错误降级成“工程同学直接执行”。

---

## 2. 当前冻结结论

在本文档未被逐项填实并复核前，第一次真实 `stock-scoped activation` 继续视为未准入。

当前必须坚持：

`没有责任冻结，就没有第一次真实 activation。`

---

## 3. 责任项

以下五项必须全部明确且落文档，才允许把“治理缺口”从未满足改成已满足。

### 3.1 Activation Approver

- 角色：`/stock` 唯一正式主链系统 owner
- 责任：
  - 决定本次是否允许进入 non-dry-run `stock-scoped activation`
  - 确认准入门全部满足
  - 对“是否启动第一次真实 activation”承担最终批准责任
- 当前状态：`已冻结`

### 3.2 Activation Executor

- 角色：`当班运维 / 发布执行人`
- 责任：
  - 按已批准 plan 原样执行
  - 不允许现场改 plan
  - 负责落盘 activation execution、verification、evidence bundle、registry registration、readiness recheck
- 当前状态：`已冻结`

### 3.3 Rollback Authority Owner

- 角色：`/stock` 唯一正式主链系统 owner
- 责任：
  - 当 Step 2-6 任一步失败时，决定是否立刻 rollback
  - 禁止拖延到“先看看能不能补救”
  - 对 current pointer 是否允许保留承担最终责任
- 当前状态：`已冻结`

### 3.4 Execution Window

- 窗口：`Asia/Shanghai 非交易时段 21:30-23:00`
- 责任：
  - 明确第一次真实 activation 的执行时间段
  - 确保执行窗口内可以完成 verification 与 evidence registration
- 当前状态：`已冻结`

### 3.5 Rollback Window

- 窗口：`Asia/Shanghai 自 activation 开始起至 23:30；任一步失败立即回滚，不等待“再观察”`
- 责任：
  - 明确失败后允许 rollback 的时间段
  - 确保 rollback decision 不会因窗口不明而漂移
- 当前状态：`已冻结`

---

## 4. 当前阶段的硬规则

1. 上述五项只要有一项仍是 `待冻结`，就不得进入第一次真实 activation
2. 不允许用口头承诺代替文档冻结
3. 不允许由同一个执行人默认同时兼任 approver 与 rollback authority，除非被明确写入并承担相应责任
4. 不允许在执行窗口开始后再临时变更责任人

---

## 5. 阶段结论

当前阶段对此项的唯一合法结论是：

`第一次真实 stock-scoped activation 的责任冻结已完成，但这不等于已准入；责任冻结只是治理闭口的一半。`
