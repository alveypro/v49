# 第一次真实 `stock-scoped` Activation 剩余缺口清单

## 1. 文档定位

本文档是 `first_real_stock_scoped_activation_admission_gate.md` 的执行配套清单。

它不重新定义准入门，只回答三件事：

- 当前哪些准入条件已经满足
- 当前哪些准入条件仍未满足
- 还未满足的项，下一步应该补什么

这份清单的用途是把“还不能做第一次真实 activation”从原则判断推进到可执行缺口管理。

---

## 2. 当前总判断

第一次真实 `stock-scoped activation` 已经合法完成。

总体状态不是：

- `ready`
- `almost ready`
- `只差执行`

而是：

`control-ready, admission-cleared, activation-completed`

也就是：

- 控制系统已经基本收口
- 准入条件已经满足并被正式批准
- 真实 activation、evidence、registry、readiness 已全部闭合

---

## 3. 准入条件状态表

| 编号 | 准入条件 | 当前状态 | 结论 |
| --- | --- | --- | --- |
| 1 | `/stock` 保持唯一正式股票主链 | 已满足 | 保持冻结 |
| 2 | `apex` 只承担内部验证 / 预发布 / 旁路角色 | 已满足 | 保持冻结 |
| 3 | `airivo.online` formal hosting boundary 已书面冻结 | 已满足 | 对第一次真实 `stock-scoped activation` 的充分性已正式满足；full-domain 仍非本阶段目标 |
| 4 | `stock-scoped rollout` 不接管 `/` `/app/` `/auth/*` `/health` `/chat` | 已满足 | 已制度冻结 |
| 5 | `/T12/` 保持 `legacy-preserved`，不进入本次切换范围 | 已满足 | 已制度冻结 |
| 6 | activation plan 为 `scope=stock-scoped` | 已满足 | 代码与远端 dry-run 一致 |
| 7 | plan 不包含整份 live nginx 替换 | 已满足 | 已从 `stock-scoped` 计划中剔除 |
| 8 | plan 不包含 `stock-ultimate-main-site.service` 重启 | 已满足 | 已剔除 |
| 9 | plan 不包含 `stock-ultimate-t12.service` 重启 | 已满足 | 已剔除 |
| 10 | plan 不要求 `8764` / `8766` 在线 | 已满足 | 已剔除 |
| 11 | `scope_constraints` 与冻结边界一致 | 已满足 | 远端 dry-run 已验证 |
| 12 | 正式机 `stock-scoped` activation plan dry-run 通过 | 已满足 | 已有远端结果 |
| 13 | 正式机 `stock-scoped` activation execution dry-run 通过 | 已满足 | 已有远端结果 |
| 14 | 正式机 post-deploy verification 通过 | 已满足 | 已有远端结果 |
| 15 | verification 输出带 `rollout_scope=stock-scoped` 与 `rollback_hint.scope=stock-scoped` | 已满足 | 已有远端结果 |
| 16 | `/stock` 与 `/apex/stock` 一致性通过 | 已满足 | 当前仍为 `1/20`、`8/20`、`晋级锁定` |
| 17 | canonical env 检查通过 | 已满足 | 已有远端结果 |
| 18 | evidence bundle 按 `scope` 生成真实 topology | 已满足 | 本地代码已收口 |
| 19 | registry 按 `scope` 记录 snapshot 与 current pointer | 已满足 | 本地代码已收口 |
| 20 | rollback 明确为 `stock-scoped rollback` | 已满足 | 本地代码已收口 |
| 21 | readiness 无真实 snapshot 时诚实失败 | 已满足 | 本地与远端已对齐 |
| 22 | 本次真实 activation 的批准人明确 | 已满足 | 已写入责任冻结文档 |
| 23 | 本次真实 activation 的执行人明确 | 已满足 | 已写入责任冻结文档 |
| 24 | rollback 决策责任人明确 | 已满足 | 已写入责任冻结文档 |
| 25 | 执行窗口明确 | 已满足 | 已写入责任冻结文档 |
| 26 | 失败后的回滚窗口明确 | 已满足 | 已写入责任冻结文档 |

---

## 4. 当前剩余缺口

本清单对应的“第一次真实 `stock-scoped activation` 准入缺口”已清零。

当前不再存在阻断本次 activation 的剩余缺口。

需要保留的仅是后续阶段边界：

- 不得把本次 `stock-scoped` 成功改写成 full-domain formal closure
- `/`、`/app/`、`/auth/*`、`/health`、`/chat`、`/T12/` 的既有托管事实仍需继续遵守

---

## 5. 下一步只允许补的动作

后续不再需要围绕“第一次真实 activation 准入”补动作。

后续开发应回到：

- `docs/STRICT_CONTINUATION_EXECUTION_STANDARD.md`

并把本阶段文档视为归档制度证据，而不是继续阻断下一阶段的活动清单。

---

## 6. 当前阶段明确不该继续做的事

当前不该继续做：

- 把本次 `stock-scoped` 结论夸大成 full-domain ready
- 把 `apex` 再抬回并列正式面
- 回写“双正式面”叙事

因为当前阶段对应的剩余缺口已经关闭。

---

## 7. 当前阶段的唯一合法结论

当前只能写出下面这句：

`第一次真实 stock-scoped activation 的控制系统、责任冻结、ownership sufficiency、evidence、registry、readiness 已全部闭合，因此本阶段可以关闭。`

在进入下一阶段时，仍然禁止写：

- “full-domain formal convergence 已完成”
- “airivo.online 全域 activation 已打开”
- “apex 可以回到并列正式面”
