# `/stock` 单一正式主链阶段开发执行标准

> 阶段状态：已于 `2026-05-01` 完成并转入归档。后续开发恢复以 `docs/STRICT_CONTINUATION_EXECUTION_STANDARD.md` 为一线执行标准。

## 1. 文档定位

本文档是 `stock_ultimate_system` 已完成阶段的阶段性开发上位标准归档。

它只服务一个目标：

`把 airivo.online/stock 固化成唯一正式主链。`

在本阶段进行期间，凡涉及以下事项，统一优先执行本文档：

- `/stock` 与 `apex` 的正式身份划分
- 正式事实源收口
- 发布链与回滚链收口
- deploy baseline 固化
- post-deploy verification 固化
- 对外文档、入口、话术收口

本文档不讨论：

- 新产品叙事扩张
- 新平台命名扩张
- UI 大改版
- 与本阶段目标无关的能力横向扩展

如本文档与其他说明性文档冲突，执行优先级如下：

1. `docs/STRICT_CONTINUATION_EXECUTION_STANDARD.md`
2. `docs/STOCK_SINGLE_FORMAL_CHAIN_EXECUTION_STANDARD.md`
3. `docs/FORMAL_HOSTING_BOUNDARY_AND_ACTIVATION_BLOCKERS.md`
4. `docs/MAIN_CHAIN_PRODUCTION_ACTION_BASELINE.md`
5. `docs/PRIMARY_RESULT_FACT_SOURCE_CLASSIFICATION.md`
6. `docs/DEVELOPMENT_GUIDE.md`

---

## 2. 阶段唯一目标

本阶段唯一目标不是“扩系统版图”，而是：

`把 /stock 做成唯一正式、唯一可信、唯一可回放、唯一可验证、唯一可积累的股票研究主系统。`

本阶段冻结口径统一为：

`当前阶段唯一任务：把 airivo.online/stock 钉成唯一正式主链，把 apex 降为内部试验环境；凡不能强化这件事的开发，一律不做。`

对本阶段的任何改动，只允许问一个问题：

`它是否强化了 /stock 的唯一正式性？`

如果答案不明确，默认不进入本阶段高优先级。

---

## 3. 正式身份冻结

当前阶段正式身份冻结如下：

- `airivo.online/stock`：唯一正式股票系统
- `airivo.online/apex`：内部试验、预发布、旁路验证环境
- 其他项目：数据源、能力源、工具链，不拥有并列正式产品身份

当前阶段明确禁止：

- 把 `/apex` 写成并列正式入口
- 让 `/apex/stock` 参与对外正式产品叙事
- 用“多系统协同正式面”描述当前系统形态
- 在主导航、主文档、主介绍中并列展示 `/stock` 与 `apex`

---

## 4. 六条硬规则

### 4.1 单一正式面规则

对外只允许一个正式股票系统入口：

`airivo.online/stock`

`apex` 可以存在，但只能承担内部工程角色，不能承担正式品牌角色。

### 4.2 单一事实源规则

`/stock` 正式主链只允许消费 canonical 事实源。

禁止：

- 从多份 `latest` 拼主结论
- 从多个项目目录拼装正式事实
- 用显示层 fallback 冒充正式真相

### 4.3 单一发布链规则

正式发布、回滚、验收、deploy evidence、verification artifact，统一只围绕 `/stock` 主链建立。

禁止：

- 再复制一套 `stock-ultimate-*` 证据生成 timer
- 用 `cp` / `rsync` 定时同步 `primary_result_*_latest.json`
- 没有 verification artifact 也视为发布完成

### 4.3.1 Activation 阻断规则

在 `airivo.online` 正式托管边界未冻结前，禁止执行会覆盖整份 live nginx 或重启未确认正式归属 service 的 non-dry-run activation。

当前阶段必须同时服从：

- `docs/FORMAL_HOSTING_BOUNDARY_AND_ACTIVATION_BLOCKERS.md`
- `docs/first_real_stock_scoped_activation_admission_gate.md`

任何把 dry-run 当成“只差最后一步”的表达，都视为阶段判断错误。

### 4.4 单一责任边界规则

任何跨项目能力，只允许以下游依赖形式接入 `/stock`。

禁止：

- 让多个项目共同承担正式产品身份
- 让试验环境反向定义正式产品真相
- 让“工程方便”压过“责任清晰”

### 4.5 fail-closed 规则

凡正式主链证据不完整、事实源冲突、pointer 不唯一、artifact 缺失、发布链缺口，系统必须：

- 明确阻断
- 明确降级
- 明确要求复核

禁止“看起来还能用就先放行”。

### 4.6 验证先于叙事规则

本阶段任何“更大、更完整、更平台化”的产品叙事，都必须晚于以下事实：

- `/stock` 主链唯一
- deploy baseline 固化
- verification artifact 自动生成
- rollback 演练可执行

未满足前，禁止扩平台叙事。

---

## 5. 本阶段允许做的事

本阶段允许且鼓励的工作：

- 文档中冻结正式身份
- 收口 `/stock` 主结果事实源
- 固化 canonical artifact 根
- 固化 deploy baseline 与 systemd drop-in
- 固化 post-deploy verification
- 审计并清理误导性正式部署资产
- 强化 pointer / result / run / artifact / lifecycle traceability
- 跑 deploy / verify / rollback 演练

---

## 6. 本阶段禁止做的事

本阶段默认禁止以下事项作为主任务：

- 大规模 UI 改版
- 新增并列正式入口
- 新增“更高层总平台”叙事
- 把 `apex` 做成第二正式面
- 扩张 `/T12` 到正式主系统角色
- 用更多页面层包装掩盖主链和 deploy 问题
- 用测试数量替代正式生产纪律

如果确有必要处理边缘项，必须同时满足：

- 不改变 `/stock` 唯一正式身份
- 不新增事实源混乱
- 不削弱 deploy / rollback / verification 主链

---

## 7. 阶段完成状态

截至 `2026-05-01`，本阶段关闭判断如下：

- `airivo.online/stock` 已固化为唯一正式股票主链
- `apex` 已制度化降为内部验证 / 预发布 / 旁路环境
- `stock-scoped` formal activation 已真实执行成功
- 正式 `server_deploy_evidence_bundle`、`deployments/current.json`、`server_go_live_readiness` 已建立
- 本阶段文档链转为归档制度证据，不再作为后续继续冻结下一阶段的主线文档

后续开发回到：

- `docs/STRICT_CONTINUATION_EXECUTION_STANDARD.md`

仍需继续保持的边界：

- 不得把本阶段 `stock-scoped` 成功误写成 full-domain convergence
- 不得把 `apex` 恢复成并列正式面

---

## 8. 30 天执行主线

### 7.1 Week 1

目标：

- 冻结正式身份
- 清理对外并列正式面表达

必须完成：

- 新增或更新正式身份冻结文档
- 清理主文档、部署文档、入口文案中的并列表述

### 7.2 Week 2

目标：

- 收口事实源与读路径

必须完成：

- `/stock` 只读 canonical 根
- 主结果只来自唯一正式链
- 明确 latest 的允许边界

### 7.3 Week 3

目标：

- 固化 deploy baseline 与发布门禁

必须完成：

- canonical env 写入 deploy assets
- activation / install / rollback 全纳入同一基线
- post-deploy verification 包含 canonical env 与 `/stock` / `/apex/stock` 一致性门禁

### 7.4 Week 4

目标：

- 形成完整验收闭环

必须完成：

- 正式验证报告
- 回滚演练记录
- 单一正式主链阶段验收报告

---

## 8. 强制验收口径

本阶段只有以下条件全部成立，才允许宣告完成：

1. `airivo.online/stock` 是唯一正式股票系统入口
2. `apex` 已被明确定义为内部试验 / 预发布环境
3. `/stock` 主结论只来自唯一正式主链
4. 正式 deploy baseline 已写入脚本与文档
5. post-deploy verification 已固化 canonical env 检查
6. post-deploy verification 已固化 `/stock` 与 `/apex/stock` 一致性门禁
7. 任一正式发布都能生成 verification artifact
8. 任一正式发布都具备 rollback commands 与可复验结果

只要有一项不成立，本阶段就没有完成。

---

## 9. 提交与评审纪律

本阶段代码与文档提交必须遵守：

- 一次提交只解决一类问题
- 不允许把身份收口、页面改版、deploy 重构、无关功能混成一个提交
- 每次提交都必须能回答“它是否强化了 `/stock` 唯一正式性”

评审必须问四个问题：

1. 它是否强化了 `/stock` 的唯一正式身份
2. 它是否新增了事实源、发布链或责任边界歧义
3. 它是否可以被测试、命令或 artifact 验证
4. 它是否混入了本阶段无关扩张

任一问题回答不清，该改动不进入主分支优先级。

---

## 10. 阶段关闭条件

当且仅当以下三类证据同时存在时，本阶段才允许关闭：

- 文档证据：正式身份、deploy baseline、执行规则已冻结
- 工程证据：读路径、artifact 路径、verification 门禁已落地
- 运维证据：正式发布验证与回滚演练均可复验

在此之前，所有开发、评审、优先级判断，统一严格服从本文档。

本阶段关闭后，执行规则立即回到：

- `docs/STRICT_CONTINUATION_EXECUTION_STANDARD.md`

同时：

- `docs/STOCK_SINGLE_FORMAL_CHAIN_EXECUTION_STANDARD.md` 转为阶段归档文档
- 它只保留“本阶段如何把 /stock 收口成唯一正式主链”的制度证据作用
- 后续不得用它为“双正式面”或并列正式入口扩张提供依据
