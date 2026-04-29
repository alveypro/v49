# Airivo 唯一主线落地标准

版本：`v1.0`  
日期：`2026-04-29`  
状态：`强制执行`  
性质：`唯一开发与落地标准`  
适用范围：`2026Qlin`、`M-agent2026`、`T12Agent`、`airivo.online`、`airivo.online/stock`、`airivo.online/T12`

## 0. 文档地位
本文件自生效起，成为 Airivo 股票相关系统的唯一主线标准。

所有开发人员、运维人员、产品人员、协作代理必须遵守本文件。  
凡与本文件冲突的旧文档、旧习惯、口头约定、临时脚本、一时判断，全部失效。

本文件不是讨论稿，不是建议稿，不是参考稿。  
它是后续：

- 开发
- 重构
- 迁移
- 部署
- 发布
- 回滚
- 验收
- 排障

的唯一标准。

## 1. 最终裁决

### 1.1 唯一平台
对外唯一平台名称：`Airivo`

### 1.2 唯一生产主线仓
唯一生产主线仓：`/Users/mac/2026Qlin`

裁决说明：

- `2026Qlin` 是唯一运行主线
- `M-agent2026` 不再是独立生产主线
- `T12Agent` 不再是独立产品线
- 两者只允许作为迁移来源与历史参考，直到退役

### 1.3 唯一正式产品矩阵
正式产品只保留三个作用域：

- `airivo.online/`
- `airivo.online/stock/`
- `airivo.online/T12/`

### 1.4 `/apex` 裁决
`/apex` 不是第四系统。  
只允许作为：

- 技术 namespace
- 兼容入口
- 内部验证入口

不得继续作为独立产品身份对外叙事。

## 2. 顶级原则
后续所有开发必须同时满足以下原则：

1. 单一主线
2. 单一事实源
3. 治理优先
4. 收敛优先
5. 删除优先于叠加
6. 运行稳定优先于功能丰富
7. 页面只消费事实，不定义事实
8. 所有关键结果必须可追溯、可回滚、可审计

任何改动如果不能加强以上至少两项，原则上不得进入主线。

## 3. 三个正式作用域的唯一职责

### 3.1 主站 `/`
主站只负责：

- 平台身份
- 方法论表达
- 入口分发
- 平台级信任说明

主站禁止：

- 输出股票主结果
- 输出治理主摘要
- 承担交易或治理控制台职责

### 3.2 `/stock`
`/stock` 是唯一业务主结果系统，负责：

- 每日候选池
- 候选排序
- 主结果
- 解释链
- 候选生命周期
- primary result 生命周期

`/stock` 禁止：

- 承担平台母层叙事
- 承担治理制度主解释
- 直接承载 T12 治理系统本体

### 3.3 `/T12`
`/T12` 是唯一治理只读系统，负责：

- 每日 buylist
- 治理摘要
- 治理阻断原因
- 审计状态
- 执行反馈只读追踪

`/T12` 禁止：

- 输出业务主判断
- 承担控制台
- 反向定义 `/stock` 主结果
- 生成独立于 `/stock` 的第二候选池

## 4. 必保留能力

### 4.1 2026Qlin 策略能力
以下能力必须保留：

生产策略池：

- `v5`
- `v8`
- `v9`
- `combo`

研究策略池：

- `v4`
- `v6`
- `v7`
- `stable`
- `ai`

裁决：

- 生产策略池保留生产资格
- 研究策略池保留研究能力，但不进入默认生产

### 4.2 `/stock` 每日产出
以下产出必须保留：

- `candidates_top_latest.*`
- `candidates_basket_*`
- `daily_research_latest.*`
- `primary_result_*`

### 4.3 `/T12` 每日产出
以下产出必须保留：

- `buylist_latest.*`
- `governance_audit_latest.*`
- `t1_execution_checklist_latest.*`
- `t12_alert_center_latest.*`
- `execution_feedback_template_*`

### 4.4 T12 五票规则
正式冻结：

- `T12` 每日 buylist 目标数量固定为 `5`

说明：

- 这是制度契约，不是普通参数
- 修改必须单独审批
- 不得随开发顺手更改

### 4.5 执行反馈闭环
以下能力必须保留：

- `overnight_recommendations`
- `overnight_execution_feedback`
- `overnight_realized_outcomes`
- `record_execution_feedback.py`
- execution feedback template

### 4.6 trading kernel
`trading_kernel` 必须保留，并升级为唯一执行治理内核。

## 5. 三层漏斗唯一模型
股票系统未来统一后的唯一业务链如下：

1. `/stock` 候选池
2. `/T12` buylist 5 票
3. `overnight execution shortlist`

解释：

- `/stock` 负责广口径候选
- `/T12` 负责治理后短名单
- overnight/execution 负责更窄的执行建议

三者必须相连，但绝不允许混同。

## 6. 唯一事实源标准

### 6.1 总规则
所有页面、脚本、服务都不得再使用多个 latest 文件拼装一个主结论。

`latest` 只能作为索引，不得作为主事实源。

### 6.2 必须建立并长期使用的 registry
主线必须维护以下五类唯一 registry：

- `run_registry`
- `result_registry`
- `artifact_registry`
- `audit_registry`
- `release_registry`

### 6.3 各类事实的唯一归属
冻结如下：

| 事实类型 | 唯一事实源 |
|---|---|
| 当前生产主策略 | strategy registry + governance decision |
| 每日候选池 | `/stock` result record |
| 每日 T12 buylist | `T12 buylist result` |
| 当前主结果 | primary result current pointer |
| 执行反馈 | `overnight_execution_feedback` / trading kernel |
| 治理状态 | governance audit result |
| 发布状态 | release registry |

### 6.4 严禁事项
严禁：

- 页面层补事实
- 模板层推断治理
- 用多个 latest 拼主结论
- T12 回写 stock 主结果
- stock 覆盖 governance 裁决

## 7. 唯一目录标准
未来主线仓必须按如下结构收敛：

```text
2026Qlin/
├── apps/
│   ├── main_site/
│   ├── stock/
│   └── t12/
├── domains/
│   ├── strategy_center/
│   ├── candidate_engine/
│   ├── governance_center/
│   ├── execution_feedback/
│   ├── primary_result/
│   └── trading_kernel/
├── platform/
│   ├── registry/
│   ├── release/
│   ├── observability/
│   ├── config/
│   └── auth/
├── tools/
│   ├── production/
│   ├── migration/
│   ├── diagnostics/
│   └── legacy/
├── docs/
├── deploy/
├── runtime/
│   ├── run/
│   ├── logs/
│   ├── exports/
│   └── tmp/
└── archive/
```

### 7.1 根目录长期允许保留
根目录只允许保留：

- `README.md`
- `requirements.txt`
- `pytest.ini`
- `.env.example`
- `.gitignore`
- 主入口 shim
- 必要启动脚本
- 必要仓级配置

### 7.2 根目录禁止继续增长
以下内容不得继续长期放在根目录：

- 历史策略主程序
- 一次性修复脚本
- 回测 CSV
- 数据库文件
- token 文件
- 日志
- 临时导出
- 散落工具脚本

## 8. 唯一服务标准
未来线上与本地主线都必须围绕三正式作用域运行：

- `main_site`
- `stock`
- `t12`

所有部署、健康检查、smoke test、回滚操作，都必须围绕这三者设计。

### 8.1 必须有的运行标准
每个作用域都必须明确：

- 服务名
- 本地端口
- upstream
- 健康检查 URL
- 错误日志路径
- 回滚命令

### 8.2 不允许的做法
- 不允许同一职责存在两套服务口径
- 不允许通过脚本约定代替正式服务定义
- 不允许“先改线上再补文档”

## 9. 唯一发布标准
所有生产发布必须遵守：

1. preflight
2. gate
3. deploy
4. smoke
5. verify
6. rollback-ready

### 9.1 发布前必须检查
- 主线仓状态
- 关键产物 freshness
- registry 一致性
- `/stock` 候选链可用
- `/T12` buylist 链可用
- governance audit 可用
- execution feedback 闭环未断

### 9.2 发布后必须验证
- 主站可用
- `/stock` 可用
- `/T12` 可用
- 错误日志无异常激增
- 关键 latest 索引已刷新
- pointer 未断裂

### 9.3 回滚要求
所有发布都必须有：

- 明确回滚步骤
- 明确回滚对象
- 明确回滚验证方法

没有回滚方案的改动，不得上线。

## 10. 唯一健康检查标准
每天必须统一检查：

1. `/stock` 候选池是否生成
2. `/T12` 5 票 buylist 是否生成
3. `governance_audit_latest` 是否刷新
4. execution feedback template 是否生成
5. current pointer 是否一致
6. 关键页面是否可用
7. 上次运行结果是否过期

系统必须 fail closed：

- 候选池异常时，不允许假装有主结果
- buylist 异常时，不允许假装有可执行名单
- 治理未刷新时，不允许显示正常推进状态

## 11. 唯一文档标准
文档体系必须分层：

### 11.1 Mandate
最高约束文件：

- 主线总纲
- 本唯一标准

### 11.2 Architecture
架构文件：

- 能力保留与迁移矩阵
- 事实源设计
- 目录结构标准
- 服务边界标准

### 11.3 Execution
执行文件：

- 30 天推进计划
- 发布 runbook
- 回滚 runbook
- daily checklist

### 11.4 Reference
参考文件：

- 历史文档
- 迁移记录
- 归档说明

任何新文档必须先判断属于哪一层。  
不允许每个文档都假装是“总文档”。

## 12. 唯一开发规则
从本文件生效起，每一项开发前必须回答六个问题：

1. 这次改动属于 `/`、`/stock`、还是 `/T12`？
2. 这次改动写入哪个唯一事实源？
3. 这次改动会不会制造第二口径？
4. 这次改动会不会破坏 `T12` 五票规则？
5. 这次改动是否可回滚、可审计？
6. 这次改动是否减少了复杂度而不是增加了复杂度？

如果其中两项答不清，不得入主线。

## 13. 唯一禁止清单
以下行为一律禁止：

- 新开第四产品线
- 新开第二生产主线仓
- 让 `M-agent2026` 继续定义生产真相
- 让 `T12Agent` 继续定义独立产品真相
- 继续把 `/apex` 讲成第四产品
- 继续让多个 latest 拼一个主结果
- 在根目录堆数据库、日志、CSV、token
- 在大文件上继续叠新职责
- 未经裁决直接改动 T12 五票规则
- 页面层自行判断治理结论

## 14. 唯一迁移标准

### 14.1 `M-agent2026` 的处理
必须迁入的能力：

- candidate selection
- buylist snapshot
- primary result lifecycle
- result/pointer/registry 设计
- `/stock` 与 `/T12` 边界规范
- release gates

迁入完成后：

- `M-agent2026` 退为 reference repo
- 不再承载生产主线职责

### 14.2 `T12Agent` 的处理
必须迁入的能力：

- governance gate
- task truth source
- execution feedback 模板
- T12 automation 规范
- 治理健康检查与制度资产

迁入完成后：

- `T12Agent` 退为 governance reference repo
- 不再承载独立产品身份

## 15. 唯一完成标准
主线收敛与优化完成，必须同时满足：

- 只有一个生产主线仓
- 只有一个正式产品矩阵
- `/stock` 每日候选不断
- `/T12` 每日五票不断
- `v5/v8/v9/combo` 生产能力持续可用
- governance audit 持续可追踪
- execution feedback 持续闭环
- 发布与回滚标准化
- 页面职责无歧义
- 历史仓不再定义生产真相

## 16. 执行要求
本文件应立即落地到主线仓内。  
从该文件创建起：

- 所有开发 PR
- 所有上线变更
- 所有迁移工作
- 所有验收判断

都必须引用本标准。

## 17. 最终裁决
Airivo 后续不再允许“多项目平行生长”。  
只允许：

- 一个主线仓
- 一个主平台
- 一个候选主链
- 一个 buylist 主链
- 一个治理裁决链
- 一个执行反馈闭环
- 一套发布回滚标准

这不是为了好看，而是为了让系统停止失控，开始成为真正能长期演进的股票平台。
