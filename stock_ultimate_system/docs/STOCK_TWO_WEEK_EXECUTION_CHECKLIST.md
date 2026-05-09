# /stock 接下来两周执行清单

## 1. 文档定位

本文档是 `airivo.online/stock` 在完成：

- 单一正式主链收口
- 主链真实性硬化
- 第二阶段首页结构重排第一批收口

之后，进入接下来两周执行期的正式清单。

它不再讨论方向，不再重复愿景，不再重新发明阶段目标，只回答三件事：

- 接下来两周哪些事必须改
- 哪些事明确禁止做
- 每天用什么口径验收，避免开发重新漂回工程后台或伪进展

执行优先级：

1. [STRICT_CONTINUATION_EXECUTION_STANDARD.md](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/docs/STRICT_CONTINUATION_EXECUTION_STANDARD.md:1)
2. [INDUSTRY_FIRST_CANDIDATE_SYSTEM_EXECUTION_STANDARD.md](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/docs/INDUSTRY_FIRST_CANDIDATE_SYSTEM_EXECUTION_STANDARD.md:1)
3. 本文档
4. [STOCK_UI_SECOND_STAGE_RECONSTRUCTION_CHECKLIST.md](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/docs/STOCK_UI_SECOND_STAGE_RECONSTRUCTION_CHECKLIST.md:1)
5. [CANDIDATE_QUALITY_EVALUATION_AND_BENCHMARK_SUITE.md](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/docs/CANDIDATE_QUALITY_EVALUATION_AND_BENCHMARK_SUITE.md:1)
6. 其他说明、口头判断、临时聊天结论

---

## 2. 必改项

### 2.1 折叠内部复核面完成产品化

#### 必须完成

- 把折叠区首层固定成“研究复核视角”，而不是后台任务视角
- 不再让 `partial_success / quick / 0 行 / 耗时 135.0s` 这类字段占据首要阅读位置
- 先展示：
  - 当前链路是否可复核
  - 待补齐环节
  - 当前为什么不能推进
  - 优先查看哪份复核材料

#### 完成标志

- 用户展开折叠区后，第一眼看到的是复核判断，不是任务控制台
- 内部复核区仍保留真实字段，但必须下沉到第二层
- `/stock` 首页不再需要用户自行把后台字段翻译成研究判断

### 2.2 首屏证据卡继续压缩阅读成本

#### 必须完成

- 继续强化快读层
- 把长句证据拆成：
  - 快读事实
  - 展开说明
- 快读层只保留：
  - 当前状态
  - 健康分
  - 候选生成时间
  - 数据日期
  - 样本进度

#### 完成标志

- 用户 3 秒内能抓住“现在处于什么状态、为什么还不能推进”
- 首屏证据卡不再用窄列长句承担主要信息
- 不再需要滚动和扫长句才能知道核心证据

### 2.3 当前对象卡继续去误导化

#### 必须完成

- 在候选未形成阶段，当前对象只能表达为：
  - `当前研究对象`
  - 或等价的正式口径
- 不能继续给出接近“主推荐位”的视觉暗示

#### 完成标志

- 候选未形成时，任何视觉权重都不能提前放大成“已确认候选”
- 当前对象卡和正式候选卡之间的产品语义差异必须清晰

### 2.4 failure priority 继续进入日常执行

#### 必须完成

- daily planner / morning brief 必须继续使用：
  - `open_owner_workloads`
  - `execution_batch`
  - `iteration_schedule`
- 不允许 priority 只存在于 artifact 或 diff 文件里

#### 完成标志

- 当天要先修什么、先跑哪一批、先看哪类失败，能从日常执行入口直接读出
- benchmark / review / candidate iteration 的执行顺序继续受 priority 驱动

### 2.5 120d 长窗样本继续真实增厚

#### 必须完成

- 每日 formal validation history 按正式链自动归档
- `candidate_quality_density_progress` 每日更新
- `120d sample_total / remaining_samples_needed / progress_ratio` 继续进入 planner 和 morning brief

#### 完成标志

- 长窗密度不再只停留在“知道缺口”，而是有连续真实推进记录
- 同一交易日重复跑批不能虚增样本数
- 后续任何“长期可信”叙事都必须建立在这条真实进度链上

---

## 3. 禁做项

### 3.1 禁止重新扩结构当作主要进展

- 不再以“新增一个模块/卡片/入口”证明进展
- 不再把同层 provenance 护栏继续无限扩张当成主线
- 不再把首页继续做成信息更厚、卡片更多的综合控制台

### 3.2 禁止让 apex 重新占用 /stock 正式主链注意力

- `apex` 继续只是内部验证环境
- 任何 `apex` 漂移都不能重新拖住 `/stock` 正式面
- 不允许把 `apex parity` 再拉回 `/stock` 正式同步门禁

### 3.3 禁止用工程字段直接污染公开面

- 禁止在公开面主判断区直接露出：
  - `partial_success`
  - `ready`
  - `blocked`
  - `quick`
  - `0 行`
  - `top_industry`
  - 其他原始工程枚举

### 3.4 禁止过早宣称长期候选质量已可信

- `120d` 样本不够厚时，禁止宣称长期可信
- `candidate_quality_diff` 缺 long-window / previous baseline / failure summary 时，禁止宣称质量已持续提升
- 样本厚度不够，只允许说：
  - 结构已建立
  - 样本在积累
  - 结论暂不放大

### 3.5 禁止再回到“页面像后台”路线

- 不允许把折叠区再次做成字段堆叠
- 不允许让内部复核区和正式首页主判断重新抢同一层注意力
- 不允许用“文案洗白”替代真正的信息层级重排

---

## 4. 每日验收口径

### 4.1 UI 产品面

每日必须确认：

- `/stock` 首屏主判断区是否仍然只讲：
  - 状态
  - 对象
  - 证据
  - 边界
  - 下一步
- 折叠内部复核区首层是否仍然先讲复核判断，而不是后台任务字段
- 当前对象是否仍未被误提升为“正式候选”

若不满足，视为产品面回退。

### 4.2 候选质量链

每日必须确认：

- `candidate_quality_summary.json` 是否可生成
- `candidate_quality_diff.json` 是否可生成
- `previous_formal_baseline` 是否仍可追溯
- `failure summary` 是否仍可进入对比链

若任一环节断裂，不允许继续放大“质量改进”叙事。

### 4.3 失败样本反哺链

每日必须确认：

- `learning feedback` 是否继续产出 `review_priority`
- `review queue` 是否继续产出 `open_owner_workloads`
- `benchmark execution` 是否继续继承 `execution_batch`
- `iteration_schedule` 是否继续进入 daily planner / morning brief

若 priority 不再影响执行顺序，视为闭环退化。

### 4.4 长窗样本进度

每日必须确认：

- `candidate_quality_validation_history` 是否成功归档
- `60d / 120d sample_total` 是否按日期去重后更新
- `remaining_samples_needed` 是否可追踪
- `progress_ratio` 是否进入 planner / morning brief

若只剩提醒、没有真实沉淀，视为长期样本链停滞。

### 4.5 正式门禁

每日必须确认：

- `/stock` 继续保持唯一正式主链
- `stock-scoped` 正式面不被 `apex` 漂移拖住
- 正式回归基线、candidate-quality 链、feedback 链不能出现“绿了但在撒谎”的情况

只要出现“产物还在，但语义已经不诚实”，视为当天开发不合格。

---

## 5. 两周结束条件

只有以下条件同时成立，才允许说“这两周执行期完成”：

- 折叠内部复核区已从后台任务面推进成正式研究终端的内部复核面
- 首屏证据快读层足够清晰，长句不再主导阅读
- 当前对象卡不再误导成已确认候选
- failure priority 已稳定驱动 review / benchmark / iteration 顺序
- `120d` 样本密度继续沿正式历史沉淀链推进，而不是停在提醒层

如果只是：

- 首页更干净了
- 卡片更顺眼了
- 结构更完整了

但：

- 失败样本闭环没继续驱动执行
- 长窗样本没继续真实增厚

则不允许判定这两周执行期完成。
