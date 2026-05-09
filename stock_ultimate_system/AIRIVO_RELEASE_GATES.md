# AIRIVO_RELEASE_GATES.md

## Airivo 发布门禁

版本：v1.0  
状态：冻结版  
性质：发布门禁规范  
适用范围：主站、`/stock`、`/T12` 的每轮迭代发布

---

## 1. 文档目的

本文件用于定义 Airivo 后续每轮迭代发布前至少需要通过的检查项，避免结构漂移、契约失稳、治理越界和体验退化。

---

## 2. 结构门禁

每轮发布至少检查：

- 主站、`/stock`、`/T12` 的角色是否被重写
- 是否有跨系统模块误挂载
- 是否有主站替代 `/stock` 的倾向
- 是否有 `/T12` 扩成交互控制台的倾向
- 主站是否仍以 `/stock` 作为主系统入口

未通过时不得发布。

三入口 scope registry 为 `src/airivo_scope_registry.py`。每轮发布必须保持：

- `main_site`：主站 `/`，品牌入口与产品矩阵，不承载业务主判断或治理主摘要
- `stock`：`/stock`，核心业务主结果与 release / baseline 机制，不承载 `/T12` 治理主摘要
- `t12`：`/T12`，只读治理与边界系统，不承载业务主判断、动作回写或控制台能力

该 registry 是三入口同步开发的最小机器可读矩阵：同步推进不表示三个入口功能同质化，而是每个入口的职责、禁区、负责模块、必跑测试和发布证据期望都被同步约束。

`/apex` 不是第四 system。它是内部验证 / 预发布 namespace，映射 main_site、`/stock`、`/T12` 三个既有 scope。发布前必须确认 `/apex` 仍只承担 namespace 角色，未被产品化成新的正式系统或并列正式入口。

`scope_readiness` 是阻断级 smoke 门禁，使用 registry 中的 `smoke_required_tests`，用于快速确认三入口同步边界没有掉队。registry 中的 `required_tests` 保留为 full 清单，适合完整 release / nightly 验证；不得把会递归触发 release pipeline 的 full 测试直接放入 smoke 门禁。

完整三入口 readiness 由 `scripts/run_airivo_scope_full_readiness.py` 执行。该入口按 `main_site`、`stock`、`t12` 分组运行 registry 中的 `required_tests`，适合 nightly、staging 验证或发布前完整检查，不替代快速 release gate。

服务器同步前置检查由 `scripts/run_server_sync_preflight.py` 执行。默认模式组合安全同步 manifest 与 `scope_readiness` smoke 门禁；`--full-readiness` 模式会额外运行完整三入口 readiness，适合 staging 或正式同步前使用。preflight 失败时不得进入 rsync 或服务器目录切换。

rsync file list 必须由 `scripts/build_server_sync_file_list.py` 从通过的 preflight JSON 生成，不应手工拼接。该脚本会拒绝失败 preflight 或非 preflight manifest，避免绕过服务器同步门禁。

生产 activation plan 之前必须运行 `scripts/run_server_domain_preflight.py`。该预检扫描 nginx 配置目录，确认 `airivo.online` 没有被非 `stock_ultimate_system` 托管配置占用，且没有和 `v49.app` 等已上线系统混在同一域名配置中；预检失败时不得生成可执行 activation commands。

服务器 staging 激活计划由 `scripts/build_server_activation_plan.py` 生成。它会检查 staging 中的关键入口文件，输出备份、覆盖、重启验证与 rollback commands；在 activation plan `status=passed` 之前，不应更新 `/opt/stock-ultimate/app`。

生产目录更新后必须运行 `scripts/run_server_post_deploy_verification.py`。该验证器检查 dashboard systemd 服务、本机 HTTP endpoint、关键 app 文件、运行态目录观察项和 dashboard error log，并在失败报告中保留 activation plan 的 rollback commands。

服务器部署拓扑必须保持三入口分离：主站 `/` 由 `stock-ultimate-main-site.service` 监听 `127.0.0.1:8764`，`/stock` 由 `stock-ultimate-dashboard.service` 监听 `127.0.0.1:8765`，`/T12` 由 `stock-ultimate-t12.service` 监听 `127.0.0.1:8766`。nginx 只负责按路径反代，不应把三个入口压回单一 `/stock` 服务。

服务器部署完成后必须生成 `scripts/build_server_deploy_evidence_bundle.py` 对应的 deploy evidence bundle，记录 preflight、file list、activation plan、post-deploy verification 四阶段报告哈希与三入口路由拓扑。没有 passed evidence bundle 的服务器同步不得标记为完成。

activation plan 不应手工复制命令执行，必须通过 `scripts/run_server_activation_plan.py` 执行。执行器要求 `--confirm-release-id` 与 plan 中的 `release_id` 一致，支持 `--dry-run`，并输出 `server_activation_execution.v1` 报告；deploy evidence bundle 必须包含非 dry-run、`action=activate`、`failed_total=0` 的执行报告。

nginx 配置变更必须随 activation plan 一起备份、复制、执行 `nginx -t` 和 reload；post-deploy verification 必须覆盖 nginx 配置有效性与 main_site、dashboard、t12 三个 error log，不能只验证 `/stock` 单进程。

服务器部署 evidence bundle 通过后必须用 `scripts/register_server_deploy.py` 登记到 server deployment registry。registry 使用不可变 history snapshot 和 `current.json` 指针记录当前部署，不接受 dry-run 证据包，并支持 rollback current pointer 到历史 deployment。

登记后必须运行 `scripts/build_server_go_live_readiness.py` 生成最终上线就绪报告。该报告从 current pointer 追溯 history snapshot 与 evidence bundle，校验证据哈希、非 dry-run activation、rollback commands、三入口 route topology 和 post-deploy 检查覆盖；只有 `status=passed` 时，命令中显式传入的主站、`/stock/`、`/T12/` 公网 URL 才能被标记为本轮可用入口。当前目标域名是 `https://airivo.online/`、`https://airivo.online/stock/`、`https://airivo.online/T12/`；如果 `airivo.online` 已由 `v49.app` 或其他线上系统占用，必须先完成域名归属、迁移窗口和回滚方案确认，不能直接覆盖生产 nginx。

服务器回滚不得只移动 registry 指针。必须通过 `scripts/run_server_deploy_rollback.py` 读取历史 deployment 的 evidence bundle 与 activation plan，执行 rollback commands，完成 post-deploy verification 后再更新 current pointer；dry-run rollback 只能演练，不得移动 current pointer。

---

## 3. 契约门禁

每轮发布至少检查：

- `/stock` 主结果契约是否被破坏
- `/stock` ViewModel 结构是否漂移
- `/stock` 渲染顺序是否变成“解释压过结论”
- 缺失值词汇是否新增
- 治理摘要是否误入 `/stock`
- canonical 内容质量是否仍受控
- fallback reason 是否仍属于受控语义集合
- canonical-only readiness 是否仍通过
- retirement gate matrix 是否仍未被触发阻断项
- 语言规范是否仍满足
- single-track canonical 运行是否仍稳定
- refinement spec 是否仍满足
- runtime observability spec 是否仍满足
- benchmark / golden set 是否仍通过
- benchmark report 结构是否仍可稳定生成
- benchmark 报告 JSON / Markdown 产物是否仍可稳定生成
- benchmark diff 结构是否仍可稳定生成
- release evidence bundle 是否仍可稳定生成
- 统一 release pipeline 摘要是否仍可稳定生成

---

## 4. 治理门禁

每轮发布至少检查：

- `/T12` 仍为只读治理系统
- 无新增按钮、表单、bridge、动作回写
- 治理摘要未回接到 `/stock`
- `risk_level` 未被升级成独立制度阻断系统

---

## 5. 体验门禁

每轮发布至少检查：

- 用户是否仍能快速识别主结果
- 用户是否仍能区分主站、`/stock`、`/T12`
- 页面信息层级是否仍稳定
- 关键命名与标签是否仍统一

---

## 6. 文档门禁

每轮发布至少检查：

- 是否违反既有 spec / contract / invariants
- 是否需要更新对应文档
- 文档是否与实现保持一致
- release gate 执行入口是否仍可统一运行
- release gate JSON 输出与退出码语义是否仍稳定
- 发布证据包产物是否仍可统一生成
- 主站转化事件 payload 结构是否仍稳定

---

## 6.1 baseline promotion 关联门禁

`/stock` baseline promotion 不是独立于 release gates 的旁路动作，晋升前至少必须满足：

- benchmark report 已生成
- benchmark diff 已生成
- release gates JSON 已生成且状态为 passed
- release evidence bundle 已生成
- release pipeline manifest 已生成
- release evidence checklist 状态为 complete
- release decision 已生成且 `decision=approved`
- release pipeline manifest 中的 benchmark report、benchmark diff、release gates 哈希与输入产物一致
- 无 blocking regression
- 无 blocking gate failure

只有满足以上条件，`scripts/promote_stock_baseline.py` 才允许写入 baseline registry，并切换 `artifacts/baselines/current.json`。promotion 必须提供 approved release decision，否则拒绝执行。

正式发布流水线可通过 `scripts/run_stock_release_pipeline.py --promote-baseline --release-decision-json <path>` 执行显式 baseline 晋升。流水线必须把晋升状态写入 release summary 与 release evidence bundle；如请求晋升但晋升失败，本轮发布不得标记为 passed。

每次统一发布流水线运行还必须写入 artifact registry JSONL 索引，登记 benchmark report、benchmark diff、release gates、release evidence bundle、release pipeline manifest、release summary，以及晋升成功时的 baseline snapshot / current pointer，保证后续可按 `run_id` 查询完整证据链。

如 release gates 已在同一受控流程中预先执行，可通过 `scripts/run_stock_release_pipeline.py --release-gates-json <path>` 显式复用既有 gate JSON。该模式不是跳过门禁：流水线必须读取 gate JSON、校验 `status=passed` 且 `failed_total=0`，复制为本轮 release 的 `release_gates.json`，并继续写入 manifest、evidence bundle 与 artifact registry。

---

## 7. 最低发布检查清单

以后每轮至少应检查：

1. 结构未越界
2. 主结果未失稳
3. 治理未回流到 `/stock`
4. 主站未承接业务主判断
5. 标签与空态未漂移
6. `/T12` 未扩成交互能力
7. fallback reason 未漂移成自由文本
8. canonical 内容质量门禁仍通过
9. canonical-only readiness 仍通过
10. retirement gate matrix 未出现阻断项
11. 语言规范未漂移
12. canonical 单轨运行仍稳定
13. refinement spec 仍满足
14. runtime observability spec 仍满足
15. benchmark / golden set 仍通过
16. 主站仍把 `/stock` 作为主入口
17. benchmark report 仍可稳定生成
18. release gate 执行入口仍可统一运行
19. benchmark JSON / Markdown 产物仍可稳定生成
20. release gate JSON 输出与退出码仍稳定
21. benchmark diff 仍可稳定生成
22. release evidence bundle 仍可稳定生成
23. release pipeline 仍可稳定执行并输出摘要
24. 主站转化事件 payload 仍稳定

---

## 8. 结论

统一执行入口：`scripts/check_release_gates.py`
benchmark 报告产物入口：`scripts/generate_stock_primary_result_benchmark_report.py`
benchmark 对比入口：`scripts/compare_stock_primary_result_benchmark_reports.py`
发布证据包入口：`scripts/build_release_evidence_bundle.py`
统一发布流水线入口：`scripts/run_stock_release_pipeline.py`

发布门禁不是额外负担，而是保证 Airivo 可以长期稳定演进到行业前 3 的最小制度。
