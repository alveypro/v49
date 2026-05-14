# Apex 内部验证同步前核查清单

文档状态：强制执行  
适用范围：`airivo.online/apex` 内部验证环境同步前预检  
上位约束：`STRICT_CONTINUATION_EXECUTION_STANDARD.md`、`MAIN_CHAIN_PRODUCTION_ACTION_BASELINE.md`、`PRIMARY_RESULT_FACT_SOURCE_CLASSIFICATION.md`  
目的：在不触碰正式 `/stock` 主入口的前提下，先验证 `apex` 内部验证环境的路由、服务、entry guard 与只读运维面是否完整可用。

---

## 1. 结论先行

这份清单只回答一个问题：

`这次改动是否具备先同步到 airivo.online/apex，再进入 airivo.online/stock 的最低条件。`

硬边界：

- `airivo.online/stock` 仍是正式 canonical 主入口
- `airivo.online/apex` 只是内部验证 / 预发布命名空间
- `apex` 验证不通过，不允许推进正式 `/stock`
- `apex` 只验证同一批已提交改动，不允许临时夹带额外变更

---

## 2. 核查目标

本轮 `apex` 预检只围绕以下对象：

- `nginx` apex 路由是否真的生效
- `18764 / 18765 / 18766` 三个内部验证服务是否存在并可启动
- `entry guard` 内部验证服务与定时器是否存在
- `stock-ai-runner` 只读 API 组是否可访问
- `stock-ai-runner` 运维页与 `result replay` 专页是否可访问
- `/apex/T12` 下这些入口是否继续阻断

---

## 3. 必须文件

同步前必须先确认下列资产在仓库内存在且为当前版本：

- `deploy/aliyun/nginx.airivo-apex.locations.conf`
- `deploy/aliyun/airivo-apex-main-site.service`
- `deploy/aliyun/airivo-apex-stock.service`
- `deploy/aliyun/airivo-apex-t12.service`
- `deploy/aliyun/airivo-apex-entry-guard.service`
- `deploy/aliyun/airivo-apex-entry-guard.timer`

如果以上任一文件缺失：

- `直接阻断 apex 同步`

---

## 4. 路由核查

### 4.1 nginx include 核查

必须确认线上 `airivo.online` 的 TLS server block 已真实 include：

- `airivo-apex.locations.conf`

禁止项：

- 只看到仓库里有该文件，就默认线上已生效
- 未核 include 状态就直接发布内部验证入口

验收：

- 线上 nginx 配置中确实存在 `location /apex/`
- 线上 nginx 配置中确实存在 `location /apex/stock/`
- 线上 nginx 配置中确实存在 `location /apex/T12/`

如果 include 未生效：

- `阻断发布`

### 4.2 apex 路由目标核查

必须确认 `apex` 路由目标与仓库定义一致：

- `/apex/` -> `127.0.0.1:18764`
- `/apex/stock/` -> `127.0.0.1:18765`
- `/apex/T12/` -> `127.0.0.1:18766`

禁止项：

- 复用正式 `/stock` 的 `8765` 端口
- 把 `apex` 路由误配到正式 `/T12`

---

## 5. 内部验证服务核查

### 5.1 systemd 资产核查

必须确认以下服务定义在线上存在：

- `airivo-apex-main-site.service`
- `airivo-apex-stock.service`
- `airivo-apex-t12.service`

必须确认其 `base-path` 与仓库定义一致：

- main-site：`/apex`
- stock：`/apex/stock`
- t12：`/apex/T12`

### 5.2 entry guard 核查

必须确认以下资产在线上存在：

- `airivo-apex-entry-guard.service`
- `airivo-apex-entry-guard.timer`

验收：

- 定时器存在
- 定时器已启用
- `entry guard` 对应产物文件能够落盘

禁止项：

- 只发应用，不发 `entry guard`
- 让 `apex` 内部验证入口在没有 guard 的情况下对外暴露 `/stock`

---

## 6. apex 发布后必须验的入口

以下入口必须逐个验证：

- `/apex/stock/api/stock-ai-runner`
- `/apex/stock/api/stock-ai-runner/latest-health`
- `/apex/stock/api/stock-ai-runner/health-rollups`
- `/apex/stock/api/stock-ai-runner/trend-summaries`
- `/apex/stock/api/stock-ai-runner/failure-top-causes`
- `/apex/stock/api/stock-ai-runner/provider-detail`
- `/apex/stock/api/stock-ai-runner/result-replay`
- `/apex/stock/ops/stock-ai-runner`
- `/apex/stock/ops/stock-ai-runner/result-replay`

验收：

- 返回码正确
- 页面或 JSON 结构与仓库 contract 一致
- 只读面可用，不触发业务页主链异常

---

## 7. `/apex/T12` 阻断核查

必须明确验证：以下入口在 `/apex/T12` 下继续不可用

- `/apex/T12/api/stock-ai-runner`
- `/apex/T12/api/stock-ai-runner/*`
- `/apex/T12/ops/stock-ai-runner`
- `/apex/T12/ops/stock-ai-runner/result-replay`

验收：

- 返回 `404` 或明确阻断

禁止项：

- 因为内部验证入口引入，只读 AI 运维入口被误挂到 `/T12`

---

## 8. 主链隔离核查

必须确认这次 `apex` 同步不改以下对象的治理语义：

- `current_result_pointer`
- `result_registry current`
- `run_registry current`
- `stock_entry_guard` 正式主链判定
- `/stock` 正式主结果裁决

验收：

- `stock-ai-runner` 相关新增能力仍然只存在于只读面
- 任何 AI telemetry / replay / ops view 都不进入主结果裁决字段

---

## 9. 发布阻断条件

命中任意一条，`阻断 apex 同步`：

- nginx 未 include `airivo-apex.locations.conf`
- `18764 / 18765 / 18766` 任一内部验证服务缺失
- `airivo-apex-entry-guard.timer` 缺失或未启用
- `/apex/stock/api/stock-ai-runner/*` 任一核心入口异常
- `/apex/stock/ops/stock-ai-runner` 或 `result-replay` 专页异常
- `/apex/T12` 下 AI runner 入口未阻断
- 发现内部验证入口变更影响正式 `/stock` 主链语义

---

## 10. 放行条件

同时满足以下条件，才允许从 `apex` 进入正式 `/stock`：

- `apex` 预检完成
- `apex` 路由可访问
- 内部验证服务与定时器正常
- 只读 API 组正常
- 运维页正常
- `/apex/T12` 阻断正常
- 无主链语义污染

---

## 11. 正式 `/stock` 推进规则

`apex` 验证通过后，推进正式 `/stock` 时必须遵守：

- 只发布已经在 `apex` 验证过的同一批代码
- 不临时夹带新改动
- 正式 `/stock` 仍以 `entry guard` 为主链门禁
- 正式 `/stock` 仍不启用 `/T12` 的 AI runner 运维入口

---

## 12. 最小执行顺序

1. 核 nginx include
2. 核内部验证 systemd 服务
3. 核 `entry guard` 内部验证定时器
4. 发 `apex`
5. 验 `stock-ai-runner` API 组
6. 验 `ops/stock-ai-runner`
7. 验 `ops/stock-ai-runner/result-replay`
8. 验 `/apex/T12` 阻断
9. 通过后再发正式 `/stock`

---

## 13. 一句话纪律

`先 apex 内部验证，后 stock 正式主链；先验证内部只读运维面，后推进正式主入口。`
