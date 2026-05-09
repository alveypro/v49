# Apex 内部验证入口线上修复执行单

文档状态：立即执行  
适用范围：`airivo.online/apex` 内部验证入口真实预检失败后的线上修复  
上位约束：`STRICT_CONTINUATION_EXECUTION_STANDARD.md`、`APEX_SYNC_PREFLIGHT_CHECKLIST.md`  
目的：把本轮 `apex preflight failed` 修复动作压成一条可执行、可验收、可中止的最小修复线，并明确 `apex` 只承担内部验证 / 预发布职责，不承担并列正式产品职责。

---

## 1. 当前失败事实

本轮真实 `apex` 内部验证入口预检已经确认以下失败事实：

1. active nginx 未 include `airivo-apex.locations.conf`
2. `airivo-apex-entry-guard.service` 不存在
3. `airivo-apex-entry-guard.timer` 不存在
4. 线上 `/opt/airivo-apex/app/run_dashboard.py` 不是当前版本
5. `/apex/stock/api/stock-ai-runner/*` 未返回预期 JSON，只回业务页 HTML
6. `/apex/T12/api/stock-ai-runner` 未阻断，返回 `200`
7. 服务器系统 `python3` 仍为 `3.6.8`，直接运行 `run_airivo_apex_preflight.py` 报 `SyntaxError`

硬结论：

- `apex internal-validation preflight failed`
- `禁止推进正式 airivo.online/stock`

---

## 2. 执行边界

本执行单只允许做以下事情：

- 同步当前仓库代码到 `/opt/airivo-apex/app`
- 安装并启用 `airivo-apex-entry-guard.service/timer`
- 把 `airivo-apex.locations.conf` 真接进 active nginx
- 用 `/opt/airivo-apex/.venv/bin/python` 跑预检
- 重启 `airivo-apex-main-site/stock/t12`
- 复验 `apex` 内部验证只读入口与 `/apex/T12` 阻断

禁止项：

- 不得推进正式 `/stock`
- 不得顺手夹带无关功能发布
- 不得把 AI runner 入口重新挂回业务页
- 不得放松 `/apex/T12` 阻断
- 不得改 `current_result_pointer` 或正式主结果裁决链

---

## 3. 执行顺序

### 步骤 1：同步当前仓库代码到 `/opt/airivo-apex/app`

目标：

- 让线上 `run_dashboard.py`、`src/stock_ai_*`、`deploy/aliyun/*` 进入当前版本，并保持 `apex` 只作为内部验证入口

最低要求：

- 同步后线上 `run_dashboard.py` 必须能 grep 到：
  - `stock-ai-runner`
  - `result-replay`
  - `ops/stock-ai-runner`

验收命令：

```bash
grep -n "stock-ai-runner\|result-replay\|ops/stock-ai-runner\|STOCK_AI_RUNNER" /opt/airivo-apex/app/run_dashboard.py
```

阻断条件：

- grep 仍为空
- 同步后代码目录不完整

---

### 步骤 2：安装并启用 `airivo-apex-entry-guard.service/timer`

目标：

- 补齐 `apex` 内部验证入口侧 `entry guard` 缺口

执行命令：

```bash
cp /opt/airivo-apex/app/deploy/aliyun/airivo-apex-entry-guard.service /etc/systemd/system/
cp /opt/airivo-apex/app/deploy/aliyun/airivo-apex-entry-guard.timer /etc/systemd/system/
systemctl daemon-reload
systemctl start airivo-apex-entry-guard.service
systemctl enable --now airivo-apex-entry-guard.timer
```

验收命令：

```bash
systemctl is-enabled airivo-apex-entry-guard.timer
systemctl is-active airivo-apex-entry-guard.service
systemctl is-active airivo-apex-entry-guard.timer
ls -l /opt/airivo-apex/app/artifacts/stock_entry_guard_latest.json
```

阻断条件：

- service/timer 任何一个不存在
- timer 未启用
- guard 产物未落盘

---

### 步骤 3：把 `airivo-apex.locations.conf` 真接进 active nginx

目标：

- 让 active `airivo.online.conf` 真正托管 `apex` 内部验证路由

硬要求：

- `nginx.airivo-apex.locations.conf` 只能放在 `/etc/nginx/snippets/`
- active `airivo.online.conf` 必须 include 该片段
- 不得把该 location 片段直接扔进 `/etc/nginx/conf.d/*.conf`

执行动作：

1. 确认片段文件存在：

```bash
ls -l /etc/nginx/snippets/airivo-apex.locations.conf
```

2. 编辑 active `/etc/nginx/conf.d/airivo.online.conf`，在 TLS server block 内加入：

```nginx
include /etc/nginx/snippets/airivo-apex.locations.conf;
```

3. 检查并 reload：

```bash
nginx -t
systemctl reload nginx
```

验收命令：

```bash
grep -n "airivo-apex.locations.conf\|location /apex/stock/\|location /apex/T12/" /etc/nginx/conf.d/airivo.online.conf
```

阻断条件：

- include 未写进 active conf
- `nginx -t` 失败
- reload 失败

---

### 步骤 4：只用 venv Python 跑 `apex` 预检

目标：

- 避开系统 `python3.6.8`
- 用实际执行环境核 `apex` 内部验证入口预检

执行命令：

```bash
/opt/airivo-apex/.venv/bin/python /opt/airivo-apex/app/scripts/run_airivo_apex_preflight.py \
  --json \
  --output /opt/airivo-apex/airivo_apex_preflight.json
```

验收命令：

```bash
cat /opt/airivo-apex/airivo_apex_preflight.json
```

验收标准：

- `status=passed`
- 不再出现 `future feature annotations` 语法错误

阻断条件：

- 仍使用系统 `python3`
- 预检输出 `status=failed`

---

### 步骤 5：重启 `airivo-apex-main-site/stock/t12`

目标：

- 让新代码、新路由、新只读内部验证面真正生效

执行命令：

```bash
systemctl restart airivo-apex-main-site.service
systemctl restart airivo-apex-stock.service
systemctl restart airivo-apex-t12.service
```

验收命令：

```bash
systemctl is-active airivo-apex-main-site.service
systemctl is-active airivo-apex-stock.service
systemctl is-active airivo-apex-t12.service
ss -lnt | egrep ":18764|:18765|:18766"
```

阻断条件：

- 任一 service 未回到 `active`
- 任一端口未监听

---

### 步骤 6：复验 `apex` 内部验证只读面与 `/apex/T12` 阻断

目标：

- 确认 API 组、运维页、result replay 专页已上线
- 确认 `/apex/T12` 下对应入口继续阻断

必须验证的入口：

```text
/apex/stock/api/stock-ai-runner
/apex/stock/api/stock-ai-runner/latest-health
/apex/stock/api/stock-ai-runner/health-rollups
/apex/stock/api/stock-ai-runner/trend-summaries
/apex/stock/api/stock-ai-runner/failure-top-causes
/apex/stock/api/stock-ai-runner/provider-detail
/apex/stock/api/stock-ai-runner/result-replay
/apex/stock/ops/stock-ai-runner
/apex/stock/ops/stock-ai-runner/result-replay
```

阻断验证入口：

```text
/apex/T12/api/stock-ai-runner
/apex/T12/api/stock-ai-runner/*
/apex/T12/ops/stock-ai-runner
/apex/T12/ops/stock-ai-runner/result-replay
```

验收标准：

- `/apex/stock/api/stock-ai-runner/*` 返回 JSON，不再回业务页 HTML
- `/apex/stock/ops/stock-ai-runner` 正常打开
- `/apex/stock/ops/stock-ai-runner/result-replay` 正常打开
- `/apex/T12/*` 相关入口返回 `404` 或明确阻断

阻断条件：

- stock 侧 API 任一仍回 HTML
- 运维页或 result replay 专页异常
- `/apex/T12` 任一入口仍 `200`

---

## 4. 最小复验命令集

```bash
curl -i http://127.0.0.1:18765/apex/stock/api/stock-ai-runner
curl -i http://127.0.0.1:18765/apex/stock/api/stock-ai-runner/latest-health
curl -i http://127.0.0.1:18765/apex/stock/api/stock-ai-runner/health-rollups
curl -i http://127.0.0.1:18765/apex/stock/api/stock-ai-runner/trend-summaries
curl -i http://127.0.0.1:18765/apex/stock/api/stock-ai-runner/failure-top-causes
curl -i http://127.0.0.1:18765/apex/stock/api/stock-ai-runner/provider-detail
curl -i "http://127.0.0.1:18765/apex/stock/api/stock-ai-runner/result-replay?result_id=demo"
curl -i http://127.0.0.1:18765/apex/stock/ops/stock-ai-runner
curl -i "http://127.0.0.1:18765/apex/stock/ops/stock-ai-runner/result-replay?result_id=demo"
curl -i http://127.0.0.1:18766/apex/T12/api/stock-ai-runner
curl -i http://127.0.0.1:18766/apex/T12/ops/stock-ai-runner
```

---

## 5. 放行标准

仅当以下条件全部满足，才允许把这批改动判定为 `apex internal-validation repaired`：

1. `run_airivo_apex_preflight.py` 输出 `status=passed`
2. `airivo-apex-entry-guard.service/timer` 存在且可用
3. active nginx 已 include `airivo-apex.locations.conf`
4. `/apex/stock/api/stock-ai-runner/*` 返回预期 JSON
5. `/apex/stock/ops/stock-ai-runner` 与 `result-replay` 专页正常
6. `/apex/T12` 下对应入口继续阻断

在以上条件未全部满足前：

- `禁止推进正式 airivo.online/stock`

---

## 6. 执行后记录

执行完成后必须留下一次修复记录，至少包含：

- 执行时间
- 执行人
- 同步代码版本或 commit
- nginx 修改说明
- 新增启用的 service/timer
- `apex internal-validation preflight` 结果
- 入口复验结果
- 是否允许进入正式 `/stock`

硬边界：

- 如果结果不是全绿，结论必须写 `不允许推进正式 /stock`
