# Aliyun Deployment

目标形态：

- 应用目录：`/opt/stock-ultimate/app`
- 数据目录：`/opt/stock-ultimate/data`
- 虚拟环境：`/opt/stock-ultimate/.venv`
- 主站端口：`127.0.0.1:8764`
- `/stock` 端口：`127.0.0.1:8765`
- `/T12` 端口：`127.0.0.1:8766`
- 反向代理：`nginx`
- 定时任务：`systemd timer`

## 1. 目录准备

```bash
mkdir -p /opt/stock-ultimate/app /opt/stock-ultimate/data /var/log/stock-ultimate
```

## 2. 代码与配置

将项目同步到 `/opt/stock-ultimate/app`，然后创建服务器配置目录：

同步前先在本地运行 preflight。该入口会生成安全同步清单，并执行三入口 scope smoke 门禁，确认只包含代码、配置、文档、测试与部署脚本，不包含 token、缓存、日志、运行数据或状态型 artifacts：

```bash
python stock_ultimate_system/scripts/run_server_sync_preflight.py \
  --project-root stock_ultimate_system \
  --output /tmp/stock_ultimate_server_sync_preflight.json \
  --json
```

发布前或 staging 前的完整检查应显式打开 full readiness：

```bash
python stock_ultimate_system/scripts/run_server_sync_preflight.py \
  --project-root stock_ultimate_system \
  --full-readiness \
  --output /tmp/stock_ultimate_server_sync_preflight.json \
  --json
```

只有当 preflight `status=passed` 时，才进入 staging 同步。需要单独审计 manifest 时，也可以直接生成同步 manifest：

```bash
python stock_ultimate_system/scripts/prepare_server_sync_manifest.py \
  --project-root stock_ultimate_system \
  --output /tmp/stock_ultimate_server_sync_manifest.json
```

基于通过的 preflight 生成 rsync file list：

```bash
python stock_ultimate_system/scripts/build_server_sync_file_list.py \
  --preflight /tmp/stock_ultimate_server_sync_preflight.json \
  --output /tmp/stock_ultimate_server_sync_files.txt \
  --summary-output /tmp/stock_ultimate_server_sync_file_list.json \
  --json

rsync -av --files-from=/tmp/stock_ultimate_server_sync_files.txt \
  stock_ultimate_system/ \
  <user>@<server>:/opt/stock-ultimate/staging/

ssh <user>@<server> "mkdir -p /opt/stock-ultimate/deploy_evidence"
scp /tmp/stock_ultimate_server_sync_preflight.json \
  /tmp/stock_ultimate_server_sync_file_list.json \
  <user>@<server>:/opt/stock-ultimate/deploy_evidence/
```

不要用未过滤的全量目录同步覆盖生产目录。

在服务器上先做域名占用预检。该检查会扫描 nginx 配置，确认 `airivo.online` 没有被非本系统配置占用，且没有和 `v49.app` 等已上线系统混在同一域名配置里：

```bash
python /opt/stock-ultimate/staging/scripts/run_server_domain_preflight.py \
  --target-domain airivo.online \
  --nginx-conf-dir /etc/nginx/conf.d \
  --nginx-conf-dir /etc/nginx/sites-enabled \
  --output /opt/stock-ultimate/server_domain_preflight.json \
  --json
```

若 `server_domain_preflight.json` 的 `status=failed`，不得继续生成正式 activation plan。需要先决定是迁移 `v49.app`、合并现有 nginx，还是另开独立域名。

从 staging 生成激活计划，确认 staging 目录具备关键文件、域名预检通过、激活命令与回滚命令都存在：

```bash
cd /opt/stock-ultimate/staging
python scripts/build_server_activation_plan.py \
  --staging-dir /opt/stock-ultimate/staging \
  --release-id "$(date +%Y%m%d%H%M%S)" \
  --domain-preflight-json /opt/stock-ultimate/server_domain_preflight.json \
  --output /opt/stock-ultimate/server_activation_plan.json \
  --json
```

只有当 activation plan `status=passed` 时，才允许进入“是否具备执行资格”的判断；这不等于已经允许真实 activation。当前 systemd 服务仍指向 `/opt/stock-ultimate/app`，因此激活计划采用保守的备份再覆盖模型，并生成对应 rollback commands；不要跳过备份直接覆盖 `/opt/stock-ultimate/app`。activation plan 还会备份 nginx 配置、复制三入口 nginx 配置、执行 `nginx -t` 并 reload nginx。

执行 non-dry-run activation 前，必须先满足 `docs/FORMAL_HOSTING_BOUNDARY_AND_ACTIVATION_BLOCKERS.md` 中的全部前置条件。若 `airivo.online` 的主站、`/app`、认证、`/health`、`/chat` 等 live 路由仍未完成正式托管归属冻结，则禁止执行会覆盖整份 `/etc/nginx/conf.d/airivo.online.conf` 的 activation。

先 dry-run 检查 activation plan。只有在 formal hosting boundary 已冻结且 topology 与 live runtime 一致后，才允许正式执行：

```bash
RELEASE_ID="$(python - <<'PY'
import json
print(json.load(open("/opt/stock-ultimate/server_activation_plan.json", encoding="utf-8"))["release_id"])
PY
)"

python scripts/run_server_activation_plan.py \
  --plan /opt/stock-ultimate/server_activation_plan.json \
  --confirm-release-id "$RELEASE_ID" \
  --dry-run \
  --output /opt/stock-ultimate/server_activation_execution_dry_run.json \
  --json

python scripts/run_server_activation_plan.py \
  --plan /opt/stock-ultimate/server_activation_plan.json \
  --confirm-release-id "$RELEASE_ID" \
  --output /opt/stock-ultimate/server_activation_execution.json \
  --json
```

`server_activation_execution_dry_run.json` 只能作为演练证据，不能用于最终 deploy evidence bundle。最终证据包必须使用不带 `--dry-run` 的 `server_activation_execution.json`。如果当前阶段仍命中 hosting boundary blockers，则必须停在 dry-run，不得继续生成正式 activation execution。

激活后必须运行 post-deploy verification，确认三入口 systemd 服务、正式 `/stock` 的 canonical artifacts 环境变量、nginx 配置、`/`、`/stock/`、`/stock/api/primary-result`、`/T12/`、`/stock` 与 `/apex/stock` 的证据进度一致性、关键 app 文件、运行态目录保护观察项，以及 main_site、dashboard、t12 三个 error log：

```bash
python /opt/stock-ultimate/app/scripts/run_server_post_deploy_verification.py \
  --activation-plan /opt/stock-ultimate/server_activation_plan.json \
  --output /opt/stock-ultimate/server_post_deploy_verification.json \
  --json
```

只有当 post-deploy verification `status=passed` 时，才把本次同步视为完成。若失败，按报告中的 `rollback_hint.rollback_commands` 回滚，并保留 verification JSON 作为事故证据。

post-deploy verification 通过后，生成服务器部署证据包：

```bash
python /opt/stock-ultimate/app/scripts/build_server_deploy_evidence_bundle.py \
  --deployment-id "$(date +%Y%m%d%H%M%S)" \
  --preflight-json /opt/stock-ultimate/deploy_evidence/stock_ultimate_server_sync_preflight.json \
  --file-list-json /opt/stock-ultimate/deploy_evidence/stock_ultimate_server_sync_file_list.json \
  --activation-plan-json /opt/stock-ultimate/server_activation_plan.json \
  --activation-execution-json /opt/stock-ultimate/server_activation_execution.json \
  --post-deploy-json /opt/stock-ultimate/server_post_deploy_verification.json \
  --output /opt/stock-ultimate/server_deploy_evidence_bundle.json
```

部署证据包会记录四阶段 JSON 的哈希、三入口路由拓扑、post-deploy 检查摘要和 rollback commands。没有该证据包，不应把本次同步标记为完成。

部署证据包通过后，登记当前服务器部署：

```bash
python /opt/stock-ultimate/app/scripts/register_server_deploy.py \
  --deployments-dir /opt/stock-ultimate/deployments \
  --evidence-bundle-json /opt/stock-ultimate/server_deploy_evidence_bundle.json
```

登记会写入不可变 history snapshot 与 `current.json` 指针。需要回滚登记指针时，使用：

```bash
python /opt/stock-ultimate/app/scripts/register_server_deploy.py \
  --deployments-dir /opt/stock-ultimate/deployments \
  --rollback-deployment-id <previous-deployment-id>
```

登记后生成最终上线就绪报告：

```bash
python /opt/stock-ultimate/app/scripts/build_server_go_live_readiness.py \
  --deployments-dir /opt/stock-ultimate/deployments \
  --main-site-url https://airivo.online/ \
  --stock-url https://airivo.online/stock/ \
  --t12-url https://airivo.online/T12/ \
  --output /opt/stock-ultimate/server_go_live_readiness.json \
  --json
```

只有当 go-live readiness `status=passed` 时，才把本次同步标记为可对外使用。当前三入口公网地址为：

- `https://airivo.online/`
- `https://airivo.online/stock/`
- `https://airivo.online/T12/`

注意：如果 `airivo.online` 当前已经由 `v49.app` 或其他线上系统占用，或者 live nginx 仍承载 repo current 未等价接管的主站、`/app`、认证、`/health`、`/chat` 等路径，不能直接覆盖 `/etc/nginx/conf.d/airivo.online.conf`。必须先确认域名归属、路由托管归属、迁移窗口和回滚方案，再执行 non-dry-run activation。

如果需要执行服务器回滚，不要只改 registry 指针。应通过 rollback 执行入口先 dry-run，再正式执行 rollback commands、post-deploy verification，并在通过后更新 current pointer：

```bash
python /opt/stock-ultimate/app/scripts/run_server_deploy_rollback.py \
  --deployments-dir /opt/stock-ultimate/deployments \
  --rollback-deployment-id <previous-deployment-id> \
  --confirm-deployment-id <previous-deployment-id> \
  --dry-run \
  --output /opt/stock-ultimate/server_deploy_rollback_dry_run.json \
  --json

python /opt/stock-ultimate/app/scripts/run_server_deploy_rollback.py \
  --deployments-dir /opt/stock-ultimate/deployments \
  --rollback-deployment-id <previous-deployment-id> \
  --confirm-deployment-id <previous-deployment-id> \
  --output /opt/stock-ultimate/server_deploy_rollback.json \
  --activation-execution-output /opt/stock-ultimate/server_rollback_activation_execution.json \
  --post-deploy-output /opt/stock-ultimate/server_rollback_post_deploy_verification.json \
  --json
```

```bash
mkdir -p /opt/stock-ultimate/app/config/server
cp /opt/stock-ultimate/app/deploy/aliyun/settings.server.yaml /opt/stock-ultimate/app/config/server/settings.yaml
cp /opt/stock-ultimate/app/config/*.yaml /opt/stock-ultimate/app/config/server/
cp /opt/stock-ultimate/app/deploy/aliyun/settings.server.yaml /opt/stock-ultimate/app/config/server/settings.yaml
```

确认：

- `/opt/openclaw/permanent_stock_database.db` 已存在
- `/root/.tushare_token` 已存在

## 3. Python 环境

```bash
python3 -m venv /opt/stock-ultimate/.venv
/opt/stock-ultimate/.venv/bin/pip install -U pip setuptools wheel
/opt/stock-ultimate/.venv/bin/pip install -r /opt/stock-ultimate/app/requirements.txt
```

## 4. systemd

```bash
cp /opt/stock-ultimate/app/deploy/aliyun/stock-ultimate-dashboard.service /etc/systemd/system/
mkdir -p /etc/systemd/system/stock-ultimate-dashboard.service.d
cp /opt/stock-ultimate/app/deploy/aliyun/stock-ultimate-dashboard.service.d/canonical-artifacts.conf /etc/systemd/system/stock-ultimate-dashboard.service.d/canonical-artifacts.conf
cp /opt/stock-ultimate/app/deploy/aliyun/stock-ultimate-main-site.service /etc/systemd/system/
cp /opt/stock-ultimate/app/deploy/aliyun/stock-ultimate-t12.service /etc/systemd/system/
cp /opt/stock-ultimate/app/deploy/aliyun/stock-ultimate-update.service /etc/systemd/system/
cp /opt/stock-ultimate/app/deploy/aliyun/stock-ultimate-update.timer /etc/systemd/system/
cp /opt/stock-ultimate/app/deploy/aliyun/stock-ultimate-daily-research.service /etc/systemd/system/
cp /opt/stock-ultimate/app/deploy/aliyun/stock-ultimate-daily-research.timer /etc/systemd/system/
cp /opt/stock-ultimate/app/deploy/aliyun/stock-ultimate-nightly-research.service /etc/systemd/system/
cp /opt/stock-ultimate/app/deploy/aliyun/stock-ultimate-nightly-research.timer /etc/systemd/system/
cp /opt/stock-ultimate/app/deploy/aliyun/stock-ultimate-weekly-long.service /etc/systemd/system/
cp /opt/stock-ultimate/app/deploy/aliyun/stock-ultimate-weekly-long.timer /etc/systemd/system/
cp /opt/stock-ultimate/app/deploy/aliyun/stock-ultimate-healthcheck.service /etc/systemd/system/
cp /opt/stock-ultimate/app/deploy/aliyun/stock-ultimate-healthcheck.timer /etc/systemd/system/
cp /opt/stock-ultimate/app/deploy/aliyun/logrotate.stock-ultimate /etc/logrotate.d/stock-ultimate
cp /opt/airivo-apex/app/deploy/aliyun/airivo-apex-primary-result-daily-closure.service /etc/systemd/system/
cp /opt/airivo-apex/app/deploy/aliyun/airivo-apex-primary-result-daily-closure.timer /etc/systemd/system/
cp /opt/airivo-apex/app/deploy/aliyun/airivo-apex-update.service /etc/systemd/system/
cp /opt/airivo-apex/app/deploy/aliyun/airivo-apex-update.timer /etc/systemd/system/
cp /opt/airivo-apex/app/deploy/aliyun/airivo-apex-candidate-handoff-gate.service /etc/systemd/system/
cp /opt/airivo-apex/app/deploy/aliyun/airivo-apex-candidate-handoff-gate.timer /etc/systemd/system/
cp /opt/airivo-apex/app/deploy/aliyun/airivo-apex-primary-result-feedback-loop.service /etc/systemd/system/
cp /opt/airivo-apex/app/deploy/aliyun/airivo-apex-primary-result-feedback-loop.timer /etc/systemd/system/
cp /opt/airivo-apex/app/deploy/aliyun/airivo-apex-candidate-basket-observation.service /etc/systemd/system/
cp /opt/airivo-apex/app/deploy/aliyun/airivo-apex-candidate-basket-observation.timer /etc/systemd/system/
cp /opt/airivo-apex/app/deploy/aliyun/airivo-apex-performance-evidence.service /etc/systemd/system/
cp /opt/airivo-apex/app/deploy/aliyun/airivo-apex-performance-evidence.timer /etc/systemd/system/
cp /opt/airivo-apex/app/deploy/aliyun/airivo-apex-promotion-readiness-gate.service /etc/systemd/system/
cp /opt/airivo-apex/app/deploy/aliyun/airivo-apex-promotion-readiness-gate.timer /etc/systemd/system/
cp /opt/airivo-apex/app/deploy/aliyun/airivo-apex-daily-operations-scoreboard.service /etc/systemd/system/
cp /opt/airivo-apex/app/deploy/aliyun/airivo-apex-daily-operations-scoreboard.timer /etc/systemd/system/
systemctl daemon-reload
systemctl enable --now stock-ultimate-dashboard.service
systemctl enable --now stock-ultimate-main-site.service
systemctl enable --now stock-ultimate-t12.service
systemctl enable --now stock-ultimate-update.timer
systemctl enable --now stock-ultimate-daily-research.timer
systemctl enable --now stock-ultimate-nightly-research.timer
systemctl enable --now stock-ultimate-weekly-long.timer
systemctl enable --now stock-ultimate-healthcheck.timer
systemctl enable --now airivo-apex-update.timer
systemctl enable --now airivo-apex-primary-result-daily-closure.timer
systemctl enable --now airivo-apex-candidate-handoff-gate.timer
systemctl enable --now airivo-apex-primary-result-feedback-loop.timer
systemctl enable --now airivo-apex-candidate-basket-observation.timer
systemctl enable --now airivo-apex-performance-evidence.timer
systemctl enable --now airivo-apex-promotion-readiness-gate.timer
systemctl enable --now airivo-apex-daily-operations-scoreboard.timer
```

正式 `/stock` 已切换为只读 canonical 证据产物，不再维护独立写入链。当前 systemd 基线必须满足：

- `stock-ultimate-dashboard.service` 通过 `stock-ultimate-dashboard.service.d/canonical-artifacts.conf` 注入：
  - `STOCK_ULTIMATE_ARTIFACTS_DIR=/opt/airivo-apex/app/artifacts`
  - `STOCK_ULTIMATE_EXPERIMENTS_DIR=/opt/airivo-apex/app/data/experiments`
- `airivo-apex-*` 是唯一证据生成链
- 禁止新增 `stock-ultimate-*` 版本的 `performance_evidence`、`promotion_readiness_gate`、`daily_operations_scoreboard` timer
- 禁止用 `cp`/`rsync` 定时同步 `primary_result_*_latest.json`

调度拆分后：

- `17:30`：`stock-ultimate-update.timer`
  - 负责股票日线、配置内 benchmark 指数更新和全 A 候选扫描
  - benchmark 指数来自 `data.benchmark_indices`，默认 `000001.SH`，写入同一张 `daily_trading_data`
- `17:30`：`airivo-apex-update.timer`
  - 在 `/opt/airivo-apex/app` 下运行独立行情更新和候选主更新
  - 刷新 `/opt/airivo-apex/app/data/experiments/update_status_latest.json`、`candidates_top_latest.csv`、`candidate_prefilter_universe_latest.json`
  - 这是 Apex 后处理链的前置入口；如果缺失，后续 timer 只会消费旧工件
  - 当前默认走 `quick-mode` 候选路径：跳过历史验证，使用轻量模型集，目标是稳定落在 `candidate_timeout_sec` 内
- `00:20`：`stock-ultimate-daily-research.timer`
  - 独立运行 `daily research`
- `21:45`：`stock-ultimate-nightly-research.timer`
  - 只负责中型研究池回测，不再和候选/日报绑在同一进程链
- `周日 02:30`：`stock-ultimate-weekly-long.timer`
  - 长样本周级回测
- `18:10`：`stock-ultimate-healthcheck.timer`
  - 运行自动化健康检查脚本，读取 `update_status_latest.json`
  - 目标是快速确认当天更新链是否在预算内完成
- `18:40`：`airivo-apex-primary-result-daily-closure.timer`
  - 自动读取当前 `/stock` 主结果、open observation、服务器配置中的数据库与 benchmark
  - 运行日终闭环编排器；数据不足时只写 blocked report，不写 terminal 或 performance ledger
- `18:35`：`airivo-apex-candidate-handoff-gate.timer`
  - 在 daily closure 之前检测当前 top candidate 与 lifecycle current pointer 是否一致
  - 若新候选与旧 lifecycle/observation 不一致，输出 handoff_required，禁止把旧 observation 当作新候选闭环依据
- `18:45`：`airivo-apex-primary-result-feedback-loop.timer`
  - 自动读取 closed primary observation、terminal outcome 与 performance ledger
  - 对 failed 或 weak-success 样本生成 failure attribution、learning feedback，并登记到 governed review queue
  - 强成功或 observation 未关闭时只写 skipped/not_required 证据；绝不自动改策略、晋升 baseline 或部署
- `18:50`：`airivo-apex-candidate-basket-observation.timer`
  - 自动读取当前 candidate basket pointer、open observation window、服务器配置中的数据库与 benchmark
  - 运行 basket observation；数据不足时只写 blocked report，不登记 basket performance ledger
- `18:58`：`airivo-apex-performance-evidence.timer`
  - 汇总 primary result performance ledger 和 candidate basket performance ledger
  - 生成 20/60/120 样本证据评估；样本不足时只标记 accumulating，不做 alpha 强结论
- `18:59`：`airivo-apex-promotion-readiness-gate.timer`
  - 合并 performance evidence、feedback review queue 与 baseline current pointer
  - 输出 promotion review 是否允许、冻结或阻断原因；不执行 baseline promotion
- `19:00`：`airivo-apex-daily-operations-scoreboard.timer`
  - 按依赖顺序刷新 candidate handoff gate、performance evidence、promotion readiness gate、daily operations scoreboard、competitive gap assessment，并最终重建 scoreboard
  - 输出 `artifacts/primary_result_operations_refresh_latest.json` 与 `artifacts/primary_result_daily_operations_scoreboard_latest.json`
  - 该编排只刷新本地证据 summary，不运行 handoff、不交易、不改策略、不晋升 baseline、不调用外部分析平台

手动验证更新链：

```bash
systemctl start stock-ultimate-update.service
journalctl -u stock-ultimate-update.service -n 200 --no-pager
```

只验证行情与 benchmark 更新，不触发候选和研究：

```bash
cd /opt/airivo-apex/app
/opt/airivo-apex/.venv/bin/python run_update_database.py \
  --config-dir config \
  --no-post-top-candidates \
  --no-post-daily-research
```

性能排查时重点看这些日志字段：

- `Batch prediction setup`
  - 关注 `models`、`pooled_build_sec`
- `Batch prediction training complete`
  - 关注 `train_sec`
- `Batch prediction finished`
  - 关注 `total_sec`、`predict_sec`、`degraded`

当前建议：

- 自动任务使用 quick path，只追求稳定产出候选
- 手工研究或排查时，直接运行 `run_top_candidates.py`，不要带 `--skip-validation --quick-mode`

手动验证每日研究链：

```bash
systemctl start stock-ultimate-daily-research.service
journalctl -u stock-ultimate-daily-research.service -n 200 --no-pager
```

手动验证健康检查链：

```bash
systemctl start stock-ultimate-healthcheck.service
journalctl -u stock-ultimate-healthcheck.service -n 50 --no-pager
tail -n 50 /var/log/stock-ultimate/automation_health.log
```

手动生成 Apex 日运营作战图：

```bash
systemctl start airivo-apex-daily-operations-scoreboard.service
journalctl -u airivo-apex-daily-operations-scoreboard.service -n 80 --no-pager
cat /opt/airivo-apex/app/artifacts/primary_result_daily_operations_scoreboard_latest.json
```

手动运行主结果失败反馈闭环：

```bash
systemctl start airivo-apex-primary-result-feedback-loop.service
journalctl -u airivo-apex-primary-result-feedback-loop.service -n 80 --no-pager
cat /opt/airivo-apex/app/data/experiments/primary_result_feedback_loop_latest.json
```

日志轮转：

```bash
logrotate -f /etc/logrotate.d/stock-ultimate
```

## 5. nginx

将 `nginx.airivo.online.conf` 放到 `/etc/nginx/conf.d/airivo.online.conf`，然后申请证书：

```bash
certbot --nginx -d airivo.online
nginx -t
systemctl reload nginx
```

## 6. DNS

为 `airivo.online` 添加 `A` 记录到 `47.90.160.87`。如果该域名已经指向 `v49.app` 的线上服务，本系统上线前必须先做迁移或反代合并方案，不能直接抢占生产域名。

## 7. Airivo Apex 内部验证入口

当前生产 `airivo.online` 已有 `/`、`/stock/`、`/T12/`、`/health`、`/chat` 等线上路由。Airivo Apex 只允许作为内部验证 / 预发布入口运行，不覆盖既有 nginx 主配置，也不承担对外正式产品职责：

- `https://airivo.online/apex/`
- `https://airivo.online/apex/stock/`
- `https://airivo.online/apex/T12/`

旁路服务端口：

- main site: `127.0.0.1:18764`
- stock: `127.0.0.1:18765`
- T12: `127.0.0.1:18766`

硬边界：

- `airivo.online/stock` 是唯一正式股票系统
- `airivo.online/apex/*` 只能用于内部验证与预发布
- 不得把 `apex` 当作并列正式入口对外传播

上线前先做 Apex 预检：

```bash
python /opt/airivo-apex/app/scripts/run_airivo_apex_preflight.py \
  --nginx-conf-dir /etc/nginx/conf.d \
  --nginx-conf-dir /etc/nginx/sites-enabled \
  --output /opt/airivo-apex/airivo_apex_preflight.json \
  --json
```

Apex nginx 片段必须放到 `/etc/nginx/snippets/airivo-apex.locations.conf`，再在现有 `airivo.online` TLS server block 内 include：

```nginx
include /etc/nginx/snippets/airivo-apex.locations.conf;
```

不要把 `nginx.airivo-apex.locations.conf` 直接放到 `/etc/nginx/conf.d/*.conf`，因为该文件只包含 `location` 指令，直接放入 `conf.d` 会导致 `nginx -t` 失败。

### 7.1 统一推进 `/stock` 主结果生命周期

常规情况下优先使用 lifecycle orchestrator，不要手动记忆 audit -> execution -> rollback -> observation 的顺序：

```bash
/opt/airivo-apex/.venv/bin/python /opt/airivo-apex/app/scripts/run_primary_result_lifecycle.py \
  --exp-dir /opt/airivo-apex/app/data/experiments \
  --max-source-age-hours 72 \
  --observation-status observing \
  --observation-reason "local observation window opened" \
  --observation-window-start "2026-04-15T09:30:00Z" \
  --output /opt/airivo-apex/app/data/experiments/primary_result_lifecycle_evidence_latest.json \
  --json
```

orchestrator 会生成或覆盖以下 per-result 工件：

- `primary_result_audit_latest.json`
- `primary_result_execution_latest.json`
- `primary_result_rollback_latest.json`
- `primary_result_observation_latest.json`
- `primary_result_lifecycle_evidence_latest.json`

证据文件会记录每一步的工件路径、hash、状态、stale artifact 检测结果和最终 API payload。`primary_result_terminal_latest.json` 不由 orchestrator 自动生成。

### 7.2 登记 `/stock` 主结果生命周期证据

orchestrator 通过后，必须登记为不可变 lifecycle snapshot。`current.json` 只表示当前指针，不保存完整历史；历史保存在 `history/*.json`，同一 lifecycle id 不允许覆盖。

```bash
/opt/airivo-apex/.venv/bin/python /opt/airivo-apex/app/scripts/register_primary_result_lifecycle.py \
  --lifecycles-dir /opt/airivo-apex/app/artifacts/primary_result_lifecycle \
  --evidence-json /opt/airivo-apex/app/data/experiments/primary_result_lifecycle_evidence_latest.json \
  --lifecycle-id primary-lifecycle-20260415-300757
```

登记脚本会拒绝：

- lifecycle evidence 非 `passed`
- 存在 blocking failure
- 缺少 audit/execution/rollback/observation 任一步
- step artifact 缺失 path/hash 或 exit_code 非 0
- final payload 未达到 `audit_status=passed`、`execution_status=ready`、`observation_status=observing|completed`

如需回滚到历史 lifecycle 指针：

```bash
/opt/airivo-apex/.venv/bin/python /opt/airivo-apex/app/scripts/register_primary_result_lifecycle.py \
  --lifecycles-dir /opt/airivo-apex/app/artifacts/primary_result_lifecycle \
  --rollback-lifecycle-id primary-lifecycle-20260415-300757
```

注意：该 rollback 只切换 primary result lifecycle registry 的 current pointer，不代表服务器版本回滚，也不执行交易或外部平台动作。

### 7.3 诊断：生成 `/stock` L3 主结果审核工件

Apex `/stock` 入口要从 L2 candidate 推进到 L3 audit，必须在服务器生成 per-result 审核工件：

```bash
/opt/airivo-apex/.venv/bin/python /opt/airivo-apex/app/scripts/run_primary_result_audit.py \
  --exp-dir /opt/airivo-apex/app/data/experiments \
  --max-source-age-hours 72 \
  --output /opt/airivo-apex/app/data/experiments/primary_result_audit_latest.json \
  --json
```

验证公开 API：

```bash
curl -s https://airivo.online/apex/stock/api/primary-result
```

通过后应看到：

- `result_lifecycle_stage`: `L3`
- `result_type`: `audit`
- `audit_status`: `passed`
- `history_source_file`: `primary_result_audit_latest.json`

### 7.4 诊断：生成 `/stock` L4 本地执行协议工件

Apex `/stock` 入口要从 L3 audit 推进到 L4 execution，必须在服务器生成 per-result execution 工件。该工件只表示本地执行协议 readiness，不表示已经接入券商或真实下单。

```bash
/opt/airivo-apex/.venv/bin/python /opt/airivo-apex/app/scripts/run_primary_result_execution.py \
  --exp-dir /opt/airivo-apex/app/data/experiments \
  --output /opt/airivo-apex/app/data/experiments/primary_result_execution_latest.json \
  --json
```

验证公开 API：

```bash
curl -s https://airivo.online/apex/stock/api/primary-result
```

通过后应看到：

- `result_lifecycle_stage`: `L4`
- `result_type`: `execution`
- `execution_status`: `ready`
- `history_source_file`: `primary_result_execution_latest.json`

### 7.5 诊断：生成 `/stock` L4 回滚决策工件

L4 execution ready 后，必须生成 per-result rollback decision。当前该工件只记录本地回滚协议判断，不执行外部交易回滚。

```bash
/opt/airivo-apex/.venv/bin/python /opt/airivo-apex/app/scripts/run_primary_result_rollback.py \
  --exp-dir /opt/airivo-apex/app/data/experiments \
  --output /opt/airivo-apex/app/data/experiments/primary_result_rollback_latest.json \
  --json
```

通过后应看到：

- `rollback_status`: `not_required`、`pending`、`triggered`、`completed` 或 `failed`
- `history_source_file`: `primary_result_rollback_latest.json`

### 7.6 诊断：生成 `/stock` L4 观察工件

L4 rollback decision 后，应进入 observation。该工件记录本地观察窗口状态，不接外部分析系统。

```bash
/opt/airivo-apex/.venv/bin/python /opt/airivo-apex/app/scripts/run_primary_result_observation.py \
  --exp-dir /opt/airivo-apex/app/data/experiments \
  --observation-status observing \
  --reason "local observation window opened" \
  --output /opt/airivo-apex/app/data/experiments/primary_result_observation_latest.json \
  --json
```

观察期完成后，可以显式更新为 completed：

```bash
/opt/airivo-apex/.venv/bin/python /opt/airivo-apex/app/scripts/run_primary_result_observation.py \
  --exp-dir /opt/airivo-apex/app/data/experiments \
  --observation-status completed \
  --reason "local observation window completed" \
  --window-start "2026-04-15T09:30:00Z" \
  --window-end "2026-04-20T09:30:00Z" \
  --observed-return 0.031 \
  --benchmark-return 0.012 \
  --max-drawdown -0.025 \
  --output /opt/airivo-apex/app/data/experiments/primary_result_observation_latest.json \
  --json
```

completed observation 必须带指标并通过本地完成条件；缺少指标会被记录为 `blocked`，指标不达标会被记录为 `failed`。

如果已有本地价格 CSV，可以用本地指标计算脚本完成观察期。CSV 必须包含：

- `ts_code`
- `trade_date`
- `close`

如果原始 CSV 不是 canonical 文件名，先导入为 canonical price history：

```bash
/opt/airivo-apex/.venv/bin/python /opt/airivo-apex/app/scripts/import_primary_result_price_history.py \
  --source-csv /opt/airivo-apex/app/data/experiments/source_primary_result_price_history.csv \
  --output-csv /opt/airivo-apex/app/data/experiments/primary_result_price_history_latest.csv \
  --manifest-output /opt/airivo-apex/app/data/experiments/primary_result_price_history_ingest_latest.json \
  --ts-code 300383.SZ \
  --benchmark-ts-code BENCHMARK \
  --window-start "2026-04-17T09:30:00Z" \
  --window-end "2026-04-20T15:00:00Z" \
  --source-label manual_local_csv \
  --json
```

如果价格证据来自服务器本地 SQLite，只允许只读导入，不允许脚本更新数据库：

```bash
/opt/airivo-apex/.venv/bin/python /opt/airivo-apex/app/scripts/import_primary_result_price_history_from_sqlite.py \
  --sqlite-db /opt/openclaw/permanent_stock_database.db \
  --sqlite-table daily_trading_data \
  --output-csv /opt/airivo-apex/app/data/experiments/primary_result_price_history_latest.csv \
  --manifest-output /opt/airivo-apex/app/data/experiments/primary_result_price_history_sqlite_ingest_latest.json \
  --ts-code 300383.SZ \
  --benchmark-ts-code 000001.SH \
  --window-start "2026-04-17T09:30:00Z" \
  --window-end "2026-04-20T15:00:00Z" \
  --source-label permanent_stock_database \
  --json
```

该 SQLite ingest 只有在 observed 与 benchmark 在窗口内都至少有两个价格点时，才会写 canonical CSV；否则只写 manifest 并返回 blocked。

导入前先检查市场数据是否已经足够关闭观察窗口：

```bash
/opt/airivo-apex/.venv/bin/python /opt/airivo-apex/app/scripts/inspect_primary_result_market_data_readiness.py \
  --sqlite-db /opt/openclaw/permanent_stock_database.db \
  --sqlite-table daily_trading_data \
  --ts-code 300383.SZ \
  --benchmark-ts-code 000001.SH \
  --window-start "2026-04-17T09:30:00Z" \
  --window-end "2026-04-20T15:00:00Z" \
  --output /opt/airivo-apex/app/data/experiments/primary_result_market_data_readiness_latest.json \
  --json
```

market data readiness 只诊断数据库覆盖，不更新行情、不导入 CSV、不关闭 observation。

也可以运行日终闭环编排器。它会按顺序执行 market data readiness、SQLite price import、price manifest、observation closure preflight、observation metrics、terminal outcome 和 performance ledger；任一前置门禁失败会立即停止：

```bash
/opt/airivo-apex/.venv/bin/python /opt/airivo-apex/app/scripts/run_primary_result_daily_closure.py \
  --sqlite-db /opt/openclaw/permanent_stock_database.db \
  --sqlite-table daily_trading_data \
  --exp-dir /opt/airivo-apex/app/data/experiments \
  --ts-code 300383.SZ \
  --benchmark-ts-code 000001.SH \
  --window-start "2026-04-17T09:30:00Z" \
  --window-end "2026-04-20T15:00:00Z" \
  --output /opt/airivo-apex/app/data/experiments/primary_result_daily_closure_latest.json \
  --json
```

先校验本地价格 CSV，并生成 manifest：

```bash
/opt/airivo-apex/.venv/bin/python /opt/airivo-apex/app/scripts/validate_primary_result_price_history.py \
  --price-history-csv /opt/airivo-apex/app/data/experiments/primary_result_price_history_latest.csv \
  --ts-code 300383.SZ \
  --benchmark-ts-code 000001.SH \
  --window-start "2026-04-17T09:30:00Z" \
  --window-end "2026-04-20T15:00:00Z" \
  --output /opt/airivo-apex/app/data/experiments/primary_result_price_history_manifest_latest.json \
  --json
```

正式写入 completed/failed observation 前，先运行只读 preflight：

```bash
/opt/airivo-apex/.venv/bin/python /opt/airivo-apex/app/scripts/inspect_primary_result_observation_closure.py \
  --exp-dir /opt/airivo-apex/app/data/experiments \
  --price-history-csv /opt/airivo-apex/app/data/experiments/primary_result_price_history_latest.csv \
  --price-history-manifest-json /opt/airivo-apex/app/data/experiments/primary_result_price_history_manifest_latest.json \
  --benchmark-ts-code 000001.SH \
  --window-end "2026-04-20T15:00:00Z" \
  --output /opt/airivo-apex/app/data/experiments/primary_result_observation_closure_preflight_latest.json \
  --json
```

preflight 只诊断，不写 observation、不记录 terminal、不登记 performance ledger。只有状态为 `ready_for_terminal_success`，才具备后续 terminal success 的前置条件。

示例：

```bash
/opt/airivo-apex/.venv/bin/python /opt/airivo-apex/app/scripts/run_primary_result_observation_metrics.py \
  --exp-dir /opt/airivo-apex/app/data/experiments \
  --price-history-csv /opt/airivo-apex/app/data/experiments/primary_result_price_history_latest.csv \
  --benchmark-ts-code 000001.SH \
  --window-end "2026-04-20T15:00:00Z" \
  --metrics-output /opt/airivo-apex/app/data/experiments/primary_result_observation_metrics_latest.json \
  --observation-output /opt/airivo-apex/app/data/experiments/primary_result_observation_latest.json \
  --json
```

该脚本会自动计算 `observed_return`、`benchmark_return`、`excess_return` 和 `max_drawdown`，再调用 observation 协议生成 completed/failed/blocked 结果。它仍然是本地协议，不代表接入外部行情源或自动交易系统。

### 7.7 登记 `/stock` 主结果长期绩效账本

观察期关闭后，应登记到 append-only performance ledger。该账本用于回答系统长期是否有效，不用于替代当前主结果页面。

```bash
/opt/airivo-apex/.venv/bin/python /opt/airivo-apex/app/scripts/register_primary_result_performance.py \
  --ledger-jsonl /opt/airivo-apex/app/artifacts/primary_result_performance/ledger.jsonl \
  --summary-json /opt/airivo-apex/app/artifacts/primary_result_performance/summary.json \
  --observation-json /opt/airivo-apex/app/data/experiments/primary_result_observation_latest.json \
  --json
```

登记脚本会拒绝：

- `observation_status=observing`
- observation window 未关闭
- 缺少 `observed_return`
- 缺少 `max_drawdown`
- 重复登记同一个 `result_id + window_ended_at + observation_status`

summary 会计算：

- entry_total
- success_total
- failed_total
- success_rate
- average_observed_return
- average_excess_return
- worst_max_drawdown

真实生产判断必须看 ledger 的长期样本，不得只看单次主结果。

### 7.8 生成 `/stock` 主结果失败归因

ledger 登记后，应对 failed observation 或 weak success 生成独立失败归因。归因不修改 ledger，只作为后续策略复盘和改进输入。

```bash
/opt/airivo-apex/.venv/bin/python /opt/airivo-apex/app/scripts/run_primary_result_failure_attribution.py \
  --observation-json /opt/airivo-apex/app/data/experiments/primary_result_observation_latest.json \
  --ledger-jsonl /opt/airivo-apex/app/artifacts/primary_result_performance/ledger.jsonl \
  --output /opt/airivo-apex/app/data/experiments/primary_result_failure_attribution_latest.json \
  --json
```

归因会拒绝仍处于 `observing` 的 observation。当前本地协议可识别：

- data_quality_failure
- risk_control_failure
- benchmark_underperformance
- negative_absolute_return
- market_drag
- source_risk_mismatch
- weak_source_signal
- weak_success
- unclassified_failure

如果结果是强成功，归因输出 `attribution_required=false`，这是正常状态，不应人为制造失败原因。

### 7.9 生成 `/stock` 主结果学习反馈

失败归因之后，可以生成 learning feedback。该工件只用于治理评审和后续 benchmark revalidation，不会自动修改策略、风控或 baseline。

```bash
/opt/airivo-apex/.venv/bin/python /opt/airivo-apex/app/scripts/run_primary_result_learning_feedback.py \
  --attribution-json /opt/airivo-apex/app/data/experiments/primary_result_failure_attribution_latest.json \
  --output /opt/airivo-apex/app/data/experiments/primary_result_learning_feedback_latest.json \
  --json
```

learning feedback 会输出：

- affected_module
- recommendation
- severity
- requires_baseline_revalidation
- evidence_category
- do_not_auto_apply

所有建议默认 `do_not_auto_apply=true`。任何影响候选选择、风控、执行时点或市场过滤的建议，都必须先进入 benchmark 与 baseline policy 流程，不能直接改生产规则。

### 7.10 入队 `/stock` 主结果学习反馈评审

learning feedback 生成后，应进入 review queue。队列记录当前状态，同时用 append-only decision history 保留每次入队和决策事件。

入队：

```bash
/opt/airivo-apex/.venv/bin/python /opt/airivo-apex/app/scripts/manage_primary_result_feedback_review_queue.py \
  --queue-dir /opt/airivo-apex/app/artifacts/primary_result_feedback_review_queue \
  --feedback-json /opt/airivo-apex/app/data/experiments/primary_result_learning_feedback_latest.json \
  --review-id primary-feedback-20260415-300757 \
  --owner reviewer
```

标记需要 benchmark：

```bash
/opt/airivo-apex/.venv/bin/python /opt/airivo-apex/app/scripts/manage_primary_result_feedback_review_queue.py \
  --queue-dir /opt/airivo-apex/app/artifacts/primary_result_feedback_review_queue \
  --review-id primary-feedback-20260415-300757 \
  --decision-status needs_benchmark \
  --reason "candidate selection and risk changes require benchmark validation" \
  --owner reviewer
```

允许的决策状态：

- accepted
- rejected
- needs_benchmark
- closed

注意：review queue 的 `accepted` 不等于生产生效。只要 feedback 带 `requires_baseline_revalidation=true`，必须先完成 benchmark、release gates 和 baseline policy，再考虑正式变更。

### 7.11 生成 `/stock` 主结果 benchmark plan

当 review item 状态为 `needs_benchmark` 时，应生成不可变 benchmark plan。该计划只定义验证范围和证据要求，不执行策略变更。

```bash
/opt/airivo-apex/.venv/bin/python /opt/airivo-apex/app/scripts/build_primary_result_benchmark_plan.py \
  --plans-dir /opt/airivo-apex/app/artifacts/primary_result_benchmark_plans \
  --review-item-json /opt/airivo-apex/app/artifacts/primary_result_feedback_review_queue/items/primary-feedback-20260415-300757.json \
  --plan-id primary-benchmark-plan-20260415-300757
```

benchmark plan 会拒绝：

- review item 不是 `needs_benchmark`
- `requires_baseline_revalidation` 不是 true
- `do_not_auto_apply` 不是 true
- 缺少 recommended changes
- 重复 plan id

计划会输出：

- affected_modules
- required_tests
- expected_evidence_artifacts
- release_gates_required
- baseline_policy_required
- current pointer
- immutable history snapshot

注意：benchmark plan 仍不是上线许可。它只是把“需要验证”变成正式验证计划。后续必须执行 benchmark report、benchmark diff、release gates、release evidence bundle，并满足 baseline policy。

### 7.12 执行 `/stock` 主结果 benchmark plan 测试

benchmark plan 生成后，可以执行 plan 中列出的 `required_tests` 并生成 execution evidence：

```bash
/opt/airivo-apex/.venv/bin/python /opt/airivo-apex/app/scripts/run_primary_result_benchmark_plan_execution.py \
  --plan-json /opt/airivo-apex/app/artifacts/primary_result_benchmark_plans/history/primary-benchmark-plan-20260415-300757.json \
  --output /opt/airivo-apex/app/artifacts/primary_result_benchmark_plan_execution_latest.json \
  --json
```

execution evidence 会记录：

- source_plan_hash
- command
- required_tests
- exit_code
- status
- stdout/stderr
- release_gates_required
- baseline_policy_required
- do_not_auto_apply

注意：该步骤只证明 plan 的 required tests 是否通过。它不是 release gate 总结果，不会执行 baseline promotion，不会应用策略或风控改动。

### 7.13 回写 benchmark execution 到 review queue

benchmark plan execution 生成后，可以把结果回写到对应 review item。该步骤只允许更新当前状态为 `needs_benchmark` 的 item：

- execution `passed` -> `accepted`
- execution `failed` -> `rejected`

```bash
/opt/airivo-apex/.venv/bin/python /opt/airivo-apex/app/scripts/apply_primary_result_benchmark_execution_decision.py \
  --execution-json /opt/airivo-apex/app/artifacts/primary_result_benchmark_plan_execution_latest.json \
  --queue-dir /opt/airivo-apex/app/artifacts/primary_result_feedback_review_queue \
  --actor benchmark_executor
```

回写会追加 decision history，并写入 execution evidence hash。注意：`accepted` 只表示 plan required tests 已通过，不代表已上线、不代表 baseline 已晋升。

### 7.14 生成 `/stock` 发布证据 checklist

当 review item 已回写为 `accepted` 后，应生成 release evidence checklist。该步骤只检查进入正式发布或 baseline promotion 讨论前的证据是否齐全，不执行上线、不晋升 baseline、不应用策略变更。

```bash
/opt/airivo-apex/.venv/bin/python /opt/airivo-apex/app/scripts/build_primary_result_release_evidence_checklist.py \
  --checklists-dir /opt/airivo-apex/app/artifacts/primary_result_release_evidence_checklists \
  --review-item-json /opt/airivo-apex/app/artifacts/primary_result_feedback_review_queue/items/primary-feedback-20260415-300757.json \
  --benchmark-report-json /opt/airivo-apex/app/artifacts/stock_primary_result_benchmark_report.json \
  --benchmark-diff-json /opt/airivo-apex/app/artifacts/stock_primary_result_benchmark_diff.json \
  --release-gates-json /opt/airivo-apex/app/artifacts/release_gates.json \
  --release-evidence-bundle-json /opt/airivo-apex/app/artifacts/release_evidence_bundle.json \
  --manifest-json /opt/airivo-apex/app/artifacts/release_pipeline_manifest.json \
  --baseline-policy-decision-json /opt/airivo-apex/app/artifacts/baseline_promotion_decision.json \
  --checklist-id primary-release-evidence-20260415-300757
```

checklist 会输出：

- `complete`：证据齐全，且未发现 blocking gate
- `incomplete`：缺 benchmark report、benchmark diff、release gates、evidence bundle、manifest 或 baseline policy decision
- `blocked`：release gates 出现阻断信号

注意：`complete` 仍不是上线许可。它只是说明 accepted review item 已具备进入正式发布评审或 baseline promotion 的最低证据条件。

### 7.15 生成 `/stock` release decision

当 checklist 状态为 `complete` 后，必须生成 release decision。该步骤是人工/制度批准工件，不执行上线、不晋升 baseline。

```bash
/opt/airivo-apex/.venv/bin/python /opt/airivo-apex/app/scripts/decide_primary_result_release.py \
  --decisions-dir /opt/airivo-apex/app/artifacts/primary_result_release_decisions \
  --checklist-json /opt/airivo-apex/app/artifacts/primary_result_release_evidence_checklists/history/primary-release-evidence-20260415-300757.json \
  --decision approved \
  --actor release_manager \
  --reason "all required release and baseline evidence is complete" \
  --decision-id primary-release-decision-20260415-300757
```

允许的 decision：

- `approved`：只允许 checklist 为 `complete` 时生成，并允许后续 baseline promotion
- `rejected`：用于记录阻断结论，不允许 baseline promotion

注意：baseline promotion 现在必须提供 approved release decision。没有 decision，或者 decision 不是 approved，promotion 会被拒绝。

### 7.16 显式记录 `/stock` L5 终局结论

只有观察期结束、人工复核或制度流程确认后，才允许记录 terminal outcome。不要为了清除 warning 自动生成终局。若 `terminal-outcome=success`，必须先有 `observation_status=completed`。

```bash
/opt/airivo-apex/.venv/bin/python /opt/airivo-apex/app/scripts/record_primary_result_terminal.py \
  --exp-dir /opt/airivo-apex/app/data/experiments \
  --terminal-outcome success \
  --reason "observation window completed in local protocol" \
  --output /opt/airivo-apex/app/data/experiments/primary_result_terminal_latest.json \
  --json
```

允许的 `terminal-outcome`：

- `success`
- `failed`
- `expired`
- `superseded`
- `rejected`
- `cancelled`
- `archived`

通过后公开 API 应看到：

- `result_lifecycle_stage`: `L5`
- `result_type`: `archive`
- `terminal_outcome`: 对应显式记录值
- `history_source_file`: `primary_result_terminal_latest.json`

### 7.17 生成 `/stock` production readiness ledger

baseline promotion、terminal outcome 和 performance ledger 都完成后，可以生成 production readiness ledger。该步骤只生成证据结论，不自动上线、不自动交易、不修改策略。

正式生成前，先运行 preflight 查看真实缺口：

```bash
/opt/airivo-apex/.venv/bin/python /opt/airivo-apex/app/scripts/inspect_primary_result_production_readiness.py \
  --release-decision-current-json /opt/airivo-apex/app/artifacts/primary_result_release_decisions/current.json \
  --baseline-current-json /opt/airivo-apex/app/artifacts/baselines/current.json \
  --terminal-json /opt/airivo-apex/app/data/experiments/primary_result_terminal_latest.json \
  --performance-ledger-jsonl /opt/airivo-apex/app/artifacts/primary_result_performance/ledger.jsonl \
  --performance-summary-json /opt/airivo-apex/app/artifacts/primary_result_performance/summary.json \
  --output /opt/airivo-apex/app/artifacts/primary_result_production_readiness/preflight_latest.json \
  --json
```

preflight 只诊断，不写 readiness history，不切换 current pointer。

```bash
/opt/airivo-apex/.venv/bin/python /opt/airivo-apex/app/scripts/build_primary_result_production_readiness.py \
  --readiness-dir /opt/airivo-apex/app/artifacts/primary_result_production_readiness \
  --release-decision-json /opt/airivo-apex/app/artifacts/primary_result_release_decisions/history/primary-release-decision-20260415-300757.json \
  --baseline-current-json /opt/airivo-apex/app/artifacts/baselines/current.json \
  --terminal-json /opt/airivo-apex/app/data/experiments/primary_result_terminal_latest.json \
  --performance-ledger-jsonl /opt/airivo-apex/app/artifacts/primary_result_performance/ledger.jsonl \
  --performance-summary-json /opt/airivo-apex/app/artifacts/primary_result_performance/summary.json \
  --readiness-id primary-production-readiness-20260415-300757
```

readiness 状态：

- `ready`：approved decision、baseline snapshot、terminal success、performance latest success 全部成立
- `blocked`：存在阻断原因，脚本返回非零退出码

注意：`ready` 仍不是自动上线命令。它只是最终证据账本，供人工或制度流程确认。
