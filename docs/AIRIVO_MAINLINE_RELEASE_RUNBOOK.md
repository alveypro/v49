# Airivo Mainline Release Runbook

日期：2026-04-30  
适用范围：`airivo.online` 主入口 `v49_app.py`  
目标：统一为一条可审计、可回滚、可追版本的发布链路

## Canonical Path

唯一标准路径：

1. 本地 `main` 提交完成
2. `git push origin main`
3. 服务器 `/opt/openclaw/app` fast-forward 到 `origin/main`
4. `systemctl restart openclaw-streamlit.service`
5. smoke 验收通过

禁止把“手工 scp 文件同步”当作常规发布路径。  
它只能用于事故处置或紧急修复。

## One-Time Bootstrap

如果服务器目录还不是 Git worktree，先执行：

```bash
bash tools/bootstrap_server_git_worktree.sh
```

该脚本只做安全引导：

- `git init -b main`
- `git remote add origin`
- `git fetch origin main`
- `git reset --mixed origin/main`

它不会覆盖当前工作树文件。  
如果服务器现有文件和 `origin/main` 有差异，会以 `git status` 的脏状态显式暴露出来，必须先治理，不允许带病进入标准发布。

## Standard Release

标准发布入口：

```bash
bash tools/release_airivo_mainline.sh
```

它会执行：

1. 本地 `release_gate`
2. 本地 `push origin main`
3. 确认服务器 Git worktree 已建立
4. `audit_airivo_mainline_drift.sh`
5. 服务器 `git fetch origin main && git merge --ff-only origin/main`
6. `systemctl restart openclaw-streamlit.service`
7. `smoke_airivo_main_entry.sh`

## Canonical Mainline Audit

主入口白名单清单：

- `tools/production/airivo_mainline_files.txt`

审计脚本：

```bash
bash tools/audit_airivo_mainline_drift.sh
```

通过标准：

- 白名单文件本地与服务器哈希一致
- `openclaw-streamlit.service` 的 `ExecStart` 指向 `/opt/openclaw/app/v49_app.py`
- 主入口 runtime 不存在远端根目录 duplicate

未通过时：

- 禁止进入标准发布
- 先做 reconciliation
- 不允许用“顺手 scp 一个文件”掩盖主线漂移

## Smoke Acceptance

验收脚本：

```bash
bash tools/smoke_airivo_main_entry.sh
```

当前最小验收内容：

- `openclaw-streamlit.service` 为 `active`
- `http://127.0.0.1:8501/_stcore/health` 返回 `ok`
- `https://airivo.online` 返回 `HTTP 200`
- 部署文件中存在关键文案：
  - `批次概览`
  - `为什么是这个结论`
  - `技术明细（调试）`
- 异步扫描后台链路 smoke：
  - 真实发起 `v5 / v8 / v9`
  - `state.json` 必须观察到 `running -> success`
  - `row_count > 0`
  - `result_csv` 文件存在
- 主入口页面链路 smoke：
  - 用 `AppTest` 打开 `v49_app.py`
  - 进入 `今日决策` 对应策略页
  - 任务区必须显示 `状态=success`
  - 任务区必须显示 `结果数 > 0`
  - 页面必须出现“后台扫描完成”成功态

### Smoke Layers

当前 `bash tools/smoke_airivo_main_entry.sh` 已经是三层验收，不再只是健康探针：

1. 服务层
- `systemctl is-active`
- `/_stcore/health`
- `https://airivo.online` 基础 HTTP 可达

2. 后台任务层
- `tools/async_scan_smoke.py`
- 检查异步扫描任务链路是否真的完成
- 这层失败，优先看：
  - `logs/openclaw/async_scan/*.state.json`
  - 对应 `stdout.log / stderr.log`
  - 数据库 / 扫描参数 / worker 进程

3. 页面展示层
- `tools/ui_async_task_smoke.py`
- 检查主入口任务区是否把成功状态正确展示给操作者
- 这层失败，优先看：
  - `v49_app.py` 路由与 fragment 刷新
  - `openclaw/runtime/async_task_ui.py`
  - `session_state` 绑定的 `*_async_task_id`
  - 最新成功 `run_id` 是否被页面正确读取

### Failure Triage

失败时不要混在一起看，按层定位：

1. 服务层失败
- 先看 `systemctl status openclaw-streamlit.service`
- 再看 `journalctl -u openclaw-streamlit.service -n 200`
- 这时不要先怀疑策略逻辑

2. 后台任务层失败
- 说明 worker 没有稳定走到 `success`
- 先看对应策略的：
  - `.state.json`
  - `.stdout.log`
  - `.stderr.log`
- 再决定是参数、数据、还是扫描实现问题

3. 页面展示层失败
- 说明后台任务成功了，但主入口没有正确显示
- 先比对：
  - smoke 用的 `run_id`
  - 页面 `task_key`
  - 页面指标 `状态 / 结果数`
- 这时优先排查 UI 会话与状态恢复，不要先改扫描引擎

## Rollback

回滚原则：

1. 回到上一个明确 commit
2. 服务器只接受 fast-forward 或显式回退到指定 commit
3. 回滚后必须重新跑 smoke

事故期如需紧急文件覆盖：

- 必须先记录目标文件
- 必须保留远端备份目录
- 事后必须把服务器重新拉回 Git 主线

## Current Reality

当前已确认事实：

- 线上主入口服务是 `openclaw-streamlit.service`
- 线上 main app 是 `/opt/openclaw/app/v49_app.py`
- 线上 canary 服务独立于主入口，不应混入主线发布

因此主线发布、恢复、验收都必须围绕这三个对象展开，而不是再回退到旧版 `终极量价暴涨系统_v49.0_长期稳健版.py` 启动链路。
