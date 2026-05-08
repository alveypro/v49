# Airivo 服务器补齐最小变更清单

日期：2026-04-29  
适用范围：`RC V1` 发布前服务器一致性补齐  
目标主机：`root@47.90.160.87`  
目标目录：`/opt/openclaw/app`

## 结论

当前不允许进入 canary。  
原因不是 SSH，而是服务器与本地 `RC V1` 存在已确认的真实不一致。

## 已确认差异

以下 4 个文件为 `DIFF`，必须先补齐：

1. `v49_app.py`
2. `openclaw/run_daily.py`
3. `openclaw/scripts_run_daily.sh`
4. `tools/openclaw_partner_daily_run.sh`

以下 1 个对象为远端重复文件，必须处理：

1. 远端根目录重复文件：`/opt/openclaw/app/strategy_tracking.py`

说明：
- canonical 路径已存在：`/opt/openclaw/app/openclaw/strategy_tracking.py`
- 根目录重复文件不应继续存在，否则会持续破坏主线唯一性判断

## 最小补齐原则

只做下面 5 个对象，不扩大范围：

1. `v49_app.py`
2. `openclaw/run_daily.py`
3. `openclaw/scripts_run_daily.sh`
4. `tools/openclaw_partner_daily_run.sh`
5. 远端 duplicate `strategy_tracking.py`

禁止顺手补别的文件，禁止扩成整仓同步，禁止引入新旁路。

## 执行顺序

### 第 1 步：先备份远端 5 个对象

至少备份：

- `/opt/openclaw/app/v49_app.py`
- `/opt/openclaw/app/openclaw/run_daily.py`
- `/opt/openclaw/app/openclaw/scripts_run_daily.sh`
- `/opt/openclaw/app/tools/openclaw_partner_daily_run.sh`
- `/opt/openclaw/app/strategy_tracking.py`

要求：
- 备份目录带时间戳
- 保证回滚时能原位恢复

### 第 2 步：覆盖 4 个真实差异文件

只同步：

- `v49_app.py`
- `openclaw/run_daily.py`
- `openclaw/scripts_run_daily.sh`
- `tools/openclaw_partner_daily_run.sh`

要求：
- 保留脚本可执行位
- 不做无关重启
- 不混入 deploy / migrations / kernel / exports

### 第 3 步：处理远端 duplicate

处理对象：

- `/opt/openclaw/app/strategy_tracking.py`

动作要求：
- 不保留在主目录
- 如需保险，先移动到备份目录
- 最终以 `openclaw/strategy_tracking.py` 作为唯一 canonical

### 第 4 步：重跑一致性检查

执行：

```bash
bash tools/check_local_server_consistency.sh
```

通过标准：
- 4 个差异文件全部变成 `OK`
- duplicate 检查变成 `OK no root duplicates`
- 整体输出 `PASSED`

### 第 5 步：一致性通过后才允许进入 canary

未通过前：
- 不进入 canary
- 不做正式发布
- 不做额外补丁式上线

## Go / No-Go

### Go

只有同时满足以下条件才允许进入 canary：

1. 4 个差异文件已补齐
2. `strategy_tracking.py` duplicate 已清理
3. 一致性脚本输出 `PASSED`
4. 本地主线门禁仍通过

### No-Go

任一条件不满足即禁止进入 canary：

1. 差异文件未全部补齐
2. duplicate 未清理
3. 一致性脚本未通过
4. 为图快而扩大同步范围

## 一句话执行口径

先补齐 4 个真实差异文件，再清理 1 个远端 duplicate，只有一致性检查通过后，才允许进入 canary。
