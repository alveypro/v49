# AIRIVO_MAIN_SITE_CONVERSION_EVENT_SPEC.md

## 主站转化事件契约

### 1. 目标

固定主站首页的最小转化事件契约，用于持续导流观测与结构稳定性验证。

### 2. 稳定事件

- `main_site_hero_impression`
- `main_site_primary_cta_click`
- `main_site_stock_card_click`
- `main_site_t12_card_click`

### 3. 最小字段

每个事件至少固定：

- `event_name`
- `hook`
- `target_path`
- `entry_role`

### 4. 角色约束

- `/stock` 相关事件必须为 `primary`
- `/T12` 相关事件必须为 `auxiliary`
- hero 曝光事件为 `platform`

### 5. 边界

- 主站事件只服务于入口和转化观测
- 主站仍不承载业务主结果判断
- `/T12` 仍是辅助入口，不扩成交互控制台
