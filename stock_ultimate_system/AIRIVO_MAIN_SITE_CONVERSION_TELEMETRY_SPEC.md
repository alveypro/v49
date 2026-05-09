# AIRIVO_MAIN_SITE_CONVERSION_TELEMETRY_SPEC.md

## 主站首页最小转化观测规范

### 1. 目标

主站首页只承担平台表达、产品矩阵组织与入口分发，因此观测面也只覆盖入口转化，不覆盖业务主判断。

### 2. 稳定观测钩子

首页必须稳定提供以下 `data-airivo-hook`：

- `main-site-hero`
- `main-site-hero-panel`
- `main-site-cta-row`
- `main-site-primary-cta`
- `main-site-secondary-cta`
- `main-site-product-matrix`
- `main-site-stock-card`
- `main-site-stock-card-link`
- `main-site-t12-card`
- `main-site-t12-card-link`

### 3. 观测语义

- `main-site-hero`
  用于识别首页首屏曝光区域
- `main-site-primary-cta`
  用于识别导向 `/stock` 的主转化入口
- `main-site-stock-card`
  用于识别产品矩阵中 `/stock` 主系统卡片曝光
- `main-site-t12-card`
  用于识别 `/T12` 辅助入口曝光

### 4. 作用域约束

- `/stock` 必须保持主入口权重
- `/T12` 只能作为辅助入口
- 主站不得承载业务主结果判断
- 观测钩子仅用于结构与转化观测，不得演变为交互控制台能力

### 5. 阻断级问题

以下情况属于阻断级问题：

- `main-site-primary-cta` 不再指向 `/stock`
- `/stock` 主入口钩子缺失
- `/T12` 辅助入口钩子缺失且替代了 `/stock` 主入口
- 首页出现业务主结果判断文本
