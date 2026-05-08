# APEX_NAMESPACE_SPEC

## Airivo Apex Namespace 规范

版本：v1.0  
状态：冻结版  
性质：入口与命名空间规范  
适用范围：`airivo.online/apex`、`airivo.online/apex/stock`、`airivo.online/apex/T12`

---

## 1. 文档目的

本文件用于明确 `airivo.online/apex` 的性质，防止后续把：

- system
- entry
- namespace

三种概念混为一谈。

`/apex` 的存在被允许，但它不是新的正式产品系统。

---

## 2. 结论先行

`/apex` 是 Airivo 的 namespace，不是第四个 system。

Airivo 的正式产品系统仍固定为：

- `airivo.online`
- `airivo.online/stock`
- `airivo.online/T12`

`/apex` 只表示内部验证、预发布、灰度或旁路复核 namespace，用于承载上述三系统在独立路径空间中的运行版本。

---

## 3. namespace 与 system 的关系

### 3.1 system

system 是长期产品角色，拥有独立职责定义。

当前正式 system 只有三类：

1. main site
2. stock
3. T12

### 3.2 namespace

namespace 是路径和运行空间，不新增产品职责。

当前允许的 namespace：

1. `production`
2. `apex`

### 3.3 映射关系

`production` namespace:

- `airivo.online/` -> main site
- `airivo.online/stock/` -> stock
- `airivo.online/T12/` -> T12

`apex` namespace:

- `airivo.online/apex/` -> main site 内部验证入口
- `airivo.online/apex/stock/` -> stock 内部验证入口
- `airivo.online/apex/T12/` -> T12 内部验证入口

结论：

- `/apex` 不新增新的 system
- `/apex/stock` 不新增新的 stock 角色
- `/apex/T12` 不新增新的 T12 角色

---

## 4. `/apex` 允许做什么

- 提供内部验证入口
- 提供灰度或预发布入口
- 提供部署前后并行运行空间
- 保持与正式三系统相同的角色映射
- 允许使用不同端口、日志目录和 nginx path 前缀

---

## 5. `/apex` 禁止做什么

- 禁止把 `/apex` 定义成第四产品系统
- 禁止给 `/apex` 添加新的长期产品职责
- 禁止让 `/apex` 拥有独立治理解释权
- 禁止让 `/apex` 取代 `airivo.online/` 正式主入口
- 禁止让 `/apex` 在产品矩阵中与 main site、`/stock`、`/T12` 并列为第四成员

---

## 6. 文案要求

`/apex` 页面可以存在，但其文案必须显式表明：

- 这是 namespace 入口
- 这是内部验证 / 预发布空间
- 这不是新的正式产品系统
- 这不是正式生产主入口

禁止在 `/apex` 页面使用以下表达：

- “第四系统”
- “独立主系统”
- “新的核心产品”

---

## 7. 代码与门禁要求

代码中不得把 namespace 语义命名成独立产品角色。

例如：

- `apex_home_semantics`

这类命名必须改成更中性的 namespace-aware 命名，避免把 `apex` 误表达为正式 system。

发布门禁必须额外确认：

- 正式 system 仍只有三类
- `/apex` 仍被定义为 namespace
- `/apex` 文案未升级成第四系统表达
- `/apex` 仍保持与 production namespace 相同的 scope 映射

---

## 8. 最终不变量

Airivo 长期保持：

- 三个正式产品系统
- 一个可选内部验证 namespace

即：

- system = `main_site / stock / T12`
- namespace = `production / apex`

任何试图把 `/apex` 升格成第四 system 的变更，均视为结构漂移。
