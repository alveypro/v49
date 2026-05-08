# STOCK PRIMARY RESULT LEGACY RETIREMENT PLAN

## Current Status

`/stock` legacy 主结果运行路径已退场。

- canonical 已成为唯一运行主路径
- legacy / shadow / fallback_legacy 不再作为运行语义存在
- 回滚依赖版本机制，不依赖常驻旧路径

## Completed Items

- legacy 运行模式入口已移除
- canonical-only readiness 已满足
- runtime metadata 已收敛到 canonical 单轨
- content quality / layout contract / integration 继续由 canonical 测试覆盖

## Ongoing Requirement

- 后续不再接受 legacy 恢复型开发
- `/stock` 单轨运行必须继续满足 canonical 内容质量与布局契约
