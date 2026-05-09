# STOCK PRIMARY RESULT BENCHMARK SPEC

## Purpose

本文件定义 `/stock` 主结果 benchmark / golden set 的样本体系。

目标：

- 用固定样本持续验证 canonical 单轨主结果是否稳定增强
- 为后续长期优化建立可重复的基准面

## Sample Categories

benchmark / golden set 至少覆盖：

- 正常态
- 空态
- 降级态
- 禁用/失效态
- 噪声字段输入
- 治理污染输入
- 主站叙事污染输入

## Core Dimensions

每类样本至少验证以下维度：

- 结论层清晰度
- 解释层受控程度
- 边界层轻量程度
- 语言统一性
- 禁止污染项

## Blocking Regressions

以下退化属于阻断级问题：

- 结论层为空或被解释层压过
- 解释层失控膨胀
- 边界层演变成 `/T12` 替身
- 治理摘要词汇进入 `/stock`
- 主站平台叙事进入 `/stock`
- 单轨 canonical 输出失稳
