# AIRIVO MAIN SITE CONVERSION PATH SPEC

## Purpose

本文件固定主站到 `/stock` 的转化路径规则。

## Core Rules

- 主站首屏主 CTA 必须指向 `/stock`
- `/stock` 必须是产品矩阵中的最高权重入口
- `/T12` 只能作为治理与边界系统的辅助入口
- 主站不承载业务主结果判断

## Conversion Priority

1. 首屏 Hero 主 CTA 指向 `/stock`
2. 产品矩阵中 `/stock` 卡片保持主系统权重
3. `/T12` 只承担辅助跳转，不争夺主入口

## Scope Guard

- 主站不得复制 `/stock` 主结果结构
- 主站不得复制 `/T12` 治理摘要结构
- 主站不得输出业务主判断
