#!/usr/bin/env python3
import os
import shutil

# 查找完整V001文件
files = []
for f in os.listdir('.'):
    if '完整V001' in f and f.endswith('.py'):
        files.append(f)
        print(f"找到文件: {f}")

# 如果找到备份文件，复制为主文件
for f in files:
    if 'backup' in f and '完整V001_13模块_智能缓存增强系统' in f:
        target = '完整V001_13模块_智能缓存增强系统.py'
        print(f"复制 {f} -> {target}")
        shutil.copy2(f, target)
        print(f"复制完成！")
        break
else:
    print("未找到合适的备份文件")
    if files:
        print("可用文件:")
        for f in files:
            print(f"  - {f}")
