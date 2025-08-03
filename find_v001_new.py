#!/usr/bin/env python3
import os
import shutil

# 查找完整V001文件
files = []
for f in os.listdir('.'):
    if '完整V001' in f and f.endswith('.py'):
        files.append(f)
        print(f"找到文件: {f}")

# 显示所有找到的文件
if files:
    print("\n可用文件:")
    for i, f in enumerate(files):
        print(f"  {i+1}. {f}")
    
    # 尝试找到最好的备份文件
    best_file = None
    for f in files:
        if 'backup_fix' in f and '完整V001_13模块_智能缓存增强系统' in f:
            best_file = f
            break
    
    if not best_file:
        # 如果没有找到backup_fix，找任何backup文件
        for f in files:
            if 'backup' in f and '完整V001_13模块_智能缓存增强系统' in f:
                best_file = f
                break
    
    if not best_file:
        # 如果没有backup文件，找主文件
        for f in files:
            if f == '完整V001_13模块_智能缓存增强系统.py':
                best_file = f
                break
    
    if best_file:
        target = '完整V001_13模块_智能缓存增强系统.py'
        if best_file != target:
            print(f"\n复制 {best_file} -> {target}")
            shutil.copy2(best_file, target)
            print(f"复制完成！")
        else:
            print(f"\n主文件已存在: {target}")
    else:
        print("\n未找到合适的文件")
else:
    print("未找到任何完整V001文件")
