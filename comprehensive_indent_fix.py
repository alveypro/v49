#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
全面修复V001文件的缩进问题
"""

import re
import shutil
import ast

def fix_indentation_comprehensive(file_path):
    # 备份原文件
    backup_path = file_path + '.backup_comprehensive'
    shutil.copy2(file_path, backup_path)
    print(f"已备份原文件到: {backup_path}")
    
    with open(file_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()
    
    fixed_lines = []
    in_class = False
    in_method = False
    method_indent = 0
    class_indent = 0
    
    for i, line in enumerate(lines):
        original_line = line
        stripped = line.strip()
        
        # 跳过空行和注释
        if not stripped or stripped.startswith('#'):
            fixed_lines.append(line)
            continue
            
        # 检测类定义
        if stripped.startswith('class '):
            in_class = True
            class_indent = 0
            in_method = False
            fixed_lines.append(stripped + '\n')
            continue
            
        # 检测方法定义
        if in_class and stripped.startswith('def '):
            in_method = True
            method_indent = 4  # 类内方法缩进4个空格
            fixed_lines.append(' ' * method_indent + stripped + '\n')
            continue
            
        # 在方法内部
        if in_method:
            # 方法内的基本缩进是8个空格
            base_indent = 8
            
            # 特殊处理各种语句
            if any(stripped.startswith(x) for x in ['if ', 'elif ', 'else:', 'for ', 'while ', 'with ', 'try:', 'except', 'finally:']):
                # 控制流语句保持8个空格
                fixed_lines.append(' ' * base_indent + stripped + '\n')
            elif stripped.startswith('st.') or stripped.startswith('cache_') or '=' in stripped:
                # 检查是否在控制流内部
                # 简单检查前面几行是否有控制流语句
                in_control_block = False
                for j in range(max(0, i-5), i):
                    prev_stripped = lines[j].strip()
                    if any(prev_stripped.startswith(x) for x in ['if ', 'elif ', 'else:', 'for ', 'while ', 'with ', 'try:', 'except']):
                        if prev_stripped.endswith(':'):
                            in_control_block = True
                            break
                
                if in_control_block:
                    # 在控制流内部，使用12个空格
                    fixed_lines.append(' ' * 12 + stripped + '\n')
                else:
                    # 普通语句，使用8个空格
                    fixed_lines.append(' ' * base_indent + stripped + '\n')
            else:
                # 其他语句，使用8个空格
                fixed_lines.append(' ' * base_indent + stripped + '\n')
        else:
            # 不在方法内，保持原样
            fixed_lines.append(line)
    
    # 写入修复后的文件
    with open(file_path, 'w', encoding='utf-8') as f:
        f.writelines(fixed_lines)
    
    print(f"缩进修复完成")
    
    # 验证语法
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        ast.parse(content)
        print("✅ 语法检查通过")
        return True
    except SyntaxError as e:
        print(f"❌ 语法错误: {e}")
        print(f"错误位置: 第{e.lineno}行")
        return False

if __name__ == "__main__":
    file_path = "完整V001_13模块_智能缓存增强系统.py"
    fix_indentation_comprehensive(file_path)
