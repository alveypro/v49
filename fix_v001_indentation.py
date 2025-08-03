#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
V001系统缩进修复脚本
"""

import re

def fix_indentation_errors():
    """修复V001系统文件中的缩进错误"""
    
    filename = '完整V001_13模块_智能缓存增强系统.py'
    
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # 修复已知的缩进问题
        lines = content.split('\n')
        fixed_lines = []
        
        for i, line in enumerate(lines):
            line_num = i + 1
            
            # 修复第316行附近的缩进问题
            if 'if cached_data is not None:' in line and line.startswith('                        '):
                # 修正为正确的缩进（8个空格）
                fixed_lines.append('        if cached_data is not None:')
                print(f"修复第{line_num}行缩进")
            elif 'return cached_data' in line and line.startswith('                            '):
                # 修正为正确的缩进（12个空格）
                fixed_lines.append('            return cached_data')
                print(f"修复第{line_num}行缩进")
            elif 'if strategy == "surge":' in line and not line.startswith('        '):
                # 修正if语句缩进
                fixed_lines.append('            if strategy == "surge":')
                print(f"修复第{line_num}行缩进")
            elif 'data = get_data_for_short_term_surge' in line and not line.startswith('            '):
                # 修正赋值语句缩进
                fixed_lines.append('                data = get_data_for_short_term_surge(count=count)')
                print(f"修复第{line_num}行缩进")
            elif 'elif strategy == "value":' in line and not line.startswith('            '):
                # 修正elif语句缩进
                fixed_lines.append('            elif strategy == "value":')
                print(f"修复第{line_num}行缩进")
            elif 'data = get_data_for_value_mining' in line and not line.startswith('            '):
                # 修正赋值语句缩进
                fixed_lines.append('                data = get_data_for_value_mining(count=count)')
                print(f"修复第{line_num}行缩进")
            else:
                fixed_lines.append(line)
        
        # 写入修复后的内容
        fixed_content = '\n'.join(fixed_lines)
        
        # 备份原文件
        backup_filename = f'{filename}.backup_indentation_fix_{int(__import__("time").time())}'
        with open(backup_filename, 'w', encoding='utf-8') as f:
            f.write(content)
        print(f"原文件已备份为: {backup_filename}")
        
        # 写入修复后的文件
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(fixed_content)
        
        print(f"✅ 缩进错误修复完成: {filename}")
        return True
        
    except Exception as e:
        print(f"❌ 修复失败: {e}")
        return False

if __name__ == "__main__":
    fix_indentation_errors()
