#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
完整修复V001系统缩进问题
"""

def fix_v001_complete():
    """完整修复V001系统的缩进和语法问题"""
    
    filename = '完整V001_13模块_智能缓存增强系统.py'
    
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # 备份原文件
        backup_filename = f'{filename}.backup_complete_fix_{int(__import__("time").time())}'
        with open(backup_filename, 'w', encoding='utf-8') as f:
            f.write(content)
        print(f"原文件已备份为: {backup_filename}")
        
        # 分行处理
        lines = content.split('\n')
        fixed_lines = []
        
        in_try_block = False
        try_indent_level = 0
        
        for i, line in enumerate(lines):
            line_num = i + 1
            
            # 检测try块开始
            if 'try:' in line and line.strip().endswith('try:'):
                in_try_block = True
                try_indent_level = len(line) - len(line.lstrip())
                fixed_lines.append(line)
                continue
            
            # 在try块内部处理缩进
            if in_try_block:
                if line.strip() == '':
                    fixed_lines.append(line)
                    continue
                    
                current_indent = len(line) - len(line.lstrip())
                
                # 检测except块开始
                if line.strip().startswith('except '):
                    # except应该与try对齐
                    fixed_line = ' ' * try_indent_level + line.strip()
                    fixed_lines.append(fixed_line)
                    in_try_block = False
                    continue
                
                # 修复特定的问题行
                if 'if strategy ==' in line or 'elif strategy ==' in line:
                    # if/elif应该在try块内缩进
                    fixed_line = ' ' * (try_indent_level + 4) + line.strip()
                    fixed_lines.append(fixed_line)
                elif 'data = get_' in line and 'strategy' not in line:
                    # 数据获取语句应该在if/elif块内缩进
                    fixed_line = ' ' * (try_indent_level + 8) + line.strip()
                    fixed_lines.append(fixed_line)
                elif 'st.success(' in line:
                    # success语句应该在if/elif块内缩进
                    fixed_line = ' ' * (try_indent_level + 8) + line.strip()
                    fixed_lines.append(fixed_line)
                elif line.strip().startswith('else:'):
                    # else应该与if/elif对齐
                    fixed_line = ' ' * (try_indent_level + 4) + line.strip()
                    fixed_lines.append(fixed_line)
                elif 'self.put_cached_data' in line or 'return data' in line:
                    # 这些语句应该在try块内缩进
                    fixed_line = ' ' * (try_indent_level + 4) + line.strip()
                    fixed_lines.append(fixed_line)
                else:
                    fixed_lines.append(line)
            else:
                fixed_lines.append(line)
        
        # 写入修复后的内容
        fixed_content = '\n'.join(fixed_lines)
        
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(fixed_content)
        
        print(f"✅ V001系统缩进问题修复完成: {filename}")
        return True
        
    except Exception as e:
        print(f"❌ 修复失败: {e}")
        return False

if __name__ == "__main__":
    fix_v001_complete()
