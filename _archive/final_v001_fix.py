#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
最终修复V001系统缩进问题
"""

def final_fix_v001():
    """最终修复V001系统的所有缩进和语法问题"""
    
    filename = '完整V001_13模块_智能缓存增强系统.py'
    
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # 备份原文件
        import time
        backup_filename = f'{filename}.backup_final_fix_{int(time.time())}'
        with open(backup_filename, 'w', encoding='utf-8') as f:
            f.write(content)
        print(f"原文件已备份为: {backup_filename}")
        
        # 分行处理
        lines = content.split('\n')
        fixed_lines = []
        
        for i, line in enumerate(lines):
            line_num = i + 1
            
            # 修复第316行的缩进错误
            if line_num == 316 and 'if cached_data is not None:' in line:
                # 这行应该有8个空格的缩进
                fixed_lines.append('        if cached_data is not None:')
                continue
            
            # 修复第318行的缩进错误
            if line_num == 318 and 'return cached_data' in line:
                # 这行应该有12个空格的缩进
                fixed_lines.append('            return cached_data')
                continue
            
            # 修复try块的缩进问题
            if 'try:' in line and line.strip() == 'try:':
                # 确保try有正确的缩进
                base_indent = len(line) - len(line.lstrip())
                fixed_lines.append(line)
                continue
            
            # 修复if/elif/else语句的缩进
            if ('if strategy ==' in line or 'elif strategy ==' in line) and 'try:' not in line:
                # 这些应该在try块内，缩进12个空格
                fixed_lines.append('            ' + line.strip())
                continue
            
            # 修复data获取语句的缩进
            if 'data = get_' in line and 'strategy' not in line:
                # 这些应该在if/elif块内，缩进16个空格
                fixed_lines.append('                ' + line.strip())
                continue
            
            # 修复st.success语句的缩进
            if 'st.success(' in line and '机构数据模块' in line:
                # 这些应该在if/elif块内，缩进16个空格
                fixed_lines.append('                ' + line.strip())
                continue
            
            # 修复else语句的缩进
            if line.strip() == 'else:' and i > 300:
                # else应该与if/elif对齐，缩进12个空格
                fixed_lines.append('            else:')
                continue
            
            # 修复except语句的缩进
            if 'except Exception as e:' in line:
                # except应该与try对齐，缩进8个空格
                fixed_lines.append('        except Exception as e:')
                continue
            
            # 修复return语句的缩进
            if 'return pd.DataFrame()' in line and i > 300:
                # return应该在except块内，缩进12个空格
                fixed_lines.append('            return pd.DataFrame()')
                continue
            
            # 修复self.put_cached_data的缩进
            if 'self.put_cached_data' in line and i > 300:
                # 这应该在try块内，缩进12个空格
                fixed_lines.append('            ' + line.strip())
                continue
            
            # 修复return data的缩进
            if 'return data' in line and i > 300 and 'return pd.DataFrame()' not in line:
                # 这应该在try块内，缩进12个空格
                fixed_lines.append('            return data')
                continue
            
            # 其他行保持不变
            fixed_lines.append(line)
        
        # 写入修复后的内容
        fixed_content = '\n'.join(fixed_lines)
        
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(fixed_content)
        
        print(f"✅ V001系统缩进问题最终修复完成: {filename}")
        return True
        
    except Exception as e:
        print(f"❌ 修复失败: {e}")
        return False

if __name__ == "__main__":
    final_fix_v001()
