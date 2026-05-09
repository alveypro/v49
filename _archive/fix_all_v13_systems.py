#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
import py_compile

def fix_file(filename):
    """修复单个文件"""
    print(f"处理文件: {filename}")
    
    try:
        # 读取文件
        with open(filename, 'r', encoding='utf-8') as f:
            content = f.read()
        
        original_content = content
        
        # 1. 移除emoji和特殊符号
        emoji_pattern = re.compile(r'[\U0001F600-\U0001F64F\U0001F300-\U0001F5FF\U0001F680-\U0001F6FF\U0001F1E0-\U0001F1FF\U00002702-\U000027B0\U000024C2-\U0001F251]+')
        content = emoji_pattern.sub('', content)
        
        # 移除常见特殊符号
        special_chars = ['🚀', '⚡', '🔥', '💎', '📈', '📊', '🎯', '💰', '🏆', '⭐', '🎨', '🌟', '💡', '🔧', '📱', '💻', '🖥️', '⌚', '📺', '📷']
        for char in special_chars:
            content = content.replace(char, '')
        
        # 2. 修复f-string中的百分号问题
        # 将 {xxx:.1f}% 改为 {xxx:.1f}%%
        content = re.sub(r'(\{[^}]+:\.1f\})%', r'\1%%', content)
        
        # 3. 修复其他可能的f-string问题
        # 将 {xxx:.2f}% 改为 {xxx:.2f}%%
        content = re.sub(r'(\{[^}]+:\.2f\})%', r'\1%%', content)
        
        # 4. 修复可能的引号问题
        content = content.replace('"""', '"""')
        content = content.replace('