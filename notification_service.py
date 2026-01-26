#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
通知服务
支持邮件、企业微信、钉钉等多种通知方式
"""

import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import requests
import json
import logging
from typing import Optional
from pathlib import Path

logger = logging.getLogger(__name__)


class NotificationService:
    """通知服务"""
    
    def __init__(self, config_file: str = "notification_config.json"):
        """
        初始化通知服务
        
        Args:
            config_file: 配置文件路径
        """
        self.config_file = config_file
        self.config = self._load_config()
        
        logger.info("📧 通知服务初始化完成")
    
    def _load_config(self) -> dict:
        """加载配置文件"""
        if Path(self.config_file).exists():
            with open(self.config_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        else:
            # 默认配置
            default_config = {
                "enabled": False,
                "email": {
                    "enabled": False,
                    "smtp_server": "smtp.qq.com",
                    "smtp_port": 587,
                    "sender_email": "",
                    "sender_password": "",
                    "receiver_emails": []
                },
                "wechat_work": {
                    "enabled": False,
                    "webhook_url": ""
                },
                "dingtalk": {
                    "enabled": False,
                    "webhook_url": "",
                    "secret": ""
                },
                "serverchan": {
                    "enabled": False,
                    "sendkey": ""
                },
                "bark": {
                    "enabled": False,
                    "device_key": ""
                }
            }
            
            # 保存默认配置
            with open(self.config_file, 'w', encoding='utf-8') as f:
                json.dump(default_config, f, indent=2, ensure_ascii=False)
            
            return default_config
    
    def send_notification(self, title: str, content: str, urgent: bool = False):
        """
        发送通知
        
        Args:
            title: 通知标题
            content: 通知内容
            urgent: 是否紧急
        """
        if not self.config.get('enabled', False):
            logger.info(f"📧 通知服务未启用: {title}")
            return
        
        # 发送邮件
        if self.config['email']['enabled']:
            self.send_email(title, content)
        
        # 发送企业微信
        if self.config['wechat_work']['enabled']:
            self.send_wechat_work(title, content, urgent)
        
        # 发送钉钉
        if self.config['dingtalk']['enabled']:
            self.send_dingtalk(title, content, urgent)
        
        # 发送Server酱（微信推送）
        if self.config.get('serverchan', {}).get('enabled', False):
            self.send_serverchan(title, content)
        
        # 发送Bark（iOS推送）
        if self.config.get('bark', {}).get('enabled', False):
            self.send_bark(title, content)
    
    def send_email(self, subject: str, body: str):
        """
        发送邮件
        
        Args:
            subject: 邮件主题
            body: 邮件正文
        """
        try:
            email_config = self.config['email']
            
            # 创建邮件
            msg = MIMEMultipart()
            msg['From'] = email_config['sender_email']
            msg['To'] = ', '.join(email_config['receiver_emails'])
            msg['Subject'] = subject
            
            # 添加正文
            msg.attach(MIMEText(body, 'plain', 'utf-8'))
            
            # 连接服务器并发送
            with smtplib.SMTP(email_config['smtp_server'], email_config['smtp_port']) as server:
                server.starttls()
                server.login(email_config['sender_email'], email_config['sender_password'])
                server.send_message(msg)
            
            logger.info(f"📧 邮件发送成功: {subject}")
        
        except Exception as e:
            logger.error(f"❌ 邮件发送失败: {e}")
    
    def send_wechat_work(self, title: str, content: str, urgent: bool = False):
        """
        发送企业微信通知
        
        Args:
            title: 标题
            content: 内容
            urgent: 是否紧急
        """
        try:
            webhook_url = self.config['wechat_work']['webhook_url']
            
            # 构造消息
            data = {
                "msgtype": "markdown",
                "markdown": {
                    "content": f"# {title}\n\n{content}"
                }
            }
            
            # 发送请求
            response = requests.post(webhook_url, json=data, timeout=10)
            
            if response.status_code == 200:
                logger.info(f"📱 企业微信发送成功: {title}")
            else:
                logger.error(f"❌ 企业微信发送失败: {response.text}")
        
        except Exception as e:
            logger.error(f"❌ 企业微信发送失败: {e}")
    
    def send_dingtalk(self, title: str, content: str, urgent: bool = False):
        """
        发送钉钉通知
        
        Args:
            title: 标题
            content: 内容
            urgent: 是否紧急
        """
        try:
            webhook_url = self.config['dingtalk']['webhook_url']
            
            # 构造消息
            data = {
                "msgtype": "text",
                "text": {
                    "content": f"{title}\n\n{content}"
                }
            }
            
            # 如果有签名，计算签名（略）
            # ...
            
            # 发送请求
            response = requests.post(webhook_url, json=data, timeout=10)
            
            if response.status_code == 200:
                logger.info(f"📱 钉钉发送成功: {title}")
            else:
                logger.error(f"❌ 钉钉发送失败: {response.text}")
        
        except Exception as e:
            logger.error(f"❌ 钉钉发送失败: {e}")
    
    def send_serverchan(self, title: str, content: str):
        """
        发送Server酱通知（推送到微信）
        
        注册地址: https://sct.ftqq.com
        
        Args:
            title: 通知标题
            content: 通知内容
        """
        try:
            sendkey = self.config['serverchan']['sendkey']
            url = f"https://sctapi.ftqq.com/{sendkey}.send"
            
            data = {
                'title': title,
                'desp': content
            }
            
            response = requests.post(url, data=data)
            
            if response.status_code == 200:
                result = response.json()
                if result.get('code') == 0:
                    logger.info(f"✅ Server酱发送成功: {title}")
                else:
                    logger.error(f"❌ Server酱发送失败: {result.get('message')}")
            else:
                logger.error(f"❌ Server酱发送失败: HTTP {response.status_code}")
        
        except Exception as e:
            logger.error(f"❌ Server酱发送失败: {e}")
    
    def send_bark(self, title: str, content: str):
        """
        发送Bark通知（iOS推送）
        
        下载Bark App: https://apps.apple.com/cn/app/bark/id1403753865
        
        Args:
            title: 通知标题
            content: 通知内容
        """
        try:
            device_key = self.config['bark']['device_key']
            url = f"https://api.day.app/{device_key}/{title}/{content}"
            
            response = requests.get(url)
            
            if response.status_code == 200:
                result = response.json()
                if result.get('code') == 200:
                    logger.info(f"✅ Bark发送成功: {title}")
                else:
                    logger.error(f"❌ Bark发送失败: {result.get('message')}")
            else:
                logger.error(f"❌ Bark发送失败: HTTP {response.status_code}")
        
        except Exception as e:
            logger.error(f"❌ Bark发送失败: {e}")
    
    def test_notification(self):
        """测试通知功能"""
        self.send_notification(
            title="🧪 通知服务测试",
            content="这是一条测试通知，如果您收到这条消息，说明通知服务配置成功！"
        )


def setup_email_config():
    """交互式配置邮件"""
    print("="*80)
    print("📧 邮件通知配置向导")
    print("="*80)
    
    print("\n常见邮箱配置:")
    print("  QQ邮箱: smtp.qq.com:587")
    print("  163邮箱: smtp.163.com:25")
    print("  Gmail: smtp.gmail.com:587")
    
    sender_email = input("\n发件邮箱: ")
    sender_password = input("授权码（不是登录密码）: ")
    receiver_emails = input("收件邮箱（多个用逗号分隔）: ").split(',')
    
    config = {
        "enabled": True,
        "email": {
            "enabled": True,
            "smtp_server": "smtp.qq.com",
            "smtp_port": 587,
            "sender_email": sender_email.strip(),
            "sender_password": sender_password.strip(),
            "receiver_emails": [e.strip() for e in receiver_emails]
        },
        "wechat_work": {
            "enabled": False,
            "webhook_url": ""
        },
        "dingtalk": {
            "enabled": False,
            "webhook_url": "",
            "secret": ""
        }
    }
    
    # 保存配置
    with open('notification_config.json', 'w', encoding='utf-8') as f:
        json.dump(config, f, indent=2, ensure_ascii=False)
    
    print("\n✅ 配置已保存到 notification_config.json")
    
    # 测试
    test = input("\n是否测试发送？(y/n): ")
    if test.lower() == 'y':
        service = NotificationService()
        service.test_notification()
        print("\n✅ 测试邮件已发送，请查收")


if __name__ == "__main__":
    setup_email_config()

