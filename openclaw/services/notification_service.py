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
import os
import base64
import hashlib
import hmac
import time
import urllib.parse
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
                config = json.load(f)
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
                "wechat_app": {
                    "enabled": False,
                    "corp_id": "",
                    "agent_id": "",
                    "secret": "",
                    "to_user": "@all"
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
            
            config = default_config

        # 支持环境变量覆盖，避免在仓库中明文保存密钥
        self._apply_env_overrides(config)
        return config

    def _apply_env_overrides(self, config: dict):
        """用环境变量覆盖配置（可选）"""
        def _as_bool(v: str) -> bool:
            return str(v).strip().lower() in ('1', 'true', 'yes', 'on')

        def _set_bool(env_key: str, path: list):
            if env_key not in os.environ:
                return
            ref = config
            for p in path[:-1]:
                if p not in ref or not isinstance(ref[p], dict):
                    ref[p] = {}
                ref = ref[p]
            ref[path[-1]] = _as_bool(os.environ[env_key])

        def _set_str(env_key: str, path: list):
            if env_key not in os.environ:
                return
            ref = config
            for p in path[:-1]:
                if p not in ref or not isinstance(ref[p], dict):
                    ref[p] = {}
                ref = ref[p]
            ref[path[-1]] = os.environ[env_key]

        def _set_int(env_key: str, path: list):
            if env_key not in os.environ:
                return
            try:
                value = int(os.environ[env_key])
            except ValueError:
                return
            ref = config
            for p in path[:-1]:
                if p not in ref or not isinstance(ref[p], dict):
                    ref[p] = {}
                ref = ref[p]
            ref[path[-1]] = value

        _set_bool('NOTIFY_ENABLED', ['enabled'])

        _set_bool('NOTIFY_EMAIL_ENABLED', ['email', 'enabled'])
        _set_str('NOTIFY_EMAIL_SMTP_SERVER', ['email', 'smtp_server'])
        _set_int('NOTIFY_EMAIL_SMTP_PORT', ['email', 'smtp_port'])
        _set_str('NOTIFY_EMAIL_SENDER', ['email', 'sender_email'])
        _set_str('NOTIFY_EMAIL_PASSWORD', ['email', 'sender_password'])
        if 'NOTIFY_EMAIL_RECEIVERS' in os.environ:
            receivers = [x.strip() for x in os.environ['NOTIFY_EMAIL_RECEIVERS'].split(',') if x.strip()]
            config['email']['receiver_emails'] = receivers

        _set_bool('NOTIFY_WECHAT_ENABLED', ['wechat_work', 'enabled'])
        _set_str('NOTIFY_WECHAT_WEBHOOK', ['wechat_work', 'webhook_url'])
        _set_bool('NOTIFY_WECHAT_APP_ENABLED', ['wechat_app', 'enabled'])
        _set_str('NOTIFY_WECHAT_APP_CORP_ID', ['wechat_app', 'corp_id'])
        _set_str('NOTIFY_WECHAT_APP_AGENT_ID', ['wechat_app', 'agent_id'])
        _set_str('NOTIFY_WECHAT_APP_SECRET', ['wechat_app', 'secret'])
        _set_str('NOTIFY_WECHAT_APP_TO_USER', ['wechat_app', 'to_user'])

        _set_bool('NOTIFY_DINGTALK_ENABLED', ['dingtalk', 'enabled'])
        _set_str('NOTIFY_DINGTALK_WEBHOOK', ['dingtalk', 'webhook_url'])
        _set_str('NOTIFY_DINGTALK_SECRET', ['dingtalk', 'secret'])

        _set_bool('NOTIFY_SERVERCHAN_ENABLED', ['serverchan', 'enabled'])
        _set_str('NOTIFY_SERVERCHAN_SENDKEY', ['serverchan', 'sendkey'])

        _set_bool('NOTIFY_BARK_ENABLED', ['bark', 'enabled'])
        _set_str('NOTIFY_BARK_DEVICE_KEY', ['bark', 'device_key'])
    
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
        
        # 发送企业微信应用消息（推荐，替代群机器人Webhook）
        if self.config.get('wechat_app', {}).get('enabled', False):
            self.send_wechat_app(title, content, urgent)
        
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
            cfg = self.config.get('dingtalk', {})
            webhook_url = str(cfg.get('webhook_url', '')).strip()
            secret = str(cfg.get('secret', '')).strip()
            if not webhook_url:
                logger.error("❌ 钉钉发送失败: webhook_url为空")
                return
            
            # 构造消息
            data = {
                "msgtype": "text",
                "text": {
                    "content": f"{title}\n\n{content}"
                }
            }
            
            # 如果配置了加签密钥，按钉钉机器人要求添加 timestamp + sign
            if secret:
                timestamp = str(round(time.time() * 1000))
                string_to_sign = f"{timestamp}\n{secret}"
                hmac_code = hmac.new(
                    secret.encode('utf-8'),
                    string_to_sign.encode('utf-8'),
                    digestmod=hashlib.sha256
                ).digest()
                sign = urllib.parse.quote_plus(base64.b64encode(hmac_code))
                sep = '&' if '?' in webhook_url else '?'
                webhook_url = f"{webhook_url}{sep}timestamp={timestamp}&sign={sign}"
            
            # 发送请求
            response = requests.post(webhook_url, json=data, timeout=10)
            resp_json = {}
            try:
                resp_json = response.json()
            except Exception:
                pass
            
            if response.status_code == 200 and (not resp_json or resp_json.get('errcode') == 0):
                logger.info(f"📱 钉钉发送成功: {title}")
            else:
                logger.error(f"❌ 钉钉发送失败: status={response.status_code}, resp={resp_json or response.text}")
        
        except Exception as e:
            logger.error(f"❌ 钉钉发送失败: {e}")

    def send_wechat_app(self, title: str, content: str, urgent: bool = False):
        """
        通过企业微信应用消息发送通知
        需要: corp_id + secret + agent_id + to_user
        """
        try:
            cfg = self.config.get('wechat_app', {})
            corp_id = str(cfg.get('corp_id', '')).strip()
            agent_id = str(cfg.get('agent_id', '')).strip()
            secret = str(cfg.get('secret', '')).strip()
            to_user = str(cfg.get('to_user', '@all')).strip() or '@all'

            if not (corp_id and agent_id and secret):
                logger.error("❌ 企业微信应用消息配置不完整(corp_id/agent_id/secret)")
                return

            # 1) 获取 access_token
            token_resp = requests.get(
                "https://qyapi.weixin.qq.com/cgi-bin/gettoken",
                params={"corpid": corp_id, "corpsecret": secret},
                timeout=10,
            )
            token_data = token_resp.json()
            if token_resp.status_code != 200 or token_data.get('errcode') != 0:
                logger.error(f"❌ 企业微信获取token失败: {token_data}")
                return

            access_token = token_data.get('access_token')
            if not access_token:
                logger.error("❌ 企业微信获取token失败: access_token为空")
                return

            # 2) 发送消息
            send_url = f"https://qyapi.weixin.qq.com/cgi-bin/message/send?access_token={access_token}"
            msg = {
                "touser": to_user,
                "msgtype": "markdown",
                "agentid": int(agent_id),
                "markdown": {
                    "content": f"**{title}**\n\n{content}"
                },
                "safe": 0
            }
            resp = requests.post(send_url, json=msg, timeout=10)
            data = resp.json()

            if resp.status_code == 200 and data.get('errcode') == 0:
                logger.info(f"✅ 企业微信应用消息发送成功: {title}")
            else:
                logger.error(f"❌ 企业微信应用消息发送失败: {data}")

        except Exception as e:
            logger.error(f"❌ 企业微信应用消息发送失败: {e}")
    
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
