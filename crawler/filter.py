#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Node Filter Module - 极速格式清洗与验证
🔧 专门用于过滤无效节点格式，确保输出 100% 可用
"""

import re
import base64
import json

class NodeFilter:
    def __init__(self):
        # 常见无效占位符/测试域名
        self.placeholder_keywords = [
            'example.com', 'your.domain', 'your-server', 'your-ip',
            'your_host', 'your_port', 'replace-me', 'change-me',
            'placeholder', 'demo', 'test', 'localhost', '127.0.0.1',
            '0.0.0.0', '192.168.', '10.0.', 'github.com', 'raw.githubusercontent'
        ]

    def clean(self, text: str) -> str:
        """清洗节点字符串：去除 Markdown、HTML、尾部标点"""
        text = text.strip().strip('`\'"')
        # 提取 Markdown 链接中的 URL: [描述](vless://...) -> vless://...
        md_match = re.search(r'\(([^)]+)\)', text)
        if md_match:
            text = md_match.group(1)
        # 去除尾部可能混入的标点符号
        text = re.sub(r'[.,;:!?)}\]>]+$', '', text)
        # 去除 HTML 标签
        text = re.sub(r'<[^>]+>', '', text)
        return text.strip()

    def is_valid(self, link: str) -> bool:
        """严格验证节点格式"""
        link = self.clean(link)
        if not link or len(link) < 20 or '://' not in link:
            return False
            
        proto = link.split('://')[0].lower()
        if proto not in ('vless', 'hysteria2', 'hy2', 'tuic', 'trojan', 'ss', 'shadowsocks', 'vmess'):
            return False

        # 1. 占位符拦截
        low = link.lower()
        if any(ph in low for ph in self.placeholder_keywords):
            return False

        # 2. 协议结构验证
        try:
            if proto in ('vless', 'hysteria2', 'hy2'):
                # 格式: proto://UUID@host:port...
                if not re.match(rf'{proto}://[a-f0-9-]{{36}}@[^:/]+:\d+', link):
                    return False
                    
            elif proto == 'tuic':
                # 格式: tuic://UUID:pass@host:port...
                if not re.match(r'tuic://[a-f0-9-]{36}:[^@]+@[^:/]+:\d+', link):
                    return False
                    
            elif proto == 'trojan':
                # 格式: trojan://pass@host:port...
                if not re.match(r'trojan://[^@]+@[^:/]+:\d+', link):
                    return False
                    
            elif proto in ('ss', 'shadowsocks'):
                # 格式: ss://base64@host:port
                if '@' not in link: return False
                b64_part = link.split('://')[1].split('@')[0]
                # 补全 Base64  padding
                b64_part += '=' * (4 - len(b64_part) % 4)
                base64.b64decode(b64_part)
                
            elif proto == 'vmess':
                # 格式: vmess://base64(json)
                b64_part = link.split('://')[1]
                b64_part += '=' * (4 - len(b64_part) % 4)
                decoded = base64.b64decode(b64_part).decode('utf-8')
                json.loads(decoded)  # 必须是合法 JSON
        except Exception:
            return False
            
        return True

    def filter_batch(self, nodes: list) -> list:
        """批量过滤节点列表"""
        valid_nodes = []
        for node in nodes:
            raw_link = node.get('link', '')
            cleaned_link = self.clean(raw_link)
            if self.is_valid(cleaned_link):
                node['link'] = cleaned_link  # 替换为清洗后的干净链接
                valid_nodes.append(node)
        return valid_nodes
