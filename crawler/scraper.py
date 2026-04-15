#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Powerful Scraper Module - 爬取 + 订阅解码 + 严格清洗 (Python 3.11 兼容)
"""

import aiohttp
import asyncio
import re
import base64
import zlib

class Scraper:
    def __init__(self, token: str):
        self.base_url = "https://api.github.com"
        self.raw_base = "https://raw.githubusercontent.com"
        self.headers = {
            "Accept": "application/vnd.github.v3+json",
            "User-Agent": "Mozilla/5.0",
            "Authorization": f"Bearer {token}" if token else ""
        }
        
        # 极度严格的节点正则
        self.patterns = {
            'vless': r'vless://[a-f0-9-]{36}@[a-zA-Z0-9.-]+:\d+(?:\?[^\s"\']*)?',
            'hysteria2': r'hysteria2://[a-f0-9-]{36}@[a-zA-Z0-9.-]+:\d+(?:\?[^\s"\']*)?',
            'hy2': r'hy2://[a-f0-9-]{36}@[a-zA-Z0-9.-]+:\d+(?:\?[^\s"\']*)?',
            'tuic': r'tuic://[a-f0-9-]{36}:[^@\s]+@[a-zA-Z0-9.-]+:\d+(?:\?[^\s"\']*)?',
            'trojan': r'trojan://[a-zA-Z0-9_-]+@[a-zA-Z0-9.-]+:\d+(?:\?[^\s"\']*)?',
            'ss': r'ss://[A-Za-z0-9+/=]+@[a-zA-Z0-9.-]+:\d+',
            'vmess': r'vmess://[A-Za-z0-9+/=]+',
        }

        self.repo_keywords = [
            "vless", "hysteria2", "tuic", "trojan", "shadowsocks",
            "subscription", "proxy-config", "nodes-list", "airport-config",
            "filename:config.json", "filename:nodes.txt"
        ]
        
        self.code_queries = [
            "vless://", "hysteria2://", "tuic://", "trojan://", "vmess://"
        ]

    async def search_code(self, query: str) -> list[dict]:
        url = f"{self.base_url}/search/code"
        params = {"q": query, "per_page": 100, "page": 1}
        results = []
        
        async with aiohttp.ClientSession(headers=self.headers) as session:
            for page in range(1, 3):
                params["page"] = page
                try:
                    async with session.get(url, params=params, timeout=15) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            results.extend(data.get("items", []))
                        else: break
                except: break
                await asyncio.sleep(1)
        return results

    async def search_repos(self, query: str) -> list[dict]:
        url = f"{self.base_url}/search/repositories"
        params = {"q": query, "sort": "updated", "order": "desc", "per_page": 50, "page": 1}
        results = []
        
        async with aiohttp.ClientSession(headers=self.headers) as session:
            try:
                async with session.get(url, params=params, timeout=15) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        results.extend(data.get("items", []))
            except: pass
        return results

    async def get_file_content(self, url: str) -> str:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=10) as resp:
                    if resp.status == 200:
                        return await resp.text(errors='ignore')
        except: pass
        return ""

    def decode_subscription(self, text: str) -> list[str]:
        decoded_links = []
        text = re.sub(r'\s+', '', text)
        padding = 4 - len(text) % 4
        if padding != 4: text += "=" * padding
        
        try:
            if re.match(r'^[A-Za-z0-9+/=]+$', text) and len(text) > 20:
                raw = base64.b64decode(text)
                try:
                    content = zlib.decompress(raw, zlib.MAX_WBITS|16).decode('utf-8', errors='ignore')
                except:
                    content = raw.decode('utf-8', errors='ignore')
                
                if any(k in content for k in ['://', 'vmess://', 'vless://']):
                    return content.split('\n')
        except: pass
        return []

    def extract_nodes(self, content: str) -> list[str]:
        valid_nodes = set()
        
        sub_links = self.decode_subscription(content)
        for link in sub_links:
            clean_link = link.strip()
            if self._is_valid_node(clean_link):
                valid_nodes.add(clean_link)

        for proto, pattern in self.patterns.items():
            matches = re.findall(pattern, content, re.IGNORECASE)
            for m in matches:
                clean_link = m.strip().strip('`\'"()[]{}')
                if self._is_valid_node(clean_link):
                    valid_nodes.add(clean_link)
                    
        return list(valid_nodes)

    def _is_valid_node(self, link: str) -> bool:
        if not link or len(link) < 20: return False
        banned = ['example.com', 'your-domain', 'your_ip', '127.0.0.1', 'github.com', 'raw.githubusercontent']
        low = link.lower()
        if any(b in low for b in banned): return False
        
        if not any(proto in low for proto in ['vless://', 'hysteria2://', 'hy2://', 'tuic://', 'trojan://', 'ss://', 'vmess://']):
            return False
        if not re.search(r'@[a-zA-Z0-9.-]+:\d+', link):
            return False
        return True
