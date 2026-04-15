#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Powerful Scraper Module - 暴力全文抓取版
🔥 核心改进：强制下载文件全文，不再依赖 API 片段
"""

import aiohttp
import asyncio
import re
import base64
import zlib
from typing import List, Dict

class Scraper:
    def __init__(self, token: str):
        self.base_url = "https://api.github.com"
        self.headers = {
            "Accept": "application/vnd.github.v3+json",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
            "Authorization": f"Bearer {token}" if token else ""
        }
        
        # 🔥 极度宽松的节点正则 (只要像节点就抓)
        self.patterns = [
            r'vless://[^\s\'"<>]+',
            r'hysteria2://[^\s\'"<>]+',
            r'hy2://[^\s\'"<>]+',
            r'tuic://[^\s\'"<>]+',
            r'trojan://[^\s\'"<>]+',
            r'ss://[^\s\'"<>]+',
            r'vmess://[^\s\'"<>]+',
            r'shadowtls://[^\s\'"<>]+',
            r'anytls://[^\s\'"<>]+',
            r'naive\+https?://[^\s\'"<>]+',
        ]

        # 🔥 扩大关键词库 (包含中文和常见文件名)
        self.code_queries = [
            "vless://", "hysteria2://", "tuic://", "trojan://", 
            "ss://", "vmess://", "shadowtls://",
            "filename:config.json", "filename:nodes.txt", "filename:sub.txt",
            "filename:subscription.yaml", "path:*.json vless", "path:*.yaml hysteria"
        ]
        
        self.repo_keywords = [
            "vless", "hysteria2", "tuic", "proxy", "clash", "sing-box",
            "mihomo", "xray", "v2ray", "subscription", "airport"
        ]

        # 黑名单
        self.banned = ['example.com', 'your-domain', 'your_server', 'github.com', 'raw.githubusercontent']

    async def search_code(self, query: str) -> List[Dict]:
        """搜索代码并返回文件 URL 列表"""
        url = f"{self.base_url}/search/code"
        params = {"q": query, "per_page": 30, "page": 1} # 每页少一点，防止超时
        results = []
        
        async with aiohttp.ClientSession(headers=self.headers) as session:
            for page in range(1, 3): # 只搜前 2 页
                params["page"] = page
                try:
                    async with session.get(url, params=params, timeout=15) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            items = data.get("items", [])
                            # 提取每个文件的 download_url (Raw 链接)
                            for item in items:
                                if item.get("download_url"):
                                    results.append({
                                        "url": item["download_url"],
                                        "repo": item["repository"]["full_name"],
                                        "path": item["path"]
                                    })
                        elif resp.status == 403:
                            print(f"   ⚠️ API Rate Limit hit for query: {query}")
                            break
                        else:
                            break
                except Exception as e:
                    print(f"   ❌ Error searching {query}: {e}")
                    break
                await asyncio.sleep(1.5) # 严格限流
        return results

    async def fetch_and_extract(self, file_info: Dict) -> List[str]:
        """下载文件全文并提取节点"""
        links = []
        url = file_info["url"]
        repo = file_info["repo"]
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=10) as resp:
                    if resp.status == 200:
                        content = await resp.text(errors='ignore')
                        links = self.extract_nodes(content)
                        if links:
                            print(f"     ✅ Found {len(links)} nodes in {repo}/{file_info['path']}")
        except Exception:
            pass # 忽略下载失败的文件
        return links

    def extract_nodes(self, content: str) -> List[str]:
        """从文本中提取并清洗节点"""
        valid_nodes = set()
        
        # 1. 尝试 Base64 解码 (订阅格式)
        decoded_content = self._try_decode_base64(content)
        if decoded_content:
            content += "\n" + decoded_content
        
        # 2. 正则匹配
        for pattern in self.patterns:
            matches = re.findall(pattern, content, re.IGNORECASE)
            for m in matches:
                link = m.strip().strip('`\'"()[]{}')
                if self._is_valid(link):
                    valid_nodes.add(link)
                    
        return list(valid_nodes)

    def _try_decode_base64(self, text: str) -> str:
        """尝试解码 Base64 订阅"""
        # 移除所有空白
        clean = re.sub(r'\s+', '', text)
        if len(clean) < 20 or not re.match(r'^[A-Za-z0-9+/=]+$', clean):
            return ""
        
        # 补全 padding
        padding = 4 - len(clean) % 4
        if padding != 4: clean += "=" * padding
        
        try:
            raw = base64.b64decode(clean)
            # 尝试 Gzip
            try:
                return zlib.decompress(raw, zlib.MAX_WBITS|16).decode('utf-8', errors='ignore')
            except:
                return raw.decode('utf-8', errors='ignore')
        except:
            return ""

    def _is_valid(self, link: str) -> bool:
        if not link or len(link) < 15: return False
        low = link.lower()
        if any(b in low for b in self.banned): return False
        if not any(p in low for p in ['vless://', 'hysteria2://', 'hy2://', 'tuic://', 'trojan://', 'ss://', 'vmess://']):
            return False
        return True

    async def run_crawl(self) -> List[str]:
        """执行爬取主逻辑"""
        all_links = []
        tasks = []
        
        print("   🔍 Searching Code...")
        # 收集所有文件下载任务
        for q in self.code_queries:
            files = await self.search_code(q)
            for f in files:
                tasks.append(self.fetch_and_extract(f))
        
        print(f"   📥 Found {len(tasks)} files to download. Starting batch download...")
        
        # 并发下载 (限制并发数防止被封)
        semaphore = asyncio.Semaphore(20)
        
        async def limited_fetch(task):
            async with semaphore:
                return await task
        
        results = await asyncio.gather(*[limited_fetch(t) for t in tasks], return_exceptions=True)
        
        for res in results:
            if isinstance(res, list):
                all_links.extend(res)
                
        return all_links
