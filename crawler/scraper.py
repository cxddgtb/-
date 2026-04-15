#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Ultimate Scraper - 暴力遍历仓库文件 + 代码搜索
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
            "User-Agent": "Mozilla/5.0",
            "Authorization": f"Bearer {token}" if token else ""
        }
        
        # 宽松正则
        self.patterns = [
            r'vless://[^\s\'"<>]+', r'hysteria2://[^\s\'"<>]+', r'hy2://[^\s\'"<>]+',
            r'tuic://[^\s\'"<>]+', r'trojan://[^\s\'"<>]+', r'ss://[^\s\'"<>]+',
            r'vmess://[^\s\'"<>]+', r'shadowtls://[^\s\'"<>]+', r'anytls://[^\s\'"<>]+'
        ]

        # 🔥 强力关键词：专门找包含 config/sub 的仓库
        self.repo_search_queries = [
            "vless in:name", "hysteria2 in:name", "tuic in:name", 
            "subscription in:name", "nodes in:name", "config in:name",
            "airport in:name", "clash in:name", "sing-box in:name"
        ]
        
        # 代码搜索关键词
        self.code_search_queries = ["vless://", "hysteria2://", "tuic://"]

        self.banned = ['example.com', 'your-domain', 'github.com', 'raw.githubusercontent']

    async def search_repos_and_traverse(self, query: str) -> List[str]:
        """搜索仓库并遍历其文件"""
        url = f"{self.base_url}/search/repositories"
        params = {"q": query, "sort": "updated", "order": "desc", "per_page": 10, "page": 1}
        all_links = []
        
        async with aiohttp.ClientSession(headers=self.headers) as session:
            try:
                async with session.get(url, params=params, timeout=15) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        repos = data.get("items", [])
                        print(f"   🔍 Found {len(repos)} repos for '{query}'. Traversing files...")
                        
                        # 遍历每个仓库的文件
                        tasks = []
                        for repo in repos[:5]: # 每个关键词只取前5个仓库，防止超时
                            owner = repo["owner"]["login"]
                            name = repo["name"]
                            tasks.append(self.traverse_repo_files(session, owner, name))
                        
                        results = await asyncio.gather(*tasks, return_exceptions=True)
                        for res in results:
                            if isinstance(res, list):
                                all_links.extend(res)
            except Exception as e:
                print(f"   ❌ Error searching repos: {e}")
                
        return all_links

    async def traverse_repo_files(self, session: aiohttp.ClientSession, owner: str, repo: str) -> List[str]:
        """递归遍历仓库文件并提取节点"""
        links = []
        url = f"{self.base_url}/repos/{owner}/{repo}/contents"
        
        try:
            async with session.get(url, timeout=10) as resp:
                if resp.status != 200: return []
                files = await resp.json()
                
                # 过滤出可能包含节点的文本文件
                target_files = [
                    f for f in files 
                    if f["type"] == "file" and any(f["name"].endswith(ext) for ext in ['.txt', '.json', '.yaml', '.yml', '.conf', '.md'])
                ]
                
                if not target_files: return []
                
                print(f"     📂 Scanning {owner}/{repo} ({len(target_files)} files)...")
                
                # 并发下载这些文件
                download_tasks = []
                for f in target_files:
                    download_tasks.append(self.fetch_file_content(session, owner, repo, f["path"]))
                
                results = await asyncio.gather(*download_tasks, return_exceptions=True)
                for res in results:
                    if isinstance(res, list):
                        links.extend(res)
                        
        except Exception:
            pass
        return links

    async def fetch_file_content(self, session: aiohttp.ClientSession, owner: str, repo: str, path: str) -> List[str]:
        """下载单个文件并提取"""
        url = f"{self.base_url}/repos/{owner}/{repo}/contents/{path}"
        try:
            async with session.get(url, timeout=5) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if "content" in data:
                        import base64
                        content = base64.b64decode(data["content"]).decode('utf-8', errors='ignore')
                        return self.extract_nodes(content)
        except:
            pass
        return []

    async def search_code_direct(self, query: str) -> List[str]:
        """直接搜索代码并下载"""
        url = f"{self.base_url}/search/code"
        params = {"q": query, "per_page": 20, "page": 1}
        all_links = []
        
        async with aiohttp.ClientSession(headers=self.headers) as session:
            for page in range(1, 2):
                params["page"] = page
                try:
                    async with session.get(url, params=params, timeout=15) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            items = data.get("items", [])
                            tasks = []
                            for item in items:
                                if item.get("download_url"):
                                    tasks.append(self.fetch_url_content(session, item["download_url"]))
                            results = await asyncio.gather(*tasks, return_exceptions=True)
                            for res in results:
                                if isinstance(res, list): all_links.extend(res)
                        else: break
                except: break
                await asyncio.sleep(1)
        return all_links

    async def fetch_url_content(self, session: aiohttp.ClientSession, url: str) -> List[str]:
        try:
            async with session.get(url, timeout=5) as resp:
                if resp.status == 200:
                    text = await resp.text(errors='ignore')
                    return self.extract_nodes(text)
        except: pass
        return []

    def extract_nodes(self, content: str) -> List[str]:
        valid = set()
        # 尝试 Base64
        decoded = self._try_decode(content)
        if decoded: content += "\n" + decoded
        
        for p in self.patterns:
            for m in re.findall(p, content, re.IGNORECASE):
                link = m.strip().strip('`\'"()[]{}')
                if self._is_valid(link): valid.add(link)
        return list(valid)

    def _try_decode(self, text):
        clean = re.sub(r'\s+', '', text)
        if len(clean) < 20 or not re.match(r'^[A-Za-z0-9+/=]+$', clean): return ""
        pad = 4 - len(clean) % 4
        if pad != 4: clean += "=" * pad
        try:
            raw = base64.b64decode(clean)
            try: return zlib.decompress(raw, zlib.MAX_WBITS|16).decode('utf-8', errors='ignore')
            except: return raw.decode('utf-8', errors='ignore')
        except: return ""

    def _is_valid(self, link):
        if not link or len(link) < 15: return False
        low = link.lower()
        if any(b in low for b in self.banned): return False
        return any(p in low for p in ['vless://', 'hysteria2://', 'hy2://', 'tuic://', 'trojan://', 'ss://', 'vmess://'])

    async def run_crawl(self) -> List[str]:
        all_links = []
        tasks = []
        
        # 任务1: 遍历仓库文件 (最有效)
        print("   🕸️  Strategy A: Traversing popular repos...")
        for q in self.repo_search_queries:
            tasks.append(self.search_repos_and_traverse(q))
            
        # 任务2: 代码搜索
        print("   🔍  Strategy B: Searching code fragments...")
        for q in self.code_search_queries:
            tasks.append(self.search_code_direct(q))
            
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for res in results:
            if isinstance(res, list): all_links.extend(res)
            
        return all_links
