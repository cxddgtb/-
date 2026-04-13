#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Simplified GitHub Crawler - All in one file
"""

import aiohttp
import asyncio
import re
import base64
import json
from typing import List, Dict
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse

class SimpleGitHubCrawler:
    def __init__(self, token: str = ""):
        self.base_url = "https://api.github.com"
        self.headers = {
            "Accept": "application/vnd.github.v3+json",
            "User-Agent": "Proxy-Crawler/2.0"
        }
        if token:
            self.headers["Authorization"] = f"token {token}"
        
        # 节点模式
        self.node_patterns = [
            (r'vless://[^\s\'"<>]+', 'vless'),
            (r'hysteria2://[^\s\'"<>]+', 'hysteria2'),
            (r'hy2://[^\s\'"<>]+', 'hysteria2'),
            (r'tuic://[^\s\'"<>]+', 'tuic'),
            (r'naiveproxy://[^\s\'"<>]+', 'naiveproxy'),
            (r'shadowtls://[^\s\'"<>]+', 'shadowtls'),
            (r'anytls://[^\s\'"<>]+', 'anytls'),
        ]
        
    async def detect_encoding(self, content: bytes) -> str:
        """简单编码检测"""
        try:
            content.decode('utf-8')
            return 'utf-8'
        except UnicodeDecodeError:
            try:
                content.decode('gbk')
                return 'gbk'
            except UnicodeDecodeError:
                return 'utf-8'
    
    async def search_repositories(self, query: str, max_pages: int = 3) -> List[Dict]:
        """搜索仓库"""
        url = f"{self.base_url}/search/repositories"
        params = {"q": query, "sort": "updated", "order": "desc", "per_page": 100, "page": 1}
        results = []
        
        async with aiohttp.ClientSession(headers=self.headers) as session:
            for page in range(1, max_pages + 1):
                params["page"] = page
                try:
                    async with session.get(url, params=params) as resp:
                        if resp.status != 200:
                            break
                        data = await resp.json()
                        items = data.get("items", [])
                        if not items:
                            break
                        results.extend(items)
                        await asyncio.sleep(0.5)
                except Exception:
                    break
        return results
    
    async def search_code(self, query: str, max_pages: int = 3) -> List[Dict]:
        """搜索代码"""
        url = f"{self.base_url}/search/code"
        params = {"q": query, "per_page": 100, "page": 1}
        results = []
        
        async with aiohttp.ClientSession(headers=self.headers) as session:
            for page in range(1, max_pages + 1):
                params["page"] = page
                try:
                    async with session.get(url, params=params) as resp:
                        if resp.status != 200:
                            break
                        data = await resp.json()
                        items = data.get("items", [])
                        if not items:
                            break
                        results.extend(items)
                        await asyncio.sleep(1)
                except Exception:
                    break
        return results
    
    async def get_repo_contents(self, owner: str, repo: str) -> List[Dict]:
        """获取仓库内容"""
        url = f"{self.base_url}/repos/{owner}/{repo}/contents"
        try:
            async with aiohttp.ClientSession(headers=self.headers) as session:
                async with session.get(url) as resp:
                    if resp.status == 200:
                        return await resp.json()
        except Exception:
            pass
        return []
    
    async def get_file_content(self, owner: str, repo: str, path: str) -> str:
        """获取文件内容"""
        url = f"{self.base_url}/repos/{owner}/{repo}/contents/{path}"
        try:
            async with aiohttp.ClientSession(headers=self.headers) as session:
                async with session.get(url) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        raw = base64.b64decode(data["content"])
                        encoding = await self.detect_encoding(raw)
                        return raw.decode(encoding, errors="ignore")
        except Exception:
            pass
        return ""
    
    async def extract_nodes_from_content(self, content: str, source: str, source_type: str) -> List[Dict]:
        """从内容中提取节点"""
        nodes = []
        for pattern, protocol in self.node_patterns:
            matches = re.findall(pattern, content, re.IGNORECASE)
            for match in matches:
                # 验证链接基本格式
                if len(match) > 20 and '://' in match:
                    nodes.append({
                        'link': match.strip(),
                        'protocol': protocol,
                        'source': source,
                        'source_type': source_type
                    })
        return nodes
    
    async def crawl_from_repos(self) -> List[Dict]:
        """从仓库爬取"""
        keywords = [
            "vless reality", "hysteria2", "tuic", "naiveproxy", 
            "shadowtls", "anytls", "proxy config", "v2ray config"
        ]
        all_nodes = []
        
        for keyword in keywords:
            repos = await self.search_repositories(keyword)
            
            for repo in repos:
                # 只处理最近30天更新的
                updated_at = datetime.fromisoformat(repo['updated_at'].replace('Z', '+00:00'))
                if (datetime.now(updated_at.tzinfo) - updated_at).days > 30:
                    continue
                
                contents = await self.get_repo_contents(repo['owner']['login'], repo['name'])
                for item in contents:
                    if item.get('type') == 'file':
                        content = await self.get_file_content(
                            repo['owner']['login'], 
                            repo['name'], 
                            item['path']
                        )
                        if content:
                            nodes = await self.extract_nodes_from_content(
                                content, 
                                f"{repo['owner']['login']}/{repo['name']}",
                                'github_repo'
                            )
                            all_nodes.extend(nodes)
                            
        return all_nodes
    
    async def crawl_from_code(self) -> List[Dict]:
        """从代码搜索爬取"""
        queries = ["vless://", "hysteria2://", "tuic://"]
        all_nodes = []
        
        for query in queries:
            code_results = await self.search_code(query)
            
            for item in code_results:
                # 获取文件内容
                owner = item['repository']['owner']['login']
                repo = item['repository']['name']
                path = item['path']
                
                content = await self.get_file_content(owner, repo, path)
                if content:
                    nodes = await self.extract_nodes_from_content(
                        content,
                        f"{owner}/{repo}",
                        'github_code'
                    )
                    all_nodes.extend(nodes)
                    
        return all_nodes
    
    async def crawl_all(self) -> List[Dict]:
        """爬取所有来源"""
        print("🔍 Crawling from repositories...")
        repo_nodes = await self.crawl_from_repos()
        print(f"✅ Found {len(repo_nodes)} nodes from repositories")
        
        print("🔍 Crawling from code search...")
        code_nodes = await self.crawl_from_code()
        print(f"✅ Found {len(code_nodes)} nodes from code search")
        
        return repo_nodes + code_nodes
