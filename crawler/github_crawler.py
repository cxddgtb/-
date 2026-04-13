#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Super Enhanced GitHub Crawler - Max Node Collection
"""

import aiohttp
import asyncio
import re
import base64
import json
import zlib
from typing import List, Dict, Set, Optional
from datetime import datetime, timezone, timedelta
from pathlib import Path
from urllib.parse import urlparse, unquote, parse_qs
import hashlib

class SuperGitHubCrawler:
    def __init__(self, token: str = ""):
        self.base_url = "https://api.github.com"
        self.raw_base = "https://raw.githubusercontent.com"
        self.headers = {
            "Accept": "application/vnd.github.v3+json",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }
        if token:
            self.headers["Authorization"] = f"Bearer {token}"
        
        # 🔥 超全节点正则（支持各种变体）
        self.node_patterns = [
            # VLESS variants
            (r'vless://[A-Za-z0-9\-_@\.:%\+~#=]+\?[^\s\'"<>]+', 'vless'),
            (r'vless://[^\s\'"<>]+', 'vless'),
            # Hysteria2 variants  
            (r'hysteria2://[A-Za-z0-9\-_@\.:%\+~#=]+@[^\s\'"<>]+', 'hysteria2'),
            (r'hy2://[^\s\'"<>]+', 'hysteria2'),
            (r'hysteria://[^\s\'"<>]+', 'hysteria'),
            # TUIC
            (r'tuic://[^\s\'"<>]+', 'tuic'),
            # Trojan
            (r'trojan://[^\s\'"<>]+', 'trojan'),
            # Shadowsocks
            (r'ss://[A-Za-z0-9\+\/=]+@[^\s\'"<>]+', 'shadowsocks'),
            (r'ss://[^\s\'"<>]+#[^\s\'"<>]+', 'shadowsocks'),
            # NaiveProxy/AnyTLS/ShadowTLS
            (r'naive\+https?://[^\s\'"<>]+', 'naiveproxy'),
            (r'anytls://[^\s\'"<>]+', 'anytls'),
            (r'shadowtls://[^\s\'"<>]+', 'shadowtls'),
            # VMess (兼容)
            (r'vmess://[A-Za-z0-9\+\/=]+', 'vmess'),
        ]
        
        # 🔥 50+ 精准搜索关键词
        self.repo_keywords = [
            # VLESS 核心词
            "vless reality", "vless vision", "vless enc", "vless xhttp",
            "xray vless", "sing-box vless", "vless subscription",
            # Hysteria2
            "hysteria2", "hysteria 2", "hy2", "hy2 config", "hysteria2 subscription",
            # TUIC
            "tuic v5", "tuic config", "tuic subscription", "tuic server",
            # 其他协议
            "naiveproxy config", "shadowtls config", "anytls config",
            "trojan go", "trojan reality",
            # 通用配置词
            "proxy config", "v2ray config", "xray config", 
            "clash config", "clash meta config", "mihomo config",
            "sing-box config", "nekobox config",
            # 订阅相关
            "subscription", "subscribe", "node list", "server list",
            "free proxy", "vpn config", "proxy node",
            # 中文关键词（爬取中文仓库）
            "机场配置", "节点订阅", "代理配置", "v2ray订阅",
            # 文件名关键词
            "filename:config.json vless",
            "filename:subscription.txt",
            "filename:nodes.yaml",
            "filename:servers.list",
        ]
        
        self.code_queries = [
            "vless://", "hysteria2://", "hy2://", "tuic://",
            "trojan://", "ss://", "vmess://",
            "path:*.json vless", "path:*.yaml hysteria",
            "path:*.txt subscription", "path:*.list proxy",
            "extension:json reality", "extension:yaml hy2",
        ]
        
        # 去重集合
        self.seen_links: Set[str] = set()
        self.seen_keys: Set[str] = set()
        
    def _make_key(self, link: str, source: str) -> str:
        """生成唯一去重key"""
        return hashlib.md5(f"{link}:{source}".encode()).hexdigest()
    
    def _is_valid_link(self, link: str) -> bool:
        """基础链接验证"""
        if not link or len(link) < 30:
            return False
        if '://' not in link:
            return False
        # 排除明显无效的
        if any(bad in link.lower() for bad in ['example.com', 'your-domain', 'placeholder', 'xxx', 'test']):
            return False
        return True
    
    def _decode_subscription(self, content: str) -> List[str]:
        """🔥 关键：解析 base64 订阅链接"""
        links = []
        
        # 尝试 base64 解码
        try:
            # 清理内容
            cleaned = content.strip().replace('\n', '').replace(' ', '')
            # 尝试标准 base64
            decoded = base64.b64decode(cleaned).decode('utf-8', errors='ignore')
            if '://' in decoded:
                # 按行分割
                for line in decoded.split('\n'):
                    line = line.strip()
                    if line and '://' in line and self._is_valid_link(line):
                        links.append(line)
        except:
            pass
        
        # 尝试 URL-safe base64
        try:
            cleaned = content.strip().replace('-', '+').replace('_', '/')
            # 补全 =
            cleaned += '=' * (4 - len(cleaned) % 4) if len(cleaned) % 4 else ''
            decoded = base64.b64decode(cleaned).decode('utf-8', errors='ignore')
            if '://' in decoded:
                for line in decoded.split('\n'):
                    line = line.strip()
                    if line and '://' in line and self._is_valid_link(line):
                        links.append(line)
        except:
            pass
            
        # 尝试 gzip 压缩的 base64 (clash 订阅常见)
        try:
            cleaned = content.strip().replace('\n', '')
            decoded = base64.b64decode(cleaned)
            decompressed = zlib.decompress(decoded, zlib.MAX_WBITS | 16).decode('utf-8', errors='ignore')
            if '://' in decompressed:
                for line in decompressed.split('\n'):
                    line = line.strip()
                    if line and '://' in line and self._is_valid_link(line):
                        links.append(line)
        except:
            pass
            
        return links
    
    async def detect_encoding(self, content: bytes) -> str:
        for enc in ['utf-8', 'gbk', 'gb2312', 'latin-1']:
            try:
                content.decode(enc)
                return enc
            except:
                continue
        return 'utf-8'
    
    async def _request(self, session: aiohttp.ClientSession, url: str, **kwargs) -> Optional[Dict]:
        """统一请求处理"""
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=30), **kwargs) as resp:
                if resp.status == 200:
                    return await resp.json()
                elif resp.status == 403:  # 限流
                    print(f"  ⚠️  Rate limited, waiting...")
                    await asyncio.sleep(60)
                    return await self._request(session, url, **kwargs)
        except Exception as e:
            pass
        return None
    
    async def search_repositories(self, query: str, max_pages: int = 10) -> List[Dict]:
        """🔥 增加页数到10"""
        url = f"{self.base_url}/search/repositories"
        params = {"q": query, "sort": "updated", "order": "desc", "per_page": 100}
        results = []
        
        async with aiohttp.ClientSession(headers=self.headers) as session:
            for page in range(1, max_pages + 1):
                params["page"] = page
                data = await self._request(session, url, params=params)
                if not data or "items" not in data:
                    break
                    
                items = data["items"]
                if not items:
                    break
                    
                results.extend(items)
                print(f"    Page {page}: {len(items)} repos")
                
                # 智能限流
                await asyncio.sleep(0.3)
                
        return results
    
    async def search_code(self, query: str, max_pages: int = 10) -> List[Dict]:
        """🔥 代码搜索也增加到10页"""
        url = f"{self.base_url}/search/code"
        params = {"q": query, "per_page": 100}
        results = []
        
        async with aiohttp.ClientSession(headers=self.headers) as session:
            for page in range(1, max_pages + 1):
                params["page"] = page
                data = await self._request(session, url, params=params)
                if not data or "items" not in data:
                    break
                    
                items = data["items"]
                if not items:
                    break
                    
                results.extend(items)
                await asyncio.sleep(1)  # 代码搜索限流更严
                
        return results
    
    async def get_raw_content(self, owner: str, repo: str, path: str, ref: str = "main") -> str:
        """🔥 直接获取 raw 文件（更快更可靠）"""
        url = f"{self.raw_base}/{owner}/{repo}/{ref}/{path}"
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=20), 
                                     headers={"User-Agent": self.headers["User-Agent"]}) as resp:
                    if resp.status == 200:
                        raw = await resp.read()
                        encoding = await self.detect_encoding(raw)
                        return raw.decode(encoding, errors="ignore")
        except:
            pass
        return ""
    
    async def extract_nodes(self, content: str, source: str, source_type: str) -> List[Dict]:
        """🔥 增强版提取：支持订阅解析 + 正则匹配"""
        nodes = []
        
        # 1️⃣ 先尝试解析订阅链接
        sub_links = self._decode_subscription(content)
        for link in sub_links:
            key = self._make_key(link, source)
            if key not in self.seen_keys and self._is_valid_link(link):
                self.seen_keys.add(key)
                self.seen_links.add(link)
                # 判断协议
                protocol = 'unknown'
                for pattern, proto in self.node_patterns:
                    if link.startswith(f"{proto}://"):
                        protocol = proto
                        break
                nodes.append({
                    'link': link,
                    'protocol': protocol,
                    'source': source,
                    'source_type': f"{source_type}_subscription"
                })
        
        # 2️⃣ 正则匹配单个节点
        for pattern, protocol in self.node_patterns:
            try:
                matches = re.findall(pattern, content, re.IGNORECASE)
                for match in matches:
                    link = match.strip()
                    if not self._is_valid_link(link):
                        continue
                    key = self._make_key(link, source)
                    if key not in self.seen_keys:
                        self.seen_keys.add(key)
                        self.seen_links.add(link)
                        nodes.append({
                            'link': link,
                            'protocol': protocol,
                            'source': source,
                            'source_type': source_type
                        })
            except re.error:
                continue
                
        return nodes
    
    async def crawl_repo(self, repo: Dict) -> List[Dict]:
        """爬取单个仓库"""
        nodes = []
        owner, repo_name = repo["owner"]["login"], repo["name"]
        default_branch = repo.get("default_branch", "main")
        
        # 获取文件列表
        contents = await self.get_repo_contents(owner, repo_name, "")
        if not contents:
            return nodes
            
        for item in contents:
            if item.get("type") != "file":
                continue
                
            # 过滤文件类型
            name = item["name"].lower()
            if not any(name.endswith(ext) for ext in ['.json', '.yaml', '.yml', '.txt', '.conf', '.list', '.md', '.sub', '.subscribe']):
                continue
                
            # 🔥 优先用 raw 链接（更快）
            content = await self.get_raw_content(owner, repo_name, item["path"], default_branch)
            
            if content and len(content) > 100:
                extracted = await self.extract_nodes(content, f"{owner}/{repo_name}", 'github_repo')
                nodes.extend(extracted)
                
        return nodes
    
    async def crawl_from_repos(self) -> List[Dict]:
        """🔥 仓库爬取主函数"""
        all_nodes = []
        
        for keyword in self.repo_keywords:
            print(f"🔍 Repos: {keyword}")
            repos = await self.search_repositories(keyword, max_pages=10)
            
            for repo in repos:
                # 时间过滤：最近60天
                try:
                    updated = datetime.fromisoformat(repo['updated_at'].replace('Z', '+00:00'))
                    if (datetime.now(timezone.utc) - updated).days > 60:
                        continue
                except:
                    pass
                    
                # 仓库过滤
                if repo.get("fork", False):  # 跳过fork
                    continue
                if repo.get("size", 0) > 100000:  # 跳过超大仓库
                    continue
                    
                repo_nodes = await self.crawl_repo(repo)
                if repo_nodes:
                    print(f"  ✅ {repo['full_name']}: {len(repo_nodes)} nodes")
                    all_nodes.extend(repo_nodes)
                    
            await asyncio.sleep(0.5)
            
        return all_nodes
    
    async def crawl_from_code(self) -> List[Dict]:
        """🔥 代码搜索爬取"""
        all_nodes = []
        
        for query in self.code_queries:
            print(f"🔍 Code: {query}")
            results = await self.search_code(query, max_pages=10)
            
            for item in results:
                try:
                    owner = item['repository']['owner']['login']
                    repo = item['repository']['name']
                    path = item['path']
                    ref = item.get('git_url', '').split('/')[-1] or 'main'
                    
                    # 跳过大文件
                    if item.get('size', 0) > 500 * 1024:  # 500KB
                        continue
                        
                    content = await self.get_raw_content(owner, repo, path, ref)
                    if content and len(content) > 100:
                        extracted = await self.extract_nodes(content, f"{owner}/{repo}", 'github_code')
                        all_nodes.extend(extracted)
                except:
                    continue
                    
            await asyncio.sleep(1)
            
        return all_nodes
    
    async def crawl_from_gists(self) -> List[Dict]:
        """🔥 新增：爬取 GitHub Gist"""
        all_nodes = []
        keywords = ["vless", "hysteria2", "tuic", "proxy", "subscription"]
        
        async with aiohttp.ClientSession(headers=self.headers) as session:
            for keyword in keywords:
                url = f"{self.base_url}/search/gists"
                params = {"q": keyword, "per_page": 100, "page": 1}
                
                for page in range(1, 6):
                    params["page"] = page
                    data = await self._request(session, url, params=params)
                    if not data or "items" not in data:
                        break
                        
                    for gist in data["items"]:
                        try:
                            # 获取 gist 详情
                            gist_url = f"{self.base_url}/gists/{gist['id']}"
                            gist_data = await self._request(session, gist_url)
                            if not gist_data:
                                continue
                                
                            # 提取内容
                            for filename, file_info in gist_data.get("files", {}).items():
                                content = file_info.get("content", "")
                                if content and len(content) > 100:
                                    extracted = await self.extract_nodes(
                                        content, f"gist/{gist['id']}", 'github_gist'
                                    )
                                    all_nodes.extend(extracted)
                        except:
                            continue
                            
                    await asyncio.sleep(0.5)
                    
        return all_nodes
    
    async def crawl_all(self) -> List[Dict]:
        """🔥 主入口：并发爬取所有源"""
        print("🚀 Starting SUPER crawler...")
        print(f"📋 Keywords: {len(self.repo_keywords)} repo + {len(self.code_queries)} code")
        
        # 🔥 并发执行三个来源
        repo_task = asyncio.create_task(self.crawl_from_repos())
        code_task = asyncio.create_task(self.crawl_from_code())
        gist_task = asyncio.create_task(self.crawl_from_gists())
        
        repo_nodes, code_nodes, gist_nodes = await asyncio.gather(
            repo_task, code_task, gist_task, return_exceptions=True
        )
        
        # 处理异常
        if isinstance(repo_nodes, Exception): repo_nodes = []
        if isinstance(code_nodes, Exception): code_nodes = []
        if isinstance(gist_nodes, Exception): gist_nodes = []
        
        all_nodes = repo_nodes + code_nodes + gist_nodes
        
        print(f"\n📊 Results:")
        print(f"  📦 Repos: {len(repo_nodes)} nodes")
        print(f"  💻 Code: {len(code_nodes)} nodes")
        print(f"  📝 Gists: {len(gist_nodes)} nodes")
        print(f"  🔗 Total unique: {len(self.seen_links)} links")
        
        return all_nodes
    
    async def get_repo_contents(self, owner: str, repo: str, path: str = "") -> List[Dict]:
        """获取仓库内容（辅助方法）"""
        url = f"{self.base_url}/repos/{owner}/{repo}/contents/{path}"
        async with aiohttp.ClientSession(headers=self.headers) as session:
            data = await self._request(session, url)
            if data:
                return data if isinstance(data, list) else [data]
        return []
