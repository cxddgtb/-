#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Super Enhanced GitHub Crawler - With Smart Deduplication
智能去重：避免不同关键词爬取同一页面
"""

import aiohttp
import asyncio
import re
import base64
import json
import zlib
from typing import List, Dict, Set, Optional, Tuple
from datetime import datetime, timezone, timedelta
from pathlib import Path
from urllib.parse import urlparse, unquote, parse_qs
import hashlib

class SuperGitHubCrawler:
    def __init__(self, token: str = "", shard_id: int = -1):
        self.base_url = "https://api.github.com"
        self.raw_base = "https://raw.githubusercontent.com"
        self.headers = {
            "Accept": "application/vnd.github.v3+json",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }
        if token:
            self.headers["Authorization"] = f"Bearer {token}"
        
        # 节点正则表达式
        self.node_patterns = [
            (r'vless://[A-Za-z0-9\-_@\.:%\+~#=]+\?[^\s\'"<>]+', 'vless'),
            (r'vless://[^\s\'"<>]+', 'vless'),
            (r'hysteria2://[A-Za-z0-9\-_@\.:%\+~#=]+@[^\s\'"<>]+', 'hysteria2'),
            (r'hy2://[^\s\'"<>]+', 'hysteria2'),
            (r'hysteria://[^\s\'"<>]+', 'hysteria'),
            (r'tuic://[^\s\'"<>]+', 'tuic'),
            (r'trojan://[^\s\'"<>]+', 'trojan'),
            (r'ss://[A-Za-z0-9\+\/=]+@[^\s\'"<>]+', 'shadowsocks'),
            (r'ss://[^\s\'"<>]+#[^\s\'"<>]+', 'shadowsocks'),
            (r'naive\+https?://[^\s\'"<>]+', 'naiveproxy'),
            (r'anytls://[^\s\'"<>]+', 'anytls'),
            (r'shadowtls://[^\s\'"<>]+', 'shadowtls'),
            (r'vmess://[A-Za-z0-9\+\/=]+', 'vmess'),
        ]
        
        # 🔥 全量关键词（单线程模式使用）
        self.all_repo_keywords = [
            "vless reality", "vless vision", "vless enc", "vless xhttp",
            "xray vless", "sing-box vless", "vless subscription",
            "hysteria2", "hysteria 2", "hy2", "hy2 config", "hysteria2 subscription",
            "tuic v5", "tuic config", "tuic subscription", "tuic server",
            "naiveproxy config", "shadowtls config", "anytls config",
            "trojan go", "trojan reality",
            "proxy config", "v2ray config", "xray config", 
            "clash config", "clash meta config", "mihomo config",
            "sing-box config", "nekobox config",
            "subscription", "subscribe", "node list", "server list",
            "free proxy", "vpn config", "proxy node",
            "机场配置", "节点订阅", "代理配置", "v2ray订阅",
            "filename:config.json vless",
            "filename:subscription.txt",
            "filename:nodes.yaml",
            "filename:servers.list",
        ]
        
        self.all_code_queries = [
            "vless://", "hysteria2://", "hy2://", "tuic://",
            "trojan://", "ss://", "vmess://",
            "path:*.json vless", "path:*.yaml hysteria",
            "path:*.txt subscription", "path:*.list proxy",
            "extension:json reality", "extension:yaml hy2",
        ]
        
        # 🔥 分片模式：根据 shard_id 分配关键词
        self.shard_id = shard_id
        if shard_id >= 0:
            self.repo_keywords = self._get_shard_keywords(shard_id)
            self.code_queries = self._get_shard_code_queries(shard_id)
            print(f"🔧 Running in shard mode: #{shard_id} with {len(self.repo_keywords)} keywords")
        else:
            self.repo_keywords = self.all_repo_keywords
            self.code_queries = self.all_code_queries
            print(f"🔧 Running in single-thread mode with {len(self.repo_keywords)} keywords")
        
        # 🔥 全局去重集合（避免重复爬取同一资源）
        self.seen_repos: Set[str] = set()      # 已爬取的仓库: owner/repo
        self.seen_files: Set[str] = set()      # 已爬取的文件: owner/repo/path@ref
        self.seen_links: Set[str] = set()      # 已提取的节点链接
        self.seen_keys: Set[str] = set()       # 节点去重key
        
        # 🔥 统计信息
        self.stats = {
            'repos_searched': 0,
            'repos_crawled': 0,
            'files_skipped': 0,
            'nodes_extracted': 0,
        }
        
    def _get_shard_keywords(self, shard_id: int) -> list:
        """根据分片ID分配关键词（减少重叠）"""
        shards = {
            0: ["vless reality", "vless vision", "vless enc", "vless xhttp", "xray vless", "sing-box vless", "vless subscription", "filename:config.json vless", "path:*.json reality"],
            1: ["hysteria2", "hysteria 2", "hy2", "hy2 config", "hysteria2 subscription", "filename:*.yaml hysteria", "extension:yaml hy2", "hysteria2 server"],
            2: ["tuic v5", "tuic config", "tuic subscription", "tuic server", "trojan go", "trojan reality", "trojan subscription", "filename:*.json trojan"],
            3: ["naiveproxy config", "shadowtls config", "anytls config", "naive proxy", "shadow-tls", "any-tls"],
            4: ["proxy config", "v2ray config", "xray config", "clash config", "clash meta config", "mihomo config", "sing-box config", "subscription", "subscribe"],
            5: ["机场配置", "节点订阅", "代理配置", "v2ray订阅", "node list", "server list", "free proxy", "vpn config", "filename:subscription.txt", "filename:nodes.yaml"],
            6: ["proxy node", "v2ray node", "xray node", "filename:servers.list", "filename:proxy.conf", "extension:json proxy", "extension:txt vless", "clash subscription", "nekobox config"],
            7: ["nekobox config", "mihomo subscription", "sing-box subscription", "xray subscription", "proxy list 2024", "free v2ray", "vpn free", "shadowsocks config"],
        }
        return shards.get(shard_id % 8, self.all_repo_keywords[:5])
    
    def _get_shard_code_queries(self, shard_id: int) -> list:
        """分片模式的代码搜索查询（只有部分分片执行）"""
        if shard_id in [6, 7]:
            return self.all_code_queries
        return []
        
    def _make_key(self, link: str, source: str) -> str:
        return hashlib.md5(f"{link}:{source}".encode()).hexdigest()
    
    def _make_file_key(self, owner: str, repo: str, path: str, ref: str = "main") -> str:
        """生成文件的唯一key，用于去重"""
        return f"{owner}/{repo}/{path}@{ref}".lower()
    
    def _make_repo_key(self, owner: str, repo: str) -> str:
        """生成仓库的唯一key"""
        return f"{owner}/{repo}".lower()
    
    def _is_valid_link(self, link: str) -> bool:
        if not link or len(link) < 30:
            return False
        if '://' not in link:
            return False
        if any(bad in link.lower() for bad in ['example.com', 'your-domain', 'placeholder', 'xxx', 'test', 'your_server', 'your_port']):
            return False
        return True
    
    def _decode_subscription(self, content: str) -> List[str]:
        """解析 base64/gzip 订阅链接"""
        links = []
        
        # 尝试标准 base64
        try:
            cleaned = content.strip().replace('\n', '').replace(' ', '')
            decoded = base64.b64decode(cleaned).decode('utf-8', errors='ignore')
            if '://' in decoded:
                for line in decoded.split('\n'):
                    line = line.strip()
                    if line and '://' in line and self._is_valid_link(line):
                        links.append(line)
        except: pass
        
        # 尝试 URL-safe base64
        try:
            cleaned = content.strip().replace('-', '+').replace('_', '/')
            cleaned += '=' * (4 - len(cleaned) % 4) if len(cleaned) % 4 else ''
            decoded = base64.b64decode(cleaned).decode('utf-8', errors='ignore')
            if '://' in decoded:
                for line in decoded.split('\n'):
                    line = line.strip()
                    if line and '://' in line and self._is_valid_link(line):
                        links.append(line)
        except: pass
            
        # 尝试 gzip 压缩
        try:
            cleaned = content.strip().replace('\n', '')
            decoded = base64.b64decode(cleaned)
            decompressed = zlib.decompress(decoded, zlib.MAX_WBITS | 16).decode('utf-8', errors='ignore')
            if '://' in decompressed:
                for line in decompressed.split('\n'):
                    line = line.strip()
                    if line and '://' in line and self._is_valid_link(line):
                        links.append(line)
        except: pass
            
        return links
    
    async def detect_encoding(self, content: bytes) -> str:
        for enc in ['utf-8', 'gbk', 'gb2312', 'latin-1']:
            try:
                content.decode(enc)
                return enc
            except: continue
        return 'utf-8'
    
    async def _request(self, session: aiohttp.ClientSession, url: str, **kwargs) -> Optional[Dict]:
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=30), **kwargs) as resp:
                if resp.status == 200:
                    return await resp.json()
                elif resp.status == 403:
                    print(f"  ⚠️  Rate limited, waiting 30s...")
                    await asyncio.sleep(30)
                    return await self._request(session, url, **kwargs)
        except Exception: pass
        return None
    
    async def search_repositories(self, query: str, max_pages: int = 10) -> List[Dict]:
        url = f"{self.base_url}/search/repositories"
        params = {"q": query, "sort": "updated", "order": "desc", "per_page": 100}
        results = []
        
        async with aiohttp.ClientSession(headers=self.headers) as session:
            for page in range(1, max_pages + 1):
                params["page"] = page
                data = await self._request(session, url, params=params)
                if not data or "items" not in data: break
                    
                items = data["items"]
                if not items: break
                    
                results.extend(items)
                await asyncio.sleep(0.3)
        return results
    
    async def search_code(self, query: str, max_pages: int = 5) -> List[Dict]:
        url = f"{self.base_url}/search/code"
        params = {"q": query, "per_page": 100}
        results = []
        
        async with aiohttp.ClientSession(headers=self.headers) as session:
            for page in range(1, max_pages + 1):
                params["page"] = page
                data = await self._request(session, url, params=params)
                if not data or "items" not in data: break
                items = data["items"]
                if not items: break
                results.extend(items)
                await asyncio.sleep(1)
        return results
    
    async def get_raw_content(self, owner: str, repo: str, path: str, ref: str = "main") -> str:
        url = f"{self.raw_base}/{owner}/{repo}/{ref}/{path}"
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=20), 
                                     headers={"User-Agent": self.headers["User-Agent"]}) as resp:
                    if resp.status == 200:
                        raw = await resp.read()
                        encoding = await self.detect_encoding(raw)
                        return raw.decode(encoding, errors="ignore")
        except: pass
        return ""
    
    def _should_crawl_file(self, owner: str, repo: str, path: str, ref: str = "main") -> bool:
        """🔥 智能去重：检查文件是否已爬取过"""
        file_key = self._make_file_key(owner, repo, path, ref)
        if file_key in self.seen_files:
            self.stats['files_skipped'] += 1
            return False
        self.seen_files.add(file_key)
        return True
    
    def _should_crawl_repo(self, owner: str, repo: str) -> bool:
        """检查仓库是否已处理过（用于统计）"""
        repo_key = self._make_repo_key(owner, repo)
        if repo_key not in self.seen_repos:
            self.seen_repos.add(repo_key)
            self.stats['repos_crawled'] += 1
            return True
        return False
    
    async def extract_nodes(self, content: str, source: str, source_type: str) -> List[Dict]:
        nodes = []
        
        # 1. 解析订阅
        sub_links = self._decode_subscription(content)
        for link in sub_links:
            key = self._make_key(link, source)
            if key not in self.seen_keys and self._is_valid_link(link):
                self.seen_keys.add(key)
                self.seen_links.add(link)
                protocol = 'unknown'
                for pattern, proto in self.node_patterns:
                    if link.startswith(f"{proto}://"):
                        protocol = proto
                        break
                nodes.append({'link': link, 'protocol': protocol, 'source': source, 'source_type': f"{source_type}_subscription"})
        
        # 2. 正则匹配
        for pattern, protocol in self.node_patterns:
            try:
                matches = re.findall(pattern, content, re.IGNORECASE)
                for match in matches:
                    link = match.strip()
                    if not self._is_valid_link(link): continue
                    key = self._make_key(link, source)
                    if key not in self.seen_keys:
                        self.seen_keys.add(key)
                        self.seen_links.add(link)
                        nodes.append({'link': link, 'protocol': protocol, 'source': source, 'source_type': source_type})
            except re.error: continue
        return nodes
    
    async def crawl_repo(self, repo: Dict) -> List[Dict]:
        nodes = []
        owner, repo_name = repo["owner"]["login"], repo["name"]
        default_branch = repo.get("default_branch", "main")
        
        # 🔥 检查仓库是否已爬取
        if not self._should_crawl_repo(owner, repo_name):
            return []
        
        contents = await self.get_repo_contents(owner, repo_name, "")
        if not contents: return nodes
            
        for item in contents:
            if item.get("type") != "file": continue
            name = item["name"].lower()
            if not any(name.endswith(ext) for ext in ['.json', '.yaml', '.yml', '.txt', '.conf', '.list', '.md', '.sub', '.subscribe']):
                continue
            
            # 🔥 关键：检查文件是否已爬取（避免不同关键词搜到同一文件）
            if not self._should_crawl_file(owner, repo_name, item["path"], default_branch):
                continue
                
            content = await self.get_raw_content(owner, repo_name, item["path"], default_branch)
            if content and len(content) > 100:
                extracted = await self.extract_nodes(content, f"{owner}/{repo_name}", 'github_repo')
                if extracted:
                    self.stats['nodes_extracted'] += len(extracted)
                    nodes.extend(extracted)
        return nodes
    
    async def crawl_from_repos(self) -> List[Dict]:
        all_nodes = []
        print(f"🔍 Repos: starting with {len(self.repo_keywords)} keywords (shard={self.shard_id})")
        
        for keyword in self.repo_keywords:
            print(f"  🔍 Searching: {keyword}")
            repos = await self.search_repositories(keyword, max_pages=10)
            self.stats['repos_searched'] += len(repos)
            
            for repo in repos:
                try:
                    updated = datetime.fromisoformat(repo['updated_at'].replace('Z', '+00:00'))
                    if (datetime.now(timezone.utc) - updated).days > 60: continue
                except: pass
                    
                if repo.get("fork", False): continue
                if repo.get("size", 0) > 100000: continue
                    
                repo_nodes = await self.crawl_repo(repo)
                if repo_nodes:
                    print(f"    ✅ {repo['full_name']}: {len(repo_nodes)} nodes")
                    all_nodes.extend(repo_nodes)
            await asyncio.sleep(0.3)
        
        print(f"  📊 Repo crawl stats: searched={self.stats['repos_searched']}, crawled={self.stats['repos_crawled']}, files_skipped={self.stats['files_skipped']}")
        return all_nodes
    
    async def crawl_from_code(self) -> List[Dict]:
        if not self.code_queries:
            print("🔍 Code: skipping (not assigned to this shard)")
            return []
            
        all_nodes = []
        print(f"🔍 Code: starting with {len(self.code_queries)} queries (shard={self.shard_id})")
        
        for query in self.code_queries:
            print(f"  🔍 Searching code: {query}")
            results = await self.search_code(query, max_pages=5)
            
            for item in results:
                try:
                    owner = item['repository']['owner']['login']
                    repo = item['repository']['name']
                    path = item['path']
                    ref = item.get('git_url', '').split('/')[-1] or 'main'
                    
                    if item.get('size', 0) > 500 * 1024: continue
                    
                    # 🔥 关键：检查文件是否已爬取
                    if not self._should_crawl_file(owner, repo, path, ref):
                        continue
                        
                    content = await self.get_raw_content(owner, repo, path, ref)
                    if content and len(content) > 100:
                        extracted = await self.extract_nodes(content, f"{owner}/{repo}", 'github_code')
                        if extracted:
                            self.stats['nodes_extracted'] += len(extracted)
                            all_nodes.extend(extracted)
                except: continue
            await asyncio.sleep(1)
        return all_nodes
    
    async def crawl_from_gists(self) -> List[Dict]:
        if self.shard_id >= 0:
            return []
        return []
    
    async def crawl_all(self) -> List[Dict]:
        print("🚀 Starting SUPER crawler...")
        
        repo_task = asyncio.create_task(self.crawl_from_repos())
        code_task = asyncio.create_task(self.crawl_from_code())
        gist_task = asyncio.create_task(self.crawl_from_gists())
        
        repo_nodes, code_nodes, gist_nodes = await asyncio.gather(
            repo_task, code_task, gist_task, return_exceptions=True
        )
        
        if isinstance(repo_nodes, Exception): repo_nodes = []
        if isinstance(code_nodes, Exception): code_nodes = []
        if isinstance(gist_nodes, Exception): gist_nodes = []
        
        all_nodes = repo_nodes + code_nodes + gist_nodes
        
        print(f"\n📊 Shard #{self.shard_id} Results:")
        print(f"  📦 Repos: {len(repo_nodes)} nodes")
        print(f"  💻 Code: {len(code_nodes)} nodes")
        print(f"  📝 Gists: {len(gist_nodes)} nodes")
        print(f"  🔗 Total unique links: {len(self.seen_links)}")
        print(f"  🗂️  Repos crawled: {self.stats['repos_crawled']}")
        print(f"  📄 Files skipped (dedup): {self.stats['files_skipped']}")
        
        return all_nodes
    
    async def get_repo_contents(self, owner: str, repo: str, path: str = "") -> List[Dict]:
        url = f"{self.base_url}/repos/{owner}/{repo}/contents/{path}"
        async with aiohttp.ClientSession(headers=self.headers) as session:
            data = await self._request(session, url)
            if data:
                return data if isinstance(data, list) else [data]
        return []
    
    def get_crawl_stats(self) -> Dict:
        """获取爬取统计信息"""
        return {
            **self.stats,
            'unique_links': len(self.seen_links),
            'unique_repos': len(self.seen_repos),
            'unique_files': len(self.seen_files),
        }
