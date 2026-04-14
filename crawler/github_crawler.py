#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
GitHub Crawler - STRICT VALIDATION
🔧 严格验证节点格式，只保留合法节点
"""

import aiohttp
import asyncio
import re
import base64
import json
import zlib
from typing import List, Dict, Set
from datetime import datetime, timezone
from pathlib import Path
import hashlib
from urllib.parse import urlparse, parse_qs

class SuperGitHubCrawler:
    def __init__(self, token: str = "", shard_id: int = -1):
        self.base_url = "https://api.github.com"
        self.raw_base = "https://raw.githubusercontent.com"
        self.headers = {
            "Accept": "application/vnd.github.v3+json",
            "User-Agent": "Mozilla/5.0"
        }
        if token:
            self.headers["Authorization"] = f"Bearer {token}"
        
        # 🔥 严格正则：只匹配标准格式
        self.node_patterns = [
            # vless://UUID@host:port?params#tag
            (r'vless://[a-f0-9-]{36}@[^\s\'"<>]+', 'vless'),
            # hysteria2://UUID@host:port?params#tag
            (r'hysteria2://[a-f0-9-]{36}@[^\s\'"<>]+', 'hysteria2'),
            (r'hy2://[a-f0-9-]{36}@[^\s\'"<>]+', 'hysteria2'),
            # tuic://UUID:password@host:port?params#tag
            (r'tuic://[a-f0-9-]{36}:[^\s\'"<>@]+@[^\s\'"<>]+', 'tuic'),
            # trojan://password@host:port?params#tag
            (r'trojan://[a-zA-Z0-9_-]+@[^\s\'"<>]+', 'trojan'),
            # ss://base64@host:port 或 ss://user:pass@host:port
            (r'ss://[A-Za-z0-9+/=]+@[^\s\'"<>]+', 'shadowsocks'),
            # vmess://base64
            (r'vmess://[A-Za-z0-9+/=]+', 'vmess'),
        ]
        
        self.shard_id = shard_id
        if shard_id >= 0:
            self.repo_keywords = self._get_shard_keywords(shard_id)
            self.code_queries = []
        else:
            self.repo_keywords = ["vless", "hysteria2", "tuic"]
            self.code_queries = []
            
        self.seen_links: Set[str] = set()
        self.stats = {'repos_searched': 0, 'repos_crawled': 0, 'files_skipped': 0, 'nodes_found': 0}

    def _get_shard_keywords(self, shard_id: int) -> list:
        shards = {
            0: ["vless reality", "vless vision", "xray vless"],
            1: ["hysteria2", "hysteria 2", "hy2 config"],
            2: ["tuic v5", "tuic config", "trojan go"],
            3: ["naiveproxy", "shadowtls", "anytls"],
            4: ["clash config", "sing-box config", "mihomo"],
            5: ["subscription", "node list", "server list"],
            6: ["free proxy", "vpn config", "v2ray node"],
            7: ["机场配置", "节点订阅", "代理配置"],
        }
        return shards.get(shard_id % 8, ["vless"])
    
    def _clean_text(self, text: str) -> str:
        """清洗文本：去除HTML标签、Markdown标记等"""
        # 去除HTML标签
        text = re.sub(r'<[^>]+>', ' ', text)
        # 去除Markdown代码块标记
        text = re.sub(r'```[a-z]*\n?', '', text)
        text = re.sub(r'`', '', text)
        # 去除URL编码的干扰
        text = text.replace('%3A', ':').replace('%40', '@').replace('%2F', '/')
        # 去除常见脏字符
        text = re.sub(r'[\\\[\\\]\{\}]', '', text)
        return text.strip()
    
    def _is_valid_node(self, link: str, protocol: str) -> bool:
        """🔥 严格验证节点格式"""
        if not link or len(link) < 20:
            return False
            
        # 清理链接
        link = self._clean_text(link)
        
        # 基本格式检查
        if not link.startswith(f"{protocol}://"):
            return False
            
        # 排除常见无效模式
        invalid_patterns = [
            'example.com', 'your-domain', 'your_server', 'your_port',
            'placeholder', 'xxx', 'test', 'demo', 'sample',
            'YOUR_', 'REPLACE_', 'CHANGE_ME', 'your-',
            'github.com', 'http://', 'https://',  # 不是代理协议
            '<', '>', '{', '}', '[', ']',  # 包含未清理的标记
            'vless://@', 'hysteria2://@',  # 缺少UUID
        ]
        
        if any(bad in link.lower() for bad in invalid_patterns):
            return False
        
        # 协议特定验证
        try:
            if protocol == 'vless':
                # vless://UUID@host:port
                match = re.match(r'vless://([a-f0-9-]{36})@([^:/]+):(\d+)', link)
                if not match:
                    return False
                uuid, host, port = match.groups()
                if not (1 <= int(port) <= 65535):
                    return False
                    
            elif protocol in ['hysteria2', 'hy2']:
                # hysteria2://UUID@host:port
                match = re.match(r'hysteria2://([a-f0-9-]{36})@([^:/]+):(\d+)', link)
                if not match:
                    return False
                uuid, host, port = match.groups()
                if not (1 <= int(port) <= 65535):
                    return False
                    
            elif protocol == 'tuic':
                # tuic://UUID:password@host:port
                match = re.match(r'tuic://([a-f0-9-]{36}):([^@]+)@([^:/]+):(\d+)', link)
                if not match:
                    return False
                    
            elif protocol == 'trojan':
                # trojan://password@host:port
                match = re.match(r'trojan://([^@]+)@([^:/]+):(\d+)', link)
                if not match:
                    return False
                    
            elif protocol == 'shadowsocks':
                # ss://base64@host:port
                if '@' not in link:
                    return False
                    
        except Exception:
            return False
        
        return True
    
    def _decode_subscription(self, content: str) -> List[str]:
        """解析订阅链接"""
        links = []
        content = self._clean_text(content)
        
        # 尝试 base64 解码
        try:
            cleaned = content.replace('\n', '').replace(' ', '')
            # 标准 base64
            decoded = base64.b64decode(cleaned).decode('utf-8', errors='ignore')
            if '://' in decoded:
                for line in decoded.split('\n'):
                    line = line.strip()
                    if line and '://' in line:
                        links.append(line)
        except: pass
        
        # 尝试 gzip
        try:
            cleaned = content.replace('\n', '')
            decoded = base64.b64decode(cleaned)
            decompressed = zlib.decompress(decoded, zlib.MAX_WBITS|16).decode('utf-8', errors='ignore')
            if '://' in decompressed:
                for line in decompressed.split('\n'):
                    line = line.strip()
                    if line and '://' in line:
                        links.append(line)
        except: pass
        
        return links
    
    async def _request(self, session: aiohttp.ClientSession, url: str, **kwargs):
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=30), **kwargs) as resp:
                if resp.status == 200:
                    return await resp.json()
                elif resp.status == 403:
                    await asyncio.sleep(30)
                    return await self._request(session, url, **kwargs)
        except: pass
        return None
    
    async def search_repositories(self, query: str, max_pages: int = 5) -> List[Dict]:
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
                await asyncio.sleep(0.5)
        return results
    
    async def get_raw_content(self, owner: str, repo: str, path: str, ref: str = "main") -> str:
        url = f"{self.raw_base}/{owner}/{repo}/{ref}/{path}"
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=20)) as resp:
                    if resp.status == 200:
                        return await resp.text(errors='ignore')
        except: pass
        return ""
    
    async def extract_nodes(self, content: str, source: str, source_type: str) -> List[Dict]:
        nodes = []
        content = self._clean_text(content)
        
        # 1. 先尝试解析订阅
        sub_links = self._decode_subscription(content)
        for link in sub_links:
            link = link.strip()
            if self._is_valid_node(link, 'vless') or self._is_valid_node(link, 'hysteria2') or \
               self._is_valid_node(link, 'tuic') or self._is_valid_node(link, 'trojan'):
                # 判断协议
                proto = 'unknown'
                for p in ['vless', 'hysteria2', 'hy2', 'tuic', 'trojan', 'ss', 'vmess']:
                    if link.startswith(f"{p}://"):
                        proto = p
                        break
                nodes.append({'link': link, 'protocol': proto, 'source': source, 'source_type': source_type})
        
        # 2. 正则匹配
        for pattern, protocol in self.node_patterns:
            matches = re.findall(pattern, content, re.IGNORECASE)
            for match in matches:
                link = match.strip()
                if self._is_valid_node(link, protocol):
                    nodes.append({'link': link, 'protocol': protocol, 'source': source, 'source_type': source_type})
        
        return nodes
    
    async def crawl_repo(self, repo: Dict) -> List[Dict]:
        nodes = []
        owner, repo_name = repo["owner"]["login"], repo["name"]
        default_branch = repo.get("default_branch", "main")
        
        # 获取文件列表
        url = f"{self.base_url}/repos/{owner}/{repo_name}/contents"
        async with aiohttp.ClientSession(headers=self.headers) as session:
            data = await self._request(session, url)
            if not data: return nodes
            
            for item in data if isinstance(data, list) else [data]:
                if item.get("type") != "file": continue
                name = item["name"].lower()
                if not any(name.endswith(ext) for ext in ['.json', '.yaml', '.yml', '.txt', '.conf', '.list']):
                    continue
                
                content = await self.get_raw_content(owner, repo_name, item["path"], default_branch)
                if content and len(content) > 100:
                    extracted = await self.extract_nodes(content, f"{owner}/{repo_name}", 'github_repo')
                    nodes.extend(extracted)
        
        return nodes
    
    async def crawl_from_repos(self) -> List[Dict]:
        all_nodes = []
        print(f"Searching with {len(self.repo_keywords)} keywords...")
        
        for keyword in self.repo_keywords:
            print(f"  🔍 {keyword}")
            repos = await self.search_repositories(keyword, max_pages=5)
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
                    print(f"    ✅ {repo['full_name']}: {len(repo_nodes)} valid nodes")
                    all_nodes.extend(repo_nodes)
                    self.stats['repos_crawled'] += 1
                    
                await asyncio.sleep(0.2)
        
        return all_nodes
    
    async def crawl_all(self) -> List[Dict]:
        print("Starting crawl...")
        nodes = await self.crawl_from_repos()
        print(f"Total valid nodes: {len(nodes)}")
        return nodes
    
    def get_crawl_stats(self) -> Dict:
        return {**self.stats, 'unique_links': len(self.seen_links)}
