#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""GitHub Code Search Module"""

import aiohttp
import asyncio
import re
import base64
from typing import List, Dict
from crawler.utils import setup_logger, detect_encoding

logger = setup_logger(__name__)

class GitHubCodeSearcher:
    def __init__(self):
        self.base_url = "https://api.github.com"
        self.token = ""
        self.headers = {
            "Accept": "application/vnd.github.v3+json",
            "User-Agent": "Proxy-Crawler/2.0"
        }
        
    async def search(self, query: str, max_pages: int = 5) -> List[Dict]:
        """搜索代码"""
        url = f"{self.base_url}/search/code"
        params = {
            "q": query,
            "per_page": 100,
            "page": 1
        }
        
        all_results = []
        
        try:
            async with aiohttp.ClientSession(headers=self.headers) as session:
                for page in range(1, max_pages + 1):
                    params["page"] = page
                    
                    async with session.get(url, params=params) as resp:
                        if resp.status != 200:
                            logger.warning(f"Search failed: {resp.status}")
                            break
                            
                        data = await resp.json()
                        items = data.get("items", [])
                        
                        if not items:
                            break
                            
                        all_results.extend(items)
                        logger.debug(f"  Page {page}: {len(items)} results")
                        
                        # GitHub代码搜索最多1000条
                        if len(all_results) >= 1000:
                            break
                            
                        await asyncio.sleep(1)  # 限流
                        
        except Exception as e:
            logger.error(f"Error searching code: {e}")
            
        return all_results
    
    async def extract_nodes(self, file_info: Dict) -> List[Dict]:
        """从文件中提取节点"""
        nodes = []
        
        try:
            owner = file_info["repository"]["owner"]["login"]
            repo = file_info["repository"]["name"]
            path = file_info["path"]
            
            # 获取文件内容
            content_url = file_info.get("git_url") or f"{self.base_url}/repos/{owner}/{repo}/contents/{path}"
            
            async with aiohttp.ClientSession(headers=self.headers) as session:
                async with session.get(content_url) as resp:
                    if resp.status != 200:
                        return nodes
                        
                    data = await resp.json()
                    
                    # 解码内容
                    if "content" in data:
                        raw = base64.b64decode(data["content"])
                        encoding = detect_encoding(raw)
                        content = raw.decode(encoding, errors='ignore')
                    else:
                        content = ""
                    
                    # 提取节点链接
                    node_patterns = [
                        (r'vless://[^\s\'"]+', 'vless'),
                        (r'hysteria2://[^\s\'"]+', 'hysteria2'),
                        (r'hy2://[^\s\'"]+', 'hysteria2'),
                        (r'tuic://[^\s\'"]+', 'tuic'),
                        (r'naiveproxy[^\s\'"]*', 'naiveproxy'),
                        (r'shadowtls[^\s\'"]*', 'shadowtls'),
                        (r'anytls[^\s\'"]*', 'anytls'),
                    ]
                    
                    for pattern, protocol in node_patterns:
                        matches = re.findall(pattern, content, re.IGNORECASE)
                        for match in matches:
                            nodes.append({
                                'link': match.strip(),
                                'protocol': protocol,
                                'source': f"{owner}/{repo}",
                                'file_path': path,
                                'source_type': 'github_code'
                            })
                    
                    # 提取JSON/YAML配置
                    if '"vless"' in content.lower() or "'vless'" in content.lower():
                        nodes.extend(self.parse_config(content, 'vless', f"{owner}/{repo}", path))
                        
        except Exception as e:
            logger.debug(f"Error extracting from file: {e}")
            
        return nodes
    
    def parse_config(self, content: str, protocol: str, source: str, path: str) -> List[Dict]:
        """解析配置文件"""
        nodes = []
        # 这里可以添加JSON/YAML解析逻辑
        return nodes
