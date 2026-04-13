#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""GitHub Gist Search Module"""

import aiohttp
import asyncio
import re
from typing import List, Dict
from crawler.utils import setup_logger

logger = setup_logger(__name__)

class GitHubGistSearcher:
    def __init__(self):
        self.base_url = "https://api.github.com"
        self.headers = {
            "Accept": "application/vnd.github.v3+json",
            "User-Agent": "Proxy-Crawler/2.0"
        }
        
    async def search(self, query: str, max_pages: int = 5) -> List[Dict]:
        """搜索Gist"""
        url = f"{self.base_url}/search/gists"
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
                            break
                            
                        data = await resp.json()
                        items = data.get("items", [])
                        
                        if not items:
                            break
                            
                        all_results.extend(items)
                        
                        await asyncio.sleep(0.5)
                        
        except Exception as e:
            logger.error(f"Error searching gists: {e}")
            
        return all_results
    
    async def parse_gist(self, gist: Dict) -> List[Dict]:
        """解析Gist"""
        nodes = []
        
        try:
            gist_id = gist["id"]
            files = gist.get("files", {})
            
            for filename, file_data in files.items():
                content = file_data.get("content", "")
                
                # 提取节点
                patterns = [
                    (r'vless://[^\s]+', 'vless'),
                    (r'hysteria2://[^\s]+', 'hysteria2'),
                    (r'tuic://[^\s]+', 'tuic'),
                ]
                
                for pattern, protocol in patterns:
                    matches = re.findall(pattern, content)
                    for match in matches:
                        nodes.append({
                            'link': match,
                            'protocol': protocol,
                            'source': f"gist/{gist_id}",
                            'source_type': 'github_gist'
                        })
                        
        except Exception as e:
            logger.debug(f"Error parsing gist: {e}")
            
        return nodes
