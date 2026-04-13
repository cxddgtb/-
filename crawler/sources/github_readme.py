#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""GitHub README Search Module"""

import aiohttp
import asyncio
import re
from typing import List, Dict
from crawler.utils import setup_logger

logger = setup_logger(__name__)

class GitHubREADMESearcher:
    def __init__(self):
        self.base_url = "https://api.github.com"
        self.headers = {
            "Accept": "application/vnd.github.v3+json",
            "User-Agent": "Proxy-Crawler/2.0"
        }
        
    async def search(self, query: str, max_pages: int = 5) -> List[Dict]:
        """搜索README文件"""
        url = f"{self.base_url}/search/code"
        params = {
            "q": f"{query} filename:readme.md",
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
                        await asyncio.sleep(1)
                        
        except Exception as e:
            logger.error(f"Error searching READMEs: {e}")
            
        return all_results
    
    async def extract_nodes(self, file_info: Dict) -> List[Dict]:
        """从README提取节点"""
        # 类似GitHubCodeSearcher的实现
        return []
