#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
GitHub Search Module
Searches GitHub repositories for proxy configurations
"""

import aiohttp
import asyncio
from typing import List, Dict, Optional
from datetime import datetime, timedelta
import os
from crawler.utils import setup_logger

logger = setup_logger(__name__)

class GitHubSearcher:
    def __init__(self):
        self.base_url = "https://api.github.com"
        self.token = os.getenv("GITHUB_TOKEN", "")
        self.headers = {
            "Accept": "application/vnd.github.v3+json",
            "User-Agent": "Proxy-Node-Crawler/1.0"
        }
        if self.token:
            self.headers["Authorization"] = f"token {self.token}"
            
    async def search_repos(self, query: str, sort: str = "updated", order: str = "desc") -> List[Dict]:
        """Search GitHub repositories"""
        url = f"{self.base_url}/search/repositories"
        params = {
            "q": query,
            "sort": sort,
            "order": order,
            "per_page": 100,
            "page": 1
        }
        
        all_results = []
        
        try:
            async with aiohttp.ClientSession(headers=self.headers) as session:
                # Fetch first page
                async with session.get(url, params=params) as response:
                    if response.status == 200:
                        data = await response.json()
                        all_results.extend(data.get("items", []))
                        
                        # Check for more pages
                        total_count = data.get("total_count", 0)
                        max_pages = min((total_count // 100) + 1, 10)  # Limit to 10 pages
                        
                        # Fetch additional pages
                        for page in range(2, max_pages + 1):
                            params["page"] = page
                            await asyncio.sleep(1)  # Rate limiting
                            
                            async with session.get(url, params=params) as resp:
                                if resp.status == 200:
                                    data = await resp.json()
                                    all_results.extend(data.get("items", []))
                                else:
                                    logger.warning(f"Search page {page} failed: {resp.status}")
                                    break
                    else:
                        logger.error(f"Search failed: {response.status} - {await response.text()}")
                        
        except Exception as e:
            logger.error(f"Error searching repos: {e}")
            
        return all_results
    
    async def get_repo_contents(self, owner: str, repo: str, path: str = "") -> List[Dict]:
        """Get repository contents"""
        url = f"{self.base_url}/repos/{owner}/{repo}/contents/{path}"
        
        try:
            async with aiohttp.ClientSession(headers=self.headers) as session:
                async with session.get(url) as response:
                    if response.status == 200:
                        return await response.json()
        except Exception as e:
            logger.error(f"Error getting repo contents: {e}")
            
        return []
    
# 在 get_file_content 方法中替换编码处理部分：

async def get_file_content(self, owner: str, repo: str, path: str) -> Optional[str]:
    """Get file content from repository"""
    url = f"{self.base_url}/repos/{owner}/{repo}/contents/{path}"
    
    try:
        async with aiohttp.ClientSession(headers=self.headers) as session:
            async with session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    import base64
                    # 解码 base64 内容
                    raw_content = base64.b64decode(data["content"])
                    # 自动检测编码
                    from crawler.utils import detect_encoding
                    encoding = detect_encoding(raw_content)
                    content = raw_content.decode(encoding, errors='ignore')
                    return content
    except Exception as e:
        logger.error(f"Error getting file content: {e}")
        
    return None
    
    async def search_code(self, query: str) -> List[Dict]:
        """Search GitHub code"""
        url = f"{self.base_url}/search/code"
        params = {
            "q": query,
            "per_page": 100,
            "page": 1
        }
        
        all_results = []
        
        try:
            async with aiohttp.ClientSession(headers=self.headers) as session:
                async with session.get(url, params=params) as response:
                    if response.status == 200:
                        data = await response.json()
                        all_results.extend(data.get("items", []))
        except Exception as e:
            logger.error(f"Error searching code: {e}")
            
        return all_results
