#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""GitHub README Search Module"""

import aiohttp
import asyncio
import re
import base64
from typing import List, Dict
from crawler.utils import setup_logger, detect_encoding

logger = setup_logger(__name__)

class GitHubREADMESearcher:
    def __init__(self):
        self.base_url = "https://api.github.com"
        self.headers = {"Accept": "application/vnd.github.v3+json", "User-Agent": "Proxy-Crawler/2.0"}

    async def search(self, query: str, max_pages: int = 3) -> List[Dict]:
        url = f"{self.base_url}/search/code"
        params = {"q": f"{query} filename:readme.md", "per_page": 100, "page": 1}
        results = []
        async with aiohttp.ClientSession(headers=self.headers) as session:
            for page in range(1, max_pages + 1):
                params["page"] = page
                async with session.get(url, params=params) as resp:
                    if resp.status != 200: break
                    items = (await resp.json()).get("items", [])
                    if not items: break
                    results.extend(items)
                    await asyncio.sleep(1)
        return results

    async def extract_nodes(self, file_info: Dict) -> List[Dict]:
        # 复用 Code 解析逻辑
        from .github_code import GitHubCodeSearcher
        return await GitHubCodeSearcher().extract_nodes(file_info)
