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
        self.headers = {"Accept": "application/vnd.github.v3+json", "User-Agent": "Proxy-Crawler/2.0"}

    async def search(self, query: str, max_pages: int = 3) -> List[Dict]:
        url = f"{self.base_url}/search/gists"
        params = {"q": query, "per_page": 100, "page": 1}
        results = []
        async with aiohttp.ClientSession(headers=self.headers) as session:
            for page in range(1, max_pages + 1):
                params["page"] = page
                async with session.get(url, params=params) as resp:
                    if resp.status != 200: break
                    items = (await resp.json()).get("items", [])
                    if not items: break
                    results.extend(items)
                    await asyncio.sleep(0.5)
        return results

    async def parse_gist(self, gist: Dict) -> List[Dict]:
        nodes = []
        try:
            content = "".join([f.get("content", "") for f in gist.get("files", {}).values()])
            for pattern, proto in [(r'vless://[^\s]+', 'vless'), (r'hysteria2://[^\s]+', 'hysteria2'), (r'tuic://[^\s]+', 'tuic')]:
                for m in re.findall(pattern, content):
                    nodes.append({'link': m.strip(), 'protocol': proto, 'source': f"gist/{gist['id']}", 'source_type': 'github_gist'})
        except: pass
        return nodes
