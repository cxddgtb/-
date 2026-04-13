#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""GitHub Repository Search & Parser"""

import aiohttp
import asyncio
import re
import base64
from typing import List, Dict
from crawler.utils import setup_logger, detect_encoding

logger = setup_logger(__name__)

class GitHubRepoSearcher:
    def __init__(self):
        self.base_url = "https://api.github.com"
        self.headers = {
            "Accept": "application/vnd.github.v3+json",
            "User-Agent": "Proxy-Crawler/2.0"
        }

    async def search(self, query: str, sort: str = "updated", order: str = "desc", max_pages: int = 3) -> List[Dict]:
        url = f"{self.base_url}/search/repositories"
        params = {"q": query, "sort": sort, "order": order, "per_page": 100, "page": 1}
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
                        if not items: break
                        results.extend(items)
                        await asyncio.sleep(0.8)
                except Exception as e:
                    logger.error(f"Repo search error: {e}")
                    break
        return results

    async def parse_repo(self, repo: Dict) -> List[Dict]:
        nodes = []
        owner, repo_name = repo["owner"]["login"], repo["name"]
        url = f"{self.base_url}/repos/{owner}/{repo_name}/contents"

        try:
            async with aiohttp.ClientSession(headers=self.headers) as session:
                async with session.get(url) as resp:
                    if resp.status != 200: return nodes
                    files = [f for f in await resp.json() 
                             if f.get("type") == "file" and 
                             any(f["name"].endswith(ext) for ext in [".json", ".yaml", ".yml", ".txt", ".conf", ".md"])]

                    for file_info in files:
                        nodes.extend(await self._extract_file(session, owner, repo_name, file_info))
        except Exception as e:
            logger.debug(f"Parse repo {owner}/{repo_name} failed: {e}")
        return nodes

    async def _extract_file(self, session: aiohttp.ClientSession, owner: str, repo: str, file_info: Dict) -> List[Dict]:
        nodes = []
        try:
            async with session.get(file_info["url"]) as resp:
                if resp.status != 200: return nodes
                data = await resp.json()
                raw = base64.b64decode(data["content"])
                content = raw.decode(detect_encoding(raw), errors="ignore")

                patterns = [
                    (r'vless://[^\s\'"<>]+', 'vless'),
                    (r'hysteria2://[^\s\'"<>]+', 'hysteria2'),
                    (r'hy2://[^\s\'"<>]+', 'hysteria2'),
                    (r'tuic://[^\s\'"<>]+', 'tuic'),
                    (r'naiveproxy://[^\s\'"<>]+', 'naiveproxy'),
                    (r'shadowtls://[^\s\'"<>]+', 'shadowtls'),
                    (r'anytls://[^\s\'"<>]+', 'anytls'),
                ]
                for pattern, proto in patterns:
                    for match in re.findall(pattern, content):
                        nodes.append({
                            'link': match.strip(),
                            'protocol': proto,
                            'source': f"{owner}/{repo}",
                            'file_path': file_info["path"],
                            'source_type': 'github_repo'
                        })
        except Exception: pass
        return nodes
