#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Validator Module - 极速并发网络验证 (Python 3.11 兼容)
"""

import aiohttp
import asyncio

class Validator:
    def __init__(self, max_concurrent: int = 100):
        self.semaphore = asyncio.Semaphore(max_concurrent)

    async def check_node(self, session: aiohttp.ClientSession, link: str) -> bool:
        """快速 TCP 端口连通性测试"""
        async with self.semaphore:
            try:
                if '@' not in link: return False
                after_at = link.split('@')[1]
                host_port = after_at.split('/')[0].split('?')[0]
                if ':' not in host_port: return False
                
                host, port_str = host_port.rsplit(':', 1)
                port = int(port_str)
                
                if not (0 < port <= 65535): return False

                # 2秒超时 TCP 握手
                reader, writer = await asyncio.wait_for(
                    asyncio.open_connection(host, port),
                    timeout=2.0
                )
                writer.close()
                await writer.wait_closed()
                return True
            except Exception:
                return False

    async def validate_batch(self, links: list[str]) -> list[str]:
        """并发验证一批链接"""
        if not links: return []
        
        valid_links = []
        async with aiohttp.ClientSession() as session:
            tasks = [self.check_node(session, link) for link in links]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            for link, result in zip(links, results):
                if result is True:
                    valid_links.append(link)
        return valid_links
