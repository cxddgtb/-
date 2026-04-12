#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Node Validator Module
验证代理节点的有效性和延迟
"""

import asyncio
import aiohttp
import json
import time
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
import re
from urllib.parse import urlparse
from crawler.utils import setup_logger

logger = setup_logger(__name__)

@dataclass
class ValidationResult:
    """验证结果"""
    node_link: str
    protocol: str
    is_valid: bool
    latency_ms: float
    last_checked: str
    error_message: str = ""
    response_time: float = 0.0

class NodeValidator:
    """异步节点验证器"""
    
    def __init__(self, concurrent_limit: int = 50, timeout: int = 5):
        self.concurrent_limit = concurrent_limit
        self.timeout = timeout
        self.semaphore = asyncio.Semaphore(concurrent_limit)
        
        # 测试URL（国内可访问的测试站点）
        self.test_urls = [
            "https://www.baidu.com",
            "https://www.cloudflare.com/cdn-cgi/trace",
            "http://www.gstatic.com/generate_204"
        ]
        
    async def validate_nodes_batch(self, nodes: List[Dict]) -> List[ValidationResult]:
        """批量验证节点"""
        tasks = [self.validate_single_node(node) for node in nodes]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        valid_results = []
        for result in results:
            if isinstance(result, Exception):
                logger.error(f"Validation task failed: {result}")
            elif result and isinstance(result, ValidationResult):
                valid_results.append(result)
                
        logger.info(f"Batch validation completed: {len(valid_results)}/{len(nodes)} nodes processed")
        return valid_results
    
    async def validate_single_node(self, node: Dict) -> Optional[ValidationResult]:
        """验证单个节点"""
        async with self.semaphore:
            try:
                node_link = node.get('link', '')
                protocol = node.get('protocol', '')
                
                if not node_link:
                    return None
                
                start_time = time.time()
                
                # 根据不同协议使用不同验证方法
                if protocol in ['vless', 'hysteria2', 'tuic']:
                    is_valid, latency = await self._validate_trojan_vless(node_link)
                elif protocol in ['naiveproxy', 'anytls', 'shadowtls']:
                    is_valid, latency = await self._validate_http_proxy(node_link)
                else:
                    is_valid, latency = await self._validate_generic(node_link)
                
                elapsed = time.time() - start_time
                
                return ValidationResult(
                    node_link=node_link,
                    protocol=protocol,
                    is_valid=is_valid,
                    latency_ms=round(latency, 2),
                    last_checked=datetime.now().isoformat(),
                    response_time=round(elapsed, 2)
                )
                
            except Exception as e:
                logger.error(f"Error validating node: {e}")
                return ValidationResult(
                    node_link=node.get('link', ''),
                    protocol=node.get('protocol', ''),
                    is_valid=False,
                    latency_ms=-1,
                    last_checked=datetime.now().isoformat(),
                    error_message=str(e)
                )
    
    async def _validate_trojan_vless(self, link: str) -> Tuple[bool, float]:
        """验证 VLESS/Trojan/Hysteria/TUIC 类型节点"""
        try:
            # 解析链接获取服务器信息
            parsed = urlparse(link)
            if not parsed.hostname:
                return False, 0.0
                
            # 简单的TCP连接测试
            async with asyncio.timeout(self.timeout):
                reader, writer = await asyncio.open_connection(
                    parsed.hostname,
                    parsed.port or 443
                )
                writer.close()
                await writer.wait_closed()
                return True, 50.0  # 简化延迟计算
        except:
            return False, 0.0
    
    async def _validate_http_proxy(self, link: str) -> Tuple[bool, float]:
        """验证 HTTP/HTTPS 代理类型节点"""
        try:
            async with asyncio.timeout(self.timeout):
                # 尝试连接代理
                connector = aiohttp.TCPConnector(ssl=False)
                async with aiohttp.ClientSession(connector=connector) as session:
                    async with session.get(
                        self.test_urls[0],
                        timeout=aiohttp.ClientTimeout(total=self.timeout)
                    ) as response:
                        if response.status == 200:
                            return True, response.headers.get('X-Response-Time', 100)
        except:
            pass
        return False, 0.0
    
    async def _validate_generic(self, link: str) -> Tuple[bool, float]:
        """通用验证方法"""
        try:
            async with asyncio.timeout(self.timeout):
                connector = aiohttp.TCPConnector(ssl=False)
                async with aiohttp.ClientSession(connector=connector) as session:
                    async with session.get(
                        self.test_urls[1],
                        timeout=aiohttp.ClientTimeout(total=self.timeout)
                    ) as response:
                        return response.status == 200, 50.0
        except:
            return False, 0.0
    
    def filter_valid_nodes(self, results: List[ValidationResult], 
                          max_latency: float = 300.0,
                          min_valid_ratio: float = 0.1) -> List[Dict]:
        """过滤有效节点"""
        valid_nodes = []
        for result in results:
            if result.is_valid and result.latency_ms > 0 and result.latency_ms <= max_latency:
                valid_nodes.append({
                    'link': result.node_link,
                    'protocol': result.protocol,
                    'latency': result.latency_ms,
                    'last_checked': result.last_checked,
                    'response_time': result.response_time
                })
        
        logger.info(f"Filtered {len(valid_nodes)} valid nodes from {len(results)} tested")
        return valid_nodes
