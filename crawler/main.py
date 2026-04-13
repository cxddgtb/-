#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Proxy Node Crawler - Main Entry Point
Enhanced with validation and deduplication
"""

import asyncio
import json
import os
from datetime import datetime
from pathlib import Path
import sys

sys.path.append(str(Path(__file__).parent.parent))

from crawler.github_search import GitHubSearcher
from crawler.parser import NodeParser
from crawler.validator import NodeValidator
from crawler.deduplicator import NodeDeduplicator
from crawler.utils import setup_logger, save_to_file
from config.settings import Config

logger = setup_logger(__name__)

class ProxyNodeCrawler:
    def __init__(self):
        self.searcher = GitHubSearcher()
        self.parser = NodeParser()
        self.validator = NodeValidator(concurrent_limit=30, timeout=5)
        self.deduplicator = NodeDeduplicator()
        self.config = Config()
        
    async def crawl_and_validate(self, protocol: str, keywords: list) -> list:
        """爬取并验证指定协议的节点"""
        logger.info(f"Starting crawl for {protocol}...")
        
        # 1. 爬取新节点
        new_nodes = []
        for keyword in keywords:
            try:
                # 优先搜索最近更新的仓库
                results = await self.searcher.search_repos(
                    keyword, 
                    sort="updated", 
                    order="desc"
                )
                
                for repo in results:
                    # 只处理最近7天更新的仓库
                    updated_at = datetime.fromisoformat(
                        repo['updated_at'].replace('Z', '+00:00')
                    )
                    if (datetime.now(updated_at.tzinfo) - updated_at).days > 7:
                        continue
                        
                    node_info = await self.parser.parse_repository(repo, protocol)
                    if node_info:
                        new_nodes.extend(node_info)
                        
            except Exception as e:
                logger.error(f"Error searching {keyword}: {e}")
        
        logger.info(f"Crawled {len(new_nodes)} new {protocol} nodes")
        
        # 2. 去重并存储
        if new_nodes:
            stats = self.deduplicator.add_or_update_nodes(new_nodes)
            logger.info(f"Deduplication: {stats}")
        
        # 3. 获取待验证节点（新节点 + 需要重新验证的旧节点）
        pending_nodes = self.deduplicator.get_recent_nodes(protocol, limit=200)
        
        # 4. 批量验证
        if pending_nodes:
            logger.info(f"Validating {len(pending_nodes)} {protocol} nodes...")
            validation_results = await self.validator.validate_nodes_batch(pending_nodes)
            
            # 更新验证结果
            valid_results = []
            for result in validation_results:
                if isinstance(result, Exception):
                    continue
                valid_results.append({
                    'link': result.node_link,
                    'protocol': result.protocol,
                    'is_valid': result.is_valid,
                    'latency_ms': result.latency_ms,
                    'response_time': result.response_time
                })
            
            self.deduplicator.update_validation_results(valid_results)
            
            # 过滤有效节点
            valid_nodes = self.validator.filter_valid_nodes(
                validation_results,
                max_latency=300.0
            )
            
            logger.info(f"Found {len(valid_nodes)} valid {protocol} nodes")
            return valid_nodes
        
        return []
    
    async def crawl_all_protocols(self):
        """爬取所有协议"""
        protocols_config = {
            'vless': ["vless reality", "vless vision", "xray vless reality"],
            'naiveproxy': ["naiveproxy config", "naive proxy"],
            'anytls': ["anytls config"],
            'shadowtls': ["shadowtls", "shadow-tls config"],
            'hysteria2': ["hysteria2", "hy2 config"],
            'tuic': ["tuic config", "tuic v5"]
        }
        
        all_valid_nodes = {}
        
        # 并发爬取所有协议
        tasks = [
            self.crawl_and_validate(protocol, keywords)
            for protocol, keywords in protocols_config.items()
        ]
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        for protocol, result in zip(protocols_config.keys(), results):
            if isinstance(result, Exception):
                logger.error(f"Failed to crawl {protocol}: {result}")
                all_valid_nodes[protocol] = []
            else:
                all_valid_nodes[protocol] = result
        
        return all_valid_nodes
    
    def generate_output(self, valid_nodes: dict):
        """生成输出文件"""
        output_dir = Path("output")
        output_dir.mkdir(exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # 生成各协议订阅
        for protocol, nodes in valid_nodes.items():
            if not nodes:
                continue
                
            # 订阅链接文件
            links = [node['link'] for node in nodes]
            save_to_file(
                output_dir / f"{protocol}_sub.txt",
                "\n".join(links)
            )
            
            # JSON详细文件
            save_to_file(
                output_dir / f"{protocol}_nodes.json",
                json.dumps(nodes, indent=2, ensure_ascii=False)
            )
        
        # 合并订阅
        all_links = []
        for nodes in valid_nodes.values():
            all_links.extend([node['link'] for node in nodes])
            
        if all_links:
            save_to_file(
                output_dir / "all_sub.txt",
                "\n".join(all_links)
            )
            
            save_to_file(
                output_dir / "all_nodes.json",
                json.dumps(valid_nodes, indent=2, ensure_ascii=False)
            )
        
        # 生成统计报告
        stats = self.deduplicator.get_stats()
        report = f"""# Proxy Nodes Report
**Generated:** {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}

## Database Statistics
- Total nodes: {stats['total']}
- Valid nodes: {stats['valid']}
- Last validation: {stats.get('last_validation', 'N/A')}

## Protocol Breakdown
"""
        for proto, data in stats.get('by_protocol', {}).items():
            report += f"- {proto}: {data['valid']}/{data['total']} valid\n"
            
        save_to_file(output_dir / "STATS.md", report)
        
        logger.info(f"Output generated in {output_dir}")
    
    async def run(self):
        """主执行流程"""
        logger.info("=== Starting Proxy Node Crawler ===")
        start_time = datetime.now()
        
        try:
            # 1. 清理过期节点
            cleaned = self.deduplicator.cleanup_old_nodes(max_age_days=14)
            logger.info(f"Cleaned {cleaned} old nodes")
            
            # 2. 爬取并验证所有协议
            valid_nodes = await self.crawl_all_protocols()
            
            # 3. 生成输出
            self.generate_output(valid_nodes)
            
            # 4. 打印统计
            stats = self.deduplicator.get_stats()
            logger.info(f"=== Crawler Completed ===")
            logger.info(f"Duration: {(datetime.now() - start_time).total_seconds():.2f}s")
            logger.info(f"Total valid nodes: {stats['valid']}")
            
        except Exception as e:
            logger.error(f"Crawler failed: {e}")
            raise

async def main():
    crawler = ProxyNodeCrawler()
    await crawler.run()

if __name__ == "__main__":
    asyncio.run(main())
