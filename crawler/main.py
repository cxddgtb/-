#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Simplified Main Entry Point
"""

import asyncio
import json
import os
from datetime import datetime
from pathlib import Path
import sys

sys.path.append(str(Path(__file__).parent.parent))

from crawler.github_crawler import SimpleGitHubCrawler
from crawler.validator import NodeValidator
from crawler.deduplicator import NodeDeduplicator
from crawler.utils import setup_logger, save_to_file

logger = setup_logger(__name__)

async def main():
    print("=" * 60)
    print("🚀 Starting Simplified Proxy Node Crawler")
    print("=" * 60)
    
    # 初始化组件
    github_token = os.getenv("GITHUB_TOKEN", "")
    crawler = SimpleGitHubCrawler(github_token)
    validator = NodeValidator(concurrent_limit=30, timeout=5)
    deduplicator = NodeDeduplicator()
    
    try:
        # 爬取节点
        print("🕷️  Crawling nodes from multiple sources...")
        all_nodes = await crawler.crawl_all()
        print(f"📊 Total crawled: {len(all_nodes)} nodes")
        
        if not all_nodes:
            print("⚠️  No nodes found! Check your search queries.")
            return
        
        # 去重
        stats = deduplicator.add_or_update_nodes(all_nodes)
        print(f"🧹 Deduplication: {stats}")
        
        # 验证节点
        print("🔍 Validating nodes...")
        pending_nodes = deduplicator.get_recent_nodes(limit=200)
        
        if pending_nodes:
            validation_results = await validator.validate_nodes_batch(pending_nodes)
            valid_results = []
            for result in validation_results:
                if hasattr(result, 'node_link'):
                    valid_results.append({
                        'link': result.node_link,
                        'protocol': result.protocol,
                        'is_valid': result.is_valid,
                        'latency_ms': result.latency_ms,
                    })
            
            deduplicator.update_validation_results(valid_results)
            valid_nodes = validator.filter_valid_nodes(validation_results, max_latency=500.0)
            print(f"✅ Valid nodes: {len(valid_nodes)}")
        else:
            valid_nodes = []
            print("⚠️  No nodes to validate")
        
        # 生成输出
        output_dir = Path("output")
        output_dir.mkdir(exist_ok=True)
        
        # 按协议分类
        by_protocol = {}
        for node in valid_nodes:
            proto = node.get('protocol', 'unknown')
            if proto not in by_protocol:
                by_protocol[proto] = []
            by_protocol[proto].append(node)
        
        # 保存文件
        for protocol, nodes in by_protocol.items():
            if nodes:
                links = [n.get('link', '') for n in nodes if n.get('link')]
                save_to_file(output_dir / f"{protocol}_sub.txt", "\n".join(links))
                save_to_file(output_dir / f"{protocol}_nodes.json", 
                           json.dumps(nodes, indent=2, ensure_ascii=False))
        
        # 合并文件
        all_links = []
        for nodes in by_protocol.values():
            all_links.extend([n.get('link', '') for n in nodes if n.get('link')])
        
        if all_links:
            save_to_file(output_dir / "all_sub.txt", "\n".join(all_links))
            save_to_file(output_dir / "all_nodes.json", 
                       json.dumps(by_protocol, indent=2, ensure_ascii=False))
        
        # 统计报告
        report = f"""# Proxy Nodes Report
Generated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
Total crawled: {len(all_nodes)}
Valid nodes: {len(valid_nodes)}

By protocol:
"""
        for proto, nodes in by_protocol.items():
            report += f"- {proto}: {len(nodes)}\n"
        
        save_to_file(output_dir / "STATS.md", report)
        print(f"📁 Output saved to {output_dir}")
        print("=" * 60)
        print("🎉 Crawler completed successfully!")
        print("=" * 60)
        
    except Exception as e:
        print(f"❌ Error: {e}")
        raise

if __name__ == "__main__":
    asyncio.run(main())
