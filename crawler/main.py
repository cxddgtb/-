#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Main Entry Point - Optimized for Large Datasets
"""

import asyncio
import json
import os
from datetime import datetime
from pathlib import Path
import sys

sys.path.append(str(Path(__file__).parent.parent))

from crawler.github_crawler import SuperGitHubCrawler
from crawler.validator import NodeValidator
from crawler.deduplicator import NodeDeduplicator
from crawler.utils import setup_logger, save_to_file

logger = setup_logger(__name__)

async def main():
    print("=" * 60)
    print("🚀 Starting Proxy Node Crawler")
    print("=" * 60)
    
    github_token = os.getenv("GITHUB_TOKEN", "")
    crawler = SuperGitHubCrawler(github_token)
    validator = NodeValidator(concurrent_limit=100, timeout=8)
    deduplicator = NodeDeduplicator()
    
    try:
        # 1. 爬取
        print("🕷️  Crawling nodes...")
        all_nodes = await crawler.crawl_all()
        print(f"📊 Total crawled: {len(all_nodes)} nodes")
        
        if not all_nodes:
            print("⚠️  No nodes found!")
            return
        
        # 2. 去重
        stats = deduplicator.add_or_update_nodes(all_nodes)
        print(f"🧹 Dedup: {stats}")
        
        # 3. 验证（🔥 限制验证数量，防止 Actions 超时）
        print("🔍 Validating (max 500 pending)...")
        pending = deduplicator.get_recent_nodes(limit=500)
        valid_nodes = []
        
        if pending:
            print(f"   Pending: {len(pending)} nodes")
            results = await validator.validate_nodes_batch(pending)
            valid_results = []
            for r in results:
                if hasattr(r, 'node_link'):
                    valid_results.append({
                        'link': r.node_link,
                        'protocol': r.protocol,
                        'is_valid': r.is_valid,
                        'latency_ms': r.latency_ms,
                    })
            deduplicator.update_validation_results(valid_results)
            valid_nodes = validator.filter_valid_nodes(results, max_latency=500.0)
            print(f"✅ Valid found: {len(valid_nodes)}")
        else:
            print("   No pending nodes to validate")
        
        # 4. 生成输出
        output_dir = Path("output")
        output_dir.mkdir(exist_ok=True)
        
        by_protocol = {}
        for node in valid_nodes:
            proto = node.get('protocol', 'unknown')
            by_protocol.setdefault(proto, []).append(node)
        
        for protocol, nodes in by_protocol.items():
            links = [n.get('link', '') for n in nodes if n.get('link')]
            if links:
                save_to_file(output_dir / f"{protocol}_sub.txt", "\n".join(links))
                save_to_file(output_dir / f"{protocol}_nodes.json", 
                           json.dumps(nodes, indent=2, ensure_ascii=False))
        
        all_links = []
        for nodes in by_protocol.values():
            all_links.extend([n.get('link', '') for n in nodes if n.get('link')])
        
        if all_links:
            save_to_file(output_dir / "all_sub.txt", "\n".join(all_links))
            save_to_file(output_dir / "all_nodes.json", 
                       json.dumps(by_protocol, indent=2, ensure_ascii=False))
        
        # 5. 统计报告
        report = f"""# Proxy Nodes Report
Generated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
Total crawled: {len(all_nodes)}
Valid nodes: {len(valid_nodes)}

By protocol:
"""
        for proto, nodes in by_protocol.items():
            report += f"- {proto}: {len(nodes)}\n"
        save_to_file(output_dir / "STATS.md", report)
        
        # 6. 🔥 自动清理数据库（已修复 VACUUM 事务冲突）
        print("🗄️  Cleaning database...")
        db_size = deduplicator.auto_cleanup(max_total_nodes=50000, max_age_days=7)
        print(f"📦 DB size: {db_size} MB")
        
        print("🎉 Crawler completed successfully!")
        
    except Exception as e:
        print(f"❌ Fatal Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())
