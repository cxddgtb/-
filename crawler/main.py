#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Main Entry Point - With Shard Support for Parallel Crawling
支持8路并行爬取的主入口
"""

import asyncio
import json
import os
import sys
import argparse
from datetime import datetime
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))

from crawler.github_crawler import SuperGitHubCrawler
from crawler.validator import NodeValidator
from crawler.deduplicator import NodeDeduplicator
from crawler.utils import setup_logger, save_to_file

logger = setup_logger(__name__)

async def main():
    # 🔥 解析命令行参数
    parser = argparse.ArgumentParser(description='Proxy Node Crawler')
    parser.add_argument('--shard', type=int, default=-1, help='Shard ID (0-7) for parallel crawling, -1 for single-thread')
    parser.add_argument('--skip-validate', action='store_true', help='Skip node validation (for fast crawl)')
    args = parser.parse_args()
    
    print("=" * 60)
    if args.shard >= 0:
        print(f"🚀 Starting Proxy Node Crawler [Shard #{args.shard}]")
    else:
        print("🚀 Starting Proxy Node Crawler [Single Thread]")
    print(f"📋 Skip validation: {args.skip_validate}")
    print("=" * 60)
    
    github_token = os.getenv("GITHUB_TOKEN", "")
    
    # 🔥 传入 shard_id
    crawler = SuperGitHubCrawler(github_token, shard_id=args.shard)
    validator = NodeValidator(concurrent_limit=100, timeout=8)
    deduplicator = NodeDeduplicator()
    
    try:
        # 1. 爬取节点
        print("🕷️  Crawling nodes...")
        all_nodes = await crawler.crawl_all()
        print(f"📊 Total crawled: {len(all_nodes)} nodes")
        
        if not all_nodes:
            print("⚠️  No nodes found!")
            return
        
        # 2. 去重存储（使用批量+冲突处理）
        print("🧹 Deduplicating and storing...")
        stats = deduplicator.add_or_update_nodes(all_nodes, batch_size=500)
        print(f"📦 Dedup stats: {stats}")
        
        # 3. 验证节点（可选，分片模式可跳过以节省时间）
        valid_nodes = []
        if not args.skip_validate and args.shard < 0:
            print("🔍 Validating nodes...")
            pending = deduplicator.get_recent_nodes(limit=500)
            
            if pending:
                print(f"   Validating {len(pending)} pending nodes...")
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
                print(f"✅ Valid nodes found: {len(valid_nodes)}")
            else:
                print("   No pending nodes to validate")
        elif args.skip_validate:
            print("⏭️  Skipping validation (fast mode)")
            # 不验证时，取最近添加的节点作为"有效"
            valid_nodes = deduplicator.get_recent_nodes(limit=1000)
        else:
            print("⏭️  Skipping validation (shard mode - will validate in merge step)")
        
        # 4. 生成输出文件
        output_dir = Path("output")
        output_dir.mkdir(exist_ok=True)
        
        by_protocol = {}
        for node in valid_nodes:
            proto = node.get('protocol', 'unknown')
            if proto not in by_protocol:
                by_protocol[proto] = []
            by_protocol[proto].append(node)
        
        for protocol, nodes in by_protocol.items():
            links = [n.get('link', '') for n in nodes if n.get('link')]
            if links:
                save_to_file(output_dir / f"{protocol}_sub.txt", "\n".join(links))
                save_to_file(output_dir / f"{protocol}_nodes.json", 
                           json.dumps(nodes, indent=2, ensure_ascii=False))
                print(f"💾 Saved {len(links)} {protocol} links")
        
        # 合并所有协议
        all_links = []
        for nodes in by_protocol.values():
            all_links.extend([n.get('link', '') for n in nodes if n.get('link')])
        
        if all_links:
            save_to_file(output_dir / "all_sub.txt", "\n".join(all_links))
            save_to_file(output_dir / "all_nodes.json", 
                       json.dumps(by_protocol, indent=2, ensure_ascii=False))
            print(f"💾 Saved {len(all_links)} total links")
        
        # 5. 生成统计报告
        report = f"""# Proxy Nodes Report
Generated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
Mode: {"Shard #" + str(args.shard) if args.shard >= 0 else "Single-thread"}
Skip validation: {args.skip_validate}

## Crawling Stats
- Total crawled this run: {len(all_nodes)}
- Stored in database: {stats.get('new', 0) + stats.get('updated', 0)}
- Valid nodes: {len(valid_nodes)}

## By Protocol
"""
        for proto, nodes in sorted(by_protocol.items()):
            report += f"- {proto}: {len(nodes)}\n"
        
        save_to_file(output_dir / "STATS.md", report)
        print(f"📊 Stats saved to output/STATS.md")
        
        # 6. 数据库清理（单线程模式执行，分片模式跳过）
        if args.shard < 0 and not args.skip_validate:
            print("🗄️  Cleaning database...")
            db_size = deduplicator.auto_cleanup(max_total_nodes=50000, max_age_days=7)
            print(f"📦 Database size: {db_size} MB")
            
            # 打印最终统计
            final_stats = deduplicator.get_stats()
            print(f"\n🎯 Final Database Stats:")
            print(f"   Total nodes: {final_stats.get('total', 0)}")
            print(f"   Valid nodes: {final_stats.get('valid', 0)}")
            print(f"   By protocol: {final_stats.get('by_protocol', {})}")
        
        print("\n" + "=" * 60)
        if args.shard >= 0:
            print(f"✅ Shard #{args.shard} completed successfully!")
            print(f"📁 Output saved for merging in merge-results job")
        else:
            print("🎉 Crawler completed successfully!")
        print("=" * 60)
        
    except Exception as e:
        print(f"❌ Fatal Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())
