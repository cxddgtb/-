#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Main Entry Point - Proxy Node Crawler (DEBUG VERSION)
🔧 带详细调试日志，帮助定位问题
"""

import asyncio
import json
import os
import sys
import argparse
from datetime import datetime
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent
sys.path.append(str(project_root))

try:
    from crawler.github_crawler import SuperGitHubCrawler
    from crawler.validator import NodeValidator
    from crawler.deduplicator import NodeDeduplicator
    from crawler.utils import setup_logger, save_to_file
except ImportError as e:
    print(f"❌ Import error: {e}")
    sys.exit(1)

logger = setup_logger(__name__)


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--shard', type=int, default=-1)
    parser.add_argument('--skip-validate', action='store_true')
    parser.add_argument('--debug', action='store_true', help='Enable debug output')
    args = parser.parse_args()
    
    print("=" * 70)
    mode = f"[分片 #{args.shard}]" if args.shard >= 0 else "[单线程]"
    print(f"🚀 Proxy Node Crawler {mode}")
    print(f"🔍 Debug mode: {args.debug}")
    print("=" * 70)
    
    github_token = os.getenv("GITHUB_TOKEN", "")
    crawler = SuperGitHubCrawler(github_token, shard_id=args.shard)
    validator = NodeValidator(concurrent_limit=100, timeout=8)
    deduplicator = NodeDeduplicator()
    
    try:
        # ========== 步骤1: 爬取 ==========
        print("\n🕷️  Step 1: Crawling nodes...")
        all_nodes = await crawler.crawl_all()
        
        # 🔥 调试输出
        if args.debug or True:  # 始终输出调试信息
            stats = crawler.get_crawl_stats()
            print(f"\n📊 Crawl Debug Info:")
            print(f"   • Total nodes extracted: {len(all_nodes)}")
            print(f"   • Repos searched: {stats.get('repos_searched', 0)}")
            print(f"   • Repos crawled: {stats.get('repos_crawled', 0)}")
            print(f"   • Files skipped (dedup): {stats.get('files_skipped', 0)}")
            print(f"   • Unique links: {stats.get('unique_links', 0)}")
            
            # 打印前5个节点示例
            if all_nodes:
                print(f"\n🔍 Sample nodes (first 5):")
                for i, node in enumerate(all_nodes[:5], 1):
                    link_preview = node.get('link', '')[:60] + "..." if len(node.get('link', '')) > 60 else node.get('link', '')
                    print(f"   {i}. [{node.get('protocol')}] {link_preview}")
                    print(f"      Source: {node.get('source')}")
            else:
                print(f"\n⚠️  WARNING: No nodes extracted!")
                print(f"   Possible causes:")
                print(f"   - GitHub API rate limited (check GITHUB_TOKEN)")
                print(f"   - Regex patterns not matching node format")
                print(f"   - All repos/files filtered out")
        
        if not all_nodes:
            print("\n⚠️  No nodes found, creating empty output files...")
        
        # ========== 步骤2: 去重存储 ==========
        print(f"\n🧹 Step 2: Deduplicating ({len(all_nodes)} nodes)...")
        stats = deduplicator.add_or_update_nodes(all_nodes, batch_size=500)
        print(f"   • New: {stats.get('new', 0)}, Updated: {stats.get('updated', 0)}, Errors: {stats.get('errors', 0)}")
        
        # ========== 步骤3: 验证（可选） ==========
        valid_nodes = []
        if not args.skip_validate and args.shard < 0:
            print(f"\n🔍 Step 3: Validating nodes...")
            pending = deduplicator.get_recent_nodes(limit=500)
            print(f"   • Pending validation: {len(pending)}")
            
            if pending:
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
                print(f"   • Valid nodes: {len(valid_nodes)}")
                
                # 🔥 调试：打印验证失败的示例
                if args.debug and results:
                    failed = [r for r in results if hasattr(r, 'is_valid') and not r.is_valid]
                    if failed:
                        print(f"\n⚠️  Sample failed validations:")
                        for r in failed[:3]:
                            print(f"   - {r.node_link[:50]}... (latency: {r.latency_ms}ms)")
        else:
            print(f"\n⏭️  Step 3: Skipping validation")
            valid_nodes = deduplicator.get_recent_nodes(limit=1000)
            print(f"   • Using recent nodes: {len(valid_nodes)}")
        
        # ========== 步骤4: 生成输出文件 ==========
        print(f"\n💾 Step 4: Generating output files...")
        output_dir = Path("output")
        output_dir.mkdir(exist_ok=True)
        
        by_protocol = {}
        for node in valid_nodes:
            proto = node.get('protocol', 'unknown')
            by_protocol.setdefault(proto, []).append(node)
        
        files_created = []
        for protocol, nodes in by_protocol.items():
            links = [n.get('link', '') for n in nodes if n.get('link')]
            if links:
                txt_path = output_dir / f"{protocol}_sub.txt"
                json_path = output_dir / f"{protocol}_nodes.json"
                
                save_to_file(txt_path, "\n".join(links))
                save_to_file(json_path, json.dumps(nodes, indent=2, ensure_ascii=False))
                
                files_created.append(f"{protocol}_sub.txt ({len(links)} links)")
                print(f"   ✅ Created: {txt_path.name} ({len(links)} links)")
        
        # 🔥 关键：生成 all_sub.txt
        all_links = []
        for nodes in by_protocol.values():
            all_links.extend([n.get('link', '') for n in nodes if n.get('link')])
        
        if all_links:
            all_sub_path = output_dir / "all_sub.txt"
            save_to_file(all_sub_path, "\n".join(all_links))
            files_created.append(f"all_sub.txt ({len(all_links)} links)")
            print(f"   ✅ Created: all_sub.txt ({len(all_links)} total links)")
        else:
            print(f"   ⚠️  WARNING: all_sub.txt would be empty!")
            print(f"      - valid_nodes: {len(valid_nodes)}")
            print(f"      - by_protocol keys: {list(by_protocol.keys())}")
            # 即使为空也创建文件，避免合并步骤找不到文件
            save_to_file(output_dir / "all_sub.txt", "")
            print(f"   ✅ Created: all_sub.txt (empty)")
        
        if not files_created:
            print(f"   ⚠️  No files created! Check if valid_nodes has links.")
        
        # ========== 步骤5: 生成报告 ==========
        report = f"""# Proxy Nodes Report
Generated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
Mode: {"Shard #" + str(args.shard) if args.shard >= 0 else "Single"}

## Results
- Crawled: {len(all_nodes)}
- Valid: {len(valid_nodes)}
- Files created: {', '.join(files_created) if files_created else 'None'}

## By Protocol
"""
        for proto, nodes in sorted(by_protocol.items()):
            report += f"- {proto}: {len(nodes)}\n"
        
        save_to_file(output_dir / "STATS.md", report)
        print(f"   ✅ Created: STATS.md")
        
        # ========== 步骤6: 清理（仅单线程） ==========
        if args.shard < 0 and not args.skip_validate:
            print(f"\n🗄️  Step 6: Cleaning database...")
            db_size = deduplicator.auto_cleanup(max_total_nodes=50000, max_age_days=7)
            final_stats = deduplicator.get_stats()
            print(f"   • DB size: {db_size} MB")
            print(f"   • Total in DB: {final_stats.get('total', 0)}")
            print(f"   • Valid in DB: {final_stats.get('valid', 0)}")
        
        # ========== 完成 ==========
        print(f"\n{'=' * 70}")
        print(f"✅ Execution completed!")
        print(f"📁 Output directory: {output_dir.absolute()}")
        if files_created:
            print(f"📄 Files: {', '.join(files_created)}")
        print(f"{'=' * 70}")
        
    except KeyboardInterrupt:
        print("\n⚠️  Interrupted by user")
        sys.exit(130)
    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
