#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Main Entry Point - Proxy Node Crawler
✅ 集成极速格式筛选模块，确保输出节点 100% 可用
🔧 直接复制替换 crawler/main.py 即可
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
    from crawler.filter import NodeFilter  # 🔥 新增导入
except ImportError as e:
    print(f"❌ 模块导入失败: {e}")
    sys.exit(1)

logger = setup_logger(__name__)

async def main():
    parser = argparse.ArgumentParser(description='Proxy Node Crawler')
    parser.add_argument('--shard', type=int, default=-1, help='分片ID (0-7)，-1为单线程')
    parser.add_argument('--skip-validate', action='store_true', help='跳过网络连通性验证')
    args = parser.parse_args()

    print("=" * 60)
    mode = f"[分片模式 #{args.shard}]" if args.shard >= 0 else "[单线程模式]"
    print(f"🚀 启动代理节点爬虫 {mode}")
    print("=" * 60)

    github_token = os.getenv("GITHUB_TOKEN", "")
    crawler = SuperGitHubCrawler(github_token, shard_id=args.shard)
    validator = NodeValidator(concurrent_limit=50, timeout=5)
    deduplicator = NodeDeduplicator()
    node_filter = NodeFilter()  # 🔥 初始化筛选器

    try:
        # ================= 步骤 1: 爬取 =================
        print("\n🕷️  步骤 1/5: 爬取原始节点...")
        raw_nodes = await crawler.crawl_all()
        print(f"📥 爬取完成: 共获取 {len(raw_nodes)} 个原始节点")
        if not raw_nodes:
            print("⚠️  未获取到任何数据，流程结束。")
            return

        # ================= 步骤 2: 极速格式筛选 (NEW) =================
        print("\n🔍 步骤 2/5: 极速格式清洗与验证...")
        valid_nodes = node_filter.filter_batch(raw_nodes)
        reject_rate = (1 - len(valid_nodes) / max(len(raw_nodes), 1)) * 100
        print(f"   ✅ 筛选通过: {len(valid_nodes)} 个")
        print(f"   🗑️  淘汰无效格式: {len(raw_nodes) - len(valid_nodes)} 个 (淘汰率: {reject_rate:.1f}%)")

        if args.shard >= 0:
            print("   💡 分片模式：仅存储清洗后的节点，跳过本地验证。")
        elif not valid_nodes:
            print("   ⚠️  无有效节点，跳过后续步骤。")
            return

        # ================= 步骤 3: 去重存储 =================
        print("\n🧹 步骤 3/5: 去重并写入数据库...")
        dedup_stats = deduplicator.add_or_update_nodes(valid_nodes, batch_size=500)
        print(f"📦 存储结果: 新增={dedup_stats.get('new', 0)}, 更新={dedup_stats.get('updated', 0)}")

        # ================= 步骤 4: 网络验证 (可选) =================
        final_output_nodes = valid_nodes
        if not args.skip_validate and args.shard < 0:
            print("\n🌐 步骤 4/5: 网络连通性验证...")
            pending = deduplicator.get_recent_nodes(limit=300)
            if pending:
                results = await validator.validate_nodes_batch(pending)
                valid_results = []
                for r in results:
                    if hasattr(r, 'node_link'):
                        valid_results.append({
                            'link': r.node_link, 'protocol': r.protocol,
                            'is_valid': r.is_valid, 'latency_ms': r.latency_ms
                        })
                deduplicator.update_validation_results(valid_results)
                final_output_nodes = validator.filter_valid_nodes(results, max_latency=500.0)
                print(f"   🟢 连通有效: {len(final_output_nodes)} 个")
            else:
                print("   ⏭️  无待验证节点")
        else:
            print("\n⏭️  步骤 4/5: 已跳过网络验证")

        # ================= 步骤 5: 生成输出 =================
        print("\n💾 步骤 5/5: 生成订阅文件...")
        output_dir = Path("output")
        output_dir.mkdir(exist_ok=True)

        by_protocol = {}
        for node in final_output_nodes:
            by_protocol.setdefault(node.get('protocol', 'unknown'), []).append(node)

        created_files = []
        for proto, nodes in by_protocol.items():
            links = [n.get('link', '') for n in nodes if n.get('link')]
            if links:
                save_to_file(output_dir / f"{proto}_sub.txt", "\n".join(links))
                save_to_file(output_dir / f"{proto}_nodes.json", json.dumps(nodes, indent=2, ensure_ascii=False))
                created_files.append(f"{proto}_sub.txt ({len(links)})")
                print(f"   ✅ {proto}: {len(links)} 条")

        # 合并 all_sub.txt
        all_links = [n.get('link', '') for nodes in by_protocol.values() for n in nodes if n.get('link')]
        if all_links:
            save_to_file(output_dir / "all_sub.txt", "\n".join(all_links))
            created_files.append(f"all_sub.txt ({len(all_links)})")
            print(f"   🌐 合并订阅: {len(all_links)} 条")

        # 生成报告
        report = f"""# Proxy Nodes Report
Generated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
Mode: {"Shard #" + str(args.shard) if args.shard >= 0 else "Single"}

## Summary
- Crawled: {len(raw_nodes)}
- Filtered: {len(valid_nodes)} ({(1-reject_rate/100)*100:.1f}% valid format)
- Output: {len(all_links)}
- Files: {', '.join(created_files) if created_files else 'None'}

## By Protocol
"""
        for p, n in sorted(by_protocol.items()): report += f"- {p}: {len(n)}\n"
        save_to_file(output_dir / "STATS.md", report)

        if args.shard < 0 and not args.skip_validate:
            db_size = deduplicator.auto_cleanup(max_total_nodes=50000, max_age_days=7)
            print(f"\n🗄️  数据库清理完成: {db_size} MB")

        print("\n" + "=" * 60)
        print("🎉 任务执行成功！输出目录: output/")
        print("=" * 60)

    except KeyboardInterrupt:
        print("\n⚠️  用户中断")
        sys.exit(130)
    except Exception as e:
        print(f"\n❌ 错误: {e}")
        import traceback; traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
