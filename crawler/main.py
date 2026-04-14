#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Main Entry Point - Proxy Node Crawler
✅ 完整优化版：集成严格格式验证 + 智能去重 + 自动清理
🔧 代码小白专用：直接复制替换 crawler/main.py 即可
"""

import asyncio
import json
import os
import sys
import argparse
from datetime import datetime
from pathlib import Path

# 添加项目根目录到系统路径
project_root = Path(__file__).resolve().parent.parent
sys.path.append(str(project_root))

# 导入核心模块
try:
    from crawler.github_crawler import SuperGitHubCrawler
    from crawler.validator import NodeValidator
    from crawler.deduplicator import NodeDeduplicator
    from crawler.utils import setup_logger, save_to_file
except ImportError as e:
    print(f"❌ 模块导入失败: {e}")
    print("请确保文件结构正确，且依赖已安装 (pip install -r requirements.txt)")
    sys.exit(1)

logger = setup_logger(__name__)


async def main():
    # 解析命令行参数
    parser = argparse.ArgumentParser(description='Proxy Node Crawler')
    parser.add_argument('--shard', type=int, default=-1, help='分片ID (0-7)，-1为单线程')
    parser.add_argument('--skip-validate', action='store_true', help='跳过节点验证（加快速度）')
    parser.add_argument('--debug', action='store_true', help='输出详细调试信息')
    args = parser.parse_args()

    print("=" * 60)
    mode = f"[分片模式 #{args.shard}]" if args.shard >= 0 else "[单线程模式]"
    print(f"🚀 启动代理节点爬虫 {mode}")
    print(f"📋 跳过验证: {args.skip_validate} | 调试模式: {args.debug}")
    print("=" * 60)

    github_token = os.getenv("GITHUB_TOKEN", "")
    crawler = SuperGitHubCrawler(github_token, shard_id=args.shard)
    validator = NodeValidator(concurrent_limit=50, timeout=5)
    deduplicator = NodeDeduplicator()

    try:
        # ================= 步骤 1: 爬取节点 =================
        print("\n🕷️  步骤 1/5: 开始爬取节点...")
        all_nodes = await crawler.crawl_all()
        print(f"📊 爬取完成，共获得 {len(all_nodes)} 个候选节点")

        if args.debug and all_nodes:
            print("🔍 节点格式抽样检查 (前 5 个):")
            for i, node in enumerate(all_nodes[:5], 1):
                link = node.get('link', '')
                proto = node.get('protocol', '')
                preview = link[:60] + "..." if len(link) > 60 else link
                print(f"   {i}. [{proto}] {preview}")
        elif not all_nodes:
            print("⚠️  未爬取到任何节点。请检查 GitHub Token 权限或网络。")

        if not all_nodes:
            # 即使没有节点，也生成空文件保持流程完整，防止合并步骤报错
            Path("output").mkdir(exist_ok=True)
            save_to_file(Path("output/all_sub.txt"), "")
            save_to_file(Path("output/STATS.md"), f"# Proxy Nodes Report\nGenerated: {datetime.now().isoformat()}\nTotal: 0")
            return

        # ================= 步骤 2: 去重与存储 =================
        print("\n🧹 步骤 2/5: 去重并写入数据库...")
        dedup_stats = deduplicator.add_or_update_nodes(all_nodes, batch_size=500)
        print(f"📦 存储统计: 新增={dedup_stats.get('new', 0)}, 更新={dedup_stats.get('updated', 0)}")

        # ================= 步骤 3: 节点验证 =================
        valid_nodes = []
        if not args.skip_validate and args.shard < 0:
            print("\n🔍 步骤 3/5: 正在验证节点连通性...")
            pending = deduplicator.get_recent_nodes(limit=300)
            print(f"   待验证队列: {len(pending)} 个")

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
                print(f"   ✅ 验证通过: {len(valid_nodes)} 个")
            else:
                print("   ⏭️  无待验证节点")
        else:
            print(f"\n⏭️  步骤 3/5: 已跳过验证")
            valid_nodes = deduplicator.get_recent_nodes(limit=10000)
            print(f"   📦 加载最近节点: {len(valid_nodes)} 个")

        # ================= 步骤 4: 生成订阅文件 =================
        print("\n💾 步骤 4/5: 生成输出文件...")
        output_dir = Path("output")
        output_dir.mkdir(exist_ok=True)

        # 按协议分类
        by_protocol = {}
        for node in valid_nodes:
            proto = node.get('protocol', 'unknown')
            by_protocol.setdefault(proto, []).append(node)

        created_files = []
        for protocol, nodes in by_protocol.items():
            # 提取链接（二次过滤，确保格式正确）
            links = [n.get('link', '') for n in nodes if n.get('link') and '://' in n.get('link', '')]
            if links:
                txt_file = output_dir / f"{protocol}_sub.txt"
                json_file = output_dir / f"{protocol}_nodes.json"
                save_to_file(txt_file, "\n".join(links))
                save_to_file(json_file, json.dumps(nodes, indent=2, ensure_ascii=False))
                created_files.append(f"{protocol}_sub.txt ({len(links)})")
                print(f"   ✅ 生成: {txt_file.name} ({len(links)} 条)")

        # 生成合并订阅 all_sub.txt
        all_links = []
        for nodes in by_protocol.values():
            all_links.extend([n.get('link', '') for n in nodes if n.get('link') and '://' in n.get('link', '')])

        if all_links:
            save_to_file(output_dir / "all_sub.txt", "\n".join(all_links))
            created_files.append(f"all_sub.txt ({len(all_links)})")
            print(f"   ✅ 生成: all_sub.txt (共 {len(all_links)} 条)")
        else:
            save_to_file(output_dir / "all_sub.txt", "")
            print("   ⚠️  all_sub.txt 为空")

        # ================= 步骤 5: 生成报告与清理 =================
        report = f"""# Proxy Nodes Report
Generated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
Mode: {"Shard #" + str(args.shard) if args.shard >= 0 else "Single"}

## Summary
- Crawled: {len(all_nodes)}
- Valid/Output: {len(all_links)}
- Files: {', '.join(created_files) if created_files else 'None'}

## By Protocol
"""
        for proto, nodes in sorted(by_protocol.items()):
            report += f"- {proto}: {len(nodes)}\n"

        save_to_file(output_dir / "STATS.md", report)
        print(f"   ✅ 生成: STATS.md")

        # 数据库清理（仅单线程模式执行）
        if args.shard < 0 and not args.skip_validate:
            print("\n🗄️  步骤 5/5: 清理数据库...")
            db_size = deduplicator.auto_cleanup(max_total_nodes=50000, max_age_days=7)
            final_stats = deduplicator.get_stats()
            print(f"   📦 数据库大小: {db_size} MB | 总记录: {final_stats.get('total', 0)}")

        print("\n" + "=" * 60)
        print("🎉 任务执行成功！")
        print("=" * 60)

    except KeyboardInterrupt:
        print("\n⚠️  用户手动中断")
        sys.exit(130)
    except Exception as e:
        print(f"\n❌ 致命错误: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    # 兼容 Windows/部分 Linux 环境的 asyncio 策略
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
