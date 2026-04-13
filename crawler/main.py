#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Main Entry Point - Proxy Node Crawler
✅ 经过语法检查，可直接复制替换
"""

import asyncio
import json
import os
import sys
import argparse
from datetime import datetime
from pathlib import Path

# 将项目根目录加入系统路径，确保能正确导入模块
project_root = Path(__file__).resolve().parent.parent
sys.path.append(str(project_root))

# 导入项目模块
try:
    from crawler.github_crawler import SuperGitHubCrawler
    from crawler.validator import NodeValidator
    from crawler.deduplicator import NodeDeduplicator
    from crawler.utils import setup_logger, save_to_file
except ImportError as e:
    print(f"❌ 模块导入失败: {e}")
    print("请确保文件结构正确: crawler/main.py, crawler/github_crawler.py 等")
    sys.exit(1)

# 设置日志
logger = setup_logger(__name__)


async def main():
    """主函数入口"""
    
    # 解析命令行参数
    parser = argparse.ArgumentParser(description='Proxy Node Crawler - 自动爬取代理节点')
    parser.add_argument('--shard', type=int, default=-1, 
                       help='分片ID (0-7) 用于并行爬取，-1表示单线程模式')
    parser.add_argument('--skip-validate', action='store_true', 
                       help='跳过节点验证（加快爬取速度）')
    args = parser.parse_args()
    
    # 打印启动信息
    print("=" * 60)
    if args.shard >= 0:
        print(f"🚀 Starting Proxy Node Crawler [分片模式 #{args.shard}]")
    else:
        print("🚀 Starting Proxy Node Crawler [单线程模式]")
    print(f"📋 跳过验证: {args.skip_validate}")
    print("=" * 60)
    
    # 获取 GitHub Token
    github_token = os.getenv("GITHUB_TOKEN", "")
    
    # 初始化组件
    crawler = SuperGitHubCrawler(github_token, shard_id=args.shard)
    validator = NodeValidator(concurrent_limit=100, timeout=8)
    deduplicator = NodeDeduplicator()
    
    try:
        # 步骤 1: 爬取节点
        print("🕷️  开始爬取节点...")
        all_nodes = await crawler.crawl_all()
        print(f"📊 本次爬取总数: {len(all_nodes)} 个节点")
        
        if not all_nodes:
            print("⚠️  未找到任何节点")
            return
        
        # 步骤 2: 去重存储
        print("🧹 正在去重并存储...")
        stats = deduplicator.add_or_update_nodes(all_nodes, batch_size=500)
        print(f"📦 存储统计: 新增={stats.get('new', 0)}, 更新={stats.get('updated', 0)}")
        
        # 步骤 3: 验证节点（可选）
        valid_nodes = []
        
        if not args.skip_validate and args.shard < 0:
            print("🔍 正在验证节点有效性...")
            pending = deduplicator.get_recent_nodes(limit=500)
            
            if pending:
                print(f"   待验证: {len(pending)} 个")
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
                print(f"✅ 有效节点: {len(valid_nodes)} 个")
            else:
                print("   无待验证节点")
        else:
            print("⏭️  跳过验证")
            valid_nodes = deduplicator.get_recent_nodes(limit=1000)
        
        # 步骤 4: 生成输出文件
        print("💾 生成输出文件...")
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
        
        all_links = []
        for nodes in by_protocol.values():
            all_links.extend([n.get('link', '') for n in nodes if n.get('link')])
        
        if all_links:
            save_to_file(output_dir / "all_sub.txt", "\n".join(all_links))
            save_to_file(output_dir / "all_nodes.json", 
                       json.dumps(by_protocol, indent=2, ensure_ascii=False))
        
        # 步骤 5: 生成报告
        report = f"""# Proxy Nodes Report
Generated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
Mode: {"Shard #" + str(args.shard) if args.shard >= 0 else "Single"}

## Stats
- Crawled: {len(all_nodes)}
- Valid: {len(valid_nodes)}
- New/Updated: {stats.get('new', 0) + stats.get('updated', 0)}
"""
        save_to_file(output_dir / "STATS.md", report)
        
        # 步骤 6: 清理数据库（仅单线程）
        if args.shard < 0 and not args.skip_validate:
            print("🗄️  清理数据库...")
            db_size = deduplicator.auto_cleanup(max_total_nodes=50000, max_age_days=7)
            print(f"📦 数据库大小: {db_size} MB")
        
        print("🎉 执行成功！")
        
    except KeyboardInterrupt:
        print("\n⚠️  用户中断")
        sys.exit(130)
    except Exception as e:
        print(f"❌ 错误: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
