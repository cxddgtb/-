#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Main Entry Point - Proxy Node Crawler
支持8路并行爬取 + 智能去重 + 自动清理
🔧 代码小白专用：完整文件，直接复制替换即可
"""

import asyncio
import json
import os
import sys
import argparse
from datetime import datetime
from pathlib import Path

# 添加项目根目录到路径
sys.path.append(str(Path(__file__).parent.parent))

# 导入项目模块
from crawler.github_crawler import SuperGitHubCrawler
from crawler.validator import NodeValidator
from crawler.deduplicator import NodeDeduplicator
from crawler.utils import setup_logger, save_to_file

# 设置日志
logger = setup_logger(__name__)


async def main():
    """主函数入口"""
    
    # 🔥 解析命令行参数（代码小白不用管，自动处理）
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
    
    # 获取 GitHub Token（从环境变量自动读取）
    github_token = os.getenv("GITHUB_TOKEN", "")
    
    # 初始化组件
    crawler = SuperGitHubCrawler(github_token, shard_id=args.shard)
    validator = NodeValidator(concurrent_limit=100, timeout=8)
    deduplicator = NodeDeduplicator()
    
    try:
        # ========================================
        # 步骤1: 爬取节点
        # ========================================
        print("🕷️  开始爬取节点...")
        all_nodes = await crawler.crawl_all()
        print(f"📊 本次爬取总数: {len(all_nodes)} 个节点")
        
        # 如果没有爬取到节点，提前结束
        if not all_nodes:
            print("⚠️  未找到任何节点，请检查搜索关键词或网络")
            return
        
        # ========================================
        # 步骤2: 去重并存储到数据库
        # ========================================
        print("🧹 正在去重并存储到数据库...")
        stats = deduplicator.add_or_update_nodes(all_nodes, batch_size=500)
        print(f"📦 去重统计: 新增={stats.get('new', 0)}, 更新={stats.get('updated', 0)}, 错误={stats.get('errors', 0)}")
        
        # ========================================
        # 步骤3: 验证节点有效性（可选）
        # ========================================
        valid_nodes = []
        
        if not args.skip_validate and args.shard < 0:
            # 单线程模式且未跳过验证时才执行
            print("🔍 正在验证节点有效性...")
            pending = deduplicator.get_recent_nodes(limit=500)
            
            if pending:
                print(f"   待验证节点: {len(pending)} 个")
                results = await validator.validate_nodes_batch(pending)
                
                # 收集验证结果
                valid_results = []
                for r in results:
                    if hasattr(r, 'node_link'):
                        valid_results.append({
                            'link': r.node_link,
                            'protocol': r.protocol,
                            'is_valid': r.is_valid,
                            'latency_ms': r.latency_ms,
                        })
                
                # 更新数据库中的验证状态
                deduplicator.update_validation_results(valid_results)
                
                # 过滤出有效节点
                valid_nodes = validator.filter_valid_nodes(results, max_latency=500.0)
                print(f"✅ 有效节点: {len(valid_nodes)} 个")
            else:
                print("   没有待验证的节点")
                
        elif args.skip_validate:
            print("⏭️  已跳过验证（快速模式）")
            # 不验证时，取最近添加的节点作为"有效"
            valid_nodes = deduplicator.get_recent_nodes(limit=1000)
        else:
            print("⏭️  已跳过验证（分片模式 - 将在合并步骤验证）")
        
        # ========================================
        # 步骤4: 生成输出文件
        # ========================================
        print("💾 正在生成输出文件...")
        output_dir = Path("output")
        output_dir.mkdir(exist_ok=True)
        
        # 按协议分类节点
        by_protocol = {}
        for node in valid_nodes:
            proto = node.get('protocol', 'unknown')
            if proto not in by_protocol:
                by_protocol[proto] = []
            by_protocol[proto].append(node)
        
        # 为每个协议生成订阅文件和JSON文件
        for protocol, nodes in by_protocol.items():
            # 提取所有链接
            links = [n.get('link', '') for n in nodes if n.get('link')]
            if links:
                # 保存订阅链接文件（.txt格式，每行一个链接）
                save_to_file(output_dir / f"{protocol}_sub.txt", "\n".join(links))
                # 保存详细节点信息（.json格式）
                save_to_file(output_dir / f"{protocol}_nodes.json", 
                           json.dumps(nodes, indent=2, ensure_ascii=False))
                print(f"   ✅ {protocol}: {len(links)} 个链接")
        
        # 生成合并的所有协议订阅文件
        all_links = []
        for nodes in by_protocol.values():
            all_links.extend([n.get('link', '') for n in nodes if n.get('link')])
        
        if all_links:
            save_to_file(output_dir / "all_sub.txt", "\n".join(all_links))
            save_to_file(output_dir / "all_nodes.json", 
                       json.dumps(by_protocol, indent=2, ensure_ascii=False))
            print(f"   ✅ 合并: {len(all_links)} 个总链接")
        
        # ========================================
        # 步骤5: 生成统计报告
        # ========================================
        report = f"""# Proxy Nodes Report 代理节点报告
生成时间: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
运行模式: {"分片 #" + str(args.shard) if args.shard >= 0 else "单线程"}
跳过验证: {args.skip_validate}

## 本次爬取统计
- 爬取节点总数: {len(all_nodes)}
- 存入数据库: {stats.get('new', 0) + stats.get('updated', 0)}
- 有效节点数: {len(valid_nodes)}

## 按协议分类
"""
        for proto, nodes in sorted(by_protocol.items()):
            report += f"- {proto}: {len(nodes)} 个\n"
        
        # 保存报告文件
        save_to_file(output_dir / "STATS.md", report)
        print(f"📊 统计报告已保存到 output/STATS.md")
        
        # ========================================
        # 步骤6: 数据库自动清理（仅单线程模式执行）
        # ========================================
        if args.shard < 0 and not args.skip_validate:
            print("🗄️  正在清理数据库...")
            db_size = deduplicator.auto_cleanup(max_total_nodes=50000, max_age_days=7)
            print(f"📦 数据库大小: {db_size} MB")
            
            # 打印最终统计信息
            final_stats = deduplicator.get_stats()
            print(f"\n🎯 数据库最终统计:")
            print(f"   总节点数: {final_stats.get('total', 0)}")
            print(f"   有效节点: {final_stats.get('valid', 0)}")
            print(f"   按协议: {final_stats.get('by_protocol', {})}")
        
        # ========================================
        # 完成
        # ========================================
        print("\n" + "=" * 60)
        if args.shard >= 0:
            print(f"✅ 分片 #{args.shard} 执行成功！")
            print(f"📁 输出文件已保存，等待合并步骤处理")
        else:
            print("🎉 爬虫执行成功！所有任务完成！")
        print("=" * 60)
        
    except KeyboardInterrupt:
        print("\n⚠️  用户中断执行")
        sys.exit(130)
        
    except Exception as e:
        print(f"❌ 发生致命错误: {e}")
        # 打印详细错误堆栈，方便排查问题
        import traceback
        traceback.print_exc()
        sys.exit(1)


# 🔥 程序入口（代码小白不用改，直接运行即可）
if __name__ == "__main__":
    # 运行主函数
    asyncio.run(main())
