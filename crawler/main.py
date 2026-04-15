#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Main Controller - 单任务流、全量爬取、混合验证 (修复版)
"""

import asyncio
import os
import sys
import json
from datetime import datetime
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

try:
    from crawler.scraper import Scraper
    from crawler.validator import Validator
    from crawler.deduplicator import NodeDeduplicator
except ImportError as e:
    print(f"❌ 模块导入失败: {e}")
    sys.exit(1)

async def main():
    print("🚀 启动强力节点爬虫 (Single Job Mode)")
    print("="*60)
    
    token = os.getenv("GITHUB_TOKEN", "")
    scraper = Scraper(token)
    validator = Validator(max_concurrent=80)
    db = NodeDeduplicator()

    # 1. 爬取阶段
    print("\n🕷️  Phase 1: Aggressive Crawling...")
    all_raw_links = []
    
    # 并行搜索代码和仓库
    tasks = []
    # 限制查询数量以防止 API 超时
    for q in scraper.code_queries[:5]: 
        tasks.append(scraper.search_code(q))
    for q in scraper.repo_keywords[:5]:
        tasks.append(scraper.search_repos(q))
        
    search_results = await asyncio.gather(*tasks, return_exceptions=True)
    
    # 处理搜索结果
    for result_list in search_results:
        if isinstance(result_list, list):
            for item in result_list:
                content = ""
                # 处理 Code Search 结果
                if "text_matches" in item and item["text_matches"]:
                    content = "\n".join([m.get('fragment', '') for m in item['text_matches']])
                # 处理 Repo Search 结果 (通常不直接包含节点，这里主要靠 Code Search)
                
                if content:
                    links = scraper.extract_nodes(content)
                    if links:
                        all_raw_links.extend(links)
                        print(f"   Found {len(links)} links from {item.get('repository', {}).get('full_name', 'search')}")

    print(f"📊 Raw links extracted: {len(all_raw_links)}")

    # 2. 入库与去重
    print("\n💾 Phase 2: Deduplication & DB Storage...")
    nodes_data = [{'link': l, 'protocol': l.split('://')[0]} for l in all_raw_links]
    db.add_or_update_nodes(nodes_data, batch_size=200)
    print("✅ Links saved to database")

    # 3. 混合验证阶段
    print("\n🔍 Phase 3: Hybrid Validation...")
    
    # 获取所有未验证或最近失败的节点 (最多验证 1000 个)
    pending_links = [n['link'] for n in db.get_recent_nodes(limit=1000)]
    print(f"🔄 Validating {len(pending_links)} pending nodes...")
    
    newly_valid = []
    if pending_links:
        newly_valid = await validator.validate_batch(pending_links)
        print(f"✅ Newly verified: {len(newly_valid)} nodes")
        
        # 更新数据库状态
        valid_update_data = [{'link': l, 'is_valid': True, 'latency_ms': 10} for l in newly_valid]
        invalid_update_data = [{'link': l, 'is_valid': False, 'latency_ms': 9999} for l in pending_links if l not in newly_valid]
        
        db.update_validation_results(valid_update_data)
        db.update_validation_results(invalid_update_data)

    # 4. 提取最终结果 (Top 5000) - 🔥 修复了 AttributeError
    print("\n📦 Phase 4: Generating Final List (Top 5000)...")
    
    final_nodes = []
    
    # 1. 优先添加刚刚验证通过的新节点
    if newly_valid:
        final_nodes.extend([{'link': l, 'protocol': l.split('://')[0]} for l in newly_valid])
        print(f"   Added {len(newly_valid)} newly verified nodes")
        
    # 2. 尝试从数据库获取历史节点补充 (使用 get_recent_nodes 避免报错)
    try:
        history_nodes = db.get_recent_nodes(limit=2000)
        count = 0
        for n in history_nodes:
            link = n.get('link')
            # 如果数据库标记为有效，且不在新验证列表中，则加入
            if n.get('is_valid') and link not in newly_valid:
                final_nodes.append(n)
                count += 1
        if count > 0:
            print(f"   Added {count} historical valid nodes from DB")
    except Exception as e:
        print(f"   ⚠️ Failed to load history nodes: {e}")
        
    # 3. 去重 (基于 link)
    seen_links = set()
    unique_nodes = []
    for node in final_nodes:
        link = node.get('link')
        if link and link not in seen_links:
            seen_links.add(link)
            unique_nodes.append(node)
    
    final_nodes = unique_nodes[:5000]
    print(f"✅ Final valid nodes count: {len(final_nodes)}")

    # 5. 输出文件
    output_dir = Path("output")
    output_dir.mkdir(exist_ok=True)
    
    all_links = [n.get('link', '') for n in final_nodes if n.get('link')]
    if all_links:
        (output_dir / "all_sub.txt").write_text("\n".join(all_links))
        print(f"💾 Saved all_sub.txt ({len(all_links)} links)")
    else:
        (output_dir / "all_sub.txt").write_text("")
        print("⚠️ all_sub.txt is empty (no valid nodes found)")

    # 按协议分类
    by_proto = {}
    for n in final_nodes:
        p = n.get('protocol', 'unknown')
        by_proto.setdefault(p, []).append(n)
        
    for p, nodes in by_proto.items():
        links = [n['link'] for n in nodes]
        (output_dir / f"{p}_sub.txt").write_text("\n".join(links))

    print("="*60)
    print("✅ Task Completed!")

if __name__ == "__main__":
    asyncio.run(main())
