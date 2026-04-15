#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Main Controller - 单任务流、全量爬取、混合验证
"""

import asyncio
import os
import sys
import json
from datetime import datetime
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

from crawler.scraper import Scraper
from crawler.validator import Validator
from crawler.deduplicator import NodeDeduplicator

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
    for q in scraper.code_queries[:5]: # 限制查询数量防超时
        tasks.append(scraper.search_code(q))
    for q in scraper.repo_keywords[:5]:
        tasks.append(scraper.search_repos(q))
        
    search_results = await asyncio.gather(*tasks)
    
    # 处理搜索结果
    for result_list in search_results:
        if isinstance(result_list, list):
            for item in result_list:
                # 获取内容
                content = ""
                if "text_matches" in item: # Code search
                    content = "\n".join([m['fragment'] for m in item['text_matches']])
                elif "url" in item: # Repo search - 简单获取 README
                    # 这里为了速度，不遍历文件，只依赖 code search 的结果
                    pass
                
                if content:
                    links = scraper.extract_nodes(content)
                    all_raw_links.extend(links)
                    print(f"   Found {len(links)} links from {item.get('name', 'search')}")

    print(f"📊 Raw links extracted: {len(all_raw_links)}")

    # 2. 入库与去重 (存入数据库)
    print("\n💾 Phase 2: Deduplication & DB Storage...")
    nodes_data = [{'link': l, 'protocol': l.split('://')[0]} for l in all_raw_links]
    db.add_or_update_nodes(nodes_data, batch_size=200)
    print("✅ Links saved to database")

    # 3. 混合验证阶段 (Hybrid Validation)
    print("\n🔍 Phase 3: Hybrid Validation...")
    
    # 获取所有未验证或最近失败的节点 (最多验证 1000 个以防超时)
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

    # 4. 提取最终结果 (Top 5000)
    print("\n📦 Phase 4: Generating Final List (Top 5000)...")
    
    # 获取数据库中所有 valid 节点 (优先取 latency 小的)
    final_nodes = db.get_valid_nodes(max_age_days=7, max_latency=2000)
    
    # 如果数据库里还不够，加入刚才新验证的
    for l in newly_valid:
        if l not in [n.get('link') for n in final_nodes]:
            final_nodes.append({'link': l, 'protocol': l.split('://')[0]})
            
    # 截断 5000
    final_nodes = final_nodes[:5000]
    print(f" Final valid nodes: {len(final_nodes)}")

    # 5. 输出文件
    output_dir = Path("output")
    output_dir.mkdir(exist_ok=True)
    
    all_links = [n.get('link', '') for n in final_nodes if n.get('link')]
    if all_links:
        (output_dir / "all_sub.txt").write_text("\n".join(all_links))
        print(f"💾 Saved all_sub.txt ({len(all_links)} links)")

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
