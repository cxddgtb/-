#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Main Controller - 单任务流 + 档案回填机制 (终极稳定版)
"""

import asyncio
import os
import sys
from datetime import datetime
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

try:
    from crawler.scraper import Scraper
    from crawler.validator import Validator
    from crawler.archiver import Archiver  # 🔥 引入档案模块
except ImportError as e:
    print(f"❌ 模块导入失败: {e}")
    sys.exit(1)

async def main():
    print("🚀 启动强力节点爬虫 (Archive & Refill Mode)")
    print("="*60)
    
    token = os.getenv("GITHUB_TOKEN", "")
    scraper = Scraper(token)
    validator = Validator(max_concurrent=100) # 提高并发加速验证
    archiver = Archiver() # 初始化档案管理器

    # 1. 爬取阶段
    print("\n🕷️  Phase 1: Aggressive Crawling...")
    all_raw_links = []
    
    # 并行搜索代码和仓库
    tasks = []
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
                
                if content:
                    links = scraper.extract_nodes(content)
                    if links:
                        all_raw_links.extend(links)
                        print(f"   Found {len(links)} links from {item.get('repository', {}).get('full_name', 'search')}")

    print(f"📊 Raw links extracted: {len(all_raw_links)}")

    # 2. 验证新节点
    print("\n🔍 Phase 2: Validating new nodes...")
    valid_links = []
    if all_raw_links:
        valid_links = await validator.validate_batch(all_raw_links)
        print(f"✅ Newly verified: {len(valid_links)} nodes")
    else:
        print("⚠️ No raw links to validate.")

    # 3. 保存到今日档案 (关键步骤！)
    print("\n💾 Phase 3: Archiving...")
    valid_links = archiver.save_daily(valid_links)

    # 4. 智能回填 (Refill Strategy)
    print("\n🔄 Phase 4: Smart Refill (Target: 5000)...")
    current_count = len(valid_links)
    
    if current_count < 5000:
        print(f"⚠️ Current nodes ({current_count}) < 5000. Activating refill...")
        
        # 1. 从最近 10 个档案加载历史节点
        backups = archiver.load_recent_archives(limit=10)
        
        # 2. 过滤掉已经在今日有效列表中的节点
        valid_set = set(valid_links)
        new_backups = [b for b in backups if b not in valid_set]
        print(f"   Found {len(new_backups)} unique backup nodes to re-validate.")
        
        # 3. 验证这些旧节点 (因为旧节点可能已失效)
        if new_backups:
            revalidated_backups = await validator.validate_batch(new_backups)
            print(f"   ✅ Re-validated {len(revalidated_backups)} backup nodes.")
            
            # 4. 合并
            valid_links.extend(revalidated_backups)
            print(f"   📈 Total nodes after refill: {len(valid_links)}")
    else:
        print(f"✅ Nodes count ({current_count}) sufficient. Skipping refill.")

    # 5. 截断并输出 (最终只保留 5000 个)
    final_links = valid_links[:5000]
    print(f"\n📦 Final Output: {len(final_links)} nodes")

    output_dir = Path("output")
    output_dir.mkdir(exist_ok=True)
    
    # 写入 all_sub.txt
    if final_links:
        (output_dir / "all_sub.txt").write_text("\n".join(final_links))
        print(f"💾 Saved all_sub.txt ({len(final_links)} links)")
    else:
        (output_dir / "all_sub.txt").write_text("")

    # 按协议分类
    by_proto = {}
    for link in final_links:
        if "://" in link:
            proto = link.split("://")[0]
            by_proto.setdefault(proto, []).append(link)
        
    for p, links in by_proto.items():
        (output_dir / f"{p}_sub.txt").write_text("\n".join(links))

    print("="*60)
    print("✅ Task Completed!")

if __name__ == "__main__":
    asyncio.run(main())
