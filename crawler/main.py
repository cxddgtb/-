#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Main Controller - 统一归档逻辑 (新+旧都存档)
✅ 核心改进：最终有效的节点（无论来源）都会写入今日的新档案
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
    from crawler.archiver import Archiver
except ImportError as e:
    print(f"❌ Import Error: {e}")
    sys.exit(1)

async def main():
    print("🚀 启动终极节点爬虫 (Unified Archive Mode)")
    print("="*60)
    
    token = os.getenv("GITHUB_TOKEN", "")
    if not token:
        print("️  WARNING: GITHUB_TOKEN is empty!")
    
    scraper = Scraper(token)
    validator = Validator(max_concurrent=80)
    archiver = Archiver()

    # ==========================================
    # 1. 爬取新节点
    # ==========================================
    print("\n️  Phase 1: Crawling New Nodes...")
    new_raw_links = await scraper.run_crawl()
    print(f"📊 Raw links found: {len(new_raw_links)}")

    # ==========================================
    # 2. 验证新节点
    # ==========================================
    print("\n Phase 2: Validating New Nodes...")
    valid_new_links = []
    if new_raw_links:
        unique_new = list(set(new_raw_links))
        print(f"   Deduplicated to {len(unique_new)} unique.")
        valid_new_links = await validator.validate_batch(unique_new)
        print(f"✅ New nodes verified: {len(valid_new_links)}")
    else:
        print("   No new raw links.")

    # ==========================================
    # 3. 智能回填 (如果新节点不足)
    # ==========================================
    print("\n🔄 Phase 3: Smart Refill (if needed)...")
    current_total = len(valid_new_links)
    refill_links = []
    
    if current_total < 5000:
        print(f"   Current ({current_total}) < 5000. Loading history...")
        backups = archiver.load_recent_archives(limit=10)
        
        # 排除已经在新节点列表中的
        new_set = set(valid_new_links)
        to_revalidate = [b for b in backups if b not in new_set]
        
        if to_revalidate:
            print(f"   Re-validating {len(to_revalidate)} historical nodes...")
            refill_links = await validator.validate_batch(to_revalidate)
            print(f"   ✅ Historical nodes revived: {len(refill_links)}")
    
    # ==========================================
    # 4. 【关键步骤】合并并统一归档
    # ==========================================
    print("\n💾 Phase 4: Merging & Unified Archiving...")
    
    # 合并：新验证通过的 + 回填验证通过的
    final_valid_set = set(valid_new_links) | set(refill_links)
    final_valid_list = list(final_valid_set)
    
    print(f"   Total valid nodes to archive: {len(final_valid_list)}")
    
    # 🔥 核心改动：无论来源，全部存入今日的新档案
    # 这样即使今天没爬到新的，但救活了旧的，档案里也会有数据，不会断档
    archived_links = archiver.save_daily(final_valid_list)
    
    # ==========================================
    # 5. 截断并输出
    # ==========================================
    # 取前 5000 个作为最终输出
    output_links = archived_links[:5000]
    print(f"\n📦 Final Output Count: {len(output_links)}")

    output_dir = Path("output")
    output_dir.mkdir(exist_ok=True)
    
    if output_links:
        (output_dir / "all_sub.txt").write_text("\n".join(output_links))
        print(f"💾 Saved all_sub.txt ({len(output_links)} links)")
    else:
        (output_dir / "all_sub.txt").write_text("")
        print("⚠️ all_sub.txt is empty (No valid nodes at all)")

    # 按协议分类输出
    by_proto = {}
    for link in output_links:
        if "://" in link:
            proto = link.split("://")[0]
            by_proto.setdefault(proto, []).append(link)
        
    for p, links in by_proto.items():
        (output_dir / f"{p}_sub.txt").write_text("\n".join(links))

    print("="*60)
    print("✅ Task Completed! Archive updated.")

if __name__ == "__main__":
    asyncio.run(main())
