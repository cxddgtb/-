#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Main Controller - 调用暴力爬虫 + 档案回填
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
    print("🚀 启动强力节点爬虫 (Aggressive Crawl Mode)")
    print("="*60)
    
    token = os.getenv("GITHUB_TOKEN", "")
    if not token:
        print("⚠️  WARNING: GITHUB_TOKEN is empty! Results may be limited.")
    
    scraper = Scraper(token)
    validator = Validator(max_concurrent=100)
    archiver = Archiver()

    # 1. 暴力爬取
    print("\n🕷️  Phase 1: Aggressive Crawling (Downloading Files)...")
    all_raw_links = await scraper.run_crawl()
    print(f"📊 Total Raw Links Extracted: {len(all_raw_links)}")

    # 2. 验证新节点
    print("\n🔍 Phase 2: Validating new nodes...")
    valid_links = []
    if all_raw_links:
        # 先去重再验证，节省时间
        unique_raw = list(set(all_raw_links))
        print(f"   Deduplicated to {len(unique_raw)} unique links.")
        valid_links = await validator.validate_batch(unique_raw)
        print(f"✅ Newly verified: {len(valid_links)} nodes")
    else:
        print("️ No raw links to validate.")

    # 3. 存档
    print("\n💾 Phase 3: Archiving...")
    valid_links = archiver.save_daily(valid_links)

    # 4. 智能回填
    print("\n🔄 Phase 4: Smart Refill (Target: 5000)...")
    current_count = len(valid_links)
    
    if current_count < 5000:
        print(f"️ Current ({current_count}) < 5000. Activating refill...")
        backups = archiver.load_recent_archives(limit=10)
        valid_set = set(valid_links)
        new_backups = [b for b in backups if b not in valid_set]
        
        if new_backups:
            print(f"   Re-validating {len(new_backups)} backup nodes...")
            revalidated = await validator.validate_batch(new_backups)
            valid_links.extend(revalidated)
            print(f"   ✅ Added {len(revalidated)} from history.")
    
    final_links = list(set(valid_links))[:5000]
    print(f"\n📦 Final Output: {len(final_links)} nodes")

    # 5. 输出
    output_dir = Path("output")
    output_dir.mkdir(exist_ok=True)
    
    if final_links:
        (output_dir / "all_sub.txt").write_text("\n".join(final_links))
        print(f"💾 Saved all_sub.txt")
    else:
        (output_dir / "all_sub.txt").write_text("")
        print("⚠️ all_sub.txt is empty")

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
