#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Main Controller - 集成时分秒存档 + 暴力爬虫
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
    print(" 启动终极节点爬虫 (Timestamp Archive Mode)")
    print("="*60)
    
    token = os.getenv("GITHUB_TOKEN", "")
    if not token:
        print("⚠️  WARNING: GITHUB_TOKEN is empty! API limits may apply.")
    
    scraper = Scraper(token)
    validator = Validator(max_concurrent=80)
    archiver = Archiver()

    # 1. 暴力爬取
    print("\n️  Phase 1: Aggressive Crawling (Repo Traverse + Code Search)...")
    all_raw_links = await scraper.run_crawl()
    print(f"📊 Total Raw Links Extracted: {len(all_raw_links)}")

    # 2. 验证
    print("\n🔍 Phase 2: Validating...")
    valid_links = []
    if all_raw_links:
        unique_raw = list(set(all_raw_links))
        print(f"   Deduplicated to {len(unique_raw)} unique links.")
        valid_links = await validator.validate_batch(unique_raw)
        print(f"✅ Newly verified: {len(valid_links)} nodes")
    else:
        print("️ No raw links found.")

    # 3. 存档 (带时间戳)
    print("\n💾 Phase 3: Archiving (Timestamped)...")
    valid_links = archiver.save_daily(valid_links)

    # 4. 回填
    print("\n Phase 4: Smart Refill...")
    if len(valid_links) < 5000:
        print(f"   Need more nodes. Loading history...")
        backups = archiver.load_recent_archives(limit=10)
        valid_set = set(valid_links)
        new_backups = [b for b in backups if b not in valid_set]
        
        if new_backups:
            print(f"   Re-validating {len(new_backups)} backups...")
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
