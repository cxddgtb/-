#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Node Archiver Module - 支持时分秒精确存档
"""

import os
import json
from datetime import datetime
from pathlib import Path

class Archiver:
    def __init__(self, archive_dir="archives"):
        self.archive_dir = Path(archive_dir)
        self.archive_dir.mkdir(exist_ok=True)

    def save_daily(self, links):
        """
        按当前时间精确到秒保存档案
        格式: archives/YYYY-MM-DD_HH-MM-SS.json
        """
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        filename = self.archive_dir / f"{timestamp}.json"
        
        # 去重
        unique_links = list(set(links))
        
        if unique_links:
            filename.write_text(json.dumps(unique_links, ensure_ascii=False), encoding='utf-8')
            print(f" 已存档 {len(unique_links)} 个节点至 {filename.name}")
        else:
            # 即使为空也创建文件，标记本次运行
            filename.write_text("[]", encoding='utf-8')
            print(f"💾 创建了空档案 {filename.name} (无新节点)")
            
        return unique_links

    def load_recent_archives(self, limit=10):
        """
        加载最近 N 个档案（自动识别带时间戳的文件名）
        """
        if not self.archive_dir.exists():
            return []
            
        # 获取所有 .json 文件，按文件名倒序（时间越晚越靠前）
        files = sorted(self.archive_dir.glob("*.json"), key=lambda x: x.name, reverse=True)
        
        recent_files = files[:limit]
        backup_nodes = set()
        
        print(f" 正在从 {len(recent_files)} 个历史档案中加载...")
        for f in recent_files:
            try:
                data = json.loads(f.read_text(encoding='utf-8'))
                if isinstance(data, list):
                    backup_nodes.update(data)
            except Exception as e:
                print(f"  ⚠️ 读取 {f.name} 失败: {e}")
                
        print(f"📦 共加载 {len(backup_nodes)} 个唯一备份节点。")
        return list(backup_nodes)
