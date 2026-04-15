#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Node Archiver Module - 节点档案管理
负责按日期保存节点，并提供从历史档案提取节点的功能。
"""

import os
import json
from datetime import datetime
from pathlib import Path

class Archiver:
    def __init__(self, archive_dir="archives"):
        # 确保存档目录存在
        self.archive_dir = Path(archive_dir)
        self.archive_dir.mkdir(exist_ok=True)

    def save_daily(self, links, date_str=None):
        """
        将有效节点按日期保存到 archives/ 目录下
        格式: archives/YYYY-MM-DD.json
        """
        if not date_str:
            date_str = datetime.now().strftime("%Y-%m-%d")
        
        filename = self.archive_dir / f"{date_str}.json"
        
        # 1. 读取当天已有的存档（防止同一天多次运行覆盖数据）
        existing = []
        if filename.exists():
            try:
                existing = json.loads(filename.read_text(encoding='utf-8'))
            except: pass
        
        # 2. 合并新节点并去重
        unique_links = list(set(existing + links))
        
        # 3. 写入文件
        filename.write_text(json.dumps(unique_links, ensure_ascii=False), encoding='utf-8')
        print(f"💾 已存档 {len(unique_links)} 个节点至 {filename.name}")
        return unique_links

    def load_recent_archives(self, limit=10):
        """
        从最近 N 个档案文件中加载节点（用于节点不足时回填）
        """
        if not self.archive_dir.exists():
            return []
            
        # 1. 获取所有 .json 文件并按文件名（日期）倒序排列
        files = sorted(self.archive_dir.glob("*.json"), key=lambda x: x.name, reverse=True)
        
        # 2. 取前 N 个
        recent_files = files[:limit]
        backup_nodes = set()
        
        print(f"📂 正在从 {len(recent_files)} 个历史档案中加载备份节点...")
        for f in recent_files:
            try:
                data = json.loads(f.read_text(encoding='utf-8'))
                backup_nodes.update(data)
            except Exception as e:
                print(f"  ⚠️ 读取 {f} 失败: {e}")
                
        print(f"📦 共加载 {len(backup_nodes)} 个唯一备份节点。")
        return list(backup_nodes)
