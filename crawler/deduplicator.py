#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Node Deduplicator Module
节点去重和时效性管理
"""

import sqlite3
import json
import hashlib
from typing import List, Dict, Optional
from datetime import datetime, timedelta
from pathlib import Path

class NodeDeduplicator:
    """节点去重和时效性管理器"""
    
    def __init__(self, db_path: str = "database/nodes.db"):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_database()
        
    def _init_database(self):
        """初始化数据库"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS nodes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    link_hash TEXT UNIQUE NOT NULL,
                    link TEXT NOT NULL,
                    protocol TEXT NOT NULL,
                    source_repo TEXT,
                    source_type TEXT DEFAULT 'unknown',
                    first_seen TEXT NOT NULL,
                    last_seen TEXT NOT NULL,
                    last_validated TEXT,
                    is_valid BOOLEAN DEFAULT 0,
                    latency_ms REAL,
                    validation_count INTEGER DEFAULT 0,
                    success_count INTEGER DEFAULT 0,
                    metadata TEXT
                )
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_protocol ON nodes(protocol)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_last_validated ON nodes(last_validated)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_is_valid ON nodes(is_valid)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_source ON nodes(source_type)")
            conn.commit()
    
    def get_node_hash(self, link: str) -> str:
        """生成节点链接的哈希值"""
        normalized = link.strip().lower()
        return hashlib.md5(normalized.encode()).hexdigest()
    
    def add_or_update_nodes(self, nodes: List[Dict]) -> Dict[str, int]:
        """添加或更新节点"""
        stats = {'new': 0, 'updated': 0, 'duplicates': 0}
        
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            now = datetime.now().isoformat()
            
            for node in nodes:
                link = node.get('link', '')
                if not link:
                    continue
                    
                link_hash = self.get_node_hash(link)
                protocol = node.get('protocol', 'unknown')
                source_repo = node.get('source', node.get('source_repo', ''))
                source_type = node.get('source_type', 'unknown')
                
                cursor.execute(
                    "SELECT id FROM nodes WHERE link_hash = ?",
                    (link_hash,)
                )
                existing = cursor.fetchone()
                
                if existing:
                    cursor.execute("""
                        UPDATE nodes SET 
                            last_seen = ?,
                            source_repo = ?,
                            source_type = ?,
                            metadata = ?
                        WHERE link_hash = ?
                    """, (now, source_repo, source_type, json.dumps(node), link_hash))
                    stats['updated'] += 1
                else:
                    cursor.execute("""
                        INSERT INTO nodes 
                        (link_hash, link, protocol, source_repo, source_type, first_seen, last_seen, metadata)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """, (link_hash, link, protocol, source_repo, source_type, now, now, json.dumps(node)))
                    stats['new'] += 1
            
            conn.commit()
            
        return stats
    
    def update_validation_results(self, results: List[Dict]):
        """更新验证结果"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            now = datetime.now().isoformat()
            
            for result in results:
                link = result.get('link', '')
                if not link:
                    continue
                    
                link_hash = self.get_node_hash(link)
                is_valid = 1 if result.get('is_valid', False) else 0
                latency = result.get('latency_ms', 0) or 0
                
                cursor.execute("""
                    UPDATE nodes SET 
                        last_validated = ?,
                        is_valid = ?,
                        latency_ms = ?,
                        validation_count = validation_count + 1,
                        success_count = success_count + ?
                    WHERE link_hash = ?
                """, (now, is_valid, latency, 1 if is_valid else 0, link_hash))
            
            conn.commit()
    
    def get_valid_nodes(self, protocol: str = None, 
                       max_age_days: int = 7,
                       min_success_rate: float = 0.5,
                       max_latency: float = 500.0) -> List[Dict]:
        """获取有效节点"""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            query = f"""
                SELECT * FROM nodes 
                WHERE is_valid = 1
                AND last_validated >= datetime('now', '-{max_age_days} days')
                AND latency_ms > 0 AND latency_ms <= {max_latency}
            """
            
            params = ()
            if protocol:
                query += " AND protocol = ?"
                params = (protocol,)
                
            query += " ORDER BY latency_ms ASC, last_validated DESC"
            
            cursor.execute(query, params)
            rows = cursor.fetchall()
            
            nodes = []
            for row in rows:
                node = dict(row)
                if node['validation_count'] > 0:
                    node['success_rate'] = node['success_count'] / node['validation_count']
                else:
                    node['success_rate'] = 0
                if node['success_rate'] >= min_success_rate:
                    nodes.append(node)
                    
            return nodes
    
    def get_recent_nodes(self, protocol: str = None, limit: int = 300) -> List[Dict]:
        """获取待验证节点"""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            query = """
                SELECT * FROM nodes 
                WHERE last_validated IS NULL OR last_validated < datetime('now', '-3 days')
            """
            params = ()
            
            if protocol:
                query += " AND protocol = ?"
                params = (protocol,)
                
            query += " ORDER BY first_seen DESC LIMIT ?"
            params = params + (limit,)
            
            cursor.execute(query, params)
            return [dict(row) for row in cursor.fetchall()]
    
    def auto_cleanup(self, max_total_nodes: int = 30000, max_age_days: int = 7):
        """🔥 自动清理：控制数据库大小"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # 1. 删除超过3天的无效节点
            cursor.execute("""
                DELETE FROM nodes 
                WHERE is_valid = 0 
                AND last_validated < datetime('now', '-3 days')
            """)
            
            # 2. 如果总数超限，删除最旧的节点
            cursor.execute("SELECT COUNT(*) FROM nodes")
            total = cursor.fetchone()[0]
            
            if total > max_total_nodes:
                excess = total - max_total_nodes
                cursor.execute("""
                    DELETE FROM nodes 
                    WHERE id IN (
                        SELECT id FROM nodes 
                        ORDER BY last_seen ASC 
                        LIMIT ?
                    )
                """, (excess,))
            
            # 3. 压缩数据库
            cursor.execute("VACUUM")
            conn.commit()
            
            # 返回当前大小（MB）
            db_size = self.db_path.stat().st_size / 1024 / 1024 if self.db_path.exists() else 0
            return round(db_size, 2)
    
    def get_stats(self) -> Dict:
        """获取统计信息"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            stats = {}
            cursor.execute("SELECT COUNT(*) FROM nodes")
            stats['total'] = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM nodes WHERE is_valid = 1")
            stats['valid'] = cursor.fetchone()[0]
            
            cursor.execute("""
                SELECT protocol, COUNT(*) as count,
                       SUM(CASE WHEN is_valid = 1 THEN 1 ELSE 0 END) as valid_count
                FROM nodes GROUP BY protocol
            """)
            stats['by_protocol'] = {row[0]: {'total': row[1], 'valid': row[2]} 
                                   for row in cursor.fetchall()}
            
            cursor.execute("SELECT MAX(last_validated) FROM nodes")
            stats['last_validation'] = cursor.fetchone()[0]
            
            # 数据库大小
            if self.db_path.exists():
                stats['db_size_mb'] = round(self.db_path.stat().st_size / 1024 / 1024, 2)
            else:
                stats['db_size_mb'] = 0
                
            return stats
