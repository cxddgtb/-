#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Deduplicator Module
节点去重和时效性管理
"""

import sqlite3
import json
import hashlib
from typing import List, Dict, Optional
from datetime import datetime, timedelta
from pathlib import Path
from crawler.utils import setup_logger

logger = setup_logger(__name__)

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
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_protocol 
                ON nodes(protocol)
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_last_validated 
                ON nodes(last_validated)
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_is_valid 
                ON nodes(is_valid)
            """)
            conn.commit()
    
    def get_node_hash(self, link: str) -> str:
        """生成节点链接的哈希值"""
        # 标准化链接（去除可能的参数差异）
        normalized = link.strip().lower()
        return hashlib.md5(normalized.encode()).hexdigest()
    
    def add_or_update_nodes(self, nodes: List[Dict]) -> Dict[str, int]:
        """添加或更新节点，返回统计信息"""
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
                source_repo = node.get('source_repo', '')
                
                # 检查是否已存在
                cursor.execute(
                    "SELECT id, validation_count, success_count FROM nodes WHERE link_hash = ?",
                    (link_hash,)
                )
                existing = cursor.fetchone()
                
                if existing:
                    # 更新现有节点
                    cursor.execute("""
                        UPDATE nodes SET 
                            last_seen = ?,
                            source_repo = ?,
                            metadata = ?
                        WHERE link_hash = ?
                    """, (now, source_repo, json.dumps(node), link_hash))
                    stats['updated'] += 1
                else:
                    # 插入新节点
                    cursor.execute("""
                        INSERT INTO nodes 
                        (link_hash, link, protocol, source_repo, first_seen, last_seen, metadata)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    """, (link_hash, link, protocol, source_repo, now, now, json.dumps(node)))
                    stats['new'] += 1
            
            conn.commit()
            
        logger.info(f"Deduplication stats: {stats}")
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
                is_valid = result.get('is_valid', False)
                latency = result.get('latency_ms', 0)
                
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
                       max_latency: float = 300.0) -> List[Dict]:
        """获取有效节点"""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            query = """
                SELECT * FROM nodes 
                WHERE is_valid = 1
                AND last_validated >= datetime('now', '-{} days')
                AND latency_ms > 0 AND latency_ms <= {}
            """.format(max_age_days, max_latency)
            
            if protocol:
                query += " AND protocol = ?"
                params = (protocol,)
            else:
                params = ()
                
            query += " ORDER BY latency_ms ASC, last_validated DESC"
            
            cursor.execute(query, params)
            rows = cursor.fetchall()
            
            nodes = []
            for row in rows:
                node = dict(row)
                # 计算成功率
                if node['validation_count'] > 0:
                    node['success_rate'] = node['success_count'] / node['validation_count']
                else:
                    node['success_rate'] = 0
                    
                # 过滤低成功率节点
                if node['success_rate'] >= min_success_rate:
                    nodes.append(node)
                    
            return nodes
    
    def get_recent_nodes(self, protocol: str = None, limit: int = 100) -> List[Dict]:
        """获取最近添加的节点（尚未验证）"""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            query = """
                SELECT * FROM nodes 
                WHERE last_validated IS NULL
            """
            params = ()
            
            if protocol:
                query += " AND protocol = ?"
                params = (protocol,)
                
            query += " ORDER BY first_seen DESC LIMIT ?"
            params = params + (limit,)
            
            cursor.execute(query, params)
            return [dict(row) for row in cursor.fetchall()]
    
    def cleanup_old_nodes(self, max_age_days: int = 30):
        """清理过期节点"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # 删除超过指定天数未验证且无效的节点
            cursor.execute("""
                DELETE FROM nodes 
                WHERE last_validated < datetime('now', '-{} days')
                AND is_valid = 0
            """.format(max_age_days))
            
            deleted = cursor.rowcount
            conn.commit()
            
            logger.info(f"Cleaned up {deleted} old invalid nodes")
            return deleted
    
    def get_stats(self) -> Dict:
        """获取数据库统计信息"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            stats = {}
            
            # 总数
            cursor.execute("SELECT COUNT(*) FROM nodes")
            stats['total'] = cursor.fetchone()[0]
            
            # 有效节点数
            cursor.execute("SELECT COUNT(*) FROM nodes WHERE is_valid = 1")
            stats['valid'] = cursor.fetchone()[0]
            
            # 按协议统计
            cursor.execute("""
                SELECT protocol, COUNT(*) as count,
                       SUM(CASE WHEN is_valid = 1 THEN 1 ELSE 0 END) as valid_count
                FROM nodes 
                GROUP BY protocol
            """)
            stats['by_protocol'] = {row[0]: {'total': row[1], 'valid': row[2]} 
                                   for row in cursor.fetchall()}
            
            # 最近验证时间
            cursor.execute("SELECT MAX(last_validated) FROM nodes")
            stats['last_validation'] = cursor.fetchone()[0]
            
            return stats
