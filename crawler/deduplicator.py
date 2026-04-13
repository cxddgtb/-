#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Node Deduplicator Module - Fixed VACUUM & Optimized
节点去重和时效性管理（修复事务冲突 + 批量优化）
"""

import sqlite3
import json
import hashlib
from typing import List, Dict, Optional
from datetime import datetime
from pathlib import Path

class NodeDeduplicator:
    def __init__(self, db_path: str = "database/nodes.db"):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_database()
        
    def _init_database(self):
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
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
            
            # 兼容旧数据库字段迁移
            cursor.execute("PRAGMA table_info(nodes)")
            cols = [c[1] for c in cursor.fetchall()]
            if 'source_type' not in cols:
                cursor.execute("ALTER TABLE nodes ADD COLUMN source_type TEXT DEFAULT 'unknown'")
            if 'metadata' not in cols:
                cursor.execute("ALTER TABLE nodes ADD COLUMN metadata TEXT")
                
            # 安全创建索引
            for sql in [
                "CREATE INDEX IF NOT EXISTS idx_protocol ON nodes(protocol)",
                "CREATE INDEX IF NOT EXISTS idx_last_validated ON nodes(last_validated)",
                "CREATE INDEX IF NOT EXISTS idx_is_valid ON nodes(is_valid)",
                "CREATE INDEX IF NOT EXISTS idx_source ON nodes(source_type)",
                "CREATE INDEX IF NOT EXISTS idx_link_hash ON nodes(link_hash)"
            ]:
                try: cursor.execute(sql)
                except: pass
            conn.commit()
    
    def get_node_hash(self, link: str) -> str:
        return hashlib.md5(link.strip().lower().encode()).hexdigest()
    
    def add_or_update_nodes(self, nodes: List[Dict]) -> Dict[str, int]:
        stats = {'new': 0, 'updated': 0, 'skipped': 0}
        now = datetime.now().isoformat()
        
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            # 批量操作提升性能
            inserts = []
            updates = []
            
            for node in nodes:
                link = node.get('link', '')
                if not link: continue
                
                link_hash = self.get_node_hash(link)
                protocol = node.get('protocol', 'unknown')
                source = node.get('source', node.get('source_repo', ''))
                source_type = node.get('source_type', 'unknown')
                
                cursor.execute("SELECT id FROM nodes WHERE link_hash = ?", (link_hash,))
                if cursor.fetchone():
                    updates.append((now, source, source_type, json.dumps(node), link_hash))
                    stats['updated'] += 1
                else:
                    inserts.append((link_hash, link, protocol, source, source_type, now, now, json.dumps(node)))
                    stats['new'] += 1
            
            if inserts:
                cursor.executemany("""
                    INSERT INTO nodes (link_hash, link, protocol, source_repo, source_type, first_seen, last_seen, metadata)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, inserts)
                
            if updates:
                cursor.executemany("""
                    UPDATE nodes SET last_seen=?, source_repo=?, source_type=?, metadata=? WHERE link_hash=?
                """, updates)
                
            conn.commit()
        return stats
    
    def update_validation_results(self, results: List[Dict]):
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            now = datetime.now().isoformat()
            batch = []
            for r in results:
                link = r.get('link', '')
                if not link: continue
                batch.append((
                    now,
                    1 if r.get('is_valid', False) else 0,
                    r.get('latency_ms', 0) or 0,
                    1 if r.get('is_valid', False) else 0,
                    self.get_node_hash(link)
                ))
            if batch:
                cursor.executemany("""
                    UPDATE nodes SET last_validated=?, is_valid=?, latency_ms=?, 
                    validation_count=validation_count+1, success_count=success_count+? 
                    WHERE link_hash=?
                """, batch)
            conn.commit()
    
    def get_recent_nodes(self, protocol: str = None, limit: int = 500) -> List[Dict]:
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            q = "SELECT * FROM nodes WHERE last_validated IS NULL OR last_validated < datetime('now', '-3 days')"
            params = []
            if protocol:
                q += " AND protocol = ?"
                params.append(protocol)
            q += " ORDER BY first_seen DESC LIMIT ?"
            params.append(limit)
            cursor.execute(q, params)
            return [dict(r) for r in cursor.fetchall()]
    
    def auto_cleanup(self, max_total_nodes: int = 50000, max_age_days: int = 7):
        """🔥 修复：VACUUM 必须在事务外执行"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM nodes WHERE is_valid = 0 AND last_validated < datetime('now', '-3 days')")
            cursor.execute("SELECT COUNT(*) FROM nodes")
            total = cursor.fetchone()[0]
            if total > max_total_nodes:
                excess = total - max_total_nodes
                cursor.execute("DELETE FROM nodes WHERE id IN (SELECT id FROM nodes ORDER BY last_seen ASC LIMIT ?)", (excess,))
            conn.commit()  # ✅ 先提交删除操作
            
        # ✅ VACUUM 单独连接执行（避开事务限制）
        try:
            conn_vac = sqlite3.connect(self.db_path)
            conn_vac.execute("VACUUM")
            conn_vac.close()
        except Exception as e:
            print(f"⚠️ VACUUM skipped: {e}")
            
        return round(self.db_path.stat().st_size / 1024 / 1024, 2) if self.db_path.exists() else 0
    
    def get_stats(self) -> Dict:
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            stats = {'total': 0, 'valid': 0, 'by_protocol': {}, 'last_validation': None, 'db_size_mb': 0}
            cursor.execute("SELECT COUNT(*) FROM nodes"); stats['total'] = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(*) FROM nodes WHERE is_valid = 1"); stats['valid'] = cursor.fetchone()[0]
            cursor.execute("SELECT MAX(last_validated) FROM nodes"); stats['last_validation'] = cursor.fetchone()[0]
            cursor.execute("SELECT protocol, COUNT(*), SUM(CASE WHEN is_valid=1 THEN 1 ELSE 0 END) FROM nodes GROUP BY protocol")
            stats['by_protocol'] = {r[0]: {'total': r[1], 'valid': r[2]} for r in cursor.fetchall()}
            if self.db_path.exists(): stats['db_size_mb'] = round(self.db_path.stat().st_size / 1024 / 1024, 2)
            return stats
