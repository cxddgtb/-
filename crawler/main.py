#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Enhanced Proxy Node Crawler - Multi-Source Strategy
"""

import asyncio
import json
from datetime import datetime
from pathlib import Path
import sys

sys.path.append(str(Path(__file__).parent.parent))

from crawler.sources import (
    GitHubRepoSearcher,
    GitHubCodeSearcher,
    GitHubGistSearcher,
    GitHubREADMESearcher
)
from crawler.validator import NodeValidator
from crawler.deduplicator import NodeDeduplicator
from crawler.utils import setup_logger, save_to_file
from config.settings import Config

logger = setup_logger(__name__)

class EnhancedProxyCrawler:
    def __init__(self):
        self.config = Config()
        self.deduplicator = NodeDeduplicator()
        self.validator = NodeValidator(concurrent_limit=50, timeout=5)
        
        # 多源爬虫
        self.sources = {
            'github_repos': GitHubRepoSearcher(),
            'github_code': GitHubCodeSearcher(),
            'github_gist': GitHubGistSearcher(),
            'github_readme': GitHubREADMESearcher(),
        }
        
        self.all_nodes = []
        self.stats = {
            'total_crawled': 0,
            'by_source': {},
            'by_protocol': {}
        }
        
    async def crawl_all_sources(self):
        """从所有源爬取数据"""
        logger.info("🚀 Starting multi-source crawling...")
        
        tasks = []
        
        # GitHub仓库搜索
        tasks.append(self.crawl_github_repos())
        
        # GitHub代码搜索
        tasks.append(self.crawl_github_code())
        
        # GitHub Gist搜索
        tasks.append(self.crawl_github_gists())
        
        # GitHub README搜索
        tasks.append(self.crawl_github_readmes())
        
        # 并发执行所有爬取任务
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # 合并结果
        for source_name, nodes in zip(self.sources.keys(), results):
            if isinstance(nodes, list):
                count = len(nodes)
                self.stats['by_source'][source_name] = count
                self.stats['total_crawled'] += count
                self.all_nodes.extend(nodes)
                logger.info(f"✅ {source_name}: {count} nodes")
            else:
                logger.error(f"❌ {source_name} failed: {nodes}")
                
        logger.info(f"📊 Total crawled: {self.stats['total_crawled']} nodes")
        
    async def crawl_github_repos(self) -> list:
        """爬取GitHub仓库"""
        searcher = self.sources['github_repos']
        nodes = []
        
        keywords = [
            # VLESS相关
            "vless reality",
            "vless vision",
            "vless enc",
            "xray vless reality",
            "sing-box vless",
            
            # Hysteria2
            "hysteria2",
            "hysteria 2",
            "hy2 config",
            
            # TUIC
            "tuic v5",
            "tuic config",
            
            # NaiveProxy
            "naiveproxy",
            "naive proxy",
            
            # ShadowTLS
            "shadowtls",
            "shadow-tls",
            
            # AnyTLS
            "anytls",
            
            # 通用关键词
            "proxy config",
            "v2ray config",
            "xray config",
            "clash config",
            "sing-box config",
        ]
        
        for keyword in keywords:
            try:
                logger.debug(f"Searching repos: {keyword}")
                repos = await searcher.search(keyword, sort="updated", order="desc")
                
                for repo in repos:
                    # 只处理最近30天更新的仓库
                    updated_at = datetime.fromisoformat(
                        repo['updated_at'].replace('Z', '+00:00')
                    )
                    days_old = (datetime.now(updated_at.tzinfo) - updated_at).days
                    
                    if days_old > 30:
                        continue
                    
                    # 解析仓库
                    repo_nodes = await searcher.parse_repo(repo)
                    if repo_nodes:
                        nodes.extend(repo_nodes)
                        logger.debug(f"  Found {len(repo_nodes)} nodes in {repo['full_name']}")
                        
            except Exception as e:
                logger.error(f"Error crawling repos for {keyword}: {e}")
                
        return nodes
    
    async def crawl_github_code(self) -> list:
        """爬取GitHub代码"""
        searcher = self.sources['github_code']
        nodes = []
        
        # 直接搜索节点链接格式
        search_queries = [
            "vless://",
            "hysteria2://",
            "tuic://",
            "naiveproxy",
            "shadowtls",
            "anytls",
            "server.*vless",
            "outbound.*vless",
            "hy2://",
        ]
        
        for query in search_queries:
            try:
                logger.debug(f"Searching code: {query}")
                code_results = await searcher.search(query)
                
                for item in code_results:
                    try:
                        file_nodes = await searcher.extract_nodes(item)
                        if file_nodes:
                            nodes.extend(file_nodes)
                    except Exception as e:
                        logger.debug(f"  Error parsing file: {e}")
                        
            except Exception as e:
                logger.error(f"Error searching code for {query}: {e}")
                
        return nodes
    
    async def crawl_github_gists(self) -> list:
        """爬取GitHub Gist"""
        searcher = self.sources['github_gist']
        nodes = []
        
        keywords = [
            "vless",
            "hysteria2",
            "tuic",
            "naiveproxy",
            "shadowtls",
            "anytls",
            "proxy config",
            "v2ray",
        ]
        
        for keyword in keywords:
            try:
                logger.debug(f"Searching gists: {keyword}")
                gists = await searcher.search(keyword)
                
                for gist in gists:
                    gist_nodes = await searcher.parse_gist(gist)
                    if gist_nodes:
                        nodes.extend(gist_nodes)
                        
            except Exception as e:
                logger.error(f"Error crawling gists for {keyword}: {e}")
                
        return nodes
    
    async def crawl_github_readmes(self) -> list:
        """爬取README文件"""
        searcher = self.sources['github_readme']
        nodes = []
        
        keywords = [
            "vless reality",
            "hysteria2",
            "tuic",
            "proxy node",
            "v2ray node",
        ]
        
        for keyword in keywords:
            try:
                logger.debug(f"Searching READMEs: {keyword}")
                readmes = await searcher.search(keyword)
                
                for readme in readmes:
                    readme_nodes = await searcher.extract_nodes(readme)
                    if readme_nodes:
                        nodes.extend(readme_nodes)
                        
            except Exception as e:
                logger.error(f"Error crawling READMEs for {keyword}: {e}")
                
        return nodes
    
    async def process_nodes(self):
        """处理爬取的节点：去重、验证、过滤"""
        logger.info("🔄 Processing nodes...")
        
        # 去重
        logger.info(f"  Deduplicating {len(self.all_nodes)} nodes...")
        dedup_stats = self.deduplicator.add_or_update_nodes(self.all_nodes)
        logger.info(f"  Dedup stats: {dedup_stats}")
        
        # 获取待验证节点
        protocols = ['vless', 'hysteria2', 'tuic', 'naiveproxy', 'shadowtls', 'anytls']
        all_valid_nodes = []
        
        for protocol in protocols:
            logger.info(f"  Validating {protocol} nodes...")
            
            # 获取未验证或需要重新验证的节点
            pending = self.deduplicator.get_recent_nodes(protocol, limit=500)
            
            if pending:
                # 批量验证
                results = await self.validator.validate_nodes_batch(pending)
                
                # 更新数据库
                valid_results = []
                for r in results:
                    if hasattr(r, 'node_link'):
                        valid_results.append({
                            'link': r.node_link,
                            'protocol': r.protocol,
                            'is_valid': r.is_valid,
                            'latency_ms': r.latency_ms,
                        })
                
                self.deduplicator.update_validation_results(valid_results)
                
                # 获取有效节点
                valid = self.validator.filter_valid_nodes(results, max_latency=500.0)
                all_valid_nodes.extend(valid)
                
                logger.info(f"    ✅ {protocol}: {len(valid)} valid nodes")
        
        return all_valid_nodes
    
    def generate_output(self, valid_nodes: list):
        """生成输出文件"""
        output_dir = Path("output")
        output_dir.mkdir(exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # 按协议分类
        by_protocol = {}
        for node in valid_nodes:
            proto = node.get('protocol', 'unknown')
            if proto not in by_protocol:
                by_protocol[proto] = []
            by_protocol[proto].append(node)
        
        # 生成各协议文件
        for protocol, nodes in by_protocol.items():
            if not nodes:
                continue
            
            # 订阅链接
            links = [n.get('link', '') for n in nodes if n.get('link')]
            save_to_file(
                output_dir / f"{protocol}_sub.txt",
                "\n".join(links)
            )
            
            # JSON详情
            save_to_file(
                output_dir / f"{protocol}_nodes.json",
                json.dumps(nodes, indent=2, ensure_ascii=False)
            )
        
        # 合并所有
        all_links = []
        for nodes in by_protocol.values():
            all_links.extend([n.get('link', '') for n in nodes if n.get('link')])
        
        if all_links:
            save_to_file(output_dir / "all_sub.txt", "\n".join(all_links))
            save_to_file(
                output_dir / "all_nodes.json",
                json.dumps(by_protocol, indent=2, ensure_ascii=False)
            )
        
        # 生成统计报告
        db_stats = self.deduplicator.get_stats()
        report = f"""# Proxy Nodes Report
**Generated:** {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}

## Crawling Statistics
- Total crawled this run: {self.stats['total_crawled']}
- Valid nodes found: {len(valid_nodes)}

## By Source
"""
        for source, count in self.stats['by_source'].items():
            report += f"- {source}: {count}\n"
            
        report += f"\n## Database Statistics\n"
        report += f"- Total nodes in DB: {db_stats.get('total', 0)}\n"
        report += f"- Valid nodes: {db_stats.get('valid', 0)}\n"
        
        save_to_file(output_dir / "STATS.md", report)
        
        logger.info(f"📁 Output saved to {output_dir}")
    
    async def run(self):
        """主执行流程"""
        logger.info("=" * 60)
        logger.info("🚀 Enhanced Proxy Node Crawler Starting...")
        logger.info("=" * 60)
        
        start_time = datetime.now()
        
        try:
            # 清理旧数据
            cleaned = self.deduplicator.cleanup_old_nodes(max_age_days=14)
            logger.info(f"🗑️ Cleaned {cleaned} old nodes")
            
            # 爬取所有源
            await self.crawl_all_sources()
            
            # 处理节点
            valid_nodes = await self.process_nodes()
            
            # 生成输出
            self.generate_output(valid_nodes)
            
            # 最终统计
            elapsed = (datetime.now() - start_time).total_seconds()
            db_stats = self.deduplicator.get_stats()
            
            logger.info("=" * 60)
            logger.info("✅ Crawler Completed Successfully!")
            logger.info(f"⏱️  Duration: {elapsed:.2f}s")
            logger.info(f"📊 Total crawled: {self.stats['total_crawled']}")
            logger.info(f"✅ Valid nodes: {len(valid_nodes)}")
            logger.info(f"💾 Database total: {db_stats.get('valid', 0)}")
            logger.info("=" * 60)
            
        except Exception as e:
            logger.error(f"❌ Crawler failed: {e}", exc_info=True)
            raise

async def main():
    crawler = EnhancedProxyCrawler()
    await crawler.run()

if __name__ == "__main__":
    asyncio.run(main())
