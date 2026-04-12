#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Proxy Node Crawler - Main Entry Point
Crawls GitHub for proxy nodes every 5 hours
"""

import asyncio
import json
import os
from datetime import datetime
from pathlib import Path
import sys

sys.path.append(str(Path(__file__).parent.parent))

from crawler.github_search import GitHubSearcher
from crawler.parser import NodeParser
from crawler.utils import setup_logger, save_to_file
from config.settings import Config

logger = setup_logger(__name__)

class ProxyNodeCrawler:
    def __init__(self):
        self.searcher = GitHubSearcher()
        self.parser = NodeParser()
        self.config = Config()
        self.all_nodes = {
            'vless': [],
            'naiveproxy': [],
            'anytls': [],
            'shadowtls': [],
            'hysteria2': [],
            'tuic': [],
            'all': []
        }
        
    async def search_protocol(self, protocol: str, keywords: list) -> list:
        """Search for specific protocol nodes"""
        logger.info(f"Searching for {protocol} nodes...")
        nodes = []
        
        for keyword in keywords:
            try:
                results = await self.searcher.search_repos(keyword)
                for repo in results:
                    node_info = await self.parser.parse_repository(repo, protocol)
                    if node_info:
                        nodes.extend(node_info)
                logger.info(f"Found {len(nodes)} {protocol} nodes so far")
            except Exception as e:
                logger.error(f"Error searching {keyword}: {e}")
                
        return nodes
    
    async def crawl_vless(self):
        """Crawl VLESS nodes (Reality/Xhttp/Vision/ENC)"""
        keywords = [
            "vless reality",
            "vless vision",
            "vless xhttp reality",
            "vless enc",
            "xray vless reality",
            "sing-box vless"
        ]
        nodes = await self.search_protocol("vless", keywords)
        self.all_nodes['vless'].extend(nodes)
        self.all_nodes['all'].extend(nodes)
        
    async def crawl_naiveproxy(self):
        """Crawl NaiveProxy nodes"""
        keywords = [
            "naiveproxy",
            "naive proxy config",
            "naiveproxy config"
        ]
        nodes = await self.search_protocol("naiveproxy", keywords)
        self.all_nodes['naiveproxy'].extend(nodes)
        self.all_nodes['all'].extend(nodes)
        
    async def crawl_anytls(self):
        """Crawl AnyTLS nodes"""
        keywords = [
            "anytls",
            "anytls config"
        ]
        nodes = await self.search_protocol("anytls", keywords)
        self.all_nodes['anytls'].extend(nodes)
        self.all_nodes['all'].extend(nodes)
        
    async def crawl_shadowtls(self):
        """Crawl ShadowTLS nodes"""
        keywords = [
            "shadowtls",
            "shadow-tls",
            "shadowtls config"
        ]
        nodes = await self.search_protocol("shadowtls", keywords)
        self.all_nodes['shadowtls'].extend(nodes)
        self.all_nodes['all'].extend(nodes)
        
    async def crawl_hysteria2(self):
        """Crawl Hysteria2 nodes"""
        keywords = [
            "hysteria2",
            "hysteria 2",
            "hy2 config",
            "hysteria2 config"
        ]
        nodes = await self.search_protocol("hysteria2", keywords)
        self.all_nodes['hysteria2'].extend(nodes)
        self.all_nodes['all'].extend(nodes)
        
    async def crawl_tuic(self):
        """Crawl TUIC nodes"""
        keywords = [
            "tuic",
            "tuic config",
            "tuic proxy"
        ]
        nodes = await self.search_protocol("tuic", keywords)
        self.all_nodes['tuic'].extend(nodes)
        self.all_nodes['all'].extend(nodes)
        
    def generate_subscriptions(self):
        """Generate subscription links for each protocol"""
        output_dir = Path("output")
        output_dir.mkdir(exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Generate individual protocol files
        for protocol, nodes in self.all_nodes.items():
            if protocol == 'all':
                continue
                
            if nodes:
                # Generate subscription link format
                sub_links = []
                for node in nodes:
                    if 'link' in node:
                        sub_links.append(node['link'])
                        
                if sub_links:
                    # Save as base64 encoded subscription
                    sub_content = "\n".join(sub_links)
                    save_to_file(
                        output_dir / f"{protocol}_sub_{timestamp}.txt",
                        sub_content
                    )
                    
                    # Save as JSON
                    save_to_file(
                        output_dir / f"{protocol}_nodes_{timestamp}.json",
                        json.dumps(nodes, indent=2, ensure_ascii=False)
                    )
        
        # Generate all-in-one subscription
        all_links = []
        for node in self.all_nodes['all']:
            if 'link' in node:
                all_links.append(node['link'])
                
        if all_links:
            save_to_file(
                output_dir / f"all_sub_{timestamp}.txt",
                "\n".join(all_links)
            )
            
            save_to_file(
                output_dir / f"all_nodes_{timestamp}.json",
                json.dumps(self.all_nodes, indent=2, ensure_ascii=False)
            )
            
        # Generate README for output
        readme_content = f"""# Proxy Nodes Collection
**Last Updated:** {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
**Total Nodes:** {len(self.all_nodes['all'])}

## Protocol Statistics:
- VLESS: {len(self.all_nodes['vless'])}
- NaiveProxy: {len(self.all_nodes['naiveproxy'])}
- AnyTLS: {len(self.all_nodes['anytls'])}
- ShadowTLS: {len(self.all_nodes['shadowtls'])}
- Hysteria2: {len(self.all_nodes['hysteria2'])}
- TUIC: {len(self.all_nodes['tuic'])}

## Subscription Links:
"""
        for protocol in ['vless', 'naiveproxy', 'anytls', 'shadowtls', 'hysteria2', 'tuic']:
            if self.all_nodes[protocol]:
                readme_content += f"- {protocol.upper()}: `{protocol}_sub_{timestamp}.txt`\n"
                
        save_to_file(output_dir / "README.md", readme_content)
        
    async def run(self):
        """Main crawler execution"""
        logger.info("Starting proxy node crawler...")
        start_time = datetime.now()
        
        try:
            # Crawl all protocols
            await asyncio.gather(
                self.crawl_vless(),
                self.crawl_naiveproxy(),
                self.crawl_anytls(),
                self.crawl_shadowtls(),
                self.crawl_hysteria2(),
                self.crawl_tuic()
            )
            
            # Generate output files
            self.generate_subscriptions()
            
            elapsed = (datetime.now() - start_time).total_seconds()
            logger.info(f"Crawler completed in {elapsed:.2f} seconds")
            logger.info(f"Total nodes found: {len(self.all_nodes['all'])}")
            
        except Exception as e:
            logger.error(f"Crawler failed: {e}")
            raise

async def main():
    crawler = ProxyNodeCrawler()
    await crawler.run()

if __name__ == "__main__":
    asyncio.run(main())
