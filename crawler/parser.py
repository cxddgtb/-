#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Node Parser Module
Parses proxy node configurations from various formats
"""

import re
import base64
import json
from typing import List, Dict, Optional
from urllib.parse import urlparse, parse_qs, unquote
from crawler.utils import setup_logger
from crawler.github_search import GitHubSearcher

logger = setup_logger(__name__)

class NodeParser:
    def __init__(self):
        self.searcher = GitHubSearcher()
        
    async def parse_repository(self, repo: Dict, protocol: str) -> List[Dict]:
        """Parse a repository for proxy nodes"""
        nodes = []
        owner = repo["owner"]["login"]
        repo_name = repo["name"]
        
        try:
            # Get repository files
            contents = await self.searcher.get_repo_contents(owner, repo_name)
            
            for item in contents:
                if item["type"] == "file":
                    node = await self.parse_file(owner, repo_name, item, protocol)
                    if node:
                        node["source_repo"] = f"{owner}/{repo_name}"
                        node["source_url"] = repo["html_url"]
                        nodes.append(node)
                        
        except Exception as e:
            logger.error(f"Error parsing repo {owner}/{repo_name}: {e}")
            
        return nodes
    
    async def parse_file(self, owner: str, repo: str, file_info: Dict, protocol: str) -> Optional[Dict]:
        """Parse a single file for proxy nodes"""
        path = file_info["path"]
        
        # Check if file extension matches expected formats
        if not self.is_relevant_file(path, protocol):
            return None
            
        try:
            content = await self.searcher.get_file_content(owner, repo, path)
            if not content:
                return None
                
            # Parse based on protocol
            if protocol == "vless":
                return self.parse_vless(content, path)
            elif protocol == "naiveproxy":
                return self.parse_naiveproxy(content, path)
            elif protocol == "anytls":
                return self.parse_anytls(content, path)
            elif protocol == "shadowtls":
                return self.parse_shadowtls(content, path)
            elif protocol == "hysteria2":
                return self.parse_hysteria2(content, path)
            elif protocol == "tuic":
                return self.parse_tuic(content, path)
                
        except Exception as e:
            logger.error(f"Error parsing file {path}: {e}")
            
        return None
    
    def is_relevant_file(self, path: str, protocol: str) -> bool:
        """Check if file is relevant for the protocol"""
        relevant_extensions = {
            "vless": [".json", ".yaml", ".yml", ".txt", ".conf"],
            "naiveproxy": [".json", ".yaml", ".yml", ".conf"],
            "anytls": [".json", ".yaml", ".yml", ".conf"],
            "shadowtls": [".json", ".yaml", ".yml", ".conf"],
            "hysteria2": [".json", ".yaml", ".yml", ".conf", ".txt"],
            "tuic": [".json", ".yaml", ".yml", ".conf", ".txt"]
        }
        
        ext = "." + path.split(".")[-1] if "." in path else ""
        return ext in relevant_extensions.get(protocol, [])
    
    def parse_vless(self, content: str, path: str) -> Optional[Dict]:
        """Parse VLESS configuration"""
        # Try to parse as subscription link
        vless_pattern = r'vless://[^\s]+'
        matches = re.findall(vless_pattern, content)
        
        if matches:
            return {
                "protocol": "vless",
                "link": matches[0],
                "file_path": path,
                "format": "subscription"
            }
        
        # Try to parse as JSON config
        try:
            config = json.loads(content)
            if "outbounds" in config:
                for outbound in config["outbounds"]:
                    if outbound.get("protocol") == "vless":
                        return {
                            "protocol": "vless",
                            "config": config,
                            "file_path": path,
                            "format": "sing-box"
                        }
            if "outbound" in config:
                if config["outbound"].get("protocol") == "vless":
                    return {
                        "protocol": "vless",
                        "config": config,
                        "file_path": path,
                        "format": "xray"
                    }
        except json.JSONDecodeError:
            pass
            
        return None
    
    def parse_naiveproxy(self, content: str, path: str) -> Optional[Dict]:
        """Parse NaiveProxy configuration"""
        try:
            config = json.loads(content)
            if "proxy" in config or "listen" in config:
                return {
                    "protocol": "naiveproxy",
                    "config": config,
                    "file_path": path,
                    "format": "json"
                }
        except json.JSONDecodeError:
            pass
            
        return None
    
    def parse_anytls(self, content: str, path: str) -> Optional[Dict]:
        """Parse AnyTLS configuration"""
        try:
            config = json.loads(content)
            if any(key in config for key in ["server", "client", "tls"]):
                return {
                    "protocol": "anytls",
                    "config": config,
                    "file_path": path,
                    "format": "json"
                }
        except json.JSONDecodeError:
            pass
            
        return None
    
    def parse_shadowtls(self, content: str, path: str) -> Optional[Dict]:
        """Parse ShadowTLS configuration"""
        try:
            config = json.loads(content)
            if any(key in config for key in ["server", "client", "shadowsocks"]):
                return {
                    "protocol": "shadowtls",
                    "config": config,
                    "file_path": path,
                    "format": "json"
                }
        except json.JSONDecodeError:
            pass
            
        return None
    
    def parse_hysteria2(self, content: str, path: str) -> Optional[Dict]:
        """Parse Hysteria2 configuration"""
        # Try subscription link
        hy2_pattern = r'hysteria2://[^\s]+'
        matches = re.findall(hy2_pattern, content)
        
        if matches:
            return {
                "protocol": "hysteria2",
                "link": matches[0],
                "file_path": path,
                "format": "subscription"
            }
        
        # Try JSON/YAML config
        try:
            config = json.loads(content)
            if any(key in config for key in ["server", "client", "hysteria"]):
                return {
                    "protocol": "hysteria2",
                    "config": config,
                    "file_path": path,
                    "format": "json"
                }
        except json.JSONDecodeError:
            pass
            
        return None
    
    def parse_tuic(self, content: str, path: str) -> Optional[Dict]:
        """Parse TUIC configuration"""
        # Try subscription link
        tuic_pattern = r'tuic://[^\s]+'
        matches = re.findall(tuic_pattern, content)
        
        if matches:
            return {
                "protocol": "tuic",
                "link": matches[0],
                "file_path": path,
                "format": "subscription"
            }
        
        # Try JSON config
        try:
            config = json.loads(content)
            if any(key in config for key in ["server", "client", "tuic"]):
                return {
                    "protocol": "tuic",
                    "config": config,
                    "file_path": path,
                    "format": "json"
                }
        except json.JSONDecodeError:
            pass
            
        return None
