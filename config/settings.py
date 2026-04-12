#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Configuration Settings
"""

import os
from pathlib import Path

class Config:
    # GitHub API settings
    GITHUB_TOKEN = os.getenv("GITHUB_TOKEN", "")
    RATE_LIMIT_DELAY = 1  # seconds between requests
    
    # Crawler settings
    MAX_REPOS_PER_SEARCH = 100
    MAX_PAGES = 10
    
    # Output settings
    OUTPUT_DIR = Path("output")
    LOG_DIR = Path("logs")
    
    # Protocols to crawl
    PROTOCOLS = [
        "vless",
        "naiveproxy",
        "anytls",
        "shadowtls",
        "hysteria2",
        "tuic"
    ]
    
    # Search keywords for each protocol
    SEARCH_KEYWORDS = {
        "vless": [
            "vless reality",
            "vless vision",
            "xray vless",
            "sing-box vless"
        ],
        "naiveproxy": [
            "naiveproxy",
            "naive proxy"
        ],
        "anytls": [
            "anytls"
        ],
        "shadowtls": [
            "shadowtls",
            "shadow-tls"
        ],
        "hysteria2": [
            "hysteria2",
            "hysteria 2",
            "hy2"
        ],
        "tuic": [
            "tuic"
        ]
    }
