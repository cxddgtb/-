#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Keyword Sharding Configuration
将搜索关键词分成8组，供并行工作流使用
"""

# 🔥 8组关键词，每组负责不同协议/来源
KEYWORD_SHARDS = {
    0: [  # VLESS 核心
        "vless reality", "vless vision", "vless enc", "vless xhttp",
        "xray vless", "sing-box vless", "vless subscription",
        "filename:config.json vless", "path:*.json reality",
    ],
    1: [  # Hysteria2
        "hysteria2", "hysteria 2", "hy2", "hy2 config", 
        "hysteria2 subscription", "filename:*.yaml hysteria",
        "extension:yaml hy2", "hysteria2 server",
    ],
    2: [  # TUIC + Trojan
        "tuic v5", "tuic config", "tuic subscription", "tuic server",
        "trojan go", "trojan reality", "trojan subscription",
        "filename:*.json trojan",
    ],
    3: [  # Naive/ShadowTLS/AnyTLS
        "naiveproxy config", "shadowtls config", "anytls config",
        "naive proxy", "shadow-tls", "any-tls",
    ],
    4: [  # 通用配置 + 订阅
        "proxy config", "v2ray config", "xray config", 
        "clash config", "clash meta config", "mihomo config",
        "sing-box config", "subscription", "subscribe",
    ],
    5: [  # 中文关键词 + 节点列表
        "机场配置", "节点订阅", "代理配置", "v2ray订阅",
        "node list", "server list", "free proxy", "vpn config",
        "filename:subscription.txt", "filename:nodes.yaml",
    ],
    6: [  # 代码搜索专用查询
        "vless:// language:json", "vless:// language:yaml",
        "hysteria2://", "hy2://", "tuic://",
        "path:*.txt subscription", "path:*.list proxy",
    ],
    7: [  # 长尾关键词 + 文件名搜索
        "proxy node", "v2ray node", "xray node",
        "filename:servers.list", "filename:proxy.conf",
        "extension:json proxy", "extension:txt vless",
        "clash subscription", "nekobox config",
    ],
}

def get_keywords_for_shard(shard_id: int) -> list:
    """获取指定分片的关键词"""
    return KEYWORD_SHARDS.get(shard_id % 8, [])
