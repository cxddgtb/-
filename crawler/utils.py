#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Utility Functions
"""

import logging
import json
from pathlib import Path
from datetime import datetime

def setup_logger(name: str) -> logging.Logger:
    """Setup logger with console and file handlers"""
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    
    # Console handler
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    
    # File handler
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    fh = logging.FileHandler(log_dir / f"crawler_{datetime.now().strftime('%Y%m%d')}.log")
    fh.setLevel(logging.DEBUG)
    
    # Formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    ch.setFormatter(formatter)
    fh.setFormatter(formatter)
    
    logger.addHandler(ch)
    logger.addHandler(fh)
    
    return logger

def save_to_file(filepath: Path, content: str):
    """Save content to file"""
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(content)
    print(f"Saved to {filepath}")

def load_from_file(filepath: Path) -> str:
    """Load content from file"""
    with open(filepath, 'r', encoding='utf-8') as f:
        return f.read()

def encode_base64(text: str) -> str:
    """Encode text to base64"""
    import base64
    return base64.b64encode(text.encode('utf-8')).decode('utf-8')

def decode_base64(text: str) -> str:
    """Decode base64 text"""
    import base64
    return base64.b64decode(text.encode('utf-8')).decode('utf-8')
