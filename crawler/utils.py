#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Utility Functions"""

import logging
import json
import chardet
from pathlib import Path
from datetime import datetime

def setup_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    
    if logger.handlers:
        return logger
    
    # Console handler
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    
    # File handler
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    fh = logging.FileHandler(log_dir / f"crawler_{datetime.now().strftime('%Y%m%d')}.log")
    fh.setLevel(logging.DEBUG)
    
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    fh.setFormatter(formatter)
    
    logger.addHandler(ch)
    logger.addHandler(fh)
    
    return logger

def detect_encoding(content: bytes) -> str:
    """Detect text encoding"""
    try:
        result = chardet.detect(content)
        return result.get('encoding', 'utf-8') or 'utf-8'
    except:
        return 'utf-8'

def save_to_file(filepath: Path, content: str):
    filepath.parent.mkdir(parents=True, exist_ok=True)
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(content)

def load_text_file(filepath: Path) -> str:
    with open(filepath, 'rb') as f:
        raw = f.read()
    encoding = detect_encoding(raw)
    return raw.decode(encoding, errors='ignore')
