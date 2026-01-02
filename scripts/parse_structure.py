#!/usr/bin/env python3
"""
Parse document structure from OCR results (parse_structure ETL step).

This script analyzes OCR JSON output to detect:
1. Body pagination: Identifies real page numbers and offsets
2. Table of Contents (TOC): Detects TOC boundaries with inference

Job ID Lineage:
  upload → run_ocr → sync_ocr (source) → parse_structure (this step)

Usage:
  # Process specific citekeys
  python parse_structure.py --citekeys dagz_v01 dagz_v02 \\
    --source-job-id 2026-01-02_00-58-42
  
  # Use wildcard patterns
  python parse_structure.py --pattern "dagz_v*" \\
    --source-job-id 2026-01-02_00-58-42
  
  # Resume failed items
  python parse_structure.py --resume-from 2026-01-02_11-32-58 \\
    --source-job-id 2026-01-02_00-58-42
  
  # Dry run preview
  python parse_structure.py --pattern "dagz_v*" \\
    --source-job-id 2026-01-02_00-58-42 --dry-run
  
  # Force rerun
  python parse_structure.py --citekeys dagz_v01 \\
    --source-job-id 2026-01-02_00-58-42 --force-rerun

⚠️  IMPORTANT: Always use explicit --source-job-id (sync_ocr job), never "latest"

Output Structure:
  data/analytics/parse_structure/
  ├── {citekey}/
  │   ├── {parse_job_id}/
  │   │   └── {citekey}.json          [Parse structure output]
  │   └── latest/ → {parse_job_id}    [Symlink to latest (safe for reading)]
  └── job_metadata/
      └── {parse_job_id}.json         [Job metadata with quality summary]

Central Registry:
  data/analytics/job_registry/{parse_job_id}.json
"""

import json
import pandas as pd
import numpy as np
from pathlib import Path
from dataclasses import dataclass
from typing import List, Tuple, Optional, Union, Dict
from datetime import datetime
import argparse
import sys
import logging
import re
from difflib import SequenceMatcher

from etl_metadata import (
    save_step_metadata, validate_job_id, get_failed_citekeys,
    expand_citekey_patterns, preview_pipeline_run, print_dry_run_summary,
    get_step_metadata, filter_citekeys_to_process
)

# ============================================================================
# LOGGING
# ============================================================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ============================================================================
# CONSTANTS
# ============================================================================

ANALYTICS_ROOT = Path(__file__).parent.parent / "data" / "analytics"
ANALYTICS_ROOT.mkdir(parents=True, exist_ok=True)

TOC_KEYWORDS = ['目录', '目錄', 'contents', 'table of contents', '目次', '目索']

# ============================================================================
# DATA CLASSES
# ============================================================================

@dataclass
class PageMapping:
    """Represents pagination mapping for document body"""
    offset: int
    body_start_page: int
    body_end_page: int
    sequence_length: int
    consistency_score: float
    position_consistency: float
    confidence: float
    gaps: List[int]
    details: dict

@dataclass
class TOCMapping:
    """Represents identified Table of Contents boundary"""
    toc_first_page: int
    toc_last_page: int
    toc_first_pdf_page: int
    toc_last_pdf_page: int
    toc_offset: int
    confidence: float
    keyword_found: bool
    keyword_position: Optional[int]
    detection_method: str
    toc_start_detection_method: str
    toc_start_confidence: float
    inferred_flags: List[str]
    details: dict

# ============================================================================
# PAGINATION DETECTION FUNCTIONS
# ============================================================================

def extract_page_numbers(ocr_json_path: str) -> pd.DataFrame:
    """Extract all number blocks from OCR JSON."""
    with open(ocr_json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    page_numbers = []
    
    for page in data.get("pages", []):
        pdf_page = page["page"]
        page_height = page.get("json", {}).get("res", {}).get("height", 1152)
        parsing_res = page.get("json", {}).get("res", {}).get("parsing_res_list", [])
        
        if not parsing_res:
            continue
        
        # Strategy 1: Explicit "number" blocks
        number_blocks = [block for block in parsing_res 
                        if block.get("block_label") == "number"]
        
        # Strategy 2: Edge blocks if no numbers found
        if not number_blocks:
            edge_blocks = [parsing_res[0], parsing_res[-1]]
            number_blocks = [b for b in edge_blocks 
                           if _is_likely_page_number(b.get("block_content", ""))]
        
        for block in number_blocks:
            num_str = block.get("block_content", "").strip()
            real_page_num = _parse_page_number(num_str)
            
            if real_page_num is None:
                continue
            
            bbox = block.get("block_bbox", [])
            position = _determine_position(bbox, page_height)
            is_edge_element = block.get("block_id") in [0, len(parsing_res)-1]
            
            page_numbers.append({
                "pdf_page": pdf_page,
                "real_page": real_page_num,
                "bbox": bbox,
                "position": position,
                "is_edge_element": is_edge_element,
                "block_content": num_str,
                "block_id": block.get("block_id"),
                "block_label": block.get("block_label"),
                "confidence_detection": 0.9 if block.get("block_label") == "number" else 0.6
            })
    
    return pd.DataFrame(page_numbers)

def _parse_page_number(text: str) -> Optional[int]:
    """Parse various page number formats."""
    text = text.strip()
    
    if text.isdigit():
        return int(text)
    
    if all(c.lower() in 'ivxlc' for c in text):
        try:
            return _roman_to_int(text)
        except:
            return None
    
    match = re.search(r'\d+', text)
    if match:
        return int(match.group())
    
    return None

def _roman_to_int(s: str) -> int:
    """Convert Roman numerals to integers."""
    roman_map = {'i': 1, 'v': 5, 'x': 10, 'l': 50, 'c': 100}
    total = 0
    prev = 0
    for char in reversed(s.lower()):
        val = roman_map.get(char, 0)
        if val < prev:
            total -= val
        else:
            total += val
        prev = val
    return total

def _is_likely_page_number(text: str) -> bool:
    """Heuristic: is this block likely a page number?"""
    text = text.strip()
    return (text.isdigit() or 
            all(c.lower() in 'ivxlc' for c in text) or
            len(text) <= 3)

def _determine_position(bbox, page_height):
    """Determine if number is in header, footer, or middle."""
    if not bbox or len(bbox) < 4:
        return "unknown"
    
    y_center = (bbox[1] + bbox[3]) / 2
    y_ratio = y_center / page_height if page_height > 0 else 0.5
    
    if y_ratio < 0.15:
        return "header"
    elif y_ratio > 0.85:
        return "footer"
    else:
        return "middle"

def identify_sequences(df: pd.DataFrame) -> List[PageMapping]:
    """Identify all valid continuous sequences of page numbers."""
    if df.empty:
        return []
    
    sequences = []
    offsets = df['real_page'].subtract(df['pdf_page']).unique()
    
    for offset in sorted(offsets):
        matching = df[df['real_page'] - df['pdf_page'] == offset].copy()
        
        if len(matching) < 3:
            continue
        
        real_pages = sorted(matching['real_page'].unique())
        ranges = _find_continuous_ranges(real_pages, max_gap=5)
        
        for start_page, end_page, gap_pages in ranges:
            if end_page - start_page < 2:
                continue
            
            seq_data = matching[(matching['real_page'] >= start_page) & 
                               (matching['real_page'] <= end_page)]
            
            consistency = _calculate_consistency(seq_data, offset)
            pos_consistency = _calculate_position_consistency(seq_data)
            
            confidence = (
                0.4 * (len(seq_data) / 20) +
                0.3 * consistency +
                0.2 * pos_consistency +
                0.1 * (seq_data['confidence_detection'].mean())
            )
            confidence = min(1.0, confidence)
            
            sequences.append(PageMapping(
                offset=int(offset),
                body_start_page=start_page,
                body_end_page=end_page,
                sequence_length=len(seq_data),
                consistency_score=consistency,
                position_consistency=pos_consistency,
                confidence=confidence,
                gaps=gap_pages,
                details={
                    "num_detections": len(seq_data),
                    "pdf_range": (seq_data['pdf_page'].min(), seq_data['pdf_page'].max()),
                    "edge_elements_ratio": seq_data['is_edge_element'].mean(),
                    "positions": seq_data['position'].value_counts().to_dict()
                }
            ))
    
    sequences.sort(key=lambda x: x.confidence, reverse=True)
    return sequences

def _find_continuous_ranges(pages: List[int], max_gap: int = 5) -> List[Tuple[int, int, List[int]]]:
    """Find continuous ranges allowing small gaps."""
    if not pages:
        return []
    
    ranges = []
    start = pages[0]
    end = pages[0]
    gaps = []
    
    for i in range(1, len(pages)):
        if pages[i] - pages[i-1] <= max_gap + 1:
            if pages[i] - pages[i-1] > 1:
                gaps.extend(range(pages[i-1] + 1, pages[i]))
            end = pages[i]
        else:
            ranges.append((start, end, gaps))
            start = pages[i]
            end = pages[i]
            gaps = []
    
    ranges.append((start, end, gaps))
    return ranges

def _calculate_consistency(df: pd.DataFrame, offset: int) -> float:
    """Measure consistency of offset across pages."""
    if df.empty:
        return 0.0
    
    actual_offsets = df['real_page'] - df['pdf_page']
    std_dev = actual_offsets.std()
    consistency = 1.0 / (1.0 + std_dev)
    return min(1.0, consistency)

def _calculate_position_consistency(df: pd.DataFrame) -> float:
    """Measure consistency of page number positioning."""
    if df.empty:
        return 0.0
    
    positions = df['position'].value_counts()
    dominant_position = positions.iloc[0]
    return dominant_position / len(df)

def analyze_pagination(ocr_json_path: str) -> Tuple[Optional[PageMapping], List[PageMapping]]:
    """Analyze body pagination: extract, identify sequences, rank by confidence."""
    df = extract_page_numbers(ocr_json_path)
    
    if df.empty:
        return None, []
    
    sequences = identify_sequences(df)
    
    if not sequences:
        return None, []
    
    best = sequences[0]
    return best, sequences

# ============================================================================
# TOC DETECTION FUNCTIONS
# ============================================================================

def identify_toc(ocr_json_path: str, body_mapping: PageMapping) -> Optional[TOCMapping]:
    """
    Identify Table of Contents using keyword detection and pagination analysis.
    Handles inference when OCR misses page numbers on keyword pages.
    """
    if body_mapping is None:
        return None
    
    with open(ocr_json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    # ========================================================================
    # STRATEGY A: Keyword Detection
    # ========================================================================
    keyword_matches = []
    
    for page in data.get("pages", []):
        pdf_page = page["page"]
        parsing_res = page.get("json", {}).get("res", {}).get("parsing_res_list", [])
        
        for block in parsing_res:
            block_content = block.get("block_content", "").strip().lower()
            block_label = block.get("block_label", "")
            
            for keyword in TOC_KEYWORDS:
                if keyword.lower() in block_content:
                    keyword_matches.append({
                        "pdf_page": pdf_page,
                        "block_label": block_label,
                        "confidence": 0.95 if block_label in ["paragraph_title", "heading"] else 0.7,
                        "content": block.get("block_content", "")
                    })
                    break
                
                if _fuzzy_match(keyword, block_content, threshold=0.8):
                    keyword_matches.append({
                        "pdf_page": pdf_page,
                        "block_label": block_label,
                        "confidence": 0.75,
                        "content": block.get("block_content", "")
                    })
    
    # ========================================================================
    # STRATEGY B: Pagination Analysis (Different Offset)
    # ========================================================================
    df = extract_page_numbers(ocr_json_path)
    toc_boundaries_diff_offset = []
    
    if not df.empty:
        body_start_pdf = body_mapping.details["pdf_range"][0]
        early_pages = df[df['pdf_page'] < body_start_pdf]
        
        if not early_pages.empty:
            body_offset = body_mapping.offset
            early_offsets = early_pages['real_page'].subtract(early_pages['pdf_page']).unique()
            
            for offset in early_offsets:
                if offset != body_offset:
                    toc_data = early_pages[early_pages['real_page'] - early_pages['pdf_page'] == offset]
                    real_pages = sorted(toc_data['real_page'].unique())
                    ranges = _find_continuous_ranges(real_pages, max_gap=2)
                    
                    for start_page, end_page, gaps in ranges:
                        if end_page - start_page < 2:
                            continue
                        
                        toc_seq_data = toc_data[
                            (toc_data['real_page'] >= start_page) & 
                            (toc_data['real_page'] <= end_page)
                        ]
                        
                        consistency = _calculate_consistency(toc_seq_data, offset)
                        toc_length = end_page - start_page + 1
                        length_score = 1.0 if 3 <= toc_length <= 100 else 0.6
                        
                        confidence = 0.5 * consistency + 0.5 * length_score
                        
                        # Check if keyword was found before first detected page
                        inferred_flags = []
                        toc_start_detection_method = "page_number_detected"
                        toc_start_confidence = 1.0
                        toc_first_page = start_page
                        toc_first_pdf_page = toc_seq_data['pdf_page'].min()
                        
                        if keyword_matches:
                            keyword_pdf = keyword_matches[0]["pdf_page"]
                            if keyword_pdf < toc_first_pdf_page:
                                inferred_first_page = keyword_pdf + offset
                                inferred_first_page = max(1, inferred_first_page)
                                
                                inferred_flags = ["missing_page_number_on_keyword_page_with_different_offset"]
                                toc_start_detection_method = "keyword_inferred"
                                toc_start_confidence = 0.85
                                toc_first_page = inferred_first_page
                                toc_first_pdf_page = keyword_pdf
                                
                                confidence = confidence * 0.98
                        
                        toc_boundaries_diff_offset.append({
                            "toc_first_page": toc_first_page,
                            "toc_last_page": end_page,
                            "toc_first_pdf_page": toc_first_pdf_page,
                            "toc_last_pdf_page": toc_seq_data['pdf_page'].max(),
                            "toc_offset": int(offset),
                            "confidence": min(1.0, confidence),
                            "detection_method": "pagination_diff_offset",
                            "toc_start_detection_method": toc_start_detection_method,
                            "toc_start_confidence": toc_start_confidence,
                            "inferred_flags": inferred_flags
                        })
    
    # ========================================================================
    # STRATEGY C: Same-Offset TOC (Keyword-based)
    # ========================================================================
    toc_boundaries_same_offset = []
    
    if keyword_matches:
        keyword_pdf = keyword_matches[0]["pdf_page"]
        body_start_pdf = body_mapping.details["pdf_range"][0]
        
        if keyword_pdf < body_start_pdf:
            df_keyword_page = df[df['pdf_page'] == keyword_pdf] if not df.empty else pd.DataFrame()
            page_number_on_keyword_page = not df_keyword_page.empty
            
            df_after_keyword = df[df['pdf_page'] >= keyword_pdf] if not df.empty else pd.DataFrame()
            
            inferred_flags = []
            toc_start_confidence = 1.0
            toc_start_detection_method = "page_number_detected"
            
            if not page_number_on_keyword_page and not df_after_keyword.empty:
                first_detected_pdf = df_after_keyword['pdf_page'].min()
                first_detected_real = df_after_keyword[df_after_keyword['pdf_page'] == first_detected_pdf]['real_page'].iloc[0]
                
                if first_detected_pdf == keyword_pdf + 1:
                    toc_first_page = first_detected_real - 1
                    toc_start_detection_method = "keyword_inferred"
                    toc_start_confidence = 0.85
                    inferred_flags = ["missing_page_number_on_keyword_page"]
                else:
                    toc_first_page = keyword_pdf + body_mapping.offset
            else:
                toc_first_page = keyword_pdf + body_mapping.offset
            
            toc_end_pdf = body_start_pdf - 1
            toc_last_page = toc_end_pdf + body_mapping.offset
            
            toc_length = toc_end_pdf - keyword_pdf + 1
            length_score = 1.0 if 3 <= toc_length <= 100 else 0.6
            
            base_confidence = (
                0.5 * keyword_matches[0]["confidence"] +
                0.5 * length_score
            )
            
            final_confidence = base_confidence * 0.95 if inferred_flags else base_confidence
            
            toc_boundaries_same_offset.append({
                "toc_first_page": toc_first_page,
                "toc_last_page": toc_last_page,
                "toc_first_pdf_page": keyword_pdf,
                "toc_last_pdf_page": toc_end_pdf,
                "toc_offset": body_mapping.offset,
                "confidence": min(1.0, final_confidence),
                "detection_method": "keyword_with_same_offset",
                "toc_start_detection_method": toc_start_detection_method,
                "toc_start_confidence": toc_start_confidence,
                "inferred_flags": inferred_flags
            })
    
    # ========================================================================
    # MERGE & SELECT BEST
    # ========================================================================
    all_candidates = toc_boundaries_diff_offset + toc_boundaries_same_offset
    
    if not all_candidates:
        return None
    
    all_candidates.sort(key=lambda x: x["confidence"], reverse=True)
    best = all_candidates[0]
    
    return TOCMapping(
        toc_first_page=best["toc_first_page"],
        toc_last_page=best["toc_last_page"],
        toc_first_pdf_page=best["toc_first_pdf_page"],
        toc_last_pdf_page=best["toc_last_pdf_page"],
        toc_offset=best["toc_offset"],
        confidence=best["confidence"],
        keyword_found=(best["detection_method"] != "pagination_diff_offset"),
        keyword_position=keyword_matches[0]["pdf_page"] if keyword_matches else None,
        detection_method=best["detection_method"],
        toc_start_detection_method=best.get("toc_start_detection_method", "page_number_detected"),
        toc_start_confidence=best.get("toc_start_confidence", 1.0),
        inferred_flags=best.get("inferred_flags", []),
        details=best
    )

def _fuzzy_match(keyword: str, text: str, threshold: float = 0.8) -> bool:
    """Check if keyword appears in text with fuzzy matching."""
    if keyword in text:
        return True
    
    words = text.split()
    for word in words:
        ratio = SequenceMatcher(None, keyword, word).ratio()
        if ratio >= threshold:
            return True
    
    return False

# ============================================================================
# ETL INTEGRATION FUNCTIONS
# ============================================================================

def find_sync_ocr_job_id(ocr_job_id: str) -> Optional[str]:
    """
    Find sync_ocr job_id that processed a given OCR job.
    
    ⚠️  IMPORTANT: ocr_job_id MUST be explicit (e.g., "2026-01-02_00-33-11")
    Never use "latest" or symlinks - they change and break reproducibility.
    """
    if not ocr_job_id or ocr_job_id == "latest":
        logger.error("❌ ocr_job_id must be explicit (e.g., '2026-01-02_00-33-11'), never use 'latest'")
        return None
    
    registry_file = ANALYTICS_ROOT / "job_registry" / f"{ocr_job_id}.json"
    
    if not registry_file.exists():
        logger.warning(f"⚠️  No central registry found for OCR job {ocr_job_id}")
        logger.warning(f"   Expected: {registry_file}")
        return None
    
    with registry_file.open() as f:
        registry = json.load(f)
    
    sync_ocr_info = registry.get("pipeline_steps", {}).get("sync_ocr")
    
    if not sync_ocr_info:
        logger.warning(f"⚠️  No sync_ocr step found in registry for {ocr_job_id}")
        return None
    
    sync_job_id = sync_ocr_info.get("job_id")
    
    if not sync_job_id:
        logger.warning(f"⚠️  sync_ocr entry has no job_id for {ocr_job_id}")
        return None
    
    logger.info(f"✓ Found sync_ocr job_id: {sync_job_id}")
    return sync_job_id

def parse_structure_step(
    input_path: Union[str, Path],
    ocr_job_id: Optional[str] = None,
    sync_job_id: Optional[str] = None,
) -> Tuple[str, Path]:
    """
    Parse document structure from OCR results.
    
    Args:
        input_path: Path to OCR JSON file(s) or directory
        ocr_job_id: Explicit OCR job ID (e.g., "2026-01-02_00-33-11")
                   Used to find corresponding sync_ocr job_id
        sync_job_id: Explicit Sync_OCR job ID (preferred, overrides ocr_job_id lookup)
    
    Returns:
        Tuple of (parse_structure_job_id, output_directory_path)
    
    ⚠️  IMPORTANT: Always use explicit job IDs, never "latest":
       ✅ --ocr-job-id 2026-01-02_00-33-11
       ✅ --sync-job-id 2026-01-02_00-58-42
       ❌ --ocr-job-id latest              (fragile, changes)
       ❌ --sync-job-id latest             (breaks reproducibility)
    """
    
    # Validate job IDs
    if sync_job_id and sync_job_id == "latest":
        logger.error("❌ sync_job_id must be explicit, never use 'latest'")
        raise ValueError("sync_job_id cannot be 'latest' - use explicit job ID like '2026-01-02_00-58-42'")
    
    if ocr_job_id and ocr_job_id == "latest":
        logger.error("❌ ocr_job_id must be explicit, never use 'latest'")
        raise ValueError("ocr_job_id cannot be 'latest' - use explicit job ID like '2026-01-02_00-33-11'")
    
    # Determine source job ID
    if not sync_job_id and ocr_job_id:
        sync_job_id = find_sync_ocr_job_id(ocr_job_id)
        if not sync_job_id:
            logger.error(f"❌ Could not find sync_ocr for {ocr_job_id}")
            raise ValueError(f"Could not find sync_ocr job for OCR job {ocr_job_id}. "
                           f"Please provide explicit --sync-job-id instead.")
    
    if not sync_job_id:
        raise ValueError("Must provide either --sync-job-id or --ocr-job-id to determine source")
    
    # Generate parse_structure job_id
    parse_job_id = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    
    logger.info(f"\n{'='*100}")
    logger.info(f"📋 PARSE_STRUCTURE STEP")
    logger.info(f"{'='*100}")
    logger.info(f"Parse Job ID: {parse_job_id}")
    logger.info(f"Source Job ID (sync_ocr): {sync_job_id}")
    logger.info(f"OCR Job ID: {ocr_job_id}")
    logger.info(f"{'='*100}\n")
    
    # Find OCR files
    input_path = Path(input_path)
    if input_path.is_file():
        ocr_files = [input_path]
    elif input_path.is_dir():
        ocr_files = list(input_path.rglob("*.json"))
    else:
        raise ValueError(f"Input path not found: {input_path}")
    
    # Analyze all documents
    parse_results = {}
    for ocr_file in sorted(ocr_files):
        # Skip metadata files
        if "job_metadata" in str(ocr_file):
            continue
        
        try:
            # Extract citekey from path
            parts = ocr_file.parts
            if "ocr" not in parts:
                continue
            
            ocr_idx = parts.index("ocr")
            if ocr_idx + 2 >= len(parts):
                continue
            
            citekey = parts[ocr_idx + 1]
            
            logger.info(f"Analyzing {citekey}...", end=" ")
            
            # Perform analysis
            body, _ = analyze_pagination(str(ocr_file))
            toc = identify_toc(str(ocr_file), body) if body else None
            
            if body is None:
                logger.info("✗ No body detected")
                continue
            
            # Build parse_structure output
            structure = {
                "citekey": citekey,
                "source_job_id": sync_job_id,
                "parse_structure_job_id": parse_job_id,
                "generated_at": datetime.now().isoformat() + "Z",
                
                "body": {
                    "first_real_page": int(body.body_start_page),
                    "last_real_page": int(body.body_end_page),
                    "first_pdf_page": int(body.details["pdf_range"][0]),
                    "last_pdf_page": int(body.details["pdf_range"][1]),
                    "pdf_page_offset": int(body.offset),
                    "confidence": float(body.confidence),
                    "detection_quality": {
                        "consistency_score": float(body.consistency_score),
                        "position_consistency": float(body.position_consistency),
                        "sequence_length": int(body.sequence_length),
                        "gaps": [int(g) for g in body.gaps]
                    }
                },
                
                "toc": {
                    "first_real_page": int(toc.toc_first_page) if toc else None,
                    "last_real_page": int(toc.toc_last_page) if toc else None,
                    "first_pdf_page": int(toc.toc_first_pdf_page) if toc else None,
                    "last_pdf_page": int(toc.toc_last_pdf_page) if toc else None,
                    "pdf_page_offset": int(toc.toc_offset) if toc else None,
                    "confidence": float(toc.confidence) if toc else None,
                    "detection_quality": {
                        "detection_method": toc.detection_method if toc else None,
                        "start_detection_method": toc.toc_start_detection_method if toc else None,
                        "start_confidence": float(toc.toc_start_confidence) if toc else None,
                        "keyword_found": toc.keyword_found if toc else False,
                        "keyword_pdf_page": int(toc.keyword_position) if toc and toc.keyword_position else None,
                        "inferred_flags": toc.inferred_flags if toc else []
                    }
                } if toc else None,
                
                "metadata": {
                    "total_pdf_pages": "varies",
                    "notes": f"Body: pages {body.body_start_page}–{body.body_end_page} (offset {body.offset:+d})"
                }
            }
            
            parse_results[citekey] = structure
            
            toc_status = f"✓ {toc.toc_first_page}–{toc.toc_last_page}" if toc else "✗"
            logger.info(f"Body {body.body_start_page}–{body.body_end_page} | TOC {toc_status}")
            
        except Exception as e:
            logger.error(f"Error analyzing {ocr_file}: {e}")
            continue
    
    # Save parse_structure outputs
    output_dir = ANALYTICS_ROOT / "parse_structure"
    
    for citekey, structure in parse_results.items():
        citekey_dir = output_dir / citekey / parse_job_id
        citekey_dir.mkdir(parents=True, exist_ok=True)
        
        # Save structure file
        struct_file = citekey_dir / f"{citekey}.json"
        with struct_file.open("w", encoding="utf-8") as f:
            json.dump(structure, f, indent=2, ensure_ascii=False)
        
        # Create symlink to latest (safe for reading, never for source job tracking)
        latest_link = output_dir / citekey / "latest"
        if latest_link.exists() or latest_link.is_symlink():
            latest_link.unlink()
        latest_link.symlink_to(parse_job_id, target_is_directory=True)
    
    # Save job metadata
    job_metadata = {
        "status": "completed",
        "source_job_id": sync_job_id,
        "generated_at": datetime.now().isoformat() + "Z",
        "citekeys": {
            "total": len(parse_results),
            "processed": len(parse_results),
            "failed": 0,
            "list": sorted(parse_results.keys())
        },
        "quality_summary": {
            "high_confidence": {
                "count": sum(1 for s in parse_results.values() if s["body"]["confidence"] >= 0.95),
                "threshold": 0.95
            },
            "with_inferences": {
                "count": sum(1 for s in parse_results.values() 
                           if s["toc"] and s["toc"]["detection_quality"]["inferred_flags"]),
                "citekeys": [c for c, s in parse_results.items() 
                           if s["toc"] and s["toc"]["detection_quality"]["inferred_flags"]]
            }
        },
        "statistics": {
            "avg_body_confidence": float(np.mean([s["body"]["confidence"] for s in parse_results.values()])) if parse_results else None,
            "avg_toc_confidence": float(np.mean([s["toc"]["confidence"] for s in parse_results.values() if s["toc"]])) if any(s["toc"] for s in parse_results.values()) else None,
            "total_pages_analyzed": sum(s["body"]["last_real_page"] - s["body"]["first_real_page"] + 1 
                                       for s in parse_results.values())
        }
    }
    
    metadata_dir = output_dir / "job_metadata"
    metadata_dir.mkdir(parents=True, exist_ok=True)
    metadata_file = metadata_dir / f"{parse_job_id}.json"
    with metadata_file.open("w", encoding="utf-8") as f:
        json.dump(job_metadata, f, indent=2, ensure_ascii=False)
    
    logger.info(f"\n{'='*100}")
    logger.info(f"✅ Parse_structure step complete")
    logger.info(f"{'='*100}")
    logger.info(f"Job ID: {parse_job_id}")
    logger.info(f"Source Job ID (sync_ocr): {sync_job_id}")
    logger.info(f"Documents processed: {len(parse_results)}")
    logger.info(f"Output directory: {output_dir}")
    logger.info(f"Metadata saved: {metadata_file}")
    logger.info(f"Lineage chain: upload → ocr → sync_ocr → parse_structure\n")
    
    return parse_job_id, output_dir

# ============================================================================
# CLI
# ============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Parse document structure from OCR results (ETL Step 4)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Process all citekeys from sync_ocr job (auto-discover)
  python parse_structure.py --source-job-id 2026-01-02_16-13-33
  
  # Process specific citekeys
  python parse_structure.py --citekeys dagz_v01 dagz_v02 \\
    --source-job-id 2026-01-02_00-58-42
  
  # Use wildcard patterns
  python parse_structure.py --pattern "dagz_v*" \\
    --source-job-id 2026-01-02_00-58-42
  
  # Resume failed items
  python parse_structure.py --resume-from 2026-01-02_11-32-58 \\
    --source-job-id 2026-01-02_00-58-42
  
  # Dry run preview
  python parse_structure.py --source-job-id 2026-01-02_16-13-33 --dry-run
  
  # Force rerun with new parameters
  python parse_structure.py --citekeys dagz_v01 \\
    --source-job-id 2026-01-02_00-58-42 --force-rerun

⚠️  IMPORTANT: Always use explicit --source-job-id (sync_ocr job), never "latest"
        """)
    
    # Standardized input group (NOW OPTIONAL)
    input_group = parser.add_mutually_exclusive_group(required=False)
    input_group.add_argument(
        "--citekeys",
        nargs="+",
        help="Explicit list of citekeys to process"
    )
    input_group.add_argument(
        "--pattern",
        nargs="+",
        help="Citekey patterns with wildcards (e.g., 'dagz_v*' 'mzdnp_y200*'). "
             "Only _v* and _y* patterns supported."
    )
    input_group.add_argument(
        "--resume-from",
        type=str,
        help="Resume failed citekeys from previous parse_structure job ID"
    )
    
    # Required source job ID
    parser.add_argument(
        "--source-job-id",
        type=str,
        required=True,
        help="Explicit sync_ocr job ID (e.g., '2026-01-02_00-58-42'). Never use 'latest'."
    )
    
    # Optional flags
    parser.add_argument(
        "--force-rerun",
        action="store_true",
        help="Reprocess even if results already exist"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview what would be processed without actually running"
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose logging"
    )
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Validate source job ID
    try:
        validate_job_id(args.source_job_id, "--source-job-id (sync_ocr)")
        if args.resume_from:
            validate_job_id(args.resume_from, "--resume-from")
    except ValueError as e:
        logger.error(str(e))
        return 1
    
    # Resolve citekeys
    if args.pattern:
        try:
            citekeys = expand_citekey_patterns(
                args.pattern,
                source_job_id=args.source_job_id
            )
            logger.info(f"📋 Expanded patterns to {len(citekeys)} citekeys")
        except ValueError as e:
            logger.error(str(e))
            return 1
    
    elif args.resume_from:
        try:
            citekeys = get_failed_citekeys(
                "parse_structure",
                args.resume_from,
                args.source_job_id,
                ANALYTICS_ROOT / "parse_structure"
            )
            if not citekeys:
                print(f"✓ No failed citekeys to resume from job {args.resume_from}")
                return 0
        except ValueError as e:
            logger.error(str(e))
            return 1
    
    elif args.citekeys:
        citekeys = args.citekeys
    
    else:
        # AUTO-DISCOVER: Load citekeys from sync_ocr job metadata
        logger.info(f"📋 Auto-discovering citekeys from sync_ocr job {args.source_job_id}")
        
        try:
            sync_metadata = get_step_metadata(
                "sync_ocr", 
                args.source_job_id, 
                ANALYTICS_ROOT / "sync_ocr"
            )
            
            if not sync_metadata:
                logger.error(f"❌ Cannot find sync_ocr metadata for {args.source_job_id}")
                logger.error(f"   Expected: {ANALYTICS_ROOT / 'sync_ocr' / 'job_metadata' / f'{args.source_job_id}.json'}")
                return 1
            
            citekeys = sync_metadata.get("citekeys", {}).get("list", [])
            
            if not citekeys:
                logger.error(f"❌ No citekeys found in sync_ocr job {args.source_job_id}")
                return 1
            
            logger.info(f"✓ Found {len(citekeys)} citekeys: {citekeys}")
            
        except Exception as e:
            logger.error(f"❌ Failed to load sync_ocr metadata: {e}")
            return 1
    
    # Dry run preview
    if args.dry_run:
        preview = preview_pipeline_run(
            step_name="parse_structure",
            citekeys=citekeys,
            source_job_id=args.source_job_id,
            force_rerun=args.force_rerun,
            output_dir=ANALYTICS_ROOT / "parse_structure"
        )
        print_dry_run_summary(preview)
        return 0
    
    # Execute
    try:
        # Get OCR job ID from sync metadata
        sync_metadata = get_step_metadata("sync_ocr", args.source_job_id, ANALYTICS_ROOT / "sync_ocr")
        if not sync_metadata:
            logger.error(f"Cannot find sync_ocr metadata for {args.source_job_id}")
            return 1
        
        ocr_job_id = sync_metadata.get("source_job_id")
        if not ocr_job_id:
            logger.error(f"sync_ocr job {args.source_job_id} has no source_job_id")
            return 1
        
        # Generate parse_structure job_id
        parse_job_id = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        output_dir = ANALYTICS_ROOT / "parse_structure"
        
        logger.info(f"\n{'='*100}")
        logger.info(f"📋 PARSE_STRUCTURE STEP")
        logger.info(f"{'='*100}")
        logger.info(f"Parse Job ID: {parse_job_id}")
        logger.info(f"Source Job ID (sync_ocr): {args.source_job_id}")
        logger.info(f"OCR Job ID: {ocr_job_id}")
        logger.info(f"Citekeys: {len(citekeys)}")
        logger.info(f"{'='*100}\n")
        
        # Filter citekeys if not force_rerun
        if not args.force_rerun:
            to_process, skipped = filter_citekeys_to_process(
                "parse_structure",
                citekeys,
                args.source_job_id,
                force_rerun=False,
                output_dir=output_dir
            )
            
            if skipped:
                logger.info(f"⊘ Skipping {len(skipped)} citekeys with existing results")
            
            if not to_process:
                logger.info("✓ All citekeys already processed. Use --force-rerun to reprocess.")
                return 0
            
            citekeys = to_process
        
        # Find OCR files for each citekey
        ocr_base = ANALYTICS_ROOT / "ocr"
        parse_results = {}
        
        for citekey in citekeys:
            ocr_file = ocr_base / citekey / ocr_job_id / f"{citekey}.json"
            
            if not ocr_file.exists():
                logger.warning(f"⚠️  OCR file not found: {ocr_file}")
                continue
            
            try:
                logger.info(f"Analyzing {citekey}...", end=" ")
                
                # Perform analysis
                body, _ = analyze_pagination(str(ocr_file))
                toc = identify_toc(str(ocr_file), body) if body else None
                
                if body is None:
                    logger.info("✗ No body detected")
                    continue
                
                # Build structure output
                structure = {
                    "citekey": citekey,
                    "source_job_id": args.source_job_id,
                    "parse_structure_job_id": parse_job_id,
                    "generated_at": datetime.now().isoformat() + "Z",
                    "body": {
                        "first_real_page": int(body.body_start_page),
                        "last_real_page": int(body.body_end_page),
                        "first_pdf_page": int(body.details["pdf_range"][0]),
                        "last_pdf_page": int(body.details["pdf_range"][1]),
                        "pdf_page_offset": int(body.offset),
                        "confidence": float(body.confidence),
                        "detection_quality": {
                            "consistency_score": float(body.consistency_score),
                            "position_consistency": float(body.position_consistency),
                            "sequence_length": int(body.sequence_length),
                            "gaps": [int(g) for g in body.gaps]
                        }
                    },
                    "toc": {
                        "first_real_page": int(toc.toc_first_page) if toc else None,
                        "last_real_page": int(toc.toc_last_page) if toc else None,
                        "first_pdf_page": int(toc.toc_first_pdf_page) if toc else None,
                        "last_pdf_page": int(toc.toc_last_pdf_page) if toc else None,
                        "pdf_page_offset": int(toc.toc_offset) if toc else None,
                        "confidence": float(toc.confidence) if toc else None,
                        "detection_quality": {
                            "detection_method": toc.detection_method if toc else None,
                            "start_detection_method": toc.toc_start_detection_method if toc else None,
                            "start_confidence": float(toc.toc_start_confidence) if toc else None,
                            "keyword_found": toc.keyword_found if toc else False,
                            "keyword_pdf_page": int(toc.keyword_position) if toc and toc.keyword_position else None,
                            "inferred_flags": toc.inferred_flags if toc else []
                        }
                    } if toc else None,
                    "metadata": {
                        "total_pdf_pages": "varies",
                        "notes": f"Body: pages {body.body_start_page}–{body.body_end_page} (offset {body.offset:+d})"
                    }
                }
                
                parse_results[citekey] = structure
                
                toc_status = f"✓ {toc.toc_first_page}–{toc.toc_last_page}" if toc else "✗"
                logger.info(f"Body {body.body_start_page}–{body.body_end_page} | TOC {toc_status}")
                
            except Exception as e:
                logger.error(f"Error analyzing {citekey}: {e}")
                continue
        
        # Save results
        for citekey, structure in parse_results.items():
            citekey_dir = output_dir / citekey / parse_job_id
            citekey_dir.mkdir(parents=True, exist_ok=True)
            
            struct_file = citekey_dir / f"{citekey}.json"
            with struct_file.open("w", encoding="utf-8") as f:
                json.dump(structure, f, indent=2, ensure_ascii=False)
            
            # Create symlink
            latest_link = output_dir / citekey / "latest"
            if latest_link.exists() or latest_link.is_symlink():
                latest_link.unlink()
            latest_link.symlink_to(parse_job_id, target_is_directory=True)
        
        # Save metadata using etl_metadata
        job_metadata = {
            "status": "completed",
            "source_job_id": args.source_job_id,
            "citekeys": {
                "total": len(citekeys),
                "processed": len(parse_results),
                "failed": len(citekeys) - len(parse_results),
                "list": sorted(citekeys),
                "failed_list": sorted(set(citekeys) - set(parse_results.keys()))
            },
            "quality_summary": {
                "high_confidence": {
                    "count": sum(1 for s in parse_results.values() if s["body"]["confidence"] >= 0.95),
                    "threshold": 0.95
                },
                "with_inferences": {
                    "count": sum(1 for s in parse_results.values() 
                               if s["toc"] and s["toc"]["detection_quality"]["inferred_flags"]),
                    "citekeys": [c for c, s in parse_results.items() 
                               if s["toc"] and s["toc"]["detection_quality"]["inferred_flags"]]
                }
            },
            "statistics": {
                "avg_body_confidence": float(np.mean([s["body"]["confidence"] for s in parse_results.values()])) if parse_results else None,
                "avg_toc_confidence": float(np.mean([s["toc"]["confidence"] for s in parse_results.values() if s["toc"]])) if any(s["toc"] for s in parse_results.values()) else None,
                "total_pages_analyzed": sum(s["body"]["last_real_page"] - s["body"]["first_real_page"] + 1 
                                           for s in parse_results.values())
            }
        }
        
        save_step_metadata("parse_structure", parse_job_id, job_metadata, output_dir=output_dir)
        
        logger.info(f"\n{'='*100}")
        logger.info(f"✅ Parse_structure step complete")
        logger.info(f"Documents processed: {len(parse_results)}/{len(citekeys)}")
        logger.info(f"Output: {output_dir}")
        logger.info(f"{'='*100}\n")
        
        print(f"\n✅ Success!")
        print(f"Parse Job ID: {parse_job_id}")
        print(f"Output: {output_dir}")
        
        return 0
        
    except Exception as e:
        logger.error(f"Failed to run parse_structure: {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(main())