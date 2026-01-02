#!/usr/bin/env python3
"""
One-time patch to save OCR metadata and create symlinks for existing results.

This script processes completed OCR results and:
1. Saves metadata to job_metadata directories
2. Updates central registry
3. Creates latest symlinks

Usage:
  python patch_ocr_metadata.py --job-id 2026-01-02_00-33-11
"""
import json
import logging
from pathlib import Path
from datetime import datetime
import argparse
import sys

# Add parent directory to path to import etl_metadata
sys.path.insert(0, str(Path(__file__).parent))

from etl_metadata import save_step_metadata

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)

WORKSPACE_ROOT = Path(__file__).parent.parent
OCR_OUTPUT_DIR = WORKSPACE_ROOT / "data" / "analytics" / "ocr"

def patch_ocr_job(job_id: str, source_job_id: str = None):
    """
    Patch a single OCR job: save metadata and create symlinks.
    
    Args:
        job_id: OCR job ID (e.g., "2026-01-02_00-33-11")
        source_job_id: Source upload job ID (optional, will be inferred if not provided)
    """
    logger.info(f"Patching OCR job: {job_id}")
    
    # Find all citekeys with this job_id
    citekeys_processed = []
    for citekey_dir in OCR_OUTPUT_DIR.iterdir():
        if not citekey_dir.is_dir():
            continue
        
        job_dir = citekey_dir / job_id
        if not job_dir.exists():
            continue
        
        citekey = citekey_dir.name
        citekeys_processed.append(citekey)
        
        # Load individual citekey metadata file
        metadata_file = job_dir / f"{citekey}.json"
        if not metadata_file.exists():
            logger.warning(f"  {citekey}: No metadata file found at {metadata_file}")
            continue
        
        try:
            with metadata_file.open("r", encoding="utf-8") as f:
                citekey_metadata = json.load(f)
            logger.info(f"  {citekey}: Loaded metadata")
        except Exception as e:
            logger.error(f"  {citekey}: Failed to load metadata: {e}")
            continue
        
        # Create latest symlink for this citekey
        latest_link = citekey_dir / "latest"
        try:
            if latest_link.exists() or latest_link.is_symlink():
                latest_link.unlink()
            latest_link.symlink_to(job_dir)
            logger.info(f"  {citekey}: Created 'latest' symlink")
        except Exception as e:
            logger.error(f"  {citekey}: Failed to create symlink: {e}")
    
    if not citekeys_processed:
        logger.warning(f"No citekeys found for job {job_id}")
        return False
    
    logger.info(f"Processed {len(citekeys_processed)} citekeys: {', '.join(citekeys_processed)}")
    
    # Save to job_metadata and update central registry
    try:
        metadata = {
            "status": "completed",
            "source_job_id": source_job_id,
            "citekeys": {
                "total": len(citekeys_processed),
                "successful": len(citekeys_processed),
                "failed": 0,
                "list": citekeys_processed
            }
        }
        
        save_step_metadata("ocr", job_id, metadata)
        logger.info(f"Saved OCR step metadata for job {job_id}")
        
    except Exception as e:
        logger.error(f"Failed to save metadata: {e}")
        return False
    
    return True


def main():
    parser = argparse.ArgumentParser(
        description="Patch OCR metadata for existing results"
    )
    parser.add_argument(
        "--job-id",
        required=True,
        help="OCR job ID (e.g., 2026-01-02_00-33-11)"
    )
    parser.add_argument(
        "--source-job-id",
        help="Source upload job ID (optional)"
    )
    
    args = parser.parse_args()
    
    success = patch_ocr_job(args.job_id, args.source_job_id)
    
    if success:
        logger.info("✓ Patching complete")
    else:
        logger.error("✗ Patching failed")
        exit(1)


if __name__ == "__main__":
    main()
