#!/usr/bin/env python3
"""
Sync OCR results from local storage to B2.

ETL Pipeline Step 3: Upload to B2
- Input: Local OCR results (data/analytics/ocr/)
- Processing: Upload to B2 analytics bucket
- Output: Results available via B2 for downstream processing

Usage:
  python sync_ocr_to_b2.py --job-id latest
  python sync_ocr_to_b2.py --job-id 2025-12-31_21-29-56 --skip-images
"""

import argparse
import json
import logging
import os
from pathlib import Path
from typing import Optional

import boto3
from dotenv import load_dotenv
from tqdm import tqdm

from etl_metadata import update_central_registry

# Load environment variables
load_dotenv()

# ============================================================
# Logging Setup
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

# ============================================================
# B2 Configuration
# ============================================================

B2_ANALYTICS_ACCESS_KEY_ID = os.getenv("B2_ANALYTICS_ACCESS_KEY_ID")
B2_ANALYTICS_SECRET_ACCESS_KEY = os.getenv("B2_ANALYTICS_SECRET_ACCESS_KEY")
B2_ANALYTICS_REGION = os.getenv("B2_ANALYTICS_REGION")
B2_ANALYTICS_ENDPOINT_URL = os.getenv("B2_ANALYTICS_ENDPOINT_URL")
B2_ANALYTICS_BUCKET = os.getenv("B2_ANALYTICS_BUCKET")


def get_b2_client():
    """Initialize B2 S3 client."""
    return boto3.client(
        "s3",
        aws_access_key_id=B2_ANALYTICS_ACCESS_KEY_ID,
        aws_secret_access_key=B2_ANALYTICS_SECRET_ACCESS_KEY,
        region_name=B2_ANALYTICS_REGION,
        endpoint_url=B2_ANALYTICS_ENDPOINT_URL,
    )


def resolve_job_id(job_id: str, local_dir: Path) -> str:
    """
    Resolve job_id, handling "latest" symlink.
    
    If job_id is "latest", resolves the metadata latest.json symlink to get
    the real job_id (timestamp). This is more reliable than checking individual
    citekey directories since single-file runs only update one citekey.
    
    Args:
        job_id: Job ID or "latest"
        local_dir: Local OCR results directory
    
    Returns:
        Real job_id (timestamp string, e.g., "2025-12-31_21-29-56")
    
    Raises:
        ValueError: If job_id cannot be resolved
    """
    if job_id != "latest":
        return job_id
    
    # Check metadata latest.json symlink (most reliable - always updated)
    metadata_dir = local_dir / "job_metadata"
    latest_metadata_link = metadata_dir / "latest.json"
    
    if latest_metadata_link.is_symlink():
        # Symlink target is like "2025-12-31_21-29-56.json"
        target_name = latest_metadata_link.resolve().name
        # Remove .json extension to get job_id
        real_job_id = target_name.replace(".json", "")
        logger.info(f"Resolved 'latest.json' symlink → {real_job_id}")
        return real_job_id
    
    # Fallback: find most recent metadata file
    if metadata_dir.exists():
        metadata_files = sorted(metadata_dir.glob("*.json"))
        # Filter out "latest.json" if it exists as a regular file
        metadata_files = [f for f in metadata_files if f.name != "latest.json"]
        
        if metadata_files:
            real_job_id = metadata_files[-1].stem  # filename without extension
            logger.info(f"No 'latest.json' symlink found, using most recent metadata: {real_job_id}")
            return real_job_id
    
    # Final fallback: check citekey directories
    citekey_dirs = [d for d in local_dir.iterdir() 
                    if d.is_dir() and d.name != "job_metadata"]
    
    if not citekey_dirs:
        raise ValueError("No citekey directories or metadata files found to resolve 'latest'")
    
    first_citekey_dir = citekey_dirs[0]
    latest_link = first_citekey_dir / "latest"
    
    if latest_link.is_symlink():
        real_job_id = latest_link.resolve().name
        logger.info(f"Resolved citekey 'latest' symlink → {real_job_id}")
        return real_job_id
    
    # Last resort: use most recent job directory
    job_dirs = [d for d in first_citekey_dir.iterdir() 
               if d.is_dir() and d.name != "latest"]
    
    if not job_dirs:
        raise ValueError(f"No job directories found in {first_citekey_dir}")
    
    real_job_id = sorted(job_dirs)[-1].name
    logger.info(f"No symlinks found, using most recent directory: {real_job_id}")
    return real_job_id


def sync_job_to_b2(job_id: str, local_dir: Path, skip_images: bool = False, quiet: bool = False):
    """
    Sync OCR results to B2.
    
    If job_id is "latest", resolves the symlink to get the real job_id.
    
    Args:
        job_id: Job ID (timestamp) or "latest" to sync latest job
        local_dir: Local OCR results directory (e.g., data/analytics/ocr)
        skip_images: Skip uploading image files
        quiet: Suppress progress output
    """
    s3_client = get_b2_client()
    
    # Resolve job_id (handles "latest")
    try:
        real_job_id = resolve_job_id(job_id, local_dir)
    except ValueError as e:
        logger.error(f"Failed to resolve job_id: {e}")
        return
    
    # Sync job metadata
    metadata_dir = local_dir / "job_metadata"
    if metadata_dir.exists():
        metadata_file = metadata_dir / f"{real_job_id}.json"
        if metadata_file.exists():
            s3_key = f"ocr/job_metadata/{real_job_id}.json"
            try:
                s3_client.upload_file(str(metadata_file), B2_ANALYTICS_BUCKET, s3_key)
                logger.info(f"✓ Uploaded {s3_key}")
            except Exception as e:
                logger.error(f"Failed to upload {s3_key}: {e}")
        else:
            logger.warning(f"Job metadata not found: {metadata_file}")
    else:
        logger.warning(f"Job metadata directory not found: {metadata_dir}")
    
    # Find all citekey directories
    citekey_dirs = [d for d in local_dir.iterdir() 
                    if d.is_dir() and d.name != "job_metadata"]
    
    if not citekey_dirs:
        logger.warning(f"No citekey directories found in {local_dir}")
        return
    
    # Sync results for each citekey
    total_uploaded = 0
    total_skipped = 0
    
    for citekey_dir in tqdm(citekey_dirs, unit="citekey", disable=quiet, desc="Syncing citekeys"):
        citekey = citekey_dir.name
        result_dir = citekey_dir / real_job_id
        
        if not result_dir.exists():
            logger.debug(f"Result directory not found for {citekey}: {result_dir}")
            total_skipped += 1
            continue
        
        # Upload OCR result JSON
        json_file = result_dir / f"{citekey}.json"
        if json_file.exists():
            s3_key = f"ocr/{citekey}/{real_job_id}/{citekey}.json"
            try:
                s3_client.upload_file(str(json_file), B2_ANALYTICS_BUCKET, s3_key)
                total_uploaded += 1
                logger.debug(f"✓ {s3_key}")
            except Exception as e:
                logger.error(f"Failed to upload {s3_key}: {e}")
        
        # Upload images (unless skipped)
        if not skip_images:
            for img_dir in result_dir.glob("page_*_images"):
                for img_file in img_dir.glob("*.png"):
                    s3_key = f"ocr/{citekey}/{real_job_id}/{img_dir.name}/{img_file.name}"
                    try:
                        s3_client.upload_file(str(img_file), B2_ANALYTICS_BUCKET, s3_key)
                        total_uploaded += 1
                        logger.debug(f"✓ {s3_key}")
                    except Exception as e:
                        logger.error(f"Failed to upload {s3_key}: {e}")
    
    logger.info(f"✓ Sync complete: {total_uploaded} uploaded, {total_skipped} skipped")
    
    # Update central registry
    sync_metadata = {
        "status": "completed",
        "files_uploaded": total_uploaded,
        "files_skipped": total_skipped,
        "b2_bucket": B2_ANALYTICS_BUCKET,
    }
    update_central_registry("sync_ocr", real_job_id, sync_metadata)
    logger.info(f"Updated central registry with sync_ocr step for job {real_job_id}")


def main():
    parser = argparse.ArgumentParser(
        description="Sync OCR results to B2",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Sync latest results for all citekeys
  python sync_ocr_to_b2.py --job-id latest
  
  # Sync specific job ID
  python sync_ocr_to_b2.py --job-id 2025-12-31_21-29-56
  
  # Sync without images (faster)
  python sync_ocr_to_b2.py --job-id latest --skip-images
  
  # Quiet mode
  python sync_ocr_to_b2.py --job-id latest --quiet
        """
    )
    
    parser.add_argument(
        "--job-id",
        default="latest",
        help="Job ID to sync (e.g., 2025-12-31_21-29-56) or 'latest' (default: latest)"
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=Path("data/analytics/ocr"),
        help="Local OCR results directory (default: data/analytics/ocr)"
    )
    parser.add_argument(
        "--skip-images",
        action="store_true",
        help="Skip uploading image files (faster)"
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Suppress progress output"
    )
    
    args = parser.parse_args()
    
    # Validate input directory
    if not args.input.exists():
        logger.error(f"Input directory not found: {args.input}")
        return
    
    # Print summary
    print("\n" + "=" * 70)
    print("📤 B2 OCR Results Sync")
    print("=" * 70)
    print(f"  Job ID: {args.job_id}")
    print(f"  Local dir: {args.input}")
    print(f"  B2 bucket: {B2_ANALYTICS_BUCKET}")
    print(f"  Skip images: {args.skip_images}")
    print("=" * 70 + "\n")
    
    # Sync to B2
    sync_job_to_b2(args.job_id, args.input, args.skip_images, args.quiet)


if __name__ == "__main__":
    main()