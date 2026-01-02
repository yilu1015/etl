#!/usr/bin/env python3
"""
Sync OCR results from local storage to B2.

ETL Pipeline Step 3: Upload to B2
- Input: Local OCR results (data/analytics/ocr/)
- Processing: Upload to B2 analytics bucket
- Output: Results available via B2 for downstream processing

Lineage Chain:
  upload → run_ocr (source) → sync_ocr (this step) → parse_structure

Usage:
  # Sync entire OCR job
  python sync_ocr.py --source-job-id 2026-01-02_00-33-11
  
  # Sync specific citekeys only
  python sync_ocr.py --citekeys dagz_v01 dagz_v02 \\
    --source-job-id 2026-01-02_00-33-11
  
  # Resume failed syncs
  python sync_ocr.py --resume-from 2026-01-02_00-58-42 \\
    --source-job-id 2026-01-02_00-33-11
  
  # Skip images (faster)
  python sync_ocr.py --source-job-id 2026-01-02_00-33-11 --skip-images

⚠️  IMPORTANT: Always use explicit --source-job-id (OCR job), never "latest"
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

from etl_metadata import (
    save_step_metadata, validate_job_id, get_failed_citekeys, get_step_metadata,
    expand_citekey_patterns, preview_pipeline_run, print_dry_run_summary,
    filter_citekeys_to_process
)

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

ANALYTICS_ROOT = Path(__file__).parent.parent / "data" / "analytics"


def get_b2_client():
    """Initialize B2 S3 client."""
    return boto3.client(
        "s3",
        aws_access_key_id=B2_ANALYTICS_ACCESS_KEY_ID,
        aws_secret_access_key=B2_ANALYTICS_SECRET_ACCESS_KEY,
        region_name=B2_ANALYTICS_REGION,
        endpoint_url=B2_ANALYTICS_ENDPOINT_URL,
    )


def sync_job_to_b2(
    source_job_id: str,
    local_dir: Path,
    citekeys: Optional[List[str]] = None,
    skip_images: bool = False,
    force_rerun: bool = False,
    quiet: bool = False
):
    """
    Sync OCR results to B2.
    
    Args:
        source_job_id: OCR job ID to sync (explicit, no "latest")
        local_dir: Local OCR results directory
        citekeys: Optional list of specific citekeys to sync
        skip_images: Skip uploading image files
        force_rerun: Reupload even if files already exist
        quiet: Suppress progress output
    """
    s3_client = get_b2_client()
    
    # Validate source_job_id
    validate_job_id(source_job_id, "--source-job-id (OCR)")
    
    # Sync job metadata
    metadata_dir = local_dir / "job_metadata"
    if metadata_dir.exists():
        metadata_file = metadata_dir / f"{source_job_id}.json"
        if metadata_file.exists():
            s3_key = f"ocr/job_metadata/{source_job_id}.json"
            try:
                s3_client.upload_file(str(metadata_file), B2_ANALYTICS_BUCKET, s3_key)
                logger.info(f"✓ Uploaded {s3_key}")
            except Exception as e:
                logger.error(f"Failed to upload {s3_key}: {e}")
        else:
            logger.warning(f"Job metadata not found: {metadata_file}")
    else:
        logger.warning(f"Job metadata directory not found: {metadata_dir}")
    
    # Find all citekey directories (or filter by provided list)
    if citekeys:
        citekey_dirs = [local_dir / ck for ck in citekeys if (local_dir / ck).is_dir()]
    else:
        citekey_dirs = [d for d in local_dir.iterdir() 
                        if d.is_dir() and d.name != "job_metadata"]
    
    if not citekey_dirs:
        logger.warning(f"No citekey directories found in {local_dir}")
        return
    
    # Filter citekeys if not force_rerun
    if not force_rerun:
        all_citekeys = [d.name for d in citekey_dirs]
        to_process, skipped = filter_citekeys_to_process(
            "sync_ocr",
            all_citekeys,
            source_job_id,
            force_rerun=False,
            output_dir=local_dir.parent / "sync_ocr"
        )
        
        if skipped:
            logger.info(f"⊘ Skipping {len(skipped)} citekeys with existing results")
            citekey_dirs = [local_dir / ck for ck in to_process]
        
        if not citekey_dirs:
            logger.info("✓ All citekeys already synced. Use --force-rerun to re-upload.")
            return
    
    # Sync results for each citekey
    total_uploaded = 0
    total_skipped = 0
    
    for citekey_dir in tqdm(citekey_dirs, unit="citekey", disable=quiet, desc="Syncing citekeys"):
        citekey = citekey_dir.name
        result_dir = citekey_dir / source_job_id
        
        if not result_dir.exists():
            logger.debug(f"Result directory not found for {citekey}: {result_dir}")
            total_skipped += 1
            continue
        
        # Upload OCR result JSON
        json_file = result_dir / f"{citekey}.json"
        if json_file.exists():
            s3_key = f"ocr/{citekey}/{source_job_id}/{citekey}.json"
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
                    s3_key = f"ocr/{citekey}/{source_job_id}/{img_dir.name}/{img_file.name}"
                    try:
                        s3_client.upload_file(str(img_file), B2_ANALYTICS_BUCKET, s3_key)
                        total_uploaded += 1
                        logger.debug(f"✓ {s3_key}")
                    except Exception as e:
                        logger.error(f"Failed to upload {s3_key}: {e}")
    
    logger.info(f"✓ Sync complete: {total_uploaded} uploaded, {total_skipped} skipped")
    
    # Save sync_ocr metadata
    from datetime import datetime
    sync_job_id = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    
    sync_metadata = {
        "status": "completed",
        "source_job_id": source_job_id,  # OCR job ID
        "files_uploaded": total_uploaded,
        "files_skipped": total_skipped,
        "b2_bucket": B2_ANALYTICS_BUCKET,
        "citekeys": {
            "total": len(citekey_dirs),
            "successful": len(citekey_dirs) - total_skipped,
            "failed": total_skipped,
            "list": sorted([d.name for d in citekey_dirs])
        }
    }
    
    save_step_metadata("sync_ocr", sync_job_id, sync_metadata, output_dir=ANALYTICS_ROOT / "sync_ocr")
    logger.info(f"Saved sync_ocr metadata with job_id: {sync_job_id}")


def main():
    parser = argparse.ArgumentParser(
        description="Sync OCR results to B2 (ETL Step 3: Sync)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Sync entire OCR job
  python sync_ocr.py --source-job-id 2026-01-02_00-33-11
  
  # Sync specific citekeys only
  python sync_ocr.py --citekeys dagz_v01 dagz_v02 \\
    --source-job-id 2026-01-02_00-33-11
  
  # Resume failed syncs
  python sync_ocr.py --resume-from 2026-01-02_00-58-42 \\
    --source-job-id 2026-01-02_00-33-11
  
  # Skip images (faster)
  python sync_ocr.py --source-job-id 2026-01-02_00-33-11 --skip-images
        """
    )
    
    # Standardized input group
    input_group = parser.add_mutually_exclusive_group()
    input_group.add_argument(
        "--citekeys",
        nargs="+",
        help="Explicit list of citekeys to sync"
    )
    input_group.add_argument(
        "--resume-from",
        type=str,
        help="Resume failed citekeys from previous sync_ocr job ID"
    )
    input_group.add_argument(
        "--pattern",
        nargs="+",
        help="Citekey patterns with wildcards (e.g., 'dagz_v*' 'mzdnp_y200*'). "
             "Only _v* and _y* patterns supported."
    )
    
    parser.add_argument(
        "--source-job-id",
        type=str,
        required=True,
        help="Explicit OCR job ID to sync (e.g., '2026-01-02_00-33-11'). Never use 'latest'."
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
    parser.add_argument(
        "--force-rerun",
        action="store_true",
        help="Reupload even if files already exist in B2"
    )
    
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview what would be synced without actually uploading"
    )
    
    args = parser.parse_args()
    
    # Validate input directory
    if not args.input.exists():
        logger.error(f"Input directory not found: {args.input}")
        return
    
    # Validate job IDs
    try:
        validate_job_id(args.source_job_id, "--source-job-id (OCR)")
        if args.resume_from:
            validate_job_id(args.resume_from, "--resume-from")
    except ValueError as e:
        logger.error(str(e))
        return
    
    # Resolve citekeys
    citekeys = None
    if args.resume_from:
        try:
            citekeys = get_failed_citekeys("sync_ocr", args.resume_from, args.source_job_id, args.input.parent / "sync_ocr")
            if not citekeys:
                print(f"✓ No failed citekeys to resume from job {args.resume_from}")
                return
        except ValueError as e:
            logger.error(str(e))
            return
    elif args.pattern:
        try:
            citekeys = expand_citekey_patterns(
                args.pattern,
                source_job_id=args.source_job_id
            )
            logger.info(f"📋 Expanded patterns to {len(citekeys)} citekeys")
        except ValueError as e:
            logger.error(str(e))
            return
    elif args.citekeys:
        citekeys = args.citekeys
    
    # Dry run preview
    if args.dry_run:
        # Get all available citekeys from local directory
        all_citekeys = citekeys if citekeys else [
            d.name for d in args.input.iterdir() 
            if d.is_dir() and d.name != "job_metadata"
        ]
        
        preview = preview_pipeline_run(
            step_name="sync_ocr",
            citekeys=all_citekeys,
            source_job_id=args.source_job_id,
            force_rerun=args.force_rerun,
            output_dir=args.input.parent / "sync_ocr"
        )
        print_dry_run_summary(preview)
        return
    
    # Print summary
    print("\n" + "=" * 70)
    print("📤 B2 OCR Results Sync (ETL Step 3)")
    print("=" * 70)
    print(f"  Source Job ID (OCR): {args.source_job_id}")
    if args.resume_from:
        print(f"  Resuming from: {args.resume_from}")
    if citekeys:
        print(f"  Citekeys: {len(citekeys)}")
    print(f"  Local dir: {args.input}")
    print(f"  B2 bucket: {B2_ANALYTICS_BUCKET}")
    print(f"  Skip images: {args.skip_images}")
    print("=" * 70 + "\n")
    
    # Sync to B2
    sync_job_to_b2(
        args.source_job_id,
        args.input,
        citekeys,
        args.skip_images,
        args.force_rerun,
        args.quiet
    )


if __name__ == "__main__":
    main()