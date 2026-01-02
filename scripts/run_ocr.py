#!/usr/bin/env python3
"""
Run OCR via Runpod (PaddleOCR-VL) using B2 presigned URLs.

ETL Pipeline Step 2: OCR Processing
- Input: List of citekeys OR directory of PDFs
- Source: B2 bucket (cna-sources) OR local files
- Processing: Runpod serverless endpoint with page-based batching
- Output: Local results under data/analytics/ocr/<citekey>/<job_id>/

Lineage Chain:
  upload_pdfs (source) → run_ocr (this step) → sync_ocr → parse_structure

Usage:
  # Standard workflow
  python run_ocr.py --citekeys dagz_v01 dagz_v02 \\
    --source-job-id 2026-01-02_00-28-28
  
  # From local directory
  python run_ocr.py --input data/sources/test \\
    --source-job-id 2026-01-02_00-28-28
  
  # Resume failed items
  python run_ocr.py --resume-from 2026-01-02_00-33-11 \\
    --source-job-id 2026-01-02_00-28-28

⚠️  IMPORTANT: Always use explicit --source-job-id, never "latest"
"""

import argparse
import base64
import json
import logging
import os
import sys
import time
import shutil
import tempfile
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Tuple, Union, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3
import requests
import yaml
from dotenv import load_dotenv
from tqdm import tqdm

from etl_metadata import (
    save_step_metadata, validate_job_id, get_failed_citekeys,
    expand_citekey_patterns, preview_pipeline_run, print_dry_run_summary
)

try:
    from PyPDF2 import PdfReader
    HAS_PYPDF = True
except ImportError:
    HAS_PYPDF = False

# ============================================================
# Logging Setup
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# ============================================================
# Environment & Configuration
# ============================================================

load_dotenv()

# Load all configuration from single YAML file
CONFIG_DIR = Path(__file__).parent.parent / "config"
OCR_CONFIG_FILE = CONFIG_DIR / "ocr_config.yaml"

with OCR_CONFIG_FILE.open("r") as f:
    OCR_CONFIG = yaml.safe_load(f)

# Extract nested configs for convenience
PIPELINE_CONFIG = OCR_CONFIG.get("pipeline", {})
PREDICT_PARAMS = OCR_CONFIG.get("predict", {})

# ============================================================
# Runpod Credentials (from environment only)
# ============================================================

RUNPOD_API_KEY = os.getenv("RUNPOD_API_KEY")
if not RUNPOD_API_KEY:
    raise RuntimeError("RUNPOD_API_KEY not set in environment or .env")

RUNPOD_ENDPOINT_ID = os.getenv("RUNPOD_ENDPOINT_ID")
if not RUNPOD_ENDPOINT_ID:
    raise RuntimeError("RUNPOD_ENDPOINT_ID not set in environment or .env")

RUNPOD_ENDPOINT = f"https://api.runpod.ai/v2/{RUNPOD_ENDPOINT_ID}/run"
RUNPOD_STATUS_ENDPOINT = f"https://api.runpod.ai/v2/{RUNPOD_ENDPOINT_ID}/status"

# ============================================================
# B2 Source Configuration (from config + env overrides)
# ============================================================

B2_BUCKET = os.getenv("B2_SOURCE_BUCKET", OCR_CONFIG["b2_source"]["bucket"])
B2_REGION = os.getenv("B2_SOURCE_REGION", OCR_CONFIG["b2_source"]["region"])
B2_ENDPOINT_URL = os.getenv("B2_SOURCE_ENDPOINT_URL", OCR_CONFIG["b2_source"]["endpoint_url"])
B2_ACCESS_KEY = os.getenv("B2_SOURCE_ACCESS_KEY_ID")
B2_SECRET_KEY = os.getenv("B2_SOURCE_SECRET_ACCESS_KEY")
PRESIGNED_URL_EXPIRY = OCR_CONFIG["b2_source"]["presigned_url_expiry"]

if not all([B2_BUCKET, B2_REGION, B2_ENDPOINT_URL, B2_ACCESS_KEY, B2_SECRET_KEY]):
    raise RuntimeError(
        "B2 source configuration incomplete. Required: "
        "B2_SOURCE_ACCESS_KEY_ID, B2_SOURCE_SECRET_ACCESS_KEY, "
        "and optionally B2_SOURCE_BUCKET, B2_SOURCE_REGION, B2_SOURCE_ENDPOINT_URL"
    )

# ============================================================
# Runpod S3 Configuration (for image extraction - optional)
# ============================================================

RUNPOD_S3_ACCESS_KEY_ID = os.getenv("RUNPOD_S3_ACCESS_KEY_ID", OCR_CONFIG["runpod_s3"]["access_key_id"])
RUNPOD_S3_SECRET_ACCESS_KEY = os.getenv("RUNPOD_S3_SECRET_ACCESS_KEY", OCR_CONFIG["runpod_s3"]["secret_access_key"])
RUNPOD_S3_ENDPOINT_URL = os.getenv("RUNPOD_S3_ENDPOINT_URL", OCR_CONFIG["runpod_s3"]["endpoint_url"])
RUNPOD_S3_REGION = os.getenv("RUNPOD_S3_REGION", OCR_CONFIG["runpod_s3"]["region"])
RUNPOD_NETWORK_VOLUME_ID = os.getenv("RUNPOD_NETWORK_VOLUME_ID", OCR_CONFIG["runpod_s3"]["network_volume_id"])

# Check if S3 credentials are configured
RUNPOD_S3_CONFIGURED = all([
    RUNPOD_S3_ACCESS_KEY_ID,
    RUNPOD_S3_SECRET_ACCESS_KEY,
    RUNPOD_S3_ENDPOINT_URL,
    RUNPOD_S3_REGION,
    RUNPOD_NETWORK_VOLUME_ID
])

if not RUNPOD_S3_CONFIGURED:
    logger.warning(
        "Runpod S3 credentials incomplete. Image extraction from network volume will be skipped. "
        "Set environment variables: RUNPOD_S3_ACCESS_KEY_ID, RUNPOD_S3_SECRET_ACCESS_KEY, "
        "RUNPOD_S3_ENDPOINT_URL, RUNPOD_S3_REGION, RUNPOD_NETWORK_VOLUME_ID"
    )

# ============================================================
# Job Submission & Polling Configuration
# ============================================================

POLL_INTERVAL_SECONDS = OCR_CONFIG["job_submission"]["poll_interval"]
MAX_POLL_SECONDS = OCR_CONFIG["job_submission"]["max_poll_duration"]
MAX_RETRIES_PER_STATUS = OCR_CONFIG["job_submission"]["max_retries_per_status"]

# ============================================================
# Batching Configuration
# ============================================================

DEFAULT_PAGES_PER_BATCH = OCR_CONFIG["batching"]["default_pages_per_batch"]
MAX_PAGES_PER_BATCH = OCR_CONFIG["batching"]["max_pages_per_batch"]

# ============================================================
# Metadata Configuration
# ============================================================

SOURCE_METADATA_DIR = Path(OCR_CONFIG["metadata"]["source_job_metadata_dir"])


# ============================================================
# File Discovery & Validation
# ============================================================

def discover_files(input_path: Union[str, Path]) -> List[Tuple[str, str]]:
    """
    Discover PDF files from input (directory or single file).
    
    Args:
        input_path: Directory containing PDFs or path to single PDF file
    
    Returns:
        List of (citekey, file_path) tuples where citekey is filename stem
    
    Raises:
        ValueError: If input is neither directory nor PDF file
    """
    input_path = Path(input_path)
    files = []
    
    if input_path.is_file() and input_path.suffix.lower() == ".pdf":
        # Single PDF file - use filename as citekey
        citekey = input_path.stem  # filename without extension
        files.append((citekey, str(input_path.resolve())))
        return files
    
    elif input_path.is_dir():
        # Directory - discover all PDFs
        pdf_files = sorted(input_path.glob("*.pdf"))
        if not pdf_files:
            raise ValueError(f"No PDF files found in {input_path}")
        
        for pdf_file in pdf_files:
            citekey = pdf_file.stem
            files.append((citekey, str(pdf_file.resolve())))
        return files
    
    else:
        raise ValueError(f"Input must be a directory or PDF file: {input_path}")

def discover_citekeys_from_directory(input_dir: Union[str, Path]) -> List[str]:
    """
    Discover citekeys from a directory of PDFs.
    
    Expects files like:
      data/sources/test/test_v01.pdf
      data/sources/test/test_v02.pdf
      data/sources/test/test_v03.pdf
    
    Extracts citekey from filename stem (e.g., 'test_v01' from 'test_v01.pdf')
    
    Args:
        input_dir: Path to directory containing PDFs
    
    Returns:
        Sorted list of citekeys
    
    Raises:
        ValueError: If directory doesn't exist or has no PDFs
    """
    files = discover_files(input_dir)
    return [citekey for citekey, _ in files]

def validate_citekey_in_b2(citekey: str) -> bool:
    """
    Check if citekey exists in B2 (was uploaded by upload_pdfs_to_b2.py).
    
    Args:
        citekey: Citation key to validate
    
    Returns:
        True if file exists in B2, False otherwise
    """
    s3_client = get_b2_client()
    try:
        s3_client.head_object(
            Bucket=B2_BUCKET,
            Key=f"{citekey}/{citekey}.pdf"
        )
        return True
    except s3_client.exceptions.NoSuchKey:
        return False
    except Exception as e:
        logger.warning(f"Error validating {citekey} in B2: {e}")
        return False

def get_latest_source_job_id(sources_metadata_dir: Path = SOURCE_METADATA_DIR) -> Optional[str]:
    """
    Find the most recent upload job ID from data/sources/job_metadata/.
    
    Args:
        sources_metadata_dir: Path to source job metadata directory
    
    Returns:
        Latest job ID (filename stem) or None if not found
    """
    if not sources_metadata_dir.exists():
        return None
    
    metadata_files = sorted(sources_metadata_dir.glob("*.json"))
    if not metadata_files:
        return None
    
    # Latest file (excluding latest.json symlink if it exists)
    latest_file = next(
        (f for f in reversed(metadata_files) if f.name != "latest.json"),
        None
    )
    
    if latest_file:
        return latest_file.stem  # filename without .json
    
    return None

def load_source_job_metadata(job_id: str, sources_metadata_dir: Path = SOURCE_METADATA_DIR) -> Optional[dict]:
    """
    Load source job metadata to verify which citekeys were uploaded.
    
    Args:
        job_id: Source job ID (e.g., '2025-12-31_22-04-05')
        sources_metadata_dir: Path to source job metadata directory
    
    Returns:
        Source job metadata dict or None if not found
    """
    metadata_file = sources_metadata_dir / f"{job_id}.json"
    
    if not metadata_file.exists():
        return None
    
    try:
        with metadata_file.open("r", encoding="utf-8") as f:
            return json.load(f)
    except json.JSONDecodeError as e:
        logger.warning(f"Failed to parse source job metadata {metadata_file}: {e}")
        return None

def find_source_job_for_citekey(citekey: str, sources_metadata_dir: Path = SOURCE_METADATA_DIR) -> Optional[str]:
    """
    Find which source job uploaded a specific citekey.
    
    Searches all source job metadata files to find which one contains this citekey.
    Returns the most recent job if citekey appears in multiple jobs.
    Only trusts jobs with status="completed".
    
    Args:
        citekey: Citation key to search for
        sources_metadata_dir: Path to source job metadata directory
    
    Returns:
        Job ID if found, None otherwise
    """
    if not sources_metadata_dir.exists():
        return None
    
    # Search all metadata files in reverse chronological order (newest first)
    for metadata_file in sorted(sources_metadata_dir.glob("*.json"), reverse=True):
        # Skip the 'latest.json' symlink if it exists
        if metadata_file.name == "latest.json":
            continue
        
        try:
            with metadata_file.open("r", encoding="utf-8") as f:
                data = json.load(f)
            
            # Only trust completed jobs
            if data.get("status") != "completed":
                continue
            
            citekeys_in_job = data.get("citekeys", {}).get("list", [])
            if citekey in citekeys_in_job:
                return data.get("job_id")  # Return most recent match
        
        except Exception as e:
            logger.debug(f"Error reading {metadata_file}: {e}")
            continue
    
    return None

# ============================================================
# B2 / URL Handling
# ============================================================

def get_b2_client():
    """Initialize S3 client for B2 analytics bucket."""
    return boto3.client(
        "s3",
        region_name=B2_REGION,
        endpoint_url=B2_ENDPOINT_URL,
        aws_access_key_id=B2_ACCESS_KEY,
        aws_secret_access_key=B2_SECRET_KEY,
    )

def get_runpod_s3_client():
    """
    Initialize S3 client for Runpod network volume.
    
    Uses environment variables set from Runpod S3 API key:
    - RUNPOD_S3_ACCESS_KEY_ID: S3 API key access key (e.g., user_***...)
    - RUNPOD_S3_SECRET_ACCESS_KEY: S3 API key secret (e.g., rps_***...)
    - RUNPOD_S3_ENDPOINT_URL: Datacenter endpoint (e.g., https://s3api-eu-ro-1.runpod.io)
    - RUNPOD_S3_REGION: Datacenter region (e.g., EU-RO-1)
    """
    if not RUNPOD_S3_CONFIGURED:
        return None
    
    logger.info(f"Initializing Runpod S3 client:")
    logger.info(f"  Endpoint: {RUNPOD_S3_ENDPOINT_URL}")
    logger.info(f"  Region: {RUNPOD_S3_REGION}")
    logger.info(f"  Volume ID: {RUNPOD_NETWORK_VOLUME_ID}")
    logger.info(f"  Access Key (first 10 chars): {RUNPOD_S3_ACCESS_KEY_ID[:10]}...")
    
    try:
        client = boto3.client(
            "s3",
            region_name=RUNPOD_S3_REGION,
            endpoint_url=RUNPOD_S3_ENDPOINT_URL,
            aws_access_key_id=RUNPOD_S3_ACCESS_KEY_ID,
            aws_secret_access_key=RUNPOD_S3_SECRET_ACCESS_KEY,
        )
        
        # Test connection
        client.head_bucket(Bucket=RUNPOD_NETWORK_VOLUME_ID)
        logger.info("✓ Successfully connected to Runpod S3")
        return client
    except Exception as e:
        logger.error(f"❌ Failed to connect to Runpod S3: {e}")
        logger.error(f"   Check your credentials and endpoint URL")
        return None

def download_image_from_runpod_volume(s3_client, relative_path: str, local_path: Path) -> bool:
    """
    Download image from Runpod network volume via S3 API.
    
    According to Runpod docs and handler design:
    - Handler saves to: /runpod-volume/ocr_images/{job_id}/{citekey}/page_001_*.png
    - S3 API accesses as: ocr_images/{job_id}/{citekey}/page_001_*.png
    
    Args:
        s3_client: Boto3 S3 client configured for Runpod
        relative_path: Path from handler (may include /runpod-volume/ prefix)
        local_path: Local path where to save the file
    
    Returns:
        True if successful, False otherwise
    """
    try:
        # Ensure path is clean (remove /runpod-volume/ prefix if present)
        s3_key = relative_path
        if s3_key.startswith("/runpod-volume/"):
            s3_key = s3_key[len("/runpod-volume/"):]
        
        logger.debug(f"Downloading from Runpod S3: bucket={RUNPOD_NETWORK_VOLUME_ID}, key={s3_key}")
        
        s3_client.download_file(
            RUNPOD_NETWORK_VOLUME_ID,
            s3_key,
            str(local_path)
        )
        logger.debug(f"✓ Downloaded: {s3_key} → {local_path}")
        return True
    except Exception as e:
        logger.warning(f"❌ Failed to download {relative_path} from Runpod S3: {e}")
        return False

def delete_image_from_runpod_volume(s3_client, relative_path: str) -> bool:
    """
    Delete image from Runpod network volume via S3 API (cleanup).
    
    Args:
        s3_client: Boto3 S3 client configured for Runpod
        relative_path: Path from handler (may include /runpod-volume/ prefix)
    
    Returns:
        True if successful, False otherwise
    """
    try:
        # Ensure path is clean (remove /runpod-volume/ prefix if present)
        s3_key = relative_path
        if s3_key.startswith("/runpod-volume/"):
            s3_key = s3_key[len("/runpod-volume/"):]
        
        logger.debug(f"Deleting from Runpod S3: bucket={RUNPOD_NETWORK_VOLUME_ID}, key={s3_key}")
        
        s3_client.delete_object(
            Bucket=RUNPOD_NETWORK_VOLUME_ID,
            Key=s3_key
        )
        logger.debug(f"✓ Deleted from volume: {s3_key}")
        return True
    except Exception as e:
        logger.warning(f"❌ Failed to delete {relative_path} from Runpod: {e}")
        return False

def get_presigned_url(citekey: str) -> str:
    """
    Generate presigned URL for citekey in B2.
    
    Args:
        citekey: Citation key (e.g., "dagz_v01")
    
    Returns:
        Presigned URL valid for 12 hours
    
    Raises:
        ValueError: If URL generation fails
    """
    s3_client = get_b2_client()
    
    try:
        url = s3_client.generate_presigned_url(
            ClientMethod="get_object",
            Params={
                "Bucket": B2_BUCKET,
                "Key": f"{citekey}/{citekey}.pdf",
            },
            ExpiresIn=PRESIGNED_URL_EXPIRY,
        )
        return url
    except Exception as e:
        raise ValueError(
            f"Failed to generate presigned URL for {citekey}: {e}. "
            f"Ensure file exists in B2: {B2_BUCKET}/{citekey}/{citekey}.pdf"
        )

def get_local_file_size(pdf_path: Path) -> float:
    """Get file size from local PDF (in MB)."""
    try:
        size_bytes = pdf_path.stat().st_size
        return size_bytes / (1024 * 1024)
    except Exception as e:
        logger.warning(f"Could not get size for {pdf_path}: {e}")
        return 0

def get_local_page_count(pdf_path: Path) -> int:
    """
    Get actual page count from local PDF using PyPDF2.
    
    Falls back to estimate from file size if PyPDF2 not available.
    """
    if not HAS_PYPDF:
        return estimate_page_count(get_local_file_size(pdf_path))
    
    try:
        with open(pdf_path, 'rb') as f:
            reader = PdfReader(f)
            page_count = len(reader.pages)
        return page_count
    except Exception as e:
        logger.warning(f"Could not get page count for {pdf_path}: {e}, using estimate")
        return estimate_page_count(get_local_file_size(pdf_path))

def get_b2_file_size(citekey: str) -> float:
    """Get file size from B2 (in MB)."""
    s3_client = get_b2_client()
    try:
        response = s3_client.head_object(
            Bucket=B2_BUCKET,
            Key=f"{citekey}/{citekey}.pdf"
        )
        return response['ContentLength'] / (1024 * 1024)
    except Exception as e:
        logger.warning(f"Could not get size for {citekey}: {e}")
        return 0  # Return 0 if we can't get size (will fail at OCR time anyway)

def estimate_page_count(file_size_mb: float) -> int:
    """
    Estimate page count from file size (for batching purposes).
    This is conservative - actual pages may be different.
    
    Assumes average PDF is ~1-2 MB per 100 pages.
    """
    if file_size_mb < 1:
        return 1
    if file_size_mb < 10:
        return int(file_size_mb * 50)  # ~50 pages/MB
    return int(file_size_mb * 20)      # ~20 pages/MB for larger files

def get_actual_page_count(citekey: str, local_path: Optional[Path] = None) -> int:
    """
    Get actual page count from PDF using PyPDF2.
    
    If local_path is provided, reads from local file.
    Otherwise, downloads from B2 to temp, counts pages, deletes temp file.
    
    Falls back to estimate if PyPDF2 not available.
    
    Args:
        citekey: Citation key (for logging)
        local_path: Optional path to local PDF file
    
    Returns:
        Actual page count
    """
    if not HAS_PYPDF:
        if local_path:
            return estimate_page_count(get_local_file_size(local_path))
        else:
            return estimate_page_count(get_b2_file_size(citekey))
    
    try:
        # If local file provided, read from it
        if local_path:
            with open(local_path, 'rb') as f:
                reader = PdfReader(f)
                page_count = len(reader.pages)
            return page_count
        
        # Otherwise, download from B2 to temp
        import tempfile
        
        s3_client = get_b2_client()
        tmp_path = None
        try:
            with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
                s3_client.download_fileobj(
                    Bucket=B2_BUCKET,
                    Key=f"{citekey}/{citekey}.pdf",
                    Fileobj=tmp
                )
                tmp_path = tmp.name
            
            with open(tmp_path, 'rb') as f:
                reader = PdfReader(f)
                page_count = len(reader.pages)
            
            return page_count
            
        finally:
            if tmp_path and os.path.exists(tmp_path):
                try:
                    os.remove(tmp_path)
                except:
                    pass
    
    except Exception as e:
        logger.warning(f"Could not get page count for {citekey}: {e}, using estimate")
        if local_path:
            return estimate_page_count(get_local_file_size(local_path))
        else:
            return estimate_page_count(get_b2_file_size(citekey))

# ============================================================
# Batching Logic
# ============================================================

def create_batches_by_pages(citekeys_with_pages: List[Tuple[str, int]], 
                            pages_per_batch: int = DEFAULT_PAGES_PER_BATCH) -> List[List[str]]:
    """
    Group citekeys into batches based on actual page count.
    
    Args:
        citekeys_with_pages: List of (citekey, actual_page_count) tuples
        pages_per_batch: Target pages per batch
    
    Returns:
        List of batches, each containing list of citekeys
    """
    if pages_per_batch <= 0:
        pages_per_batch = DEFAULT_PAGES_PER_BATCH
    
    if pages_per_batch > MAX_PAGES_PER_BATCH:
        pages_per_batch = MAX_PAGES_PER_BATCH
    
    batches = []
    current_batch = []
    current_pages = 0
    
    for citekey, page_count in citekeys_with_pages:
        # If adding this file would exceed threshold AND we have files in current batch
        if current_pages + page_count > pages_per_batch and current_batch:
            batches.append(current_batch)
            current_batch = [citekey]
            current_pages = page_count
        else:
            current_batch.append(citekey)
            current_pages += page_count
    
    if current_batch:
        batches.append(current_batch)
    
    return batches

# ============================================================
# Symlink Management
# ============================================================

def create_latest_symlink(output_dir: Path, citekey: str, job_id: str):
    """
    Create/update a 'latest' symlink for easy access to newest results.
    
    Args:
        output_dir: Base output directory (e.g., data/analytics/ocr)
        citekey: Citation key
        job_id: Current job ID (timestamp)
    """
    citekey_dir = output_dir / citekey
    latest_link = citekey_dir / "latest"
    
    try:
        # Ensure citekey directory exists
        citekey_dir.mkdir(parents=True, exist_ok=True)
        
        # Remove old symlink if it exists
        if latest_link.exists() or latest_link.is_symlink():
            latest_link.unlink()
        
        # Create new symlink (target, then link name)
        latest_link.symlink_to(job_id)
        logger.debug(f"Created symlink: {latest_link} -> {job_id}")
    except Exception as e:
        logger.warning(f"Failed to create symlink for {citekey}: {e}")


def create_metadata_latest_symlink(output_dir: Path, job_id: str):
    """
    Create/update a 'latest.json' symlink in job_metadata folder for easy access to newest metadata.
    
    Args:
        output_dir: Base output directory (e.g., data/analytics/ocr)
        job_id: Current job ID (timestamp)
    """
    metadata_dir = output_dir / "job_metadata"
    latest_link = metadata_dir / "latest.json"
    target_file = f"{job_id}.json"
    
    try:
        # Ensure metadata directory exists
        metadata_dir.mkdir(parents=True, exist_ok=True)
        
        # Remove old symlink if it exists
        if latest_link.exists() or latest_link.is_symlink():
            latest_link.unlink()
        
        # Create new symlink pointing to the job metadata file
        latest_link.symlink_to(target_file)
        logger.debug(f"Created symlink: {latest_link} -> {target_file}")
    except Exception as e:
        logger.warning(f"Failed to create metadata symlink: {e}")

# ============================================================
# Job Submission & Polling
# ============================================================

def submit_batch_to_runpod(citekeys: List[str],
                           batch_idx: int,
                           job_id: str,
                           source_job_ids: Dict[str, Optional[str]],
                           headers: dict,
                           pipeline_config: dict,
                           predict_params: dict) -> Tuple[str, List[str]]:
    """
    Submit a batch of citekeys to Runpod with full metadata.
    
    Args:
        citekeys: List of citekeys to process
        batch_idx: Batch index for logging
        job_id: ETL job ID (shared across all batches)
        source_job_ids: Dict mapping citekey -> source job ID for lineage tracking (values can be None)
        headers: Request headers with authorization
        pipeline_config: PaddleOCRVL config
        predict_params: PaddleOCRVL predict params
    
    Returns:
        Tuple of (runpod_job_id, citekey_list)
    """
    # Generate presigned URLs for all citekeys with comprehensive metadata
    files_payload = []
    for citekey in citekeys:
        try:
            url = get_presigned_url(citekey)
            
            # Build metadata dict with all relevant information
            metadata = {
                "citekey": citekey,
                "job_id": job_id,                           # ETL job ID
                "pipeline_step": "ocr",                     # Task identifier
                "batch_idx": batch_idx,                     # Which batch this file is in
                "source_job_id": source_job_ids.get(citekey),  # Per-citekey lineage
            }
            
            files_payload.append({
                "type": "url",
                "url": url,
                "metadata": metadata,              # Handler will echo this back
            })
        except ValueError as e:
            raise ValueError(f"Batch {batch_idx}, citekey {citekey}: {e}")
    
    # Build request with job_id at batch level
    request_payload = {
        "input": {
            "files": files_payload,
            "job_id": job_id,                      # Also pass at batch level for context
        }
    }
    
    if pipeline_config:
        request_payload["input"]["pipeline_config"] = pipeline_config
    
    if predict_params:
        request_payload["input"]["predict_params"] = predict_params
    else:
        request_payload["input"]["predict_params"] = {"output_format": ["json"]}
    
    # Submit
    resp = requests.post(
        RUNPOD_ENDPOINT,
        headers=headers,
        json=request_payload,
        timeout=30,
    )
    resp.raise_for_status()
    
    payload = resp.json()
    job_id_resp = payload.get("id")
    
    if not job_id_resp:
        raise RuntimeError(f"Malformed Runpod response: {payload}")
    
    return job_id_resp, citekeys

def poll_runpod_job(job_id: str, headers: dict) -> dict:
    """
    Poll Runpod job status with exponential backoff retry on transient errors.
    
    Retries temporary connection issues (DNS, timeout) but fails fast on permanent errors.
    
    Args:
        job_id: Runpod job ID to poll
        headers: Request headers with authorization
    
    Returns:
        Completed job result dictionary
    
    Raises:
        RuntimeError: If job failed or too many connection errors
        TimeoutError: If job doesn't complete within MAX_POLL_SECONDS
    """
    start_time = time.time()
    consecutive_errors = 0
    
    while time.time() - start_time < MAX_POLL_SECONDS:
        try:
            resp = requests.get(
                f"{RUNPOD_STATUS_ENDPOINT}/{job_id}",
                headers=headers,
                timeout=10,
            )
            
            # Reset error counter on successful request
            consecutive_errors = 0
            
            if resp.status_code == 200:
                status_payload = resp.json()
                status = status_payload.get("status")
                
                if status == "COMPLETED":
                    return status_payload
                
                if status == "FAILED":
                    error_msg = status_payload.get("error", "Unknown error")
                    raise RuntimeError(f"Runpod job failed: {error_msg}")
                
                # Still running
                time.sleep(POLL_INTERVAL_SECONDS)
            else:
                logger.warning(f"Job {job_id}: Unexpected status code {resp.status_code}")
                time.sleep(POLL_INTERVAL_SECONDS)
        
        except (requests.ConnectionError, requests.Timeout, OSError) as e:
            # Transient network errors - retry with backoff
            consecutive_errors += 1
            
            if consecutive_errors >= MAX_RETRIES_PER_STATUS:
                logger.error(
                    f"Job {job_id}: Too many connection errors ({consecutive_errors}), giving up: {e}"
                )
                raise RuntimeError(f"Job {job_id}: Connection failed after {consecutive_errors} retries: {e}")
            
            wait_time = min(POLL_INTERVAL_SECONDS * (2 ** (consecutive_errors - 1)), 30)
            logger.warning(
                f"Job {job_id}: Connection error (attempt {consecutive_errors}/{MAX_RETRIES_PER_STATUS}), "
                f"retrying in {wait_time:.0f}s: {type(e).__name__}"
            )
            time.sleep(wait_time)
        
        except requests.RequestException as e:
            logger.error(f"Job {job_id}: Request error: {e}")
            raise
    
    # Timeout - do one final status check before giving up
    try:
        resp = requests.get(
            f"{RUNPOD_STATUS_ENDPOINT}/{job_id}",
            headers=headers,
            timeout=10,
        )
        result = resp.json()
        if result.get("status") == "COMPLETED":
            logger.info(f"Job {job_id} completed after timeout check")
            return result
    except Exception as e:
        logger.warning(f"Job {job_id}: Final status check failed: {e}")
    
    raise TimeoutError(f"Job {job_id} did not complete within {MAX_POLL_SECONDS}s")

# ============================================================
# Configuration Loading
# ============================================================

def load_yaml_config(config_str: str) -> dict:
    """
    Load YAML configuration from string or file path.
    
    Args:
        config_str: Path to YAML file or YAML string content
    
    Returns:
        Parsed configuration dictionary, or empty dict if config_str is None/empty
    """
    if not config_str:
        return {}
    
    config_path = Path(config_str)
    if config_path.exists() and config_path.is_file():
        with config_path.open("r") as f:
            return yaml.safe_load(f)
    
    try:
        return yaml.safe_load(config_str)
    except yaml.YAMLError as e:
        logger.error(f"Failed to parse YAML config: {e}")
        return {}


# ============================================================
# Result Saving
# ============================================================

def save_images_from_result(ocr_result: dict, output_dir: Path, s3_client=None):
    """
    Extract and save images from OCR result.
    
    Handles two image formats:
    1. Base64-encoded images (legacy/fallback format)
    2. Runpod S3 network volume paths (primary format from handler)
       - Handler structure: ocr_images/{job_id}/{citekey}/page_001_*.png
       - Falls back to: ocr_images/{job_id}/page_001_*.png (for standalone jobs)
    
    For network volume paths, downloads images via S3 API and cleans up the source.
    """
    images_saved = 0
    
    for page in ocr_result.get("pages", []):
        page_num = page["page"]
        images = page.get("images", {})
        
        if not images:
            continue
        
        page_dir = output_dir / f"page_{page_num:04d}_images"
        page_dir.mkdir(parents=True, exist_ok=True)
        
        for img_name, img_data in images.items():
            try:
                # Handle network volume path format (primary - from handler)
                if isinstance(img_data, dict) and img_data.get("type") == "volume_path":
                    relative_path = img_data.get("path")
                    
                    if not s3_client:
                        logger.debug(f"S3 client not configured, falling back to base64 for: {img_name}")
                        continue
                    
                    if not relative_path:
                        logger.warning(f"Missing relative path for image {img_name}")
                        continue
                    
                    # Download image from Runpod network volume
                    # Path format: ocr_images/{job_id}/{citekey}/page_001_layout.png
                    local_img_path = page_dir / f"{img_name}.png"
                    
                    if download_image_from_runpod_volume(s3_client, relative_path, local_img_path):
                        images_saved += 1
                        
                        # Clean up from network volume after successful download
                        delete_image_from_runpod_volume(s3_client, relative_path)
                    else:
                        logger.warning(f"Failed to download image: {relative_path}")
                
                # Handle legacy base64 format (fallback)
                elif isinstance(img_data, str):
                    try:
                        img_bytes = base64.b64decode(img_data)
                        img_path = page_dir / f"{img_name}.png"
                        img_path.write_bytes(img_bytes)
                        images_saved += 1
                        logger.debug(f"Saved image from base64: {img_name}")
                    except Exception as e:
                        logger.warning(f"Failed to decode base64 for {img_name}: {e}")
                
                else:
                    logger.warning(f"Unknown image data format for {img_name}: {type(img_data)}")
            
            except Exception as e:
                logger.warning(f"Failed to save image {img_name} from page {page_num}: {e}")
    
    return images_saved

def save_run_metadata(output_dir: Path, job_id: str, citekeys: List[str], job_ids: Dict[str, str],
                      source_job_id: Optional[str], pipeline_config: dict, predict_params: dict, 
                      results: list, batching_info: dict = None):
    """
    Save OCR job metadata using the shared etl_metadata system.
    
    This function prepares the metadata and delegates to save_step_metadata(),
    which handles:
    - Saving task-specific metadata to data/analytics/ocr/job_metadata/
    - Automatically updating central registry at data/analytics/job_registry/
    """
    successful = [r for r in results if r.get("status") == "success"]
    total_pages = sum(len(r.get("pages", [])) for r in successful)
    total_images = sum(
        sum(len(p.get("images", {})) for p in r.get("pages", []))
        for r in successful
    )
    
    # Prepare metadata for the OCR step
    metadata = {
        "status": "completed",
        "job_id": job_id,
        "job_timestamp": datetime.now().isoformat(),
        "pipeline_step": "ocr",
        "source_job_id": source_job_id,  # Link to source upload job for lineage
        "citekeys": {
            "total": len(citekeys),
            "successful": len(successful),
            "failed": len(citekeys) - len(successful),
            "list": citekeys,
        },
        "runpod_jobs": job_ids,  # runpod_job_id -> [citekeys_processed]
        "results_summary": {
            "total_pages": total_pages,
            "total_images": total_images,
        },
        "config": {
            "pipeline_config": pipeline_config,
            "predict_params": predict_params,
        }
    }
    
    if batching_info:
        metadata["batching"] = batching_info
    
    # Save metadata using shared system (handles both task-specific + central registry)
    save_step_metadata("ocr", job_id, metadata, output_dir=output_dir)

# ============================================================
# Main
# ============================================================

def main():
    parser = argparse.ArgumentParser(
        description="Run OCR via Runpod on B2 sources (ETL Step 2: OCR)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Standard workflow: Process citekeys from B2
  python run_ocr.py --citekeys dagz_v01 dagz_v02 \\
    --source-job-id 2026-01-02_00-28-28
  
  # From local directory
  python run_ocr.py --input data/sources/test \\
    --source-job-id 2026-01-02_00-28-28
  
  # Resume failed items from previous OCR job
  python run_ocr.py --resume-from 2026-01-02_00-33-11 \\
    --source-job-id 2026-01-02_00-28-28
  
  # Force rerun (ignore existing results)
  python run_ocr.py --citekeys dagz_v01 \\
    --source-job-id 2026-01-02_00-28-28 \\
    --force-rerun
  
  # With options
  python run_ocr.py --citekeys dagz_v01 dagz_v02 \\
    --source-job-id 2026-01-02_00-28-28 \\
    --batch-pages 200 \\
    --extract-images
        """
    )

    # Standardized input group
    input_group = parser.add_mutually_exclusive_group(required=True)
    input_group.add_argument(
        "--input",
        type=str,
        help="Input: directory of PDFs or path to single PDF file"
    )
    input_group.add_argument(
        "--citekeys",
        nargs="+",
        help="Explicit list of citekeys to process (must exist in B2)"
    )
    input_group.add_argument(
        "--resume-from",
        type=str,
        help="Resume failed citekeys from previous OCR job ID (e.g., '2026-01-02_00-33-11')"
    )

    # Required for lineage tracking
    parser.add_argument(
        "--source-job-id",
        type=str,
        help="Explicit upload job ID from upload_pdfs.py (REQUIRED unless resuming). "
             "Never use 'latest' - use explicit ID like '2026-01-02_00-28-28'"
    )

    parser.add_argument(
        "--output",
        default="data/analytics/ocr",
        help="Base directory for OCR outputs (default: data/analytics/ocr)"
    )

    parser.add_argument(
        "--extract-images",
        action="store_true",
        help="Extract and save visualization images as PNG files"
    )

    parser.add_argument(
        "--batch-pages",
        type=int,
        default=DEFAULT_PAGES_PER_BATCH,
        help=f"Target pages per batch (default: {DEFAULT_PAGES_PER_BATCH})"
    )

    parser.add_argument(
        "--force-rerun",
        action="store_true",
        help="Reprocess even if results already exist"
    )

    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Suppress progress output"
    )
    
    parser.add_argument(
        "--pipeline-config",
        type=str,
        default=None,
        help="YAML or JSON file path with PaddleOCRVL init parameters (default: config/ocr_config.yaml under 'pipeline')"
    )
    
    parser.add_argument(
        "--predict-params",
        type=str,
        default=None,
        help="YAML or JSON file path with PaddleOCRVL predict() parameters (default: config/ocr_config.yaml under 'predict')"
    )

    args = parser.parse_args()

    # Validate source-job-id (required unless resuming with auto-detect)
    if args.resume_from:
        # When resuming, can auto-detect source_job_id from resume metadata
        if args.source_job_id:
            try:
                validate_job_id(args.source_job_id, "--source-job-id")
            except ValueError as e:
                print(f"✗ {e}")
                sys.exit(1)
    else:
        # Normal run requires explicit source_job_id
        if not args.source_job_id:
            print("✗ --source-job-id is required")
            print("   Provide the job ID from upload_pdfs.py (e.g., '2026-01-02_00-28-28')")
            print("   To find latest: ls -t data/sources/job_metadata/ | head -1")
            sys.exit(1)
        
        try:
            validate_job_id(args.source_job_id, "--source-job-id")
        except ValueError as e:
            print(f"✗ {e}")
            sys.exit(1)

    # Resolve input files and build citekey list
    file_mapping = {}  # citekey -> local file path
    
    if args.input:
        try:
            discovered_files = discover_files(args.input)
            citekeys = [citekey for citekey, _ in discovered_files]
            file_mapping = {citekey: filepath for citekey, filepath in discovered_files}
            
            if not citekeys:
                print("✗ No PDF files found in input")
                sys.exit(1)
        except ValueError as e:
            print(f"✗ Error: {e}")
            sys.exit(1)
    
    elif args.resume_from:
        # Auto-detect source_job_id from resume metadata if not provided
        if not args.source_job_id:
            from etl_metadata import get_step_metadata
            prev_metadata = get_step_metadata("ocr", args.resume_from, output_dir=Path(args.output))
            if not prev_metadata:
                print(f"✗ Cannot find metadata for resume job {args.resume_from}")
                sys.exit(1)
            
            args.source_job_id = prev_metadata.get("source_job_id")
            if not args.source_job_id:
                print(f"✗ Resume job {args.resume_from} has no source_job_id in metadata")
                sys.exit(1)
            
            if not args.quiet:
                print(f"📦 Auto-detected source_job_id: {args.source_job_id}")
        
        # Get failed citekeys from previous job
        try:
            citekeys = get_failed_citekeys("ocr", args.resume_from, args.source_job_id, Path(args.output))
            if not citekeys:
                print(f"✓ No failed citekeys to resume from job {args.resume_from}")
                sys.exit(0)
        except ValueError as e:
            print(f"✗ {e}")
            sys.exit(1)
    
    else:
        citekeys = args.citekeys

    # ============================================================
    # Store source job ID for lineage tracking
    # ============================================================
    
    # source_job_id is already validated and set
    source_job_ids = {ck: args.source_job_id for ck in citekeys}
    
    # Create job ID (timestamp-based)
    job_id = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    
    # Get file sizes and actual page counts for batching
    if not args.quiet:
        print("\n" + "=" * 70)
        print("🚀 PaddleOCR-VL ETL Pipeline (Step 2: OCR)")
        print("=" * 70)
        print(f"Job ID: {job_id}")
        print(f"Source Job ID: {args.source_job_id}")
        if args.resume_from:
            print(f"Resuming from: {args.resume_from}")
        print(f"Citekeys: {len(citekeys)}")
        print(f"Output: {args.output}")
        print("=" * 70)
        print()
        
        # Show appropriate source
        if args.input:
            print(f"Reading file info from local directory: {args.input}")
        else:
            print(f"Validating {len(citekeys)} citekey(s) in B2...")
            print("Fetching B2 file info (sizes and page counts)...")
    
    citekeys_with_pages = []
    for citekey in citekeys:
        try:
            # If --input was provided, read from local file
            if args.input and citekey in file_mapping:
                pdf_path = Path(file_mapping[citekey])
                size_mb = get_local_file_size(pdf_path)
                page_count = get_local_page_count(pdf_path)  # Use local reading
            else:
                # Otherwise, fetch from B2
                size_mb = get_b2_file_size(citekey)
                page_count = get_actual_page_count(citekey)  # Download from B2
            
            citekeys_with_pages.append((citekey, page_count))
            if not args.quiet:
                print(f"  {citekey:20s}: {size_mb:8.1f}MB ({page_count:5d} pages)")
        except Exception as e:
            print(f"✗ Error accessing {citekey}: {e}")
            sys.exit(1)
    
    if not args.quiet:
        print()

    # Load or override configuration from arguments
    # Use global configs loaded from YAML files as defaults
    pipeline_config = load_yaml_config(args.pipeline_config) if args.pipeline_config else PIPELINE_CONFIG
    predict_params = load_yaml_config(args.predict_params) if args.predict_params else PREDICT_PARAMS

    # Add image extraction if requested
    if args.extract_images:
        if "output_format" not in predict_params:
            predict_params["output_format"] = []
        if isinstance(predict_params["output_format"], str):
            predict_params["output_format"] = [predict_params["output_format"]]
        if "images" not in predict_params["output_format"]:
            predict_params["output_format"].append("images")

    # Create batches using actual page counts
    batches = create_batches_by_pages(citekeys_with_pages, pages_per_batch=args.batch_pages)
    
    if not args.quiet:
        print(f"📦 Creating batches (target {args.batch_pages} pages per batch)...")
        print(f"   {len(batches)} batch(es):")
        for i, batch in enumerate(batches, 1):
            total_pages = sum(p for c, p in citekeys_with_pages if c in batch)
            # Use local files if available, otherwise use B2
            if file_mapping:
                total_size_mb = sum(
                    get_local_file_size(Path(file_mapping[c])) for c in batch
                )
            else:
                total_size_mb = sum(get_b2_file_size(c) for c in batch)
            print(f"     Batch {i}: {len(batch):2d} file(s), {total_size_mb:8.0f}MB ({total_pages:5d} pages)")
        print()

    # Submit batches
    headers = {
        "Authorization": f"Bearer {RUNPOD_API_KEY}",
        "Content-Type": "application/json",
    }

    if not args.quiet:
        print(f"📤 Submitting {len(batches)} batch(es) to Runpod...")

    job_ids = {}  # runpod_job_id -> citekeys_in_batch
    batch_mapping = {}  # runpod_job_id -> citekeys
    
    for batch_idx, batch in enumerate(batches, 1):
        try:
            runpod_job_id, citekeys_batch = submit_batch_to_runpod(
                batch,
                batch_idx,
                job_id,
                source_job_ids,
                headers,
                pipeline_config,
                predict_params
            )
            job_ids[runpod_job_id] = citekeys_batch
            batch_mapping[runpod_job_id] = citekeys_batch
            
            if not args.quiet:
                print(f"   ✓ Batch {batch_idx}: Job {runpod_job_id} ({len(citekeys_batch)} citekeys)")
        except Exception as e:
            print(f"✗ Batch {batch_idx} submission failed: {e}")
            sys.exit(1)

    if not args.quiet:
        print(f"\n✓ All batches submitted\n")
        print("⏳ Waiting for processing...")

    # Poll all jobs in parallel
    completed_jobs = {}
    
    with ThreadPoolExecutor(max_workers=len(job_ids)) as executor:
        future_to_job = {
            executor.submit(poll_runpod_job, jid, headers): jid
            for jid in job_ids.keys()
        }
        
        for future in as_completed(future_to_job):
            job_id_resp = future_to_job[future]
            try:
                completed_payload = future.result()
                completed_jobs[job_id_resp] = completed_payload
                if not args.quiet:
                    print(f"✓ Job {job_id_resp} completed")
            except Exception as e:
                print(f"✗ Job {job_id_resp} failed: {e}")

    if not args.quiet:
        print()

    # Aggregate results and organize by citekey
    results_by_citekey = {}
    
    for job_id_resp in job_ids.keys():
        if job_id_resp not in completed_jobs:
            print(f"⚠️  Job {job_id_resp} did not complete")
            continue
        
        job_output = completed_jobs[job_id_resp].get("output", {})
        batch_results = job_output.get("results", [])
        
        citekeys_batch = batch_mapping[job_id_resp]
        for citekey, result in zip(citekeys_batch, batch_results):
            results_by_citekey[citekey] = result

    # Save results organized by citekey
    if not args.quiet:
        print("💾 Saving OCR results by citekey...")

    # Initialize S3 client for image downloads (if configured)
    runpod_s3_client = get_runpod_s3_client() if args.extract_images else None
    
    if args.extract_images and not runpod_s3_client:
        logger.warning("Image extraction requested but Runpod S3 credentials not configured")

    total_images_saved = 0
    
    for citekey in tqdm(
        citekeys,
        unit="citekey",
        disable=args.quiet,
        desc="   Saving",
        position=0,
        leave=True
    ):
        ocr_result = results_by_citekey.get(citekey)
        if not ocr_result:
            continue
        
        # Create citekey-specific output directory
        citekey_output = Path(args.output) / citekey / job_id
        citekey_output.mkdir(parents=True, exist_ok=True)
        
        # Save OCR result
        out_path = citekey_output / f"{citekey}.json"
        with out_path.open("w", encoding="utf-8") as f:
            json.dump(ocr_result, f, indent=2, ensure_ascii=False)
        
        # Save images if requested
        if args.extract_images and ocr_result.get("status") == "success":
            num_images = save_images_from_result(ocr_result, citekey_output, runpod_s3_client)
            total_images_saved += num_images

    # Create 'latest' symlinks for easy access
    if not args.quiet:
        print("🔗 Creating 'latest' symlinks...")
    
    for citekey in citekeys:
        create_latest_symlink(Path(args.output), citekey, job_id)
    
    if not args.quiet:
        print()

    # Prepare batching metadata
    batching_info = {
        "num_batches": len(batches),
        "target_pages_per_batch": args.batch_pages,
        "batches": [
            {
                "batch_idx": i,
                "citekeys": batch,
                "pages": sum(p for c, p in citekeys_with_pages if c in batch),
            }
            for i, batch in enumerate(batches, 1)
        ]
    }
    
    # Save job metadata to centralized job_metadata folder (with source_job_id)
    # Use the first non-None source job ID for the summary (all citekeys should share the same one)
    source_job_id = next((sjid for sjid in source_job_ids.values() if sjid is not None), None)
    
    save_run_metadata(
        Path(args.output),
        job_id,
        citekeys,
        job_ids,
        source_job_id,
        pipeline_config,
        predict_params,
        list(results_by_citekey.values()),
        batching_info=batching_info
    )
    
    # Create 'latest.json' symlink in job_metadata folder
    create_metadata_latest_symlink(Path(args.output), job_id)

    # Summary
    successful = [r for r in results_by_citekey.values() if r.get("status") == "success"]
    total_pages = sum(len(r.get("pages", [])) for r in successful)
    
    if not args.quiet:
        print("=" * 70)
        print("✅ OCR Run Complete")
        print("=" * 70)
        print(f"  Job ID: {job_id}")
        print(f"  Citekeys: {len(successful)}/{len(citekeys)}")
        print(f"  Total pages: {total_pages}")
        if args.extract_images:
            print(f"  Images saved: {total_images_saved}")
        if source_job_id:
            print(f"  Source job: {source_job_id}")
        print(f"\n  Job metadata: {args.output}/job_metadata/{job_id}.json")
        print("=" * 70)

# ============================================================
# Entry point
# ============================================================

if __name__ == "__main__":
    main()