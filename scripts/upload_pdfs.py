#!/usr/bin/env python3
"""
Upload PDFs to Backblaze B2 with citekey validation and checksum-based skipping.

ETL Pipeline Step 0: Upload (Entry Point)
- Input: Local PDF files or directories
- Processing: Validate, normalize, upload to B2
- Output: PDFs in B2, metadata in data/sources/job_metadata/

This is the ONLY step without --source-job-id (it's the pipeline source!)

Canonical target structure in bucket `cna-sources`:

cna-sources/
  <citekey>/
    <citekey>.pdf
  run_metadata/
    <job_id>.json

Usage:
  # Upload from directory
  python upload_pdfs.py --input data/sources/batch_2026/
  
  # Upload specific files
  python upload_pdfs.py --input dagz_v01.pdf dagz_v02.pdf
  
  # Upload by citekeys (finds PDFs in --local-dir)
  python upload_pdfs.py --citekeys dagz_v01 dagz_v02 --local-dir /path/to/pdfs/
  
  # Upload by wildcard pattern
  python upload_pdfs.py --pattern "mzdnp2023_v*" --local-dir /path/to/pdfs/
  
  # Dry run (validate only)
  python upload_pdfs.py --input data/sources/test/ --dry-run

This script:
- validates citekeys
- normalizes messy local folder structures
- computes SHA256 checksums
- uploads only new or modified PDFs
- keeps a local checksum cache via job metadata
- saves and uploads metadata about the upload batch
- shows progress with tqdm

Secrets are read ONLY from environment variables or .env.
"""

import argparse
import hashlib
import json
import os
import re
import shutil
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple
from uuid import uuid4

import boto3
import yaml
from dotenv import load_dotenv
from tqdm import tqdm
import fnmatch

# ============================================================
# CONFIGURATION
# ============================================================

load_dotenv()

# Load configuration from upload_config.yaml
CONFIG_FILE = Path(__file__).parent.parent / "config" / "upload_config.yaml"
with CONFIG_FILE.open("r") as f:
    CONFIG = yaml.safe_load(f)

# B2 credentials (from environment variables only)
B2_ACCESS_KEY = os.getenv("B2_SOURCE_ACCESS_KEY_ID")
B2_SECRET_KEY = os.getenv("B2_SOURCE_SECRET_ACCESS_KEY")

# B2 settings (from config with env var overrides)
DEFAULT_BUCKET = os.getenv("B2_SOURCE_BUCKET", CONFIG["b2"]["default_bucket"])
DEFAULT_REGION = os.getenv("B2_SOURCE_REGION", CONFIG["b2"]["default_region"])
DEFAULT_ENDPOINT_URL = os.getenv("B2_SOURCE_ENDPOINT_URL", CONFIG["b2"]["default_endpoint_url"])

if not all([DEFAULT_BUCKET, DEFAULT_REGION, DEFAULT_ENDPOINT_URL, B2_ACCESS_KEY, B2_SECRET_KEY]):
    print("❌ B2 source configuration incomplete. Required: "
          "B2_SOURCE_ACCESS_KEY_ID, B2_SOURCE_SECRET_ACCESS_KEY, "
          "and optionally B2_SOURCE_BUCKET, B2_SOURCE_REGION, B2_SOURCE_ENDPOINT_URL")
    sys.exit(1)

# Directory settings (from config)
DEFAULT_STAGING_DIR = Path(CONFIG["directories"]["staging"])
DEFAULT_METADATA_DIR = Path(CONFIG["directories"]["metadata"])

# Citekey validation rules (from config)
THREE_DIGIT_VOLUME_SERIES = set(CONFIG["citekey_rules"]["three_digit_volume_series"])

# ============================================================
# CITEKEY VALIDATION
# ============================================================

CITEKEY_RE = re.compile(
    r"""
    ^[a-z0-9]+                    # work slug
    (?:_s\d{2,3})?                # optional series
    (?:_y\d{4}(?:_\d{4})?)?       # optional year or year range
    (?:_v\d{2,3})?$               # optional volume
    """,
    re.VERBOSE,
)


def validate_citekey(citekey: str) -> None:
    if citekey.lower() != citekey:
        raise ValueError(f"{citekey}: citekey must be lowercase")

    if "-" in citekey:
        raise ValueError(f"{citekey}: hyphens are not allowed in citekeys")

    if not CITEKEY_RE.match(citekey):
        raise ValueError(
            f"{citekey}: does not match citekey grammar "
            "(<slug>[_sNN][_yYYYY|_yYYYY_YYYY][_vNN|_vNNN])"
        )

    parts = citekey.split("_")

    # validate year or year range
    for part in parts:
        if part.startswith("y"):
            years = part[1:].split("_")
            if len(years) == 1:
                if len(years[0]) != 4:
                    raise ValueError(f"{citekey}: invalid year format")
            elif len(years) == 2:
                y1, y2 = years
                if len(y1) != 4 or len(y2) != 4:
                    raise ValueError(f"{citekey}: invalid year range format")
                if int(y2) <= int(y1):
                    raise ValueError(
                        f"{citekey}: year range must be ascending (YYYY_YYYY)"
                    )
            else:
                raise ValueError(f"{citekey}: invalid year encoding")

    # validate volume padding
    if "_v" in citekey:
        slug = citekey.split("_")[0]
        vol = citekey.split("_v")[-1]

        if slug in THREE_DIGIT_VOLUME_SERIES:
            if len(vol) != 3:
                raise ValueError(
                    f"{citekey}: volume must be 3 digits for series '{slug}'"
                )
        else:
            if len(vol) != 2:
                raise ValueError(
                    f"{citekey}: volume must be 2 digits for series '{slug}'"
                )


# ============================================================
# UTILS
# ============================================================

def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def load_checksums_from_metadata(metadata_dir: Path) -> Dict[str, str]:
    """
    Load accumulated checksums from ALL completed metadata files in order.
    Later files override earlier ones (in case a file was re-uploaded).
    Only trusts jobs marked with status="completed".
    """
    accumulated_checksums: Dict[str, str] = {}
    
    if not metadata_dir.exists():
        return {}
    
    # Sort by filename (ISO timestamp ensures chronological order)
    metadata_files = sorted(metadata_dir.glob("*.json"))
    
    for metadata_file in metadata_files:
        try:
            with metadata_file.open("r", encoding="utf-8") as f:
                data = json.load(f)
                # Only trust completed jobs
                if data.get("status") == "completed":
                    accumulated_checksums.update(data.get("checksums", {}))
        except Exception as e:
            print(f"⚠ Warning: failed to read {metadata_file}: {e}")
            continue
    
    return accumulated_checksums


# ============================================================
# NORMALIZATION
# ============================================================

def collect_pdfs(input_paths: List[Path]) -> List[Path]:
    """
    Collect PDFs from input paths.
    Accepts both directories (recursively searched) and individual PDF files.
    """
    pdfs: List[Path] = []
    for p in input_paths:
        if not p.exists():
            raise FileNotFoundError(f"Input path not found: {p}")
        
        if p.is_file() and p.suffix.lower() == ".pdf":
            # Single PDF file
            pdfs.append(p)
        elif p.is_dir():
            # Directory: recursively search for PDFs
            pdfs.extend(p.rglob("*.pdf"))
        else:
            raise ValueError(f"Input must be a PDF file or directory: {p}")
    
    return pdfs


def normalize_pdfs(
    pdfs: List[Path],
    staging_dir: Path,
    quiet: bool = False,
) -> Dict[str, Path]:
    """
    Normalize PDFs into:
      staging/<citekey>/<citekey>.pdf
    Returns:
      dict[citekey] -> normalized pdf path
    """
    if staging_dir.exists():
        shutil.rmtree(staging_dir)
    staging_dir.mkdir(parents=True)

    normalized: Dict[str, Path] = {}

    for pdf in pdfs:
        citekey = pdf.stem
        validate_citekey(citekey)

        target_dir = staging_dir / citekey
        target_dir.mkdir(parents=True, exist_ok=True)

        target_pdf = target_dir / f"{citekey}.pdf"

        if target_pdf.exists():
            raise RuntimeError(f"Duplicate PDF for citekey {citekey}")

        shutil.copy2(pdf, target_pdf)
        normalized[citekey] = target_pdf

        if not quiet:
            print(f"✓ normalized: {citekey}")

    return normalized


# ============================================================
# B2 CLIENT
# ============================================================

def make_s3_client(endpoint_url: str, region: str):
    return boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        region_name=region,
        aws_access_key_id=B2_ACCESS_KEY,
        aws_secret_access_key=B2_SECRET_KEY,
    )


# ============================================================
# METADATA
# ============================================================

def save_upload_metadata_local(
    metadata_dir: Path,
    job_id: str,
    citekeys: list,
    checksums: Dict[str, str],
    bucket: str,
    endpoint_url: str,
    num_uploaded: int,
    num_skipped: int,
) -> Path:
    """
    Save metadata about this upload batch locally.
    Marks job as "completed" to indicate successful upload.
    Returns path to metadata file.
    """
    metadata_dir.mkdir(parents=True, exist_ok=True)
    
    metadata = {
        "job_id": job_id,
        "timestamp": datetime.now().isoformat(),
        "bucket": bucket,
        "endpoint_url": endpoint_url,
        "status": "completed",
        "citekeys": {
            "total": len(citekeys),
            "uploaded": num_uploaded,
            "skipped": num_skipped,
            "list": sorted(citekeys),
        },
        "checksums": checksums,
    }
    
    metadata_file = metadata_dir / f"{job_id}.json"
    with metadata_file.open("w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=2, ensure_ascii=False)
    
    return metadata_file


def sync_metadata_to_b2(
    s3,
    bucket: str,
    metadata_file: Path,
    job_id: str,
    quiet: bool = False,
) -> bool:
    """
    Sync metadata file to B2.
    
    Returns:
        True if successful, False otherwise
    """
    if not metadata_file.exists():
        print(f"❌ Metadata file not found: {metadata_file}")
        return False
    
    try:
        s3_key = f"run_metadata/{job_id}.json"
        s3.upload_file(str(metadata_file), bucket, s3_key)
        
        if not quiet:
            print(f"✓ Uploaded metadata to {s3_key}")
        
        return True
    except Exception as e:
        print(f"❌ Metadata upload failed: {e}")
        return False


# ============================================================
# PDF UPLOAD
# ============================================================

def upload_changed_pdfs(
    s3,
    bucket: str,
    normalized_pdfs: Dict[str, Path],
    checksum_cache: Dict[str, str],
    dry_run: bool = False,
    quiet: bool = False,
) -> Tuple[Dict[str, str], int, int]:
    """
    Upload only PDFs whose checksum has changed.
    Skips only if checksum matches cached value (from previous completed jobs).
    Returns tuple: (updated_checksum_cache, uploaded_count, skipped_count)
    """
    updated_checksums = dict(checksum_cache)

    uploaded = 0
    skipped = 0

    items = sorted(normalized_pdfs.items())

    with tqdm(
        total=len(items),
        unit="file",
        desc="Uploading PDFs",
        disable=quiet,
    ) as pbar:
        for citekey, pdf_path in items:
            checksum = sha256(pdf_path)
            previous = checksum_cache.get(citekey)

            if previous == checksum:
                skipped += 1
                pbar.set_postfix(uploaded=uploaded, skipped=skipped)
                pbar.update(1)
                continue

            s3_key = f"{citekey}/{citekey}.pdf"

            if not dry_run:
                s3.upload_file(
                    str(pdf_path),
                    bucket,
                    s3_key,
                )

            uploaded += 1
            updated_checksums[citekey] = checksum

            pbar.set_postfix(uploaded=uploaded, skipped=skipped)
            pbar.update(1)

    if not quiet:
        print(f"\nSummary: {uploaded} uploaded, {skipped} skipped.")

    return updated_checksums, uploaded, skipped


# ============================================================
# CLI
# ============================================================

def parse_args():
    parser = argparse.ArgumentParser(
        description="Upload PDFs to Backblaze B2 (ETL Step 0: Entry Point)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Upload from directory
  python upload_pdfs.py --input data/sources/batch_2026/
  
  # Upload specific files
  python upload_pdfs.py --input dagz_v01.pdf dagz_v02.pdf
  
  # Upload by citekeys (finds PDFs in --local-dir)
  python upload_pdfs.py --citekeys dagz_v01 dagz_v02 --local-dir /path/to/pdfs/
  
  # Upload by wildcard pattern
  python upload_pdfs.py --pattern "mzdnp2023_v*" --local-dir /path/to/pdfs/
  
  # Dry run (validate only)
  python upload_pdfs.py --input data/sources/test/ --dry-run

Note: This is the pipeline entry point. No --source-job-id needed.
      The job_id generated here becomes the source for subsequent steps.
        """
    )

    # Standardized input group
    input_group = parser.add_mutually_exclusive_group(required=True)
    input_group.add_argument(
        "--input",
        nargs="+",
        help="One or more input directories or PDF files"
    )
    input_group.add_argument(
        "--citekeys",
        nargs="+",
        help="Explicit list of citekeys (finds PDFs in --local-dir)"
    )
    input_group.add_argument(
        "--pattern",
        nargs="+",
        help="Citekey patterns with wildcards (e.g., 'mzdnp2023_v*'). "
             "Finds matching PDFs in --local-dir. Only _v* and _y* patterns supported."
    )

    parser.add_argument(
        "--local-dir",
        type=Path,
        help="Local directory containing PDFs (required with --citekeys or --pattern)"
    )

    parser.add_argument(
        "--bucket",
        default=DEFAULT_BUCKET,
        help="B2 bucket name (default: cna-sources)",
    )

    parser.add_argument(
        "--staging",
        default=str(DEFAULT_STAGING_DIR),
        help="Temporary staging directory",
    )

    parser.add_argument(
        "--metadata-dir",
        default=str(DEFAULT_METADATA_DIR),
        help="Local metadata directory for job records",
    )

    parser.add_argument(
        "--endpoint-url",
        default=DEFAULT_ENDPOINT_URL,
        help="S3 endpoint URL for B2",
    )

    parser.add_argument(
        "--region",
        default=DEFAULT_REGION,
        help="AWS region for B2",
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate and normalize, but do not upload",
    )

    parser.add_argument(
        "--no-metadata-sync",
        action="store_true",
        help="Skip uploading metadata to B2 (metadata is still saved locally)",
    )

    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Suppress progress bar and per-file messages",
    )

    return parser.parse_args()


# ============================================================
# MAIN
# ============================================================

def main():
    load_dotenv()

    args = parse_args()

    # Resolve input
    if args.input:
        input_paths = [Path(p) for p in args.input]
    elif args.citekeys:
        if not args.local_dir:
            print("❌ --local-dir required when using --citekeys")
            sys.exit(1)
        
        local_dir = Path(args.local_dir)
        if not local_dir.exists():
            print(f"❌ Local directory not found: {local_dir}")
            sys.exit(1)
        
        input_paths = [local_dir / f"{ck}.pdf" for ck in args.citekeys]
    else:
        print("❌ Must provide --input or --citekeys")
        sys.exit(1)

    staging_dir = Path(args.staging)
    metadata_dir = Path(args.metadata_dir)

    # Generate job ID
    job_id = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

    if not args.quiet:
        print("Collecting PDFs...")

    pdfs = sorted(collect_pdfs(input_paths))

    if not pdfs:
        if not args.quiet:
            print("No PDFs found. Exiting.")
        sys.exit(0)

    if not args.quiet:
        print(f"Found {len(pdfs)} PDF(s).")
        print("\nNormalizing structure...")

    normalized = normalize_pdfs(
        pdfs,
        staging_dir,
        quiet=args.quiet,
    )

    if not args.quiet:
        print("\nLoading checksum history from previous jobs...")

    checksums = load_checksums_from_metadata(metadata_dir)

    if not args.quiet:
        print("Preparing B2 client...")

    s3 = make_s3_client(args.endpoint_url, args.region)

    updated_checksums, num_uploaded, num_skipped = upload_changed_pdfs(
        s3=s3,
        bucket=args.bucket,
        normalized_pdfs=normalized,
        checksum_cache=checksums,
        dry_run=args.dry_run,
        quiet=args.quiet,
    )

    if not args.dry_run:
        # Save metadata locally
        if not args.quiet:
            print("\nSaving upload metadata...")
        
        metadata_file = save_upload_metadata_local(
            metadata_dir=metadata_dir,
            job_id=job_id,
            citekeys=list(normalized.keys()),
            checksums=updated_checksums,
            bucket=args.bucket,
            endpoint_url=args.endpoint_url,
            num_uploaded=num_uploaded,
            num_skipped=num_skipped,
        )
        
        if not args.quiet:
            print(f"✓ Saved metadata locally to {metadata_file}")

        # Sync metadata to B2 unless disabled
        if not args.no_metadata_sync:
            if not args.quiet:
                print("\nUploading metadata to B2...")
            
            sync_metadata_to_b2(
                s3=s3,
                bucket=args.bucket,
                metadata_file=metadata_file,
                job_id=job_id,
                quiet=args.quiet,
            )

        if staging_dir.exists() and staging_dir.is_dir():
            shutil.rmtree(staging_dir)

    if not args.quiet:
        print("\n✓ Done.")
        print(f"\n📦 Job ID: {job_id}")
        print(f"Use this job ID as --source-job-id in subsequent pipeline steps (run_ocr.py)")


if __name__ == "__main__":
    main()