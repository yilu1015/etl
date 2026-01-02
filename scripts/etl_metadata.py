"""
Shared ETL metadata utilities for all pipeline steps.

Provides a centralized system for:
- Saving task-specific metadata (per step)
- Automatically updating central registry
- Querying job history and status
- Rebuilding registry from scratch
- Common utilities: validation, filtering, pattern expansion

Architecture:
  data/
    sources/job_metadata/              # Upload step metadata
    analytics/
      job_registry/                    # Central: full pipeline trace
        {job_id}.json
      {step_name}/job_metadata/        # Task-specific: {step_name} configs/results
        {job_id}.json
"""

import json
import logging
import re
import fnmatch
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, List, Tuple

logger = logging.getLogger(__name__)

# Directories
SCRIPT_DIR = Path(__file__).parent
WORKSPACE_ROOT = SCRIPT_DIR.parent
ANALYTICS_ROOT = WORKSPACE_ROOT / "data" / "analytics"
SOURCES_ROOT = WORKSPACE_ROOT / "data" / "sources"

CENTRAL_REGISTRY_DIR = ANALYTICS_ROOT / "job_registry"
SOURCES_METADATA_DIR = SOURCES_ROOT / "job_metadata"


# ============================================================
# Common Validation Utilities
# ============================================================

def validate_job_id(job_id: str, arg_name: str = "--source-job-id") -> str:
    """
    Ensure job ID is explicit (not 'latest' or symlink).
    
    Args:
        job_id: Job ID to validate
        arg_name: Argument name for error messages (e.g., "--source-job-id")
    
    Returns:
        Validated job ID
    
    Raises:
        ValueError: If job ID is invalid or "latest"
    
    Example:
        >>> validate_job_id("2026-01-02_11-32-58", "--source-job-id")
        '2026-01-02_11-32-58'
        
        >>> validate_job_id("latest", "--source-job-id")
        ValueError: --source-job-id cannot be 'latest'...
    """
    if not job_id:
        raise ValueError(f"{arg_name} is required")
    
    if job_id.lower() == "latest":
        # Provide helpful error message with how to find latest job
        step_hint = ""
        if "source" in arg_name.lower():
            step_hint = "sources/job_metadata"
        elif "ocr" in arg_name.lower():
            step_hint = "analytics/ocr/job_metadata"
        elif "sync" in arg_name.lower():
            step_hint = "analytics/sync_ocr/job_metadata"
        elif "parse" in arg_name.lower():
            step_hint = "analytics/parse_structure/job_metadata"
        
        error_msg = (
            f"{arg_name} cannot be 'latest'. Use explicit ID like '2026-01-02_11-32-58'.\n"
            f"Reason: 'latest' changes over time and breaks reproducibility/auditability."
        )
        
        if step_hint:
            error_msg += f"\n\nTo find latest job: ls -t data/{step_hint}/ | head -1"
        
        raise ValueError(error_msg)
    
    # Validate format (YYYY-MM-DD_HH-MM-SS)
    pattern = r'^\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2}$'
    if not re.match(pattern, job_id):
        raise ValueError(
            f"Invalid job ID format for {arg_name}: {job_id}\n"
            f"Expected format: YYYY-MM-DD_HH-MM-SS (e.g., '2026-01-02_11-32-58')"
        )
    
    return job_id


def get_failed_citekeys(
    step_name: str,
    resume_job_id: str,
    source_job_id: str,
    output_dir: Optional[Path] = None
) -> List[str]:
    """
    Extract failed/incomplete citekeys from previous job in the SAME step.
    
    Args:
        step_name: Pipeline step name (e.g., "ocr", "sync_ocr", "parse_structure")
        resume_job_id: Job ID to resume from (must be from same step)
        source_job_id: Source job ID for lineage verification
        output_dir: Optional custom output directory
    
    Returns:
        List of citekeys that failed in previous job
    
    Raises:
        ValueError: If resume job not found or has invalid source
    
    Example:
        >>> get_failed_citekeys("ocr", "2026-01-02_00-33-11", "2026-01-02_00-28-28")
        ['dagz_v03', 'dagz_v04']  # Failed citekeys
    """
    # Validate job IDs
    validate_job_id(resume_job_id, f"--resume-from ({step_name})")
    validate_job_id(source_job_id, "--source-job-id")
    
    # Load metadata from previous job
    prev_metadata = get_step_metadata(step_name, resume_job_id, output_dir)
    
    if not prev_metadata:
        raise ValueError(
            f"Cannot find {step_name} job {resume_job_id}.\n"
            f"Check: data/analytics/{step_name}/job_metadata/{resume_job_id}.json"
        )
    
    # Verify source lineage matches (safety check)
    prev_source = prev_metadata.get("source_job_id")
    if prev_source != source_job_id:
        logger.warning(
            f"⚠️  Resume job {resume_job_id} has different source ({prev_source}) "
            f"than provided ({source_job_id}). Proceeding with caution."
        )
    
    # Extract failed items
    citekeys_info = prev_metadata.get("citekeys", {})
    
    # Option 1: Explicit failure tracking (preferred)
    if "failed_list" in citekeys_info:
        failed = citekeys_info["failed_list"]
        if failed:
            logger.info(f"📋 Resuming {len(failed)} failed citekeys from job {resume_job_id}")
        return failed
    
    # Option 2: Infer from counts
    total = citekeys_info.get("total", 0)
    successful_key = "successful" if "successful" in citekeys_info else "processed"
    successful = citekeys_info.get(successful_key, 0)
    
    if total == successful:
        logger.info(f"✓ Job {resume_job_id} had no failures")
        return []
    
    # Option 3: Infer from output directory
    if output_dir is None:
        output_dir = ANALYTICS_ROOT / step_name
    
    all_citekeys = set(citekeys_info.get("list", []))
    
    successful_citekeys = set()
    for citekey in all_citekeys:
        citekey_job_dir = output_dir / citekey / resume_job_id
        
        # Check if output exists and is complete
        result_file = citekey_job_dir / f"{citekey}.json"
        if result_file.exists():
            successful_citekeys.add(citekey)
    
    failed = list(all_citekeys - successful_citekeys)
    
    if not failed:
        logger.info(f"✓ Job {resume_job_id} had no failures. Nothing to resume.")
        return []
    
    logger.info(f"📋 Found {len(failed)} incomplete citekeys: {failed}")
    return failed

def should_process_citekey(
    step_name: str,
    citekey: str,
    source_job_id: str,
    force_rerun: bool = False,
    output_dir: Optional[Path] = None
) -> Tuple[bool, str]:
    """
    Determine if a citekey should be processed.
    
    Args:
        step_name: Pipeline step (e.g., "parse_structure")
        citekey: Citekey to check
        source_job_id: Source job ID for this run
        force_rerun: If True, always process (ignore existing results)
        output_dir: Optional custom output directory
    
    Returns:
        (should_process: bool, reason: str)
    
    Logic:
        1. If force_rerun=True → always process
        2. Check if result exists for this citekey + source_job_id
        3. If exists → skip (unless force_rerun)
        4. If missing → process
    
    Example:
        >>> should_process_citekey("parse_structure", "dagz_v01", "2026-01-02_00-58-42")
        (False, "Result exists in job 2026-01-02_11-32-58")
        
        >>> should_process_citekey("parse_structure", "dagz_v01", "2026-01-02_00-58-42", force_rerun=True)
        (True, "Force rerun requested")
    """
    if force_rerun:
        return True, "Force rerun requested"
    
    # Check if output exists for this citekey
    if output_dir is None:
        output_dir = ANALYTICS_ROOT / step_name
    
    # Look for existing results with same source
    citekey_dir = output_dir / citekey
    if not citekey_dir.exists():
        return True, "No previous results found"
    
    # Check all job directories for this citekey
    for job_dir in citekey_dir.iterdir():
        if not job_dir.is_dir() or job_dir.name == "latest":
            continue
        
        # Load job metadata to check source_job_id
        try:
            result_file = job_dir / f"{citekey}.json"
            if result_file.exists():
                with result_file.open("r") as f:
                    result = json.load(f)
                
                # Check if this result came from same source
                result_source = result.get("source_job_id")
                if result_source == source_job_id:
                    return False, f"Result exists in job {job_dir.name}"
        except Exception as e:
            logger.debug(f"Could not read {result_file}: {e}")
            continue
    
    return True, "No matching result for this source"


def filter_citekeys_to_process(
    step_name: str,
    citekeys: List[str],
    source_job_id: str,
    force_rerun: bool = False,
    output_dir: Optional[Path] = None
) -> Tuple[List[str], Dict[str, str]]:
    """
    Filter citekeys based on existing results and force_rerun flag.
    
    Args:
        step_name: Pipeline step name
        citekeys: List of citekeys to check
        source_job_id: Source job ID
        force_rerun: If True, process all citekeys
        output_dir: Optional custom output directory
    
    Returns:
        (citekeys_to_process: List[str], skip_reasons: Dict[str, str])
    
    Example:
        >>> to_process, skipped = filter_citekeys_to_process(
        ...     "parse_structure",
        ...     ["dagz_v01", "dagz_v02", "dagz_v03"],
        ...     "2026-01-02_00-58-42",
        ...     force_rerun=False
        ... )
        >>> to_process
        ['dagz_v03']
        >>> skipped
        {'dagz_v01': 'Result exists in job 2026-01-02_11-32-58',
         'dagz_v02': 'Result exists in job 2026-01-02_11-32-58'}
    """
    to_process = []
    skip_reasons = {}
    
    for citekey in citekeys:
        should_process, reason = should_process_citekey(
            step_name, citekey, source_job_id, force_rerun, output_dir
        )
        
        if should_process:
            to_process.append(citekey)
        else:
            skip_reasons[citekey] = reason
    
    return to_process, skip_reasons


def expand_citekey_patterns(
    patterns: List[str],
    source_job_id: Optional[str] = None,
    available_citekeys: Optional[List[str]] = None
) -> List[str]:
    """
    Expand citekey patterns (with volume/year wildcards) to concrete citekey list.
    
    Only supports _v* and _y* patterns (volume and year patterns) to avoid errors.
    
    Args:
        patterns: List of patterns (can include _v* and _y*)
                  Examples: ["dagz_v*", "mzdnp_y200*", "gbda1987"]
        source_job_id: Optional source job to filter against
        available_citekeys: Optional list of valid citekeys to match against
                            If None, loads from source job metadata
    
    Returns:
        List of concrete citekeys (sorted, deduplicated)
    
    Raises:
        ValueError: If pattern matches no citekeys or uses invalid wildcard
    
    Example:
        >>> expand_citekey_patterns(["dagz_v0*"], available_citekeys=["dagz_v01", "dagz_v02", "gbda1987"])
        ['dagz_v01', 'dagz_v02']
        
        >>> expand_citekey_patterns(["mzdnp_y200*"], available_citekeys=["mzdnp_y2001", "mzdnp_y2002"])
        ['mzdnp_y2001', 'mzdnp_y2002']
        
        >>> expand_citekey_patterns(["xyz*"])
        ValueError: Pattern 'xyz*' matched no citekeys
    """
    # Load available citekeys if not provided
    if available_citekeys is None:
        if source_job_id:
            source_metadata = load_source_job_metadata(source_job_id)
            if source_metadata:
                available_citekeys = source_metadata.get("citekeys", {}).get("list", [])
        
        if not available_citekeys:
            raise ValueError(
                "Cannot expand patterns: no available citekeys.\n"
                "Provide --source-job-id or use explicit --citekeys list."
            )
    
    expanded = set()
    unmatched_patterns = []
    invalid_patterns = []
    
    for pattern in patterns:
        # Check if pattern contains wildcards
        if "*" in pattern or "?" in pattern:
            # Validate wildcard position - only allow _v* and _y* patterns
            if not ("_v*" in pattern or "_v?" in pattern or "_y*" in pattern or "_y?" in pattern):
                invalid_patterns.append(pattern)
                continue
            
            matches = fnmatch.filter(available_citekeys, pattern)
            if not matches:
                unmatched_patterns.append(pattern)
            else:
                expanded.update(matches)
                logger.info(f"📋 Pattern '{pattern}' matched {len(matches)} citekeys: {matches}")
        else:
            # Literal citekey (no wildcard)
            if pattern in available_citekeys or not available_citekeys:
                expanded.add(pattern)
            else:
                logger.warning(f"⚠️  Citekey '{pattern}' not found in source job")
                expanded.add(pattern)  # Add anyway (will fail later if truly invalid)
    
    if invalid_patterns:
        raise ValueError(
            f"Invalid wildcard patterns: {invalid_patterns}\n"
            f"Only _v* and _y* wildcards are supported (e.g., 'dagz_v*', 'mzdnp_y200*')\n"
            f"This restriction prevents errors from overly broad patterns."
        )
    
    if unmatched_patterns:
        raise ValueError(
            f"Patterns matched no citekeys: {unmatched_patterns}\n"
            f"Available citekeys: {sorted(available_citekeys)[:20]}..."
        )
    
    return sorted(expanded)


def preview_pipeline_run(
    step_name: str,
    citekeys: List[str],
    source_job_id: str,
    force_rerun: bool = False,
    output_dir: Optional[Path] = None
) -> Dict:
    """
    Preview what a pipeline run would do (dry run).
    
    Args:
        step_name: Pipeline step name (current step, e.g., "parse_structure")
        citekeys: List of citekeys to process
        source_job_id: Source job ID (from PREVIOUS step, e.g., sync_ocr job)
        force_rerun: Whether force rerun is enabled
        output_dir: Optional custom output directory (for THIS step)
    
    Returns:
        Dict with preview information:
        {
            "step_name": "parse_structure",
            "source_job_id": "2026-01-02_00-58-42",
            "source_step": "sync_ocr",
            "total_citekeys": 10,
            "to_process": ["dagz_v03", ...],
            "to_skip": {"dagz_v01": "Result exists", ...},
            "estimated_new_job_id": "2026-01-02_15-30-00",
            "force_rerun": False
        }
    
    Example:
        >>> preview = preview_pipeline_run("parse_structure", ["dagz_v01", "dagz_v02"], "2026-01-02_00-58-42")
        >>> print(f"Will process {len(preview['to_process'])} citekeys")
    """
    # Validate source job
    validate_job_id(source_job_id, "--source-job-id")
    
    # Map current step to its source step
    SOURCE_STEP_MAP = {
        "ocr": "upload_pdfs",           # OCR reads from upload
        "sync_ocr": "ocr",              # Sync reads from OCR
        "parse_structure": "sync_ocr",  # Parse reads from sync
    }
    
    source_step = SOURCE_STEP_MAP.get(step_name)
    
    # Check source metadata from the CORRECT step's directory
    source_metadata = None
    if source_step:
        if source_step == "upload_pdfs":
            # Special case: upload metadata is in sources/
            source_metadata_path = SOURCES_METADATA_DIR / f"{source_job_id}.json"
            if source_metadata_path.exists():
                with source_metadata_path.open("r") as f:
                    import json
                    source_metadata = json.load(f)
                logger.info(f"✓ Loaded source metadata from {source_step} job {source_job_id}")
            else:
                logger.warning(f"⚠️  Source job {source_job_id} metadata not found in {source_step}")
        else:
            # Analytics steps (ocr, sync_ocr)
            source_metadata = get_step_metadata(
                source_step, 
                source_job_id, 
                ANALYTICS_ROOT / source_step
            )
            if source_metadata:
                logger.info(f"✓ Loaded source metadata from {source_step} job {source_job_id}")
            else:
                logger.warning(f"⚠️  Source job {source_job_id} metadata not found in {source_step}")
    
    # Filter citekeys (check THIS step's output directory for existing results)
    to_process, skip_reasons = filter_citekeys_to_process(
        step_name, citekeys, source_job_id, force_rerun, output_dir
    )
    
    # Estimate job ID (current timestamp)
    estimated_job_id = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    
    preview = {
        "step_name": step_name,
        "source_job_id": source_job_id,
        "source_step": source_step,
        "total_citekeys": len(citekeys),
        "to_process": to_process,
        "to_skip": skip_reasons,
        "estimated_new_job_id": estimated_job_id,
        "force_rerun": force_rerun,
        "will_create_new_job": len(to_process) > 0
    }
    
    return preview


def print_dry_run_summary(preview: Dict):
    """
    Print human-readable dry run summary.
    
    Args:
        preview: Output from preview_pipeline_run()
    
    Example:
        >>> preview = preview_pipeline_run("parse_structure", ["dagz_v01"], "2026-01-02_00-58-42")
        >>> print_dry_run_summary(preview)
        # Prints formatted summary
    """
    print("\n" + "="*70)
    print(f"🔍 DRY RUN PREVIEW: {preview['step_name']}")
    print("="*70)
    
    print(f"\n📋 Configuration:")
    print(f"  Source Job ID:    {preview['source_job_id']}")
    print(f"  Force Rerun:      {preview['force_rerun']}")
    print(f"  Total Citekeys:   {preview['total_citekeys']}")
    
    print(f"\n✅ Citekeys to Process ({len(preview['to_process'])}):")
    if preview['to_process']:
        for ck in preview['to_process']:
            print(f"    • {ck}")
    else:
        print("    (none - all results already exist)")
    
    if preview['to_skip']:
        print(f"\n⊘ Citekeys to Skip ({len(preview['to_skip'])}):")
        for ck, reason in list(preview['to_skip'].items())[:10]:  # Show max 10
            print(f"    • {ck}: {reason}")
        if len(preview['to_skip']) > 10:
            print(f"    ... and {len(preview['to_skip']) - 10} more")
    
    print(f"\n🆔 Estimated Job ID:")
    print(f"  {preview['estimated_new_job_id']}")
    
    if preview['will_create_new_job']:
        print(f"\n✅ Will create new job and process {len(preview['to_process'])} citekeys")
    else:
        print(f"\n⚠️  Nothing to process (all citekeys already have results)")
        print(f"    Use --force-rerun to reprocess anyway")
    
    print("\n" + "="*70)
    print("💡 To execute: Remove --dry-run flag")
    print("="*70 + "\n")

# ============================================================
# Metadata Management
# ============================================================

def save_step_metadata(step_name: str, job_id: str, metadata: dict, output_dir: Optional[Path] = None) -> Path:
    """
    Save metadata for a pipeline step and automatically update central registry.
    
    This is the primary API for all pipeline steps.
    
    Args:
        step_name: Name of the pipeline step (e.g., "ocr", "toc", "segmentation")
        job_id: Job ID (timestamp, e.g., "2025-12-31_19-00-45")
        metadata: Step-specific metadata dict (configs, results, citekeys, etc.)
        output_dir: Optional custom output directory. If None, uses ANALYTICS_ROOT/{step_name}
    
    Returns:
        Path to saved task-specific metadata file
    
    Saves to TWO places:
    1. Task-specific: data/analytics/{step_name}/job_metadata/{job_id}.json
    2. Central registry: data/analytics/job_registry/{job_id}.json (auto-updated)
    
    Example:
        metadata = {
            "status": "completed",
            "source_job_id": "2026-01-01_20-48-14",
            "citekeys": {"total": 10, "successful": 10, "failed": 0, "list": [...]},
            "config": {...}
        }
        save_step_metadata("ocr", "2026-01-01_21-37-23", metadata)
    """
    # Default to step-specific directory
    if output_dir is None:
        output_dir = ANALYTICS_ROOT / step_name
    
    # Save task-specific metadata
    task_metadata_dir = output_dir / "job_metadata"
    task_metadata_dir.mkdir(parents=True, exist_ok=True)
    
    task_metadata_file = task_metadata_dir / f"{job_id}.json"
    with task_metadata_file.open("w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=2, ensure_ascii=False)
    
    logger.info(f"Saved {step_name} metadata to {task_metadata_file}")
    
    # Update central registry (append or merge this step's info)
    update_central_registry(step_name, job_id, metadata)
    
    return task_metadata_file


def update_central_registry(step_name: str, job_id: str, step_metadata: dict):
    """
    Update central registry with info about a completed step.
    
    Central registry format:
    {
      "job_id": "2026-01-01_21-37-23",
      "timestamp": "2026-01-01T21:37:23Z",
      "source": {
        "job_id": "2026-01-01_20-48-14",
        "bucket": "cna-sources",
        "citekeys": {...},
        "checksums": {...}
      },
      "pipeline_steps": {
        "ocr": {
          "job_id": "2026-01-01_21-37-23",
          "status": "completed",
          "metadata_path": "ocr/job_metadata/2026-01-01_21-37-23.json",
          "citekeys": {...},
          "results_summary": {...}
        },
        "toc": {
          "job_id": null,
          "status": "pending"
        }
      },
      "execution_trace": [
        {"step": "ocr", "job_id": "...", "timestamp": "...", "status": "completed"},
        ...
      ]
    }
    """
    CENTRAL_REGISTRY_DIR.mkdir(parents=True, exist_ok=True)
    
    central_file = CENTRAL_REGISTRY_DIR / f"{job_id}.json"
    
    # Load existing or create new
    if central_file.exists():
        with central_file.open("r", encoding="utf-8") as f:
            central_registry = json.load(f)
    else:
        central_registry = {
            "job_id": job_id,
            "timestamp": datetime.now().isoformat(),
            "source": None,
            "pipeline_steps": {},
            "execution_trace": []
        }
    
    # Extract source job ID and load source metadata (only once)
    source_job_id = step_metadata.get("source_job_id")
    if source_job_id and not central_registry.get("source"):
        source_metadata = load_source_job_metadata(source_job_id)
        if source_metadata:
            central_registry["source"] = {
                "job_id": source_job_id,
                "bucket": source_metadata.get("bucket"),
                "endpoint_url": source_metadata.get("endpoint_url"),
                "citekeys": source_metadata.get("citekeys"),
                "checksums": source_metadata.get("checksums"),
            }
    
    # Extract step info
    step_info = {
        "job_id": job_id,
        "status": step_metadata.get("status", "completed"),
        "timestamp": datetime.now().isoformat(),
        "metadata_path": f"{step_name}/job_metadata/{job_id}.json",
    }
    
    if "citekeys" in step_metadata:
        step_info["citekeys"] = step_metadata["citekeys"]
    
    if "results_summary" in step_metadata:
        step_info["results_summary"] = step_metadata["results_summary"]
    
    # Include any additional fields from step_metadata (e.g., files_uploaded, b2_bucket for sync_ocr)
    for key, value in step_metadata.items():
        if key not in ["status"] and key not in step_info:
            step_info[key] = value
    
    # Add step to pipeline_steps
    central_registry["pipeline_steps"][step_name] = step_info
    
    # Update execution trace (append new step if not already there)
    existing_trace = [t for t in central_registry.get("execution_trace", []) if t["step"] != step_name]
    existing_trace.append({
        "step": step_name,
        "job_id": job_id,
        "timestamp": datetime.now().isoformat(),
        "status": step_metadata.get("status", "completed")
    })
    central_registry["execution_trace"] = existing_trace
    
    # Save updated registry
    with central_file.open("w", encoding="utf-8") as f:
        json.dump(central_registry, f, indent=2, ensure_ascii=False)
    
    logger.info(f"Updated central registry: {central_file}")


def load_source_job_metadata(source_job_id: str) -> Optional[dict]:
    """
    Load source job metadata from data/sources/job_metadata/.
    
    Args:
        source_job_id: Job ID from upload step
    
    Returns:
        Source metadata dict or None if not found
    """
    metadata_file = SOURCES_METADATA_DIR / f"{source_job_id}.json"
    
    if not metadata_file.exists():
        return None
    
    try:
        with metadata_file.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logger.warning(f"Failed to load source metadata {source_job_id}: {e}")
        return None


def get_step_metadata(step_name: str, job_id: str, output_dir: Optional[Path] = None) -> Optional[dict]:
    """
    Load metadata for a specific pipeline step.
    
    Args:
        step_name: Pipeline step name (e.g., "ocr")
        job_id: Job ID
        output_dir: Optional custom output directory
    
    Returns:
        Metadata dict or None if not found
    """
    if output_dir is None:
        output_dir = ANALYTICS_ROOT / step_name
    
    metadata_file = output_dir / "job_metadata" / f"{job_id}.json"
    
    if not metadata_file.exists():
        return None
    
    with metadata_file.open("r", encoding="utf-8") as f:
        return json.load(f)


def get_central_registry(job_id: str) -> Optional[dict]:
    """
    Load central registry for a job.
    
    Shows which pipeline steps have completed and their status.
    
    Args:
        job_id: Job ID to retrieve
    
    Returns:
        Central registry dict or None if not found
    """
    registry_file = CENTRAL_REGISTRY_DIR / f"{job_id}.json"
    
    if not registry_file.exists():
        return None
    
    with registry_file.open("r", encoding="utf-8") as f:
        return json.load(f)


def list_all_jobs(step_name: Optional[str] = None) -> List[str]:
    """
    List all completed jobs (optionally filtered by step).
    
    Args:
        step_name: Optional step name to filter by (e.g., "ocr")
                   If None, returns all jobs from central registry
    
    Returns:
        Sorted list of job IDs (newest first)
    """
    if step_name:
        step_dir = ANALYTICS_ROOT / step_name / "job_metadata"
        if not step_dir.exists():
            return []
        return sorted(
            [f.stem for f in step_dir.glob("*.json") if f.name != "latest.json"],
            reverse=True
        )
    else:
        # List from central registry
        if not CENTRAL_REGISTRY_DIR.exists():
            return []
        return sorted(
            [f.stem for f in CENTRAL_REGISTRY_DIR.glob("*.json") if f.name != "latest.json"],
            reverse=True
        )


def get_latest_job(step_name: Optional[str] = None) -> Optional[str]:
    """
    Get the most recent job ID.
    
    Args:
        step_name: Optional step name filter. If None, uses central registry.
    
    Returns:
        Latest job ID or None
    """
    jobs = list_all_jobs(step_name)
    return jobs[0] if jobs else None


def rebuild_central_registry():
    """
    Scan all step directories and rebuild central registry from scratch.
    
    Useful if central registry gets corrupted.
    
    This function:
    1. Scans all step directories (ocr/, toc/, segmentation/, etc.)
    2. For each step's job_metadata, creates/updates central registry entry
    3. Overwrites existing central registry files
    
    Returns:
        Number of jobs processed
    """
    CENTRAL_REGISTRY_DIR.mkdir(parents=True, exist_ok=True)
    
    job_count = 0
    
    # Find all step directories
    for step_dir in ANALYTICS_ROOT.iterdir():
        if not step_dir.is_dir() or step_dir.name in ["job_registry", "exports"]:
            continue
        
        metadata_dir = step_dir / "job_metadata"
        if not metadata_dir.exists():
            continue
        
        step_name = step_dir.name
        
        # Process all metadata files for this step
        for metadata_file in sorted(metadata_dir.glob("*.json")):
            if metadata_file.name == "latest.json":
                continue
            
            try:
                with metadata_file.open("r", encoding="utf-8") as f:
                    step_metadata = json.load(f)
                
                job_id = metadata_file.stem
                update_central_registry(step_name, job_id, step_metadata)
                job_count += 1
            except Exception as e:
                logger.warning(f"Failed to process {metadata_file}: {e}")
                continue
    
    logger.info(f"Rebuilt central registry with {job_count} jobs")
    return job_count


# ============================================================
# Utilities for querying and analyzing jobs
# ============================================================

def get_job_pipeline_status(job_id: str) -> Optional[Dict]:
    """
    Get a summary of which pipeline steps have been completed for a job.
    
    Args:
        job_id: Job ID to check
    
    Returns:
        Dict with step statuses or None if job not found
        
    Example output:
        {
            "job_id": "2026-01-01_21-37-23",
            "timestamp": "2026-01-01T21:37:23Z",
            "steps_completed": ["ocr"],
            "steps_pending": ["toc", "segmentation"],
            "source_citekeys": ["citekey1", "citekey2"]
        }
    """
    registry = get_central_registry(job_id)
    if not registry:
        return None
    
    steps_completed = []
    steps_pending = []
    
    for step_name, step_info in registry.get("pipeline_steps", {}).items():
        if step_info.get("status") == "completed":
            steps_completed.append(step_name)
        else:
            steps_pending.append(step_name)
    
    source_citekeys = []
    if registry.get("source"):
        source_citekeys = registry["source"].get("citekeys", {}).get("list", [])
    
    return {
        "job_id": job_id,
        "timestamp": registry.get("timestamp"),
        "steps_completed": steps_completed,
        "steps_pending": steps_pending,
        "source_citekeys": source_citekeys,
        "source_job_id": registry.get("source", {}).get("job_id")
    }


def find_jobs_for_citekey(citekey: str) -> List[Dict]:
    """
    Find all jobs that processed a specific citekey.
    
    Searches all central registry files for the citekey.
    
    Args:
        citekey: Citation key to search for
    
    Returns:
        List of dicts with job_id, step_name, status
    """
    results = []
    
    if not CENTRAL_REGISTRY_DIR.exists():
        return results
    
    for registry_file in CENTRAL_REGISTRY_DIR.glob("*.json"):
        if registry_file.name == "latest.json":
            continue
        
        try:
            with registry_file.open("r", encoding="utf-8") as f:
                registry = json.load(f)
            
            job_id = registry_file.stem
            
            # Check source citekeys
            if registry.get("source", {}).get("citekeys", {}).get("list"):
                if citekey in registry["source"]["citekeys"]["list"]:
                    results.append({
                        "job_id": job_id,
                        "step": "source (upload)",
                        "status": "completed"
                    })
            
            # Check each pipeline step
            for step_name, step_info in registry.get("pipeline_steps", {}).items():
                if step_info.get("citekeys", {}).get("list"):
                    if citekey in step_info["citekeys"]["list"]:
                        results.append({
                            "job_id": job_id,
                            "step": step_name,
                            "status": step_info.get("status")
                        })
        except Exception as e:
            logger.debug(f"Error reading {registry_file}: {e}")
            continue
    
    return results
