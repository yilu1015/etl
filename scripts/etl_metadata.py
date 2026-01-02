"""
Shared ETL metadata utilities for all pipeline steps.

Provides a centralized system for:
- Saving task-specific metadata (per step)
- Automatically updating central registry
- Querying job history and status
- Rebuilding registry from scratch

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
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, List

logger = logging.getLogger(__name__)

# Directories
SCRIPT_DIR = Path(__file__).parent
WORKSPACE_ROOT = SCRIPT_DIR.parent
ANALYTICS_ROOT = WORKSPACE_ROOT / "data" / "analytics"
SOURCES_ROOT = WORKSPACE_ROOT / "data" / "sources"

CENTRAL_REGISTRY_DIR = ANALYTICS_ROOT / "job_registry"
SOURCES_METADATA_DIR = SOURCES_ROOT / "job_metadata"


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
