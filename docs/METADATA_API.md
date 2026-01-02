# ETL Metadata Quick Reference

## Command Line Usage

### Run OCR Pipeline

```bash
# Auto-detect source job (recommended)
uv run scripts/run_ocr.py --citekeys test_v04 --output data/analytics/ocr

# Explicit source job
uv run scripts/run_ocr.py --citekeys test_v04 --source-job-id 2026-01-01_20-48-14 --output data/analytics/ocr

# Check help
uv run scripts/run_ocr.py --help
```

### Sync OCR Results to B2

```bash
# Sync latest job
uv run scripts/sync_ocr.py --job-id latest

# Sync specific job
uv run scripts/sync_ocr.py --job-id 2026-01-01_21-45-14

# Skip images (faster)
uv run scripts/sync_ocr.py --job-id latest --skip-images

# Quiet mode
uv run scripts/sync_ocr.py --job-id latest --quiet
```

## Python API Usage

### Check Job Status

```python
from scripts.etl_metadata import get_job_pipeline_status, get_central_registry

# Quick status summary
status = get_job_pipeline_status("2026-01-01_21-45-14")
print(f"Completed steps: {status['steps_completed']}")
print(f"Pending steps: {status['steps_pending']}")
print(f"Source: {status['source_job_id']}")

# Full registry with all details
registry = get_central_registry("2026-01-01_21-45-14")
print(registry['pipeline_steps']['ocr']['citekeys'])
print(registry['pipeline_steps']['sync_ocr']['files_uploaded'])
```

### Find Jobs Processing a Citekey

```python
from scripts.etl_metadata import find_jobs_for_citekey

jobs = find_jobs_for_citekey("test_v04")
for job in jobs:
    print(f"  {job['job_id']} > {job['step']}: {job['status']}")
# Output:
#   2026-01-01_21-45-14 > source (upload): completed
#   2026-01-01_21-45-14 > ocr: completed
#   2026-01-01_21-45-14 > sync_ocr: completed
```

### List All Jobs

```python
from scripts.etl_metadata import list_all_jobs, get_latest_job

# All jobs from central registry
all_jobs = list_all_jobs()
print(f"Total jobs: {len(all_jobs)}")

# Latest job
latest = get_latest_job()
print(f"Latest: {latest}")

# Jobs from specific step only
ocr_jobs = list_all_jobs(step_name="ocr")
print(f"OCR jobs: {ocr_jobs}")
```

### Access Task-Specific Metadata

```python
from scripts.etl_metadata import get_step_metadata

# Get OCR task metadata
ocr_meta = get_step_metadata("ocr", "2026-01-01_21-45-14")
print(f"Pages processed: {ocr_meta['pages']['total']}")
print(f"Config: {ocr_meta['config']}")

# Get sync metadata
sync_meta = get_step_metadata("sync_ocr", "2026-01-01_21-45-14")
print(f"Files uploaded: {sync_meta['files_uploaded']}")
```

### Record New Pipeline Step Results

```python
from scripts.etl_metadata import save_step_metadata

# When implementing a new step (e.g., TOC extraction)
metadata = {
    "status": "completed",
    "source_job_id": "2026-01-01_21-45-14",
    "citekeys": {
        "total": 1,
        "successful": 1,
        "failed": 0,
        "list": ["test_v04"]
    },
    # Any additional fields specific to your step
    "custom_metric": 95.2
}

# This saves metadata AND updates central registry automatically
save_step_metadata("toc", "2026-01-01_22-30-00", metadata)
```

## Directory Locations

**Central Registry:** `data/analytics/job_registry/`

- `{job_id}.json` - Complete pipeline trace
- `latest.json` - Symlink to most recent job

**Step Metadata:**

- OCR: `data/analytics/ocr/job_metadata/{job_id}.json`
- Sync: `data/analytics/sync_ocr/job_metadata/{job_id}.json` (created by sync_ocr.py)
- TOC (future): `data/analytics/toc/job_metadata/{job_id}.json`
- Segmentation (future): `data/analytics/segmentation/job_metadata/{job_id}.json`

**Source Metadata:** `data/sources/job_metadata/{job_id}.json`

## Registry Schema

```json
{
  "job_id": "string - timestamp like 2026-01-01_21-45-14",
  "timestamp": "ISO 8601 timestamp when registry was created",
  
  "source": {
    "job_id": "string - source upload job ID",
    "bucket": "string - B2 bucket name",
    "endpoint_url": "string - B2 endpoint",
    "citekeys": {
      "total": "number",
      "uploaded": "number",
      "skipped": "number",
      "list": ["array of citekey strings"]
    },
    "checksums": {
      "citekey": "sha256 checksum",
      ...
    }
  },
  
  "pipeline_steps": {
    "step_name": {
      "job_id": "string",
      "status": "string - completed|pending|failed",
      "timestamp": "ISO 8601 timestamp of step execution",
      "metadata_path": "string - path to step-specific metadata",
      "citekeys": "object - citekey summary (if applicable)",
      "... any additional fields from step_metadata"
    },
    ...
  },
  
  "execution_trace": [
    {
      "step": "string - step name",
      "job_id": "string - job ID",
      "timestamp": "ISO 8601 timestamp",
      "status": "string - completed|pending|failed"
    },
    ...
  ]
}
```

## Common Patterns

### Check if OCR is Done for a Job

```python
status = get_job_pipeline_status("2026-01-01_21-45-14")
if "ocr" in status['steps_completed']:
    print("OCR is complete")
else:
    print("OCR pending or not started")
```

### Get All OCR Results for a Citekey

```python
from pathlib import Path

citekey = "test_v04"
job_id = "2026-01-01_21-45-14"
ocr_dir = Path(f"data/analytics/ocr/{citekey}/{job_id}")

# Read individual page results
for json_file in sorted(ocr_dir.glob("page_*.json")):
    print(f"Processing {json_file}")
```

### Monitor Pipeline Progress

```python
from scripts.etl_metadata import get_central_registry
import json

registry = get_central_registry("2026-01-01_21-45-14")

print("=== Pipeline Progress ===")
for step, info in registry['pipeline_steps'].items():
    print(f"{step}: {info['status']}")
    
print("\n=== Execution Order ===")
for i, trace in enumerate(registry['execution_trace'], 1):
    print(f"{i}. {trace['step']} at {trace['timestamp']}")
```

### Rebuild Registry (if corrupted)

```python
from scripts.etl_metadata import rebuild_central_registry

# Scans all step directories and rebuilds from metadata
count = rebuild_central_registry()
print(f"Rebuilt registry with {count} jobs")
```

## Troubleshooting

**Registry missing a step:**

```python
from scripts.etl_metadata import rebuild_central_registry
rebuild_central_registry()  # Rescans all step metadata
```

**Find all jobs processed by a citekey:**

```python
from scripts.etl_metadata import find_jobs_for_citekey
jobs = find_jobs_for_citekey("problematic_citekey")
```

**Get detailed config for an OCR job:**

```python
from scripts.etl_metadata import get_step_metadata
meta = get_step_metadata("ocr", "2026-01-01_21-45-14")
import json
print(json.dumps(meta['config'], indent=2))
```

## Integration with New Steps

When adding a new pipeline step (e.g., `extract_toc.py`):

```python
from scripts.etl_metadata import save_step_metadata
from datetime import datetime

# Your step processing...
job_id = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

# Prepare metadata
metadata = {
    "status": "completed",  # required
    "source_job_id": "2026-01-01_21-45-14",  # optional
    "citekeys": {  # optional but recommended
        "total": 1,
        "successful": 1,
        "failed": 0,
        "list": ["citekey1"]
    },
    # Add any custom fields
    "custom_metric": 42.5
}

# Save - this automatically updates central registry
save_step_metadata("my_step", job_id, metadata)

# Later: query results
from scripts.etl_metadata import get_central_registry
registry = get_central_registry(job_id)
print(registry['pipeline_steps']['my_step'])
```

That's all you need! The metadata system handles the rest.
