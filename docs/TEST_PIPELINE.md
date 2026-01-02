# Complete ETL Pipeline Test

This document verifies the complete pipeline flow: Upload → OCR → Sync to B2.

## Architecture

```
Upload (source_job_id) 
  ↓
  → data/sources/job_metadata/{job_id}.json (source metadata)
  → data/sources/test/ (uploaded PDFs by citekey)
  
OCR (job_id = new timestamp)
  ↓
  → Uses source_job_id to find source metadata
  → data/analytics/ocr/job_metadata/{job_id}.json (OCR config & results)
  → data/analytics/ocr/{citekey}/{job_id}/ (OCR output files)
  → data/analytics/job_registry/{job_id}.json (central registry - step: ocr)
  
Sync to B2
  ↓
  → Uploads data/analytics/ocr/ to B2 bucket
  → Updates data/analytics/job_registry/{job_id}.json (add step: sync_ocr)
```

## Central Registry Structure

All jobs are tracked in `data/analytics/job_registry/{job_id}.json`:

```json
{
  "job_id": "2026-01-01_21-45-14",
  "timestamp": "2026-01-01T23:00:01.098980",
  "source": {
    "job_id": "2026-01-01_20-48-14",
    "bucket": "cna-sources",
    "citekeys": {...}
  },
  "pipeline_steps": {
    "ocr": {
      "job_id": "2026-01-01_21-45-14",
      "status": "completed",
      "citekeys": {...}
    },
    "sync_ocr": {
      "job_id": "2026-01-01_21-45-14",
      "status": "completed",
      "files_uploaded": 150,
      "files_skipped": 0,
      "b2_bucket": "cna-analytics"
    }
  },
  "execution_trace": [...]
}
```

## Key Functions

### etl_metadata.py

- `save_step_metadata(step_name, job_id, metadata, output_dir)`
  - Saves task-specific metadata to `{step_name}/job_metadata/{job_id}.json`
  - Calls `update_central_registry()` automatically
  
- `update_central_registry(step_name, job_id, step_metadata)`
  - Updates/creates `job_registry/{job_id}.json`
  - Merges all step metadata into single file
  - Maintains execution trace

- `get_central_registry(job_id)`
  - Loads complete pipeline trace for a job
  
- `find_jobs_for_citekey(citekey)`
  - Returns all jobs that processed a given citekey

### run_ocr.py

- Auto-detects `source_job_id` from source metadata if not provided
- Builds per-citekey source job ID mapping
- Saves metadata via `save_step_metadata("ocr", ...)`

### sync_ocr.py

- Uploads OCR results to B2
- Records sync info in central registry via `update_central_registry("sync_ocr", ...)`

## Test Steps

1. **Verify source job exists**

   ```bash
   cat data/sources/job_metadata/*.json | jq '.job_id'
   ```

2. **Verify OCR metadata was saved**

   ```bash
   cat data/analytics/ocr/job_metadata/*.json | jq '.source_job_id'
   ```

3. **Verify central registry has both steps**

   ```bash
   cat data/analytics/job_registry/*.json | jq '.pipeline_steps | keys'
   # Should show: ["ocr", "sync_ocr"]
   ```

4. **Trace a citekey through the pipeline**

   ```bash
   python -c "from scripts.etl_metadata import find_jobs_for_citekey; print(find_jobs_for_citekey('test_v04'))"
   ```

## Current Status

✅ Infrastructure complete:

- Upload metadata stored in sources/job_metadata/
- OCR metadata stored in ocr/job_metadata/ + central registry
- Sync metadata stored in central registry

✅ Lineage tracking:

- Each job records its source_job_id
- Central registry captures all steps
- Can trace citekeys through pipeline

✅ Metadata simplification:

- All operations treated as equal "steps"
- Single central registry per job
- No separate "operation_type" distinction
