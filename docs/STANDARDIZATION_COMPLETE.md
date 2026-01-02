# ETL Pipeline Standardization Complete ✅

**Date:** January 2, 2026

All four pipeline scripts now implement the **standardized interface** with advanced features.

## Summary of Changes

### 1. **upload_pdfs.py** ✅

- **Added**: `--pattern` argument for wildcard matching (e.g., `mzdnp2023_v*`)
- **Restriction**: Only `_v*` and `_y*` wildcards supported
- **Usage**: `--pattern "mzdnp2023_v*" --local-dir /path/to/pdfs/`

### 2. **run_ocr.py** ✅

- **Added**: `--pattern` argument for wildcard citekey matching
- **Added**: `--dry-run` flag for preview without execution
- **Added**: `--force-rerun` flag for reprocessing existing results
- **Imports**: Added `expand_citekey_patterns`, `preview_pipeline_run`, `print_dry_run_summary`

### 3. **sync_ocr.py** ✅

- **Added**: `--pattern` argument for wildcard matching
- **Added**: `--dry-run` flag for preview
- **Fixed**: Force-rerun now properly filters citekeys using `filter_citekeys_to_process()`
- **Imports**: Added `expand_citekey_patterns`, `preview_pipeline_run`, `print_dry_run_summary`, `filter_citekeys_to_process`

### 4. **parse_structure.py** ✅

- **Complete overhaul** to standardized interface
- **Removed**: `--input`, `--ocr-job-id`, `--sync-job-id` arguments
- **Added**: `--citekeys`, `--pattern`, `--resume-from`, `--source-job-id`, `--force-rerun`, `--dry-run`
- **Simplified**: Now only references sync_ocr job (single source)
- **Imports**: Added full etl_metadata utilities

---

## Standardized Interface

All steps (except `upload_pdfs`) now follow this pattern:

```bash
python scripts/{step}.py \
    [--citekeys CK1 CK2 ... | --pattern "pattern*" | --resume-from JOB_ID] \
    --source-job-id EXPLICIT_TIMESTAMP \
    [--force-rerun] \
    [--dry-run]
```

### Input Selection (Mutually Exclusive)

- `--citekeys`: Explicit list
- `--pattern`: Wildcard patterns (only `_v*` and `_y*`)
- `--resume-from`: Resume failed items from previous job
- `--input`: (upload_pdfs only) Directory or files

### Required Arguments

- `--source-job-id`: Explicit reference to prior step's job (never "latest")

### Optional Flags

- `--force-rerun`: Reprocess all (ignore existing results)
- `--dry-run`: Preview without execution

---

## Feature Matrix

| Feature | upload_pdfs | run_ocr | sync_ocr | parse_structure |
|---------|-------------|---------|----------|-----------------|
| **Wildcards** | ✅ | ✅ | ✅ | ✅ |
| **Dry Run** | ✅ (already had) | ✅ | ✅ | ✅ |
| **Force Rerun** | N/A | ✅ | ✅ | ✅ |
| **Resume** | N/A | ✅ | ✅ | ✅ |
| **Validation** | ✅ | ✅ | ✅ | ✅ |
| **Source Tracking** | N/A (entry point) | ✅ | ✅ | ✅ |

---

## Usage Examples

### Upload PDFs with Wildcard

```bash
# Upload all volumes of mzdnp2023
python scripts/upload_pdfs.py \
    --pattern "mzdnp2023_v*" \
    --local-dir data/sources/mzdnp/
```

### Run OCR with Pattern and Dry Run

```bash
# Preview OCR for all dagz volumes
python scripts/run_ocr.py \
    --pattern "dagz_v*" \
    --source-job-id 2026-01-02_00-28-28 \
    --dry-run

# Execute
python scripts/run_ocr.py \
    --pattern "dagz_v*" \
    --source-job-id 2026-01-02_00-28-28
```

### Sync with Force Rerun

```bash
# Re-upload all results (even if already in B2)
python scripts/sync_ocr.py \
    --citekeys dagz_v01 dagz_v02 \
    --source-job-id 2026-01-02_00-33-11 \
    --force-rerun
```

### Parse Structure with Resume

```bash
# First run (3 citekeys, 1 fails)
python scripts/parse_structure.py \
    --citekeys dagz_v01 dagz_v02 dagz_v03 \
    --source-job-id 2026-01-02_11-15-30

# Resume just the failed one
python scripts/parse_structure.py \
    --resume-from 2026-01-02_11-32-58 \
    --source-job-id 2026-01-02_11-15-30
```

---

## Wildcard Patterns

**Supported patterns:**

- `_v*`: Volume wildcards (e.g., `dagz_v*` matches `dagz_v01`, `dagz_v02`, ...)
- `_y*`: Year wildcards (e.g., `mzdnp_y200*` matches `mzdnp_y2001`, `mzdnp_y2009`)

**Why restricted?**

- Prevents accidental broad matches (e.g., `dagz*` matching everything)
- Matches common corpus naming patterns
- Reduces risk of unintended batch operations

**Invalid patterns:**

```bash
# ❌ These will error
--pattern "dagz*"      # Too broad
--pattern "d*_v01"     # Wildcard not on _v or _y
```

---

## Dry Run Preview

All steps now support `--dry-run` for safe exploration:

```bash
python scripts/parse_structure.py \
    --pattern "dagz_v*" \
    --source-job-id 2026-01-02_11-15-30 \
    --dry-run
```

**Output:**

```
======================================================================
🔍 DRY RUN PREVIEW: parse_structure
======================================================================

📋 Configuration:
  Source Job ID:    2026-01-02_11-15-30
  Force Rerun:      False
  Total Citekeys:   5

✅ Citekeys to Process (2):
    • dagz_v04
    • dagz_v05

⊘ Citekeys to Skip (3):
    • dagz_v01: Result exists in job 2026-01-02_11-32-58
    • dagz_v02: Result exists in job 2026-01-02_11-32-58
    • dagz_v03: Result exists in job 2026-01-02_11-32-58

🆔 Estimated Job ID:
  2026-01-02_15-30-00

✅ Will create new job and process 2 citekeys

======================================================================
💡 To execute: Remove --dry-run flag
======================================================================
```

---

## Force Rerun Semantics

**Without `--force-rerun` (default):**

- Checks if result exists for citekey + source_job_id
- Skips if found
- Only processes missing results
- May reuse existing job_id if nothing to process

**With `--force-rerun`:**

- Always creates new job_id
- Processes ALL citekeys (ignores existing results)
- Use when parameters change

**Example:**

```bash
# Regular run: skips existing results
python scripts/parse_structure.py \
    --citekeys dagz_v01 \
    --source-job-id 2026-01-02_11-15-30
# → dagz_v01 already exists, nothing to process

# Force rerun: reprocesses everything
python scripts/parse_structure.py \
    --citekeys dagz_v01 \
    --source-job-id 2026-01-02_11-15-30 \
    --force-rerun
# → Creates new job, reprocesses dagz_v01
```

---

## Testing Checklist

Run these commands to verify everything works:

```bash
# Test wildcards
python scripts/upload_pdfs.py --pattern "test_v*" --local-dir data/sources/test/ --dry-run
python scripts/run_ocr.py --pattern "test_v*" --source-job-id <UPLOAD_JOB> --dry-run
python scripts/sync_ocr.py --pattern "test_v*" --source-job-id <OCR_JOB> --dry-run
python scripts/parse_structure.py --pattern "test_v*" --source-job-id <SYNC_JOB> --dry-run

# Test force rerun
python scripts/parse_structure.py --citekeys test_v01 --source-job-id <SYNC_JOB> --force-rerun

# Test resume
# (Requires a previous job with failures)
python scripts/parse_structure.py --resume-from <PREV_JOB> --source-job-id <SYNC_JOB>
```

---

## Documentation

Updated notebooks:

- ✅ [`notebooks/reference/pipeline_overview.ipynb`](notebooks/reference/pipeline_overview.ipynb) - Complete pipeline guide with run modes
- ✅ [`notebooks/reference/standardization_journey.ipynb`](notebooks/reference/standardization_journey.ipynb) - Design evolution and lessons learned

Core utilities:

- ✅ [`scripts/etl_metadata.py`](scripts/etl_metadata.py) - All validation, filtering, pattern expansion, and preview functions

---

## Migration from Old Interface

### parse_structure.py

**Before:**

```bash
python scripts/parse_structure.py \
    --input data/analytics/ocr \
    --sync-job-id 2026-01-02_00-58-42
```

**After:**

```bash
# Get all citekeys from sync job, then use pattern or explicit list
python scripts/parse_structure.py \
    --pattern "dagz_v*" \
    --source-job-id 2026-01-02_00-58-42
```

**Key Changes:**

- Removed `--input` (no longer accepts directory)
- Removed `--ocr-job-id` (only sync_ocr job needed)
- Renamed `--sync-job-id` → `--source-job-id`
- Must explicitly specify citekeys (via `--citekeys`, `--pattern`, or `--resume-from`)

---

## Next Steps

1. **Test thoroughly** with your actual data
2. **Update any automation scripts** that call these tools
3. **Train team members** on new interface patterns
4. **Monitor first few runs** to catch edge cases
5. **Celebrate** 🎉 - You now have a robust, reproducible pipeline!

---

## Questions?

See the notebooks:

- [Pipeline Overview](notebooks/reference/pipeline_overview.ipynb) - Usage patterns and examples
- [Standardization Journey](notebooks/reference/standardization_journey.ipynb) - Design rationale and lessons learned
