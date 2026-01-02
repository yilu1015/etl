# Parse_Structure ETL Step - Design & Implementation

## Overview

The `parse_structure` step is a critical component of the ETL pipeline that:

1. Analyzes OCR JSON files to detect body and TOC page ranges
2. Generates standardized output JSON with detection quality metrics
3. Maintains proper job ID lineage for data provenance tracking
4. Updates central registry for audit trail and monitoring

## Job ID Lineage Architecture

### The Problem

After OCR produces results, those results are archived by `sync_ocr` step to B2. The question is: **should parse_structure track OCR's job_id or sync_ocr's job_id as its source?**

### The Solution: Use Sync_OCR's Job ID

**Rationale:**

1. **Honest Provenance**: parse_structure literally reads files from sync_ocr's output
2. **Enables Selective Reruns**: Can re-parse without re-running OCR (useful for algorithm improvements)
3. **Single Authority**: Data comes from one source (sync_ocr/B2), not dual sources
4. **Clear Data Lineage**: Natural sequence: parse_structure ← sync_ocr ← ocr ← upload
5. **Future Flexibility**: If OCR is re-run, sync_ocr job_id changes, allowing proper re-parsing

### Pipeline Flow

```
Upload (PDF → S3)
  ↓ job_id: 2026-01-02_00-28-28
  
OCR (PDF → OCR JSON)
  ↓ job_id: 2026-01-02_00-33-11
    source_job_id: 2026-01-02_00-28-28
  
Sync_OCR (OCR JSON → B2 Archive)
  ↓ job_id: 2026-01-02_00-58-42
    source_job_id: 2026-01-02_00-33-11
  
Parse_Structure (OCR JSON → Page Mappings)
  ↓ job_id: 2026-01-02_01-15-47
    source_job_id: 2026-01-02_00-58-42 ← KEY DECISION
```

## Implementation Details

### Key Functions

#### `find_sync_ocr_job_id(ocr_job_id: str) → Optional[str]`

Locates the sync_ocr job that processed a given OCR result.

**Process:**

1. Load central registry for the OCR job_id
2. Look for `pipeline_steps.sync_ocr.job_id`
3. Return sync_job_id or None with logging

**Example:**

```python
ocr_job_id = "2026-01-02_00-33-11"
sync_job_id = find_sync_ocr_job_id(ocr_job_id)
# Returns: "2026-01-02_00-58-42"
```

#### `parse_structure_step(ocr_input, ocr_job_id, sync_job_id) → tuple[str, Path]`

Complete ETL pipeline step that:

1. Determines source_job_id (prefers sync_job_id, falls back to finding it)
2. Generates unique parse_job_id (timestamp format)
3. Analyzes all OCR documents
4. Saves structured output for each document
5. Aggregates job metadata
6. Updates central registry
7. Returns (parse_job_id, output_directory)

**Key Features:**

- Type conversion: Converts numpy int64/float64 to Python native types
- Error resilience: Continues on per-document failures with logging
- Output structure: Proper directory layout with latest/ symlinks
- Metadata tracking: Quality summaries and statistics

## Output Structure

### Individual Document Output

**Location:** `data/analytics/parse_structure/{citekey}/{job_id}/{citekey}.json`

```json
{
  "citekey": "dwgz1992",
  "source_job_id": "2026-01-02_00-58-42",
  "parse_structure_job_id": "2026-01-02_01-15-47",
  "generated_at": "2026-01-02T01:15:47Z",
  
  "body": {
    "first_real_page": 3,
    "last_real_page": 1373,
    "first_pdf_page": 36,
    "last_pdf_page": 1406,
    "pdf_page_offset": -33,
    "confidence": 1.0,
    "detection_quality": {
      "consistency_score": 1.0,
      "position_consistency": 1.0,
      "sequence_length": 1337,
      "gaps": [240, 541, 585, ...]
    }
  },
  
  "toc": {
    "first_real_page": 1,
    "last_real_page": 27,
    "first_pdf_page": 1,
    "last_pdf_page": 27,
    "pdf_page_offset": 0,
    "confidence": 0.98,
    "detection_quality": {
      "detection_method": "keyword",
      "start_detection_method": "keyword_found",
      "start_confidence": 0.98,
      "keyword_found": true,
      "keyword_pdf_page": 3,
      "inferred_flags": []
    }
  },
  
  "metadata": {
    "total_pdf_pages": "varies",
    "notes": "Body: pages 3–1373 (offset -33)"
  }
}
```

### Job Metadata

**Location:** `data/analytics/parse_structure/job_metadata/{job_id}.json`

```json
{
  "status": "completed",
  "source_job_id": "2026-01-02_00-58-42",
  "citekeys": {
    "total": 7,
    "processed": 7,
    "failed": 0,
    "list": ["dagz_v01", "dagz_v02", "dwgz1992", ...]
  },
  "quality_summary": {
    "high_confidence": {
      "count": 7,
      "threshold": 0.95
    },
    "with_inferences": {
      "count": 1,
      "citekeys": ["dwgz1992"]
    }
  },
  "statistics": {
    "avg_body_confidence": 1.0,
    "avg_toc_confidence": 0.9971,
    "total_pages_analyzed": 3642
  }
}
```

### Central Registry Update

**Location:** `data/analytics/job_registry/{parse_job_id}.json`

```json
{
  "job_id": "2026-01-02_01-15-47",
  "timestamp": "2026-01-02T01:15:47.123456",
  "source": null,
  "pipeline_steps": {
    "parse_structure": {
      "job_id": "2026-01-02_01-15-47",
      "status": "completed",
      "timestamp": "2026-01-02T01:15:47.123456",
      "metadata_path": "parse_structure/job_metadata/2026-01-02_01-15-47.json",
      "citekeys": {
        "total": 7,
        "processed": 7,
        "failed": 0,
        "list": [...]
      },
      "source_job_id": "2026-01-02_00-58-42",
      "quality_summary": {...},
      "statistics": {...}
    }
  },
  "execution_trace": [
    {
      "step": "parse_structure",
      "job_id": "2026-01-02_01-15-47",
      "timestamp": "2026-01-02T01:15:47.123456",
      "status": "completed"
    }
  ]
}
```

## Directory Structure

```
data/analytics/parse_structure/
├── job_metadata/
│   └── {parse_job_id}.json              [Job metadata with quality summary]
├── {citekey}/
│   ├── {parse_job_id}/
│   │   └── {citekey}.json               [Parse structure output]
│   └── latest/                          [Symlink to latest job_id]
│       └── {citekey}.json               [Redirects to latest/{citekey}.json]
├── dagz_v01/
│   ├── 2026-01-02_01-15-47/
│   │   └── dagz_v01.json
│   └── latest/ → 2026-01-02_01-15-47
├── dwgz1992/
│   ├── 2026-01-02_01-15-47/
│   │   └── dwgz1992.json
│   └── latest/ → 2026-01-02_01-15-47
... (similar for each citekey)
```

## Integration with Downstream Steps

The parse_structure output enables:

### Extract_Body Step

- Reads parse_structure/{citekey}/latest/{citekey}.json
- Uses `body.first_real_page` and `body.last_real_page` to identify body PDF pages
- Uses `body.pdf_page_offset` to convert real page numbers to PDF indices
- Extracts text from body pages in OCR JSON

### Extract_TOC Step

- Reads parse_structure/{citekey}/latest/{citekey}.json
- Uses `toc.first_real_page` and `toc.last_real_page` to identify TOC PDF pages
- Uses `toc.pdf_page_offset` for page number conversion
- Extracts and structures TOC entries

### Quality Monitoring

- Aggregated metadata enables monitoring parse_structure health
- Track high_confidence vs. with_inferences ratio
- Monitor avg_body_confidence and avg_toc_confidence trends
- Alert on significant drops in detection quality

## Usage Example

```python
from pathlib import Path
import json

# Step 1: Get OCR job ID
ocr_job_id = "2026-01-02_00-33-11"

# Step 2: Find sync_ocr job that produced this OCR's output
sync_job_id = find_sync_ocr_job_id(ocr_job_id)

# Step 3: Run parse_structure step
parse_job_id, output_dir = parse_structure_step(
    ocr_input="data/analytics/ocr",
    ocr_job_id=ocr_job_id,
    sync_job_id=sync_job_id
)

# Step 4: Access results
metadata_file = Path("data/analytics/parse_structure/job_metadata") / f"{parse_job_id}.json"
with metadata_file.open() as f:
    metadata = json.load(f)

print(f"Parse Job ID: {parse_job_id}")
print(f"Documents processed: {metadata['citekeys']['total']}")
print(f"Avg body confidence: {metadata['statistics']['avg_body_confidence']:.1%}")
print(f"With inferences: {metadata['quality_summary']['with_inferences']['count']}")
```

## Test Results

**Run:** 2026-01-02_11-32-58

✓ All 7 test documents processed successfully

- Average body confidence: 100%
- Average TOC confidence: 99.71%
- Total pages analyzed: 3,642
- High confidence documents: 7/7
- Documents with inferences: 1 (dwgz1992)

## Production Readiness

✅ **Implemented:**

- Core parsing algorithms (body + TOC detection)
- ETL pipeline integration (job_id tracking)
- Output standardization (JSON schemas)
- Error handling and logging
- Type conversion for JSON serialization
- Central registry updates
- Symlink management for easy querying

✅ **Tested:**

- All 7 documents in test dataset
- Various body/TOC configurations
- Edge cases (missing page numbers, gaps)
- Registry integration

✅ **Ready for:**

- Extraction to `scripts/parse_structure.py`
- Integration into workflow orchestration
- Deployment to production
- Extension with downstream steps

## Code Location

- **Implementation:** [notebooks/exploration/segment_pdfs_guide.ipynb](../notebooks/exploration/segment_pdfs_guide.ipynb)
  - Cell: "Step 4: ETL Integration - Parse Structure Step"
- **Functions:** `find_sync_ocr_job_id()`, `parse_structure_step()`
- **Usage Example:** Final cell of the notebook
