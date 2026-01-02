# ETL Scripts

This directory contains the core ETL pipeline scripts for processing PDF documents through OCR and metadata extraction.

## Scripts

### Active Scripts

- **upload_pdfs_to_b2.py** - Upload PDF files to Backblaze B2 cloud storage
- **run_ocr.py** - Main OCR orchestration script using PaddleOCR
- **ocr_generic.py** - Generic OCR processing utilities
- **sync_ocr_to_b2.py** - Synchronize OCR results back to B2
- **etl_metadata.py** - Shared ETL utilities and metadata handling
- **b2_utils.py** - Backblaze B2 API helpers and utilities

### Future Scripts

- **extract_toc.py** - Extract table of contents from documents (planned)
- **segment_documents.py** - Segment documents into pages/sections (planned)

## Usage

Each script can be run with `--help` to see available options:

```bash
python scripts/run_ocr.py --help
```

## Configuration

Scripts reference configuration from the `config/` directory:

- `pipeline_config.json` - PaddleOCR model and processing parameters
- `predict_params.json` - OCR prediction parameters
- `etl_workflows.yaml` - Workflow orchestration (future)
