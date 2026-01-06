# Folder Structure Migration Summary

## Migration Completed: January 5, 2026

### Overview

Successfully migrated from ad-hoc `data/analytics/` structure to stage-based organization with centralized metadata tracking.

### New Structure

```
data/
├── sources/                     # Raw PDFs (not in Git)
│   ├── dagz/, test/, etc.
│   └── job_metadata/           # Upload tracking (in Git)
│
├── stage_1_ingest/             # PDF Upload/Validation
│   └── metadata/
│       ├── job_metadata/       # Per-job upload metadata
│       ├── citekey_history/    # Version tracking
│       └── job_metadata_archive/  # Old metadata
│
├── stage_2_ocr/                # OCR & TOC Extraction
│   ├── ocr_output/             # OCR results by citekey
│   ├── toc_extraction/         # Multi-model TOC extractions
│   └── toc_consensus/          # Consensus results
│
├── stage_3_structure/          # Structure Parsing
│   ├── segmentation/           # Document segmentation
│   └── structure_parsing/      # Parsed structure by citekey
│
├── pipeline_metadata/          # Central tracking (in Git)
│   ├── job_registry.json       # Consolidated job history
│   ├── job_registry_old/       # Individual job files
│   ├── citekey_status/         # Current status per citekey
│   ├── versions/               # Version snapshots
│   └── iteration_history/      # Iteration tracking
│
└── processed/                  # Final outputs (not in Git)
```

### Changes Made

#### Created Directories

- `data/stage_1_ingest/metadata/` - Upload tracking
- `data/stage_2_ocr/` - OCR and TOC extraction outputs
- `data/stage_3_structure/` - Structure parsing outputs
- `data/pipeline_metadata/` - Centralized metadata
- `data/processed/` - Final outputs

#### Migrated Content

| Old Location | New Location | Files |
|--------------|--------------|-------|
| `data/analytics/ocr/` | `data/stage_2_ocr/ocr_output/` | 36 files |
| `data/analytics/extract_toc_titles/` | `data/stage_2_ocr/toc_extraction/` | 58 files |
| `data/analytics/compare_toc_models/` | `data/stage_2_ocr/toc_consensus/` | 16 files |
| `data/analytics/parse_structure/` | `data/stage_3_structure/structure_parsing/` | 33 files |
| `data/analytics/segmentation/` | `data/stage_3_structure/segmentation/` | - |
| `data/analytics/job_registry/` | `data/pipeline_metadata/job_registry_old/` | 18 files |

#### Remaining in Old Location

These directories still need manual decision:

- `data/analytics/sync_ocr/` - Determine if this is part of Stage 2 or separate
- `data/analytics/toc/` - Old TOC work, can archive or delete
- `data/analytics/toc_extracted/` - Old extraction results, can archive

### New Metadata System

#### Consolidated Job Registry

Created `data/pipeline_metadata/job_registry.json`:

- 19 jobs tracked
- Consolidated view of all pipeline steps
- Easy querying by job_id or step_id

#### Citekey Status Tracking

New directory structure supports:

- Per-citekey status files
- Version snapshots
- Iteration history
- Easy querying of "what's the latest version of X?"

### Configuration-Driven Approach

All stage and step definitions now centralized in:

- `config/pipeline/stage_definitions.yaml`

Benefits:

- Easy renaming of stages/steps
- Dynamic path resolution
- Single source of truth

### Next Steps

1. **Update Scripts** (IMPORTANT)
   - [ ] Move scripts to new locations:
     - `scripts/extract_toc_titles.py` → `scripts/pipeline/stage_2_extract_toc_titles.py`
     - `scripts/compare_toc_models.py` → `scripts/pipeline/stage_2_compare_toc_models.py`
     - `scripts/etl_metadata.py` → `scripts/core/etl_metadata.py`
   - [ ] Update import statements in all scripts
   - [ ] Update path references to use new structure

2. **Create Pipeline Config**
   - [ ] Create `config/pipeline/stage_definitions.yaml`
   - [ ] Update scripts to use config-based paths

3. **Test Migration**
   - [ ] Run OCR pipeline with one test citekey
   - [ ] Verify metadata is saved in correct locations
   - [ ] Check symlinks (`latest/`) work correctly

4. **Clean Up**
   - [ ] Delete or archive remaining `data/analytics/` subdirs
   - [ ] Remove old `data/analytics/job_registry/` after confirming consolidated version works
   - [ ] Update `.gitignore` if needed

5. **Git Commit**

   ```bash
   git add data/pipeline_metadata/
   git add data/stage_*/*/job_metadata/
   git commit -m "Migrate to stage-based folder structure

   - Reorganized data/ into stages (ingest, OCR, structure)
   - Centralized metadata in pipeline_metadata/
   - Created consolidated job registry
   - Preserved all existing data
   "
   ```

### Rollback Instructions

If needed, the migration can be rolled back:

1. Backup available at: `backups/20260105_211723/structure_manifest.json`
2. Old job registry preserved at: `data/pipeline_metadata/job_registry_old/`
3. Migration log available at: `data/pipeline_metadata/migration_log.json`

### Benefits of New Structure

1. **Clear Separation**: Each stage has its own directory
2. **Git-Friendly**: Metadata tracked, large files excluded
3. **Easy Navigation**: Predictable paths for each stage
4. **Scalability**: Easy to add new stages (stage_4_content, stage_5_review)
5. **Version Tracking**: Built-in support for citekey versioning
6. **Centralized Metadata**: Single source of truth for job status
7. **Configuration-Driven**: Easy to rename/reorganize without code changes

### References

- Migration script: `scripts/maintenance/migrate_folder_structure.py`
- Migration log: `data/pipeline_metadata/migration_log.json`
- Backup manifest: `backups/20260105_211723/structure_manifest.json`
