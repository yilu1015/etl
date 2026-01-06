#!/usr/bin/env python3
"""
Migrate existing folder structure to stage-based organization.

CAUTION: This will move files! Backup recommended before running with --execute.

Usage:
    # Preview changes (safe)
    python scripts/maintenance/migrate_folder_structure.py --dry-run

    # Execute migration
    python scripts/maintenance/migrate_folder_structure.py --execute

    # Verify after migration
    python scripts/maintenance/migrate_folder_structure.py --verify
"""

import argparse
import shutil
import json
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Tuple
import sys

PROJECT_ROOT = Path(__file__).parent.parent.parent

# ============================================
# MIGRATION MAPPINGS
# ============================================

DIRECTORY_MIGRATIONS = {
    # Stage 1: Ingest (create new structure, sources stays)
    "data/analytics/job_metadata": "data/stage_1_ingest/metadata/job_metadata_archive",
    
    # Stage 2: OCR consolidation
    "data/analytics/ocr": "data/stage_2_ocr/ocr_output",
    "data/analytics/extract_toc_titles": "data/stage_2_ocr/toc_extraction",
    "data/analytics/compare_toc_models": "data/stage_2_ocr/toc_consensus",
    
    # Stage 3: Structure
    "data/analytics/parse_structure": "data/stage_3_structure/structure_parsing",
    "data/analytics/segmentation": "data/stage_3_structure/segmentation",
    
    # Central metadata
    "data/analytics/job_registry": "data/pipeline_metadata/job_registry_old",
}

# Files to create in new locations
NEW_DIRECTORIES = [
    "data/stage_1_ingest/metadata/job_metadata",
    "data/stage_1_ingest/metadata/citekey_history",
    "data/stage_2_ocr/ocr_output/job_metadata",
    "data/stage_2_ocr/toc_extraction/job_metadata",
    "data/stage_2_ocr/toc_consensus/job_metadata",
    "data/stage_3_structure/segmentation/job_metadata",
    "data/stage_3_structure/structure_parsing/job_metadata",
    "data/pipeline_metadata/citekey_status",
    "data/pipeline_metadata/versions",
    "data/pipeline_metadata/iteration_history",
    "data/processed",
    "config/pipeline",
    "config/corrections",
    "scripts/pipeline",
    "scripts/review",
    "scripts/monitoring",
    "scripts/maintenance",
    "scripts/core",
]

# ============================================
# HELPER FUNCTIONS
# ============================================

def backup_current_state(backup_dir: Path):
    """Create backup of current data structure."""
    print(f"\n📦 Creating backup at {backup_dir}...")
    
    backup_dir.mkdir(parents=True, exist_ok=True)
    
    # Backup data directory structure (not actual files, just metadata)
    if (PROJECT_ROOT / "data").exists():
        # Create structure manifest
        manifest = {
            "backup_date": datetime.now().astimezone().isoformat(),
            "directories": [],
            "files": []
        }
        
        for path in (PROJECT_ROOT / "data").rglob("*"):
            rel_path = path.relative_to(PROJECT_ROOT / "data")
            if path.is_symlink():
                # Skip broken symlinks
                try:
                    path.stat()
                except FileNotFoundError:
                    continue
            if path.is_dir():
                manifest["directories"].append(str(rel_path))
            elif path.is_file():
                manifest["files"].append({
                    "path": str(rel_path),
                    "size": path.stat().st_size
                })
        
        with (backup_dir / "structure_manifest.json").open("w") as f:
            json.dump(manifest, f, indent=2)
        
        print(f"✓ Manifest saved: {len(manifest['directories'])} directories, {len(manifest['files'])} files")


def migrate_directory(old_path: Path, new_path: Path, dry_run: bool = True) -> Tuple[bool, str]:
    """Migrate a single directory."""
    if not old_path.exists():
        return False, f"Source not found: {old_path}"
    
    if new_path.exists():
        return False, f"Target already exists: {new_path}"
    
    if dry_run:
        file_count = sum(1 for _ in old_path.rglob("*") if _.is_file())
        return True, f"Would move {file_count} files"
    else:
        new_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(old_path), str(new_path))
        return True, f"Moved successfully"


def create_new_directories(dry_run: bool = True) -> List[Path]:
    """Create new directory structure."""
    created = []
    
    for dir_path in NEW_DIRECTORIES:
        full_path = PROJECT_ROOT / dir_path
        
        if full_path.exists():
            continue
        
        if dry_run:
            print(f"📋 Would create: {dir_path}")
            created.append(full_path)
        else:
            full_path.mkdir(parents=True, exist_ok=True)
            print(f"✓ Created: {dir_path}")
            created.append(full_path)
    
    return created


def move_job_registry_files(dry_run: bool = True):
    """
    Move job registry files to new central location.
    
    Old: data/analytics/job_registry/*.json
    New: data/pipeline_metadata/job_registry/ (individual files)
         data/pipeline_metadata/job_registry.json (consolidated)
    """
    old_registry_dir = PROJECT_ROOT / "data/analytics/job_registry"
    new_registry_dir = PROJECT_ROOT / "data/pipeline_metadata/job_registry"
    
    if not old_registry_dir.exists():
        print("⏭️  No job registry to migrate")
        return
    
    if dry_run:
        files = list(old_registry_dir.glob("*.json"))
        print(f"📋 Would migrate {len(files)} job registry files")
        return
    
    # Create new directory
    new_registry_dir.mkdir(parents=True, exist_ok=True)
    
    # Move individual files
    files = list(old_registry_dir.glob("*.json"))
    for file in files:
        shutil.copy2(file, new_registry_dir / file.name)
        print(f"✓ Copied: {file.name}")
    
    # Create consolidated registry
    all_jobs = []
    for file in new_registry_dir.glob("*.json"):
        with file.open() as f:
            job_data = json.load(f)
        
        # Extract job info
        if "pipeline_steps" in job_data:
            # This is a job registry entry
            for step_name, step_data in job_data["pipeline_steps"].items():
                all_jobs.append({
                    "job_id": job_data["job_id"],
                    "step_id": step_name,
                    "timestamp": step_data.get("timestamp"),
                    "status": step_data.get("status"),
                    "citekeys": step_data.get("citekeys", {}).get("list", [])
                })
    
    # Save consolidated
    consolidated = {
        "last_updated": datetime.now().astimezone().isoformat(),
        "total_jobs": len(all_jobs),
        "jobs": sorted(all_jobs, key=lambda x: x["job_id"])
    }
    
    registry_file = PROJECT_ROOT / "data/pipeline_metadata/job_registry.json"
    with registry_file.open("w") as f:
        json.dump(consolidated, f, indent=2, ensure_ascii=False)
    
    print(f"✓ Created consolidated registry: {len(all_jobs)} jobs")


def verify_migration() -> bool:
    """Verify migration was successful."""
    print("\n🔍 Verifying migration...")
    
    errors = []
    warnings = []
    
    # Check new directories exist
    for dir_path in NEW_DIRECTORIES:
        full_path = PROJECT_ROOT / dir_path
        if not full_path.exists():
            errors.append(f"Missing directory: {dir_path}")
    
    # Check stage 2 structure
    stage_2 = PROJECT_ROOT / "data/stage_2_ocr"
    if stage_2.exists():
        required_subdirs = ["ocr_output", "toc_extraction", "toc_consensus"]
        for subdir in required_subdirs:
            if not (stage_2 / subdir).exists():
                errors.append(f"Missing Stage 2 subdirectory: {subdir}")
    else:
        errors.append("Stage 2 directory not created")
    
    # Check central metadata
    pipeline_meta = PROJECT_ROOT / "data/pipeline_metadata"
    if pipeline_meta.exists():
        if not (pipeline_meta / "job_registry.json").exists():
            warnings.append("Consolidated job_registry.json not created")
    else:
        errors.append("Pipeline metadata directory not created")
    
    # Print results
    if errors:
        print(f"\n❌ Verification failed with {len(errors)} errors:")
        for error in errors:
            print(f"  - {error}")
        return False
    
    if warnings:
        print(f"\n⚠️  Verification passed with {len(warnings)} warnings:")
        for warning in warnings:
            print(f"  - {warning}")
    else:
        print("\n✅ Verification passed!")
    
    return True


def create_migration_log(migrations_executed: List[Dict]):
    """Create log of migration for reference."""
    log_file = PROJECT_ROOT / "data/pipeline_metadata/migration_log.json"
    log_file.parent.mkdir(parents=True, exist_ok=True)
    
    log_data = {
        "migrated_at": datetime.now().astimezone().isoformat(),
        "migrations": migrations_executed,
        "new_structure_version": "2.0",
        "notes": "Migrated to stage-based organization"
    }
    
    with log_file.open('w') as f:
        json.dump(log_data, f, indent=2, ensure_ascii=False)
    
    print(f"\n✅ Migration log saved to {log_file}")


# ============================================
# MAIN MIGRATION
# ============================================

def main():
    parser = argparse.ArgumentParser(
        description="Migrate folder structure to stage-based organization",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--dry-run", action="store_true", help="Preview changes without executing")
    parser.add_argument("--execute", action="store_true", help="Execute migration")
    parser.add_argument("--verify", action="store_true", help="Verify migration")
    args = parser.parse_args()
    
    if args.verify:
        success = verify_migration()
        sys.exit(0 if success else 1)
    
    if not args.dry_run and not args.execute:
        print("❌ Must specify either --dry-run or --execute (or --verify)")
        sys.exit(1)
    
    dry_run = args.dry_run
    
    print(f"\n{'='*70}")
    print(f"FOLDER STRUCTURE MIGRATION {'(DRY RUN)' if dry_run else '(EXECUTING)'}")
    print('='*70)
    
    # Create backup
    if not dry_run:
        backup_dir = PROJECT_ROOT / "backups" / datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_current_state(backup_dir)
    
    executed = []
    
    # 1. Create new directory structure
    print("\n📁 Creating new directory structure...")
    created_dirs = create_new_directories(dry_run)
    print(f"{'Would create' if dry_run else 'Created'} {len(created_dirs)} new directories")
    
    # 2. Migrate directories
    print("\n📁 Migrating existing directories...")
    for old, new in DIRECTORY_MIGRATIONS.items():
        old_path = PROJECT_ROOT / old
        new_path = PROJECT_ROOT / new
        
        success, message = migrate_directory(old_path, new_path, dry_run)
        
        status = "📋" if dry_run else ("✓" if success else "⚠️")
        print(f"{status} {old} → {new}")
        print(f"   {message}")
        
        if success:
            executed.append({
                "old": str(old),
                "new": str(new),
                "status": "success" if not dry_run else "planned"
            })
    
    # 3. Handle job registry specially
    print("\n📄 Migrating job registry...")
    move_job_registry_files(dry_run)
    
    # 4. Create migration log
    if not dry_run:
        create_migration_log(executed)
    
    # 5. Summary
    print(f"\n{'='*70}")
    print("MIGRATION " + ("PLAN" if dry_run else "COMPLETE"))
    print('='*70)
    print(f"Directories migrated: {len(executed)}")
    print(f"New directories created: {len(created_dirs)}")
    
    if dry_run:
        print("\n💡 Review the plan above, then run with --execute to apply changes")
        print("   Backup will be created automatically before execution")
    else:
        print("\n✅ Migration complete!")
        print("\nNext steps:")
        print("1. Run verification: python scripts/maintenance/migrate_folder_structure.py --verify")
        print("2. Update import statements in scripts")
        print("3. Test pipeline with one citekey")
        print("4. Commit changes: git add data/ && git commit -m 'Migrated to stage-based structure'")
    
    print('='*70)


if __name__ == "__main__":
    main()
